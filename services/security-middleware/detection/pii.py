"""PII (Personally Identifiable Information) detector — Strategy implementation.
PII in log messages is a compliance risk: GDPR, HIPAA, and PCI-DSS all prohibit
storing raw PII in application logs without explicit consent and controls. More
critically for this system, PII in a log message that reaches an LLM prompt is
sent to a third-party API (OpenAI), which may log it. This detector ensures PII
is replaced with typed redaction tokens before logs enter the AI pipeline.
Design decisions:
  - Regex detection is chosen over ML-based NER for determinism and auditability.
    In a compliance context, "the regex fired on this pattern" is easier to
    audit than "the model assigned a 0.87 PII probability".
  - Luhn check before CC redaction: random 16-digit strings appear in logs
    (transaction IDs, reference numbers). Without Luhn, these would all be
    redacted, destroying useful debug context.
  - Private IPs are NOT redacted: service mesh IPs, pod IPs, and loopback
    addresses appear in every service log. Redacting them would make logs
    useless for debugging while providing no compliance benefit — internal
    IPs are not PII under any major regulatory framework.
"""

import re
from typing import Final

from detection.base import BaseDetector, DetectionResult


def _luhn_check(number_str: str) -> bool:
    """Return True if number_str passes the Luhn checksum algorithm.
    The Luhn algorithm is a simple check-digit formula designed to catch
    accidental errors in credit card numbers. It does NOT validate that the
    card exists — it only validates the format. Random digit strings of the
    right length rarely pass, making it an effective false-positive filter.
    Algorithm (ISO/IEC 7812):
      1. Reverse the digits.
      2. Double every digit at an odd index in the reversed list.
      3. If doubling produces a two-digit number, subtract 9 (same as summing the digits).
      4. Sum all values. Valid if sum mod 10 == 0.
    """
    # Strip everything that is not a digit (handles formatted input like "4532-0151-...")
    digits = [int(d) for d in number_str if d.isdigit()]

    # Credit card numbers are 13-16 digits. Reject anything outside this range
    # before doing arithmetic — avoids processing arbitrary-length strings.
    if not (13 <= len(digits) <= 16):
        return False

    # Reverse so index 0 = rightmost (check digit), index 1 = first to double, etc.
    digits.reverse()

    total = 0
    for i, digit in enumerate(digits):
        if i % 2 == 1:
            # Every second digit from the right is doubled.
            doubled = digit * 2
            # If doubling exceeds 9, subtract 9 (equivalent to cross-summing the result).
            total += doubled - 9 if doubled > 9 else doubled
        else:
            total += digit

    # A valid Luhn number has a checksum divisible by 10.
    return total % 10 == 0


# --- IPv4 validation regex ---
# Matches any valid IPv4 address (each octet 0-255). Private IP filtering is
# done in Python code rather than regex because the required negative-lookaheads
# for all RFC 1918 ranges would be unreadably complex and hard to audit.
_IPV4_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b"
)

# --- PII pattern registry ---
# Each tuple: (field_type, compiled_regex, replacement_token).
# Patterns are applied sequentially — later patterns see already-sanitised text.
# This means an email like "user@203.0.113.42" is fully sanitised: the email
# pattern fires first, removing the IP from the domain, so the IP pattern
# never sees it as a standalone IP address.
_PII_PATTERNS: Final[list[tuple[str, re.Pattern[str], str]]] = [
    (
        "email",
        # Standard email format per RFC 5321 (simplified).
        # \b anchors prevent partial matches inside longer tokens.
        re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
        "[REDACTED_EMAIL]",
    ),
    (
        "ipv4",
        # Shared compiled pattern. Private IPs filtered in _redact_public_ips.
        _IPV4_PATTERN,
        "[REDACTED_IP]",
    ),
    (
        "credit_card",
        # Matches 13-16 consecutive decimal digits. \b prevents matching inside
        # longer numbers. Luhn check in _redact_valid_ccs filters false positives.
        re.compile(r"\b\d{13,16}\b"),
        "[REDACTED_CC]",
    ),
    (
        "phone_international",
        # E.164 format: + followed by country code (1 non-zero digit) then
        # 7-14 subscriber digits. Total: 8-15 digits after +.
        # \b at the end prevents matching substrings of longer digit strings.
        re.compile(r"\+[1-9]\d{7,14}\b"),
        "[REDACTED_PHONE]",
    ),
    (
        "phone_us",
        # Matches (555) 123-4567, 555-123-4567, 555.123.4567.
        # (?<!\d) and (?!\d) prevent matching within longer digit sequences
        # (e.g., a 10-digit transaction ID should not be mistaken for a phone number).
        re.compile(r"(?<!\d)\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}(?!\d)"),
        "[REDACTED_PHONE]",
    ),
    (
        "user_id_context",
        # Only UUIDs directly preceded by user_id=, userId=, or customer_id=.
        # A bare UUID (trace ID, request ID) must NOT be redacted — it is not PII
        # without context establishing it identifies a natural person.
        re.compile(
            r"(?:user_id|userId|customer_id)="
            r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
            re.IGNORECASE,
        ),
        "[REDACTED_USER_ID]",
    ),
]


class PIIDetector(BaseDetector):
    """Detects and redacts PII from log messages using regex pattern matching.
    Strategy pattern: conforms to BaseDetector, swappable with an ML-based
    NER detector (e.g., spaCy, AWS Comprehend) without changing callers.
    Thread safety: stateless — _PII_PATTERNS is a class-level constant and
    detect creates only local variables.
    """

    # Class-level constant so compiled patterns are created once at import time.
    _PII_PATTERNS: Final[list[tuple[str, re.Pattern[str], str]]] = _PII_PATTERNS

    def detect(self, message: str) -> DetectionResult:
        """Apply PII patterns sequentially; return redacted message and metadata.
        Patterns chain: each pattern operates on the output of the previous one.
        This ensures a message containing multiple PII types is fully sanitised
        even when PII types appear adjacent to each other.
        Returns DetectionResult with:
          detected: True if any redaction occurred.
          sanitized_message: message with PII replaced by typed tokens.
          details: fields_redacted (list of field types), redaction_count (int).
        """
        sanitized = message
        fields_redacted: list[str] = []
        redaction_count = 0

        for field_type, pattern, replacement in self._PII_PATTERNS:
            # IPv4 and credit card require custom substitution logic beyond
            # a simple pattern.sub call — they need per-match validation.
            if field_type == "ipv4":
                new_text, count = self._redact_public_ips(sanitized, pattern, replacement)
            elif field_type == "credit_card":
                new_text, count = self._redact_valid_ccs(sanitized, pattern, replacement)
            else:
                # All other patterns: substitute every match unconditionally.
                new_text, count = self._simple_redact(sanitized, pattern, replacement)

            if count > 0:
                # Only add the field type if at least one redaction was made.
                fields_redacted.append(field_type)
                redaction_count += count

            # Pass the (possibly modified) text to the next pattern in the chain.
            sanitized = new_text

        return DetectionResult(
            detected=bool(fields_redacted),
            sanitized_message=sanitized,
            details={
                "fields_redacted": fields_redacted,
                "redaction_count": redaction_count,
            },
        )

    def detector_type(self) -> str:
        """Return the stable identifier for this strategy."""
        return "pii"

    # --- Private helpers ---
    # Each helper returns (new_text, count) so the detect loop stays readable.

    def _simple_redact(
        self,
        text: str,
        pattern: re.Pattern[str],
        replacement: str,
    ) -> tuple[str, int]:
        """Replace every match of pattern with replacement; return (new_text, count)."""
        # finditer before sub gives the match count without a second scan.
        matches = list(pattern.finditer(text))
        # sub with a string replacement is safe here — no per-match logic needed.
        return pattern.sub(replacement, text), len(matches)

    def _redact_public_ips(
        self,
        text: str,
        pattern: re.Pattern[str],
        replacement: str,
    ) -> tuple[str, int]:
        """Replace non-private IPv4 addresses; skip RFC 1918 / loopback ranges."""
        count = 0

        def replacer(match: re.Match) -> str:
            # nonlocal allows the closure to mutate the enclosing scope's counter.
            nonlocal count
            ip = match.group(0)
            # Private IPs appear in normal service logs — do not redact them.
            if self._is_private_ip(ip):
                return ip
            count += 1
            return replacement

        # sub with a callable invokes replacer for each match individually.
        return pattern.sub(replacer, text), count

    def _redact_valid_ccs(
        self,
        text: str,
        pattern: re.Pattern[str],
        replacement: str,
    ) -> tuple[str, int]:
        """Replace digit sequences that pass Luhn validation; skip invalid ones."""
        count = 0

        def replacer(match: re.Match) -> str:
            nonlocal count
            digits = match.group(0)
            # Luhn check rejects transaction IDs and other non-CC digit strings.
            if not _luhn_check(digits):
                return digits
            count += 1
            return replacement

        return pattern.sub(replacer, text), count

    def _is_private_ip(self, ip: str) -> bool:
        """Return True for RFC 1918 addresses and loopback that must NOT be redacted.
        Private ranges per RFC 1918 + loopback per RFC 990:
          127.0.0.0/8 — loopback
          10.0.0.0/8 — Class A private
          172.16.0.0/12 — Class B private (172.16 – 172.31)
          192.168.0.0/16 — Class C private
        """
        parts = ip.split(".")
        if len(parts) != 4:
            return False

        try:
            # Convert each octet to int for numeric range comparison.
            o = [int(p) for p in parts]
        except ValueError:
            # Non-numeric octet — the IPv4 regex should prevent this, but be safe.
            return False

        # Loopback: 127.x.x.x
        if o[0] == 127:
            return True
        # Class A private: 10.x.x.x
        if o[0] == 10:
            return True
        # Class B private: 172.16.x.x – 172.31.x.x
        if o[0] == 172 and 16 <= o[1] <= 31:
            return True
        # Class C private: 192.168.x.x
        if o[0] == 192 and o[1] == 168:
            return True

        return False
