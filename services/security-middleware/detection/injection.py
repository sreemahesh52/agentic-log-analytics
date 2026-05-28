"""Prompt injection detector — Strategy implementation of BaseDetector.
Prompt injection is an attack where a malicious actor embeds LLM instructions
inside data the LLM will process (e.g., a log message). If the log reaches the
RCA Agent prompt without sanitisation, the attacker can override the system
prompt, exfiltrate the prompt, or cause the LLM to produce harmful output.
This detector catches the most common injection patterns via regex. Regex is
appropriate here because the attack surface is well-defined text phrases, and
regex has deterministic, auditable behaviour — unlike ML-based classifiers
which can drift or be fooled by adversarial inputs in unexpected ways.
"""

import re
from typing import Final

from detection.base import BaseDetector, DetectionResult


# --- Injection pattern registry ---
# Type: list of (pattern_name, compiled_regex) tuples.
# The pattern_name becomes part of the redaction token so downstream systems
# can identify exactly which attack pattern fired.
# All patterns use re.IGNORECASE — attackers trivially bypass case-sensitive checks.
# Patterns are applied sequentially. Earlier patterns modify the string seen
# by later patterns, preventing bypass via pattern chaining (e.g., embedding
# "reveal" and "system prompt" far apart to avoid a single regex).
_INJECTION_PATTERNS: Final[list[tuple[str, re.Pattern[str]]]] = [
    (
        "ignore_instructions",
        # "ignore previous instructions" / "ignore all instructions" etc.
        # The most common canonical injection opener.
        re.compile(
            r"ignore\s+(previous|above|prior|all)\s+instructions?",
            re.IGNORECASE,
        ),
    ),
    (
        "you_are_now",
        # "you are now a pirate" — persona hijacking via identity reassignment.
        # \b ensures we don't match "you are now available" etc.
        re.compile(r"\byou\s+are\s+now\s+(a|an)\b", re.IGNORECASE),
    ),
    (
        "act_as",
        # "act as a ...", "pretend as if you are ...", "roleplay as an ..." —
        # broad family of persona-adoption injection attempts.
        re.compile(
            r"\b(act|behave|pretend|roleplay)\s+as\s+(if\s+you\s+(are|were)|a|an)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "reveal_prompt",
        # "reveal your system prompt", "show instructions", "output context" —
        # attempts to exfiltrate the LLM's system prompt or context window.
        re.compile(
            r"\b(reveal|show|print|display|output)\s+(your\s+)?"
            r"(system\s+prompt|instructions|prompt|context)",
            re.IGNORECASE,
        ),
    ),
    (
        "override",
        # "forget everything", "disregard all", "override your previous" —
        # attempts to nullify prior instructions before issuing new ones.
        re.compile(
            r"\b(forget|disregard|ignore|override)\s+"
            r"(everything|all|your\s+(previous|prior|above))",
            re.IGNORECASE,
        ),
    ),
    (
        "jailbreak_delimiters",
        # Special tokens used by LLM instruction formats (Llama, ChatML).
        # Their presence in user-supplied data almost always signals an attempt
        # to inject a fake system/user/assistant message boundary.
        re.compile(
            r"(\[INST\]|\[\/INST\]|<\|im_start\|>|<\|im_end\|>|<\|endoftext\|>)",
            re.IGNORECASE,
        ),
    ),
    (
        "jailbreak_keywords",
        # Explicit jailbreak terminology. "DAN mode" and "developer mode" are
        # well-known prompts that circulate publicly to bypass LLM safety layers.
        re.compile(
            r"\b(jailbreak|DAN\s+mode|developer\s+mode|prompt\s+injection)\b",
            re.IGNORECASE,
        ),
    ),
]


class InjectionDetector(BaseDetector):
    """Detects prompt injection attempts in log messages using regex pattern matching.
    Strategy pattern: conforms to BaseDetector so the consumer pipeline can swap
    this for an ML-based classifier without changing any calling code. The caller
    only knows about BaseDetector and DetectionResult.
    Thread safety: stateless — _INJECTION_PATTERNS is a class-level constant.
    Each detect call creates its own local variables and returns a new result.
    """

    # Class-level constant so compiled patterns are created once at import time,
    # not on every detect call. Pattern compilation is expensive.
    _INJECTION_PATTERNS: Final[list[tuple[str, re.Pattern[str]]]] = _INJECTION_PATTERNS

    def detect(self, message: str) -> DetectionResult:
        """Scan message for injection patterns; replace each match with a redaction token.
        Patterns are applied sequentially — the output of pattern N is the input
        to pattern N+1. This prevents bypass attempts that rely on splitting an
        injection phrase across two patterns by first modifying the surrounding text.
        Returns DetectionResult with:
          detected: True if any pattern matched.
          sanitized_message: message with matches replaced by [INJECTION_REDACTED_<name>].
          details: matched_patterns (list of pattern names), match_count (int).
        """
        sanitized = message
        matched_patterns: list[str] = []
        match_count = 0

        # --- Sequential pattern scan ---
        for pattern_name, pattern in self._INJECTION_PATTERNS:
            # finditer on the current sanitized text (not the original) so that
            # matches already replaced by earlier patterns are not double-counted.
            matches = list(pattern.finditer(sanitized))

            if matches:
                # Record the pattern name exactly once, even if it matched multiple times.
                matched_patterns.append(pattern_name)
                # Accumulate the total number of individual match spans.
                match_count += len(matches)

            # Replace all spans for this pattern. Token format is uppercase and
            # bracketed so redactions are visually obvious in downstream log viewers.
            replacement = f"[INJECTION_REDACTED_{pattern_name}]"
            # re.Pattern.sub replaces all non-overlapping matches in one pass.
            sanitized = pattern.sub(replacement, sanitized)

        return DetectionResult(
            detected=bool(matched_patterns),
            sanitized_message=sanitized,
            details={
                "matched_patterns": matched_patterns,
                "match_count": match_count,
            },
        )

    def detector_type(self) -> str:
        """Return the stable identifier for this strategy."""
        return "injection"
