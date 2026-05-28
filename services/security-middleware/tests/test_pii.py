"""Unit tests for PIIDetector.
All tests run with zero external dependencies — no Kafka, no PostgreSQL, no network.
Tests cover each PII pattern individually, the Luhn filter, and multi-type chaining.
Test naming convention: test_<scenario>_<expected_outcome>
Tests with _NOT_ in the name verify that something is explicitly NOT redacted
(false positive prevention is as important as true positive detection).
"""

import pytest

from detection.pii import PIIDetector, _luhn_check


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def detector() -> PIIDetector:
    """Provide a fresh PIIDetector instance for each test."""
    return PIIDetector()


# ---------------------------------------------------------------------------
# Luhn algorithm unit tests (tested independently before testing the detector)
# ---------------------------------------------------------------------------


def test_luhn_valid_visa_number_passes() -> None:
    """Standalone Luhn check: a known valid Visa test number must pass."""
    # 4532015112830366 is a well-known test number used in PCI-DSS documentation.
    assert _luhn_check("4532015112830366") is True


def test_luhn_sequential_digits_fails() -> None:
    """Standalone Luhn check: a sequential digit string must fail."""
    # 1234567890123456 fails Luhn — checksum is 64 (not divisible by 10).
    assert _luhn_check("1234567890123456") is False


# ---------------------------------------------------------------------------
# Happy path — clean message
# ---------------------------------------------------------------------------


def test_clean_message_not_detected(detector: PIIDetector) -> None:
    """A message with no PII must produce detected=False and an unchanged message."""
    message = "Service restarted after health check failure on pod abc-123"
    result = detector.detect(message)

    assert result.detected is False
    assert result.sanitized_message == message
    assert result.details["fields_redacted"] == []
    assert result.details["redaction_count"] == 0


# ---------------------------------------------------------------------------
# Individual PII pattern tests
# ---------------------------------------------------------------------------


def test_email_detected_and_redacted(detector: PIIDetector) -> None:
    """Email addresses must be replaced with [REDACTED_EMAIL]."""
    result = detector.detect("Password reset for user@example.com failed")

    assert result.detected is True
    assert "[REDACTED_EMAIL]" in result.sanitized_message
    # Original email must be completely gone.
    assert "user@example.com" not in result.sanitized_message
    assert "email" in result.details["fields_redacted"]
    assert result.details["redaction_count"] >= 1


def test_private_ip_NOT_detected(detector: PIIDetector) -> None:
    """Private IP ranges (RFC 1918 + loopback) must never be redacted.
    These IPs appear in every service log: pod IPs, sidecar IPs, localhost.
    Redacting them destroys debugging context without any compliance benefit —
    internal IPs are not PII under GDPR, HIPAA, or PCI-DSS.
    """
    private_ips = [
        "192.168.1.1",   # Class C private (192.168.0.0/16)
        "10.0.0.1",      # Class A private (10.0.0.0/8)
        "172.16.0.1",    # Class B private start (172.16.0.0/12)
        "172.31.255.254", # Class B private end
        "127.0.0.1",     # Loopback
    ]
    for private_ip in private_ips:
        result = detector.detect(f"Connection from {private_ip} refused")
        assert result.detected is False, (
            f"Private IP {private_ip} was incorrectly flagged as PII"
        )
        assert private_ip in result.sanitized_message, (
            f"Private IP {private_ip} was incorrectly redacted"
        )


def test_public_ip_detected_and_redacted(detector: PIIDetector) -> None:
    """Public IPv4 addresses must be replaced with [REDACTED_IP]."""
    # 203.0.113.0/24 is a documentation range (RFC 5737) — public, not private.
    result = detector.detect("Request from 203.0.113.42 was blocked by WAF")

    assert result.detected is True
    assert "[REDACTED_IP]" in result.sanitized_message
    assert "203.0.113.42" not in result.sanitized_message
    assert "ipv4" in result.details["fields_redacted"]


def test_credit_card_valid_luhn_detected(detector: PIIDetector) -> None:
    """A 16-digit number that passes the Luhn checksum must be redacted as a CC."""
    # 4532015112830366 is a valid Visa test number — passes Luhn, 16 digits.
    result = detector.detect("Payment card 4532015112830366 was used for purchase")

    assert result.detected is True
    assert "[REDACTED_CC]" in result.sanitized_message
    assert "4532015112830366" not in result.sanitized_message
    assert "credit_card" in result.details["fields_redacted"]


def test_credit_card_invalid_luhn_NOT_detected(detector: PIIDetector) -> None:
    """A 16-digit string that fails Luhn must NOT be redacted.
    Transaction reference numbers, invoice IDs, and other 16-digit business
    identifiers appear frequently in logs. Without Luhn filtering, all of them
    would be incorrectly redacted, destroying critical audit trails.
    """
    # 1234567890123456 fails Luhn: checksum = 64, not divisible by 10.
    result = detector.detect("Transaction ref 1234567890123456 recorded in ledger")

    # The number must remain in the sanitized message.
    assert "1234567890123456" in result.sanitized_message
    assert "[REDACTED_CC]" not in result.sanitized_message


def test_phone_international_detected(detector: PIIDetector) -> None:
    """E.164 international phone numbers (starting with +) must be redacted."""
    # +14155551234 — valid E.164 format (US number with country code 1).
    result = detector.detect("SMS sent to +14155551234 successfully")

    assert result.detected is True
    assert "[REDACTED_PHONE]" in result.sanitized_message
    assert "+14155551234" not in result.sanitized_message
    assert "phone_international" in result.details["fields_redacted"]


def test_phone_us_format_detected(detector: PIIDetector) -> None:
    """US-format phone numbers like (555) 123-4567 must be redacted."""
    result = detector.detect("Callback number (555) 123-4567 logged for case")

    assert result.detected is True
    assert "[REDACTED_PHONE]" in result.sanitized_message
    assert "(555) 123-4567" not in result.sanitized_message
    assert "phone_us" in result.details["fields_redacted"]


def test_user_id_uuid_in_context_detected(detector: PIIDetector) -> None:
    """UUIDs preceded by user_id=, userId=, or customer_id= must be redacted."""
    uuid = "550e8400-e29b-41d4-a716-446655440000"
    result = detector.detect(f"Auth failed for user_id={uuid} from service payments")

    assert result.detected is True
    assert "[REDACTED_USER_ID]" in result.sanitized_message
    # The UUID itself must be gone — the whole 'user_id=<UUID>' span is replaced.
    assert uuid not in result.sanitized_message
    assert "user_id_context" in result.details["fields_redacted"]


def test_bare_uuid_without_context_NOT_detected(detector: PIIDetector) -> None:
    """A UUID without a user_id=/userId=/customer_id= prefix must NOT be redacted.
    UUIDs are used as trace IDs, request IDs, session IDs, and many other
    non-PII identifiers. Redacting all UUIDs would destroy the trace context
    that engineers rely on to correlate logs across services.
    Only UUIDs in an explicit user-identity context are PII.
    """
    uuid = "550e8400-e29b-41d4-a716-446655440000"
    result = detector.detect(f"Processing request trace_id={uuid} on worker-3")

    # No redaction should occur — trace_id= is not in the context prefix list.
    assert result.detected is False
    assert uuid in result.sanitized_message


# ---------------------------------------------------------------------------
# Multi-type and chaining tests
# ---------------------------------------------------------------------------


def test_multiple_pii_types_all_redacted(detector: PIIDetector) -> None:
    """A message with multiple PII types must have every type independently redacted."""
    message = "User test@example.com connected from 203.0.113.42"
    result = detector.detect(message)

    assert result.detected is True
    # Both email and IP must be replaced.
    assert "[REDACTED_EMAIL]" in result.sanitized_message
    assert "[REDACTED_IP]" in result.sanitized_message
    # Original values must be gone.
    assert "test@example.com" not in result.sanitized_message
    assert "203.0.113.42" not in result.sanitized_message
    # Both field types must appear in the details.
    assert "email" in result.details["fields_redacted"]
    assert "ipv4" in result.details["fields_redacted"]
    # Total count reflects two individual redactions.
    assert result.details["redaction_count"] == 2


def test_redaction_chain_applied_sequentially(detector: PIIDetector) -> None:
    """Later patterns must operate on the output of earlier patterns.
    This tests the chain property: if email is sanitised first, the IP redaction
    must still fire on the already-modified text. If patterns were applied to
    the original message in parallel (instead of sequentially), this test would
    still pass — but the sequential guarantee matters for cases where one pattern's
    replacement text could interfere with another pattern's regex.
    """
    # Email redacted first → "Contact [REDACTED_EMAIL] or trace from 8.8.8.8 for support"
    # IP redacted second → "Contact [REDACTED_EMAIL] or trace from [REDACTED_IP] for support"
    # 8.8.8.8 is Google Public DNS — a public IP that must be redacted.
    message = "Contact admin@corp.io or trace from 8.8.8.8 for support"
    result = detector.detect(message)

    assert result.detected is True
    assert "[REDACTED_EMAIL]" in result.sanitized_message
    assert "[REDACTED_IP]" in result.sanitized_message
    # Both field types must be independently recorded.
    assert "email" in result.details["fields_redacted"]
    assert "ipv4" in result.details["fields_redacted"]
    # Total redaction count must account for both.
    assert result.details["redaction_count"] >= 2
    # Original values must be gone.
    assert "admin@corp.io" not in result.sanitized_message
    assert "8.8.8.8" not in result.sanitized_message
