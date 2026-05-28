"""Pydantic v2 models for log messages flowing through the security middleware.
These models define the data contracts between:
  - Log Ingestion Service (Go) → Security Middleware (Python): RawLogMessage
  - Security Middleware → Log Consumer (Go): CleanLogMessage
  - Security Middleware → security.events Kafka topic: SecurityEvent
Pydantic validates all fields at construction time and raises ValidationError
immediately if a required field is missing or the wrong type. This is the
"fail fast" approach: corrupt data is rejected at the boundary, never silently
propagated into the system.
"""

from datetime import datetime, timezone
from uuid import uuid4

from pydantic import BaseModel, Field


class RawLogMessage(BaseModel):
    """A log entry as received from the Log Ingestion Service via Kafka logs.raw.
    All fields are validated by Pydantic at construction. The model is immutable
    by default (model_config not set here — Pydantic v2 models are mutable unless
    model_config = ConfigDict(frozen=True), which we omit for flexibility).
    """

    service: str
    # level must be one of the five standard log levels. Downstream PostgreSQL
    # schema enforces the same CHECK constraint — validate early, enforce at DB.
    level: str
    # The raw message text — may contain injection attempts or PII at this stage.
    message: str
    trace_id: str
    # Arbitrary key-value pairs from the producing service (request IDs, versions, etc.).
    metadata: dict = {}
    # ISO 8601 string with UTC offset, e.g. "2024-01-15T10:23:45.123+00:00".
    # Stored as str to preserve the exact format the ingestion service produced.
    # Never convert to datetime here — the ingestion service is authoritative on format.
    timestamp: str


class CleanLogMessage(RawLogMessage):
    """A RawLogMessage that has passed through both security detectors.
    Extends RawLogMessage — Liskov Substitution holds: CleanLogMessage is
    always a valid RawLogMessage plus additional security metadata fields.
    Any code that accepts RawLogMessage will also accept CleanLogMessage.
    The message field at this point contains the sanitised text (injection
    patterns replaced with tokens, PII replaced with [REDACTED_*] tokens).
    """

    # True if InjectionDetector.detect found at least one injection pattern.
    injection_attempted: bool
    # List of PII field type names that were redacted, e.g. ["email", "ipv4"].
    # Empty list means no PII was detected. Used for audit logging and Prometheus metrics.
    pii_fields_redacted: list[str]


class SecurityEvent(BaseModel):
    """Emitted when a detector fires; published to the security.events Kafka topic.
    One SecurityEvent is published per detection, not per message. If both
    injection and PII fire on the same message, two SecurityEvents are emitted:
    one with event_type="injection", one with event_type="pii".
    """

    # UUID generated at creation time for deduplication and downstream correlation.
    # lambda: str(uuid4) is called once per instance — Field(default_factory=...)
    # ensures a NEW uuid is generated for each SecurityEvent, not shared across instances.
    event_id: str = Field(default_factory=lambda: str(uuid4()))

    # tenant_id is not present in the raw Kafka payload — it is resolved by the
    # consumer layer after looking up the API key in the tenants table.
    tenant_id: str | None = None

    service: str

    # Must be "injection" or "pii" — matches the detector_type return values.
    # Downstream consumers switch on this field to route to the correct handler.
    event_type: str

    # .isoformat on a timezone-aware datetime includes the +00:00 offset.
    # Never use str on a datetime — it omits timezone information.
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    # Detector-specific detail dict:
    #   injection events: {"matched_patterns": [...], "match_count": int}
    #   pii events: {"fields_redacted": [...], "redaction_count": int}
    details: dict
