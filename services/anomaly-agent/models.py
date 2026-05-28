"""Pydantic data models for the anomaly-agent service.
These models represent data that crosses serialisation boundaries:
  - LogEvent: the enriched log message consumed from Kafka 'logs.enriched'.
  - AlertPayload: the alert message published to Kafka 'alerts' and written
                  to the PostgreSQL 'alerts' table.
Pydantic v2 is used (not dataclasses) for these models because they require:
  1. JSON parsing from Kafka message bytes.
  2. Automatic type coercion and validation.
  3. .model_dump for serialisation back to JSON for Kafka publishing.
  4. Clear error messages when required fields are missing from the wire format.
Internal domain objects (AnomalyDetectionResult) use plain dataclasses —
they never cross a serialisation boundary and do not need Pydantic overhead.
"""

from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _snake_to_pascal(name: str) -> str:
    """Map a Python snake_case field name to PascalCase for Go Kafka wire format.
    Pydantic's alias_generator is called with the Python attribute name and must
    return the external alias. The Go log-consumer publishes PascalCase keys, so
    we capitalise each word segment. Irregular fields (TenantID, TraceID,
    InjectionAttempted) override this via explicit Field(alias=...) below.
    """
    return "".join(word.capitalize() for word in name.split("_"))


class LogEvent(BaseModel):
    """Represents one log entry consumed from the 'logs.enriched' Kafka topic.
    The Go log-consumer publishes PascalCase JSON keys (TenantID, Service …).
    alias_generator=_snake_to_pascal maps Python names → PascalCase aliases so
    model_validate accepts the Go wire format directly.
    populate_by_name=True lets tests construct LogEvent with snake_case kwargs.
    """

    model_config = ConfigDict(
        alias_generator=_snake_to_pascal,
        populate_by_name=True,
    )

    # Explicit aliases for Go's abbreviated ID fields which don't follow simple
    # PascalCase rules: "TenantID" → alias "TenantID" (not "Tenant_I_D").
    tenant_id: UUID = Field(alias="TenantID")
    service: str
    level: str
    message: str
    timestamp: datetime
    trace_id: Optional[UUID] = Field(default=None, alias="TraceID")
    metadata: dict[str, Any] = Field(default_factory=dict)
    injection_attempted: bool = Field(default=False, alias="InjectionAttempted")

    @field_validator("level")
    @classmethod
    def validate_level(cls, v: str) -> str:
        """Reject log levels not in the allowed set.
        The DB CHECK constraint also enforces this, but validating here means
        we reject bad messages before attempting a DB write — fail fast.
        """
        allowed = {"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
        if v not in allowed:
            raise ValueError(f"level must be one of {allowed}, got '{v}'")
        return v

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp_aware(cls, v: datetime) -> datetime:
        """Reject naive datetimes — all timestamps must be timezone-aware.
        A naive datetime has no timezone context. If we stored or compared it
        with a UTC value, the comparison would be silently wrong. Rejecting at
        ingestion time prevents this class of bug entirely.
        """
        if v.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware (UTC)")
        return v


class AlertPayload(BaseModel):
    """Represents an alert published to the Kafka 'alerts' topic.
    Published by the anomaly-agent orchestrator after a detector confirms an anomaly.
    Consumed by the Alert Correlation Engine (step 8).
    alert_id is generated here so the Kafka message and the PostgreSQL row share
    the same UUID — the DB row is inserted first, then the same UUID is published.
    This means a consumer can look up the DB row using only the Kafka message.
    """

    alert_id: UUID = Field(default_factory=uuid4)
    tenant_id: UUID
    service: str
    # anomaly_type mirrors the CHECK constraint on the alerts table
    anomaly_type: str
    # severity mirrors the CHECK constraint on the alerts table
    severity: str
    confidence: float
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    # details carries the raw detector output (z_score, baseline_mean, etc.)
    details: dict[str, Any] = Field(default_factory=dict)

    @field_validator("anomaly_type")
    @classmethod
    def validate_anomaly_type(cls, v: str) -> str:
        """Reject anomaly types not matching the DB CHECK constraint."""
        allowed = {"statistical", "semantic", "combined"}
        if v not in allowed:
            raise ValueError(f"anomaly_type must be one of {allowed}, got '{v}'")
        return v

    @field_validator("severity")
    @classmethod
    def validate_severity(cls, v: str) -> str:
        """Reject severity values not matching the DB CHECK constraint."""
        allowed = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
        if v not in allowed:
            raise ValueError(f"severity must be one of {allowed}, got '{v}'")
        return v

    @field_validator("confidence")
    @classmethod
    def validate_confidence_range(cls, v: float) -> float:
        """Confidence must be in [0.0, 1.0] — matches the DB CHECK constraint."""
        if not 0.0 <= v <= 1.0:
            raise ValueError(f"confidence must be between 0.0 and 1.0, got {v}")
        return v
