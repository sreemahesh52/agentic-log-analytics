"""Audit repository — Repository Pattern for all audit SQL.
Two tables live here:
  llm_audit_log — every outbound LLM call (future steps will use this)
  security_events — injection and PII detections from the security middleware
Design decisions:
  - Repository Pattern: all SQL lives here, never in business logic classes.
  - Never raises on error: audit failures must not break the calling pipeline.
    A missed audit record is recoverable (manual replay); a crashed consumer
    drops all messages in its Kafka partition batch. Fail-open on audit writes
    is the correct trade-off.
  - SHA-256 hashing: stores the hash of message content, not the content itself.
    This protects PII that may not yet have been redacted (e.g., in the raw input
    before the PII detector has run), and keeps the audit table storage bounded.
"""

import hashlib
import json
from typing import Any

import asyncpg
import structlog

logger = structlog.get_logger()

# Encoding used when hashing text content. UTF-8 is the canonical choice
# for modern systems — ensures consistent byte representation across platforms.
_HASH_ENCODING = "utf-8"


class AuditRepository:
    """Repository for audit writes: llm_audit_log and security_events tables.
    The pool is injected — this class never opens database connections directly.
    All methods are fire-and-forget: they log at ERROR on failure but never raise.
    This is intentional: a DB failure in the audit layer must never block the
    Kafka consumer from processing and publishing clean messages.
    """

    def __init__(self, db_pool: asyncpg.Pool) -> None:
        """Accept an asyncpg pool — never store a bare connection."""
        # Pool, not a connection: each query borrows from the pool for its
        # duration and returns it automatically, enabling connection reuse.
        self._pool = db_pool

    async def log_llm_call(
        self,
        tenant_id: str,
        service: str,
        prompt_version: str | None,
        model: str,
        input_text: str,
        output_text: str,
        pii_fields_redacted: list[str],
        call_latency_ms: int,
    ) -> None:
        """Insert a row into llm_audit_log. Never raises on failure.
        Called after every outbound LLM call. The security middleware itself
        does not call LLMs in this step — this method is ready for future steps
        (Anomaly Verifier, RCA Agent) that will use this same audit repository.
        """
        # SHA-256 hex digest: 64 lowercase hex characters, deterministic for
        # the same input. sha256.hexdigest never returns raw bytes — always str.
        input_hash = hashlib.sha256(input_text.encode(_HASH_ENCODING)).hexdigest()
        output_hash = hashlib.sha256(output_text.encode(_HASH_ENCODING)).hexdigest()

        try:
            async with self._pool.acquire() as conn:
                # Parameterised query — $1..$8 are asyncpg placeholders.
                # Never use f-strings or string concatenation in SQL.
                await conn.execute(
                    """
                    INSERT INTO llm_audit_log (
                        tenant_id, service, prompt_version, model,
                        input_hash, output_hash, pii_fields_redacted, call_latency_ms
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """,
                    tenant_id,
                    service,
                    prompt_version,
                    model,
                    input_hash,
                    output_hash,
                    pii_fields_redacted,
                    call_latency_ms,
                )
        except Exception as exc:
            # Log at ERROR but do not re-raise. audit must never
            # break the calling pipeline — this is an explicit design decision.
            logger.error(
                "audit_llm_call_write_failed",
                service=service,
                model=model,
                exc_type=type(exc).__name__,
            )

    async def log_security_event(
        self,
        tenant_id: str | None,
        service: str,
        event_type: str,
        details: dict[str, Any],
        original_message: str,
    ) -> None:
        """Insert a row into security_events. Never raises on failure.
        tenant_id is nullable: security events may be recorded before the
        message has been matched to a tenant in the pipeline.
        """
        # Hash the original (pre-sanitisation) message — never store raw PII in audit.
        original_message_hash = hashlib.sha256(
            original_message.encode(_HASH_ENCODING)
        ).hexdigest()

        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO security_events (
                        tenant_id, service, event_type, details, original_message_hash
                    ) VALUES ($1, $2, $3, $4, $5)
        """,
                    # asyncpg casts None → NULL for nullable UUID columns.
                    tenant_id,
                    service,
                    event_type,
                    # asyncpg with the JSONB codec registered in the pool init
                    # serialises Python dicts automatically. json.dumps used as
                    # a fallback if the codec is not registered.
                    json.dumps(details),
                    original_message_hash,
                )
        except Exception as exc:
            logger.error(
                "audit_security_event_write_failed",
                service=service,
                event_type=event_type,
                exc_type=type(exc).__name__,
            )
