"""Kafka consumer/processing pipeline for the security middleware.
SecurityMiddlewareHandler orchestrates the following per-message steps:
  1. Consume a message from logs.raw
  2. Validate as RawLogMessage (Pydantic)
  3. Run InjectionDetector on raw message text
  4. Run PIIDetector on injection-sanitized output (chained, not parallel)
  5. For each detected threat: publish SecurityEvent to Kafka + write audit DB record
  6. Build CleanLogMessage with fully sanitized text
  7. Publish CleanLogMessage to logs.raw.clean (with one retry on failure)
  8. Commit the Kafka offset only after a successful clean-topic publish
Kafka offset commit semantics (at-least-once delivery):
  Offsets are committed manually, only after the clean message is confirmed
  published. If the service crashes mid-processing, Kafka redelivers the message.
  This means a message may be processed twice but never dropped — at-least-once.
  Exactly-once requires Kafka transactions (overkill for this pipeline).
"""

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import structlog
from confluent_kafka import Consumer, KafkaError, Producer
from prometheus_client import Counter
from pydantic import ValidationError

from audit import AuditRepository
from detection.base import BaseDetector, DetectionResult
from models import CleanLogMessage, RawLogMessage, SecurityEvent

logger = structlog.get_logger()

# How long confluent-kafka's poll waits for a message before returning None.
# 1 second: responsive enough for real-time processing without spinning the CPU.
KAFKA_POLL_TIMEOUT_SECONDS = 1.0

# Delay before the single retry when publishing to logs.raw.clean fails.
# 1 second gives a transient broker issue time to resolve without long delays.
PUBLISH_RETRY_DELAY_SECONDS = 1.0


@dataclass
class SecurityMetrics:
    """Prometheus counter handles injected into SecurityMiddlewareHandler.
    Dataclass (not Pydantic) because this is internal wiring, not a schema.
    Injected rather than instantiated inside the handler — this is Dependency
    Inversion: tests can inject mock counters without a real Prometheus registry.
    """

    # Counter: security_injection_attempts_total{service, tenant}
    # Incremented once per message where InjectionDetector.detect fires.
    injection_attempts: Counter

    # Counter: security_pii_redactions_total{field_type, tenant}
    # Incremented once per distinct PII field type redacted in a message.
    pii_redactions: Counter


class SecurityMiddlewareHandler:
    """Consumes logs.raw, applies security detection, publishes to clean/events topics.
    Single Responsibility: orchestration only.
      - Detection logic: delegated to BaseDetector implementations (injected)
      - DB audit writes: delegated to AuditRepository (injected)
      - Kafka I/O: delegated to Producer/Consumer (injected)
    Dependency Inversion: depends on BaseDetector (abstract interface), not
    InjectionDetector or PIIDetector. Any detector conforming to BaseDetector
    can be injected — including mock detectors in tests.
    Open/Closed: to add a new detector (e.g., a secrets scanner), create a new
    BaseDetector subclass and extend __init__ — this class does not change.
    """

    def __init__(
        self,
        producer: Producer,
        consumer: Consumer,
        injection_detector: BaseDetector,
        pii_detector: BaseDetector,
        audit_repo: AuditRepository,
        metrics: SecurityMetrics,
        clean_topic: str,
        security_topic: str,
    ) -> None:
        """Accept all dependencies by injection — never instantiate here."""
        self._producer = producer
        self._consumer = consumer
        self._injection_detector = injection_detector
        self._pii_detector = pii_detector
        self._audit_repo = audit_repo
        self._metrics = metrics
        self._clean_topic = clean_topic
        self._security_topic = security_topic
        # Checked by run on each loop iteration. stop sets this to False.
        self._running = True

    async def process_message(self, raw_message: dict[str, Any]) -> bool:
        """Run one raw log through the full security pipeline.
        Returns True → caller should commit the Kafka offset.
        Returns False → caller should NOT commit (publish failed; Kafka will redeliver).
        """
        # --- Validate incoming payload against RawLogMessage schema ---
        try:
            log_entry = RawLogMessage.model_validate(raw_message)
        except ValidationError as exc:
            logger.warning("message_parse_error", error=str(exc))
            await self._publish_json(self._security_topic, _build_parse_error(raw_message, exc))
            # Commit: malformed messages cannot be fixed by redelivery.
            return True

        # tenant_id may be embedded by the ingestion service in metadata.
        # It is optional — the pipeline functions correctly without it.
        tenant_id: str | None = (raw_message.get("metadata") or {}).get("tenant_id")

        # --- Chain detectors: injection first, PII on injection-sanitized output ---
        # Why chained rather than parallel? PII patterns might match the text of
        # an injection token itself (e.g., "reveal system prompt user@example.com").
        # Running PII on the injection-sanitized output prevents double-matching.
        injection_result = self._injection_detector.detect(log_entry.message)
        pii_result = self._pii_detector.detect(injection_result.sanitized_message)

        if injection_result.detected:
            await self._handle_injection_event(log_entry, injection_result, tenant_id)
        if pii_result.detected:
            await self._handle_pii_event(log_entry, pii_result, tenant_id)

        clean = _build_clean_message(log_entry, injection_result, pii_result)
        return await self._publish_with_retry(self._clean_topic, clean.model_dump())

    async def run(self) -> None:
        """Main consumer loop. Commits offset only after successful publish."""
        # get_event_loop returns the running loop — safe to call inside an async function.
        loop = asyncio.get_event_loop()
        logger.info("consumer_loop_started")

        while self._running:
            try:
                # run_in_executor runs the blocking poll in a thread pool so
                # the asyncio event loop stays free for other coroutines (DB writes).
                msg = await loop.run_in_executor(
                    None, self._consumer.poll, KAFKA_POLL_TIMEOUT_SECONDS
                )
                if msg is None:
                    # Normal: no message arrived within the poll timeout.
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("kafka_consumer_error", error=str(msg.error()))
                    continue

                await self._handle_kafka_message(msg, loop)

            except Exception as exc:
                # Guard: a single bad message must never terminate the consumer.
                logger.error(
                    "consumer_loop_unexpected_error",
                    exc_type=type(exc).__name__,
                    error=str(exc),
                )

        logger.info("consumer_loop_stopped")

    def stop(self) -> None:
        """Signal run to exit after finishing the current message."""
        # Setting the flag here is thread-safe for a boolean — the run loop
        # checks it on each iteration and exits cleanly without mid-message abort.
        self._running = False

    async def _handle_kafka_message(
        self, msg: Any, loop: asyncio.AbstractEventLoop
    ) -> None:
        """Decode one Kafka message and process it; commit offset on success."""
        try:
            raw = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error("kafka_message_decode_error", error=str(exc))
            # Commit even on decode failure: the message bytes are corrupted and
            # redelivery will produce the same failure indefinitely.
            await loop.run_in_executor(None, self._consumer.commit, msg)
            return

        success = await self.process_message(raw)

        if success:
            # Commit after confirmed publish — this is the at-least-once guarantee.
            # On crash before commit, Kafka redelivers this message to the next consumer.
            await loop.run_in_executor(None, self._consumer.commit, msg)
        else:
            logger.warning(
                "offset_not_committed_publish_failed",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

    async def _handle_injection_event(
        self,
        log_entry: RawLogMessage,
        result: DetectionResult,
        tenant_id: str | None,
    ) -> None:
        """Publish injection SecurityEvent to Kafka and write audit DB record."""
        event = SecurityEvent(
            service=log_entry.service,
            event_type="injection",
            tenant_id=tenant_id,
            details=result.details,
        )
        await self._publish_json(self._security_topic, event.model_dump())
        await self._audit_repo.log_security_event(
            tenant_id=tenant_id,
            service=log_entry.service,
            event_type="injection",
            details=result.details,
            original_message=log_entry.message,
        )
        # Label with "unknown" if tenant_id is not yet resolved — prevents
        # null label values which cause cardinality issues in Prometheus.
        self._metrics.injection_attempts.labels(
            service=log_entry.service,
            tenant=tenant_id or "unknown",
        ).inc()

    async def _handle_pii_event(
        self,
        log_entry: RawLogMessage,
        result: DetectionResult,
        tenant_id: str | None,
    ) -> None:
        """Publish PII SecurityEvent to Kafka and write audit DB record."""
        event = SecurityEvent(
            service=log_entry.service,
            event_type="pii",
            tenant_id=tenant_id,
            details=result.details,
        )
        await self._publish_json(self._security_topic, event.model_dump())
        await self._audit_repo.log_security_event(
            tenant_id=tenant_id,
            service=log_entry.service,
            event_type="pii",
            details=result.details,
            original_message=log_entry.message,
        )
        # Increment once per distinct PII field type — gives per-type visibility
        # in Grafana Panel 20 (PII redactions by field type).
        for field_type in result.details.get("fields_redacted", []):
            self._metrics.pii_redactions.labels(
                field_type=field_type,
                tenant=tenant_id or "unknown",
            ).inc()

    async def _publish_json(self, topic: str, payload: dict[str, Any]) -> None:
        """Publish a JSON-encoded dict to a Kafka topic. Fire-and-forget.
        Errors are logged at ERROR but not raised. This method is used for
        events where publish failure is tolerable (audit/observability events,
        not the primary clean log stream).
        """
        try:
            # produce is non-blocking: it enqueues the message in librdkafka's
            # internal delivery buffer. poll(0) triggers delivery callbacks
            # immediately without blocking — frees delivered slots in the buffer.
            self._producer.produce(topic, value=json.dumps(payload).encode("utf-8"))
            self._producer.poll(0)
        except Exception as exc:
            logger.error(
                "kafka_publish_failed",
                topic=topic,
                exc_type=type(exc).__name__,
            )

    async def _publish_with_retry(self, topic: str, payload: dict[str, Any]) -> bool:
        """Publish to topic; retry once after PUBLISH_RETRY_DELAY_SECONDS on failure.
        Returns True on success, False after both attempts fail.
        Used for the critical clean-topic publish — failure here means the Kafka
        offset is not committed, so the message is redelivered on next startup.
        """
        for attempt in range(1, 3):
            try:
                self._producer.produce(topic, value=json.dumps(payload).encode("utf-8"))
                self._producer.poll(0)
                return True
            except Exception as exc:
                logger.warning(
                    "clean_topic_publish_attempt_failed",
                    topic=topic,
                    attempt=attempt,
                    exc_type=type(exc).__name__,
                )
                if attempt < 2:
                    # Brief pause before retry — transient broker issues often
                    # resolve within a second without requiring a full reconnect.
                    await asyncio.sleep(PUBLISH_RETRY_DELAY_SECONDS)

        logger.error("clean_topic_publish_failed_after_retry", topic=topic)
        return False


# --- Module-level helper functions (no class state needed) ---

def _build_clean_message(
    log_entry: RawLogMessage,
    injection_result: DetectionResult,
    pii_result: DetectionResult,
) -> CleanLogMessage:
    """Construct CleanLogMessage from the original entry and both detection results."""
    return CleanLogMessage(
        service=log_entry.service,
        level=log_entry.level,
        # Use PII-sanitized text (which was run on injection-sanitized text).
        # This is the fully cleaned message that downstream consumers will see.
        message=pii_result.sanitized_message,
        trace_id=log_entry.trace_id,
        metadata=log_entry.metadata,
        timestamp=log_entry.timestamp,
        injection_attempted=injection_result.detected,
        pii_fields_redacted=pii_result.details.get("fields_redacted", []),
    )


def _build_parse_error(raw_message: Any, exc: ValidationError) -> dict[str, Any]:
    """Build a parse-error Kafka event dict for unprocessable messages.
    NOT a SecurityEvent Pydantic model: the DB security_events table has a
    CHECK constraint that only allows 'injection' or 'pii' as event_type.
    Parse errors are published to Kafka for observability but not stored in DB.
    """
    return {
        "event_id": str(uuid4()),
        "event_type": "parse_error",
        "service": "security-middleware",
        "details": {
            "error": str(exc),
            "raw_keys": list(raw_message.keys()) if isinstance(raw_message, dict) else [],
        },
        # .isoformat on a timezone-aware datetime includes the +00:00 offset.
        # never datetime.utcnow — always datetime.now(timezone.utc).
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
