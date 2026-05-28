# --- Kafka consumer / producer handler ---
# Single Responsibility: this module only handles Kafka I/O.
# It delegates correlation to AlertCorrelator and persistence to IncidentRepository.
# Neither of those classes knows about Kafka — Dependency Inversion in action.

import json
from datetime import datetime, timezone
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from tenacity import (
    AsyncRetrying,
    RetryError,
    stop_after_attempt,
    wait_exponential,
)

from correlator import AlertCorrelator
from exceptions import DatabaseWriteError, KafkaPublishError, SchemaValidationError
from metrics import CASCADE_TOTAL, SINGLE_TOTAL
from postgres.repository import IncidentRepository

logger = structlog.get_logger()

# Required fields that every alert message from the anomaly-agent must contain.
_REQUIRED_ALERT_FIELDS = {"alert_id", "tenant_id", "service", "anomaly_type", "severity"}

# Exponential backoff parameters for Kafka publish retries.
_RETRY_MULTIPLIER = 1
_RETRY_MIN_WAIT = 1   # seconds
_RETRY_MAX_WAIT = 8   # seconds


class KafkaHandler:
    """Consumes alerts, correlates them, persists incidents, publishes downstream.
    KafkaProducer and KafkaConsumer are interfaces in the aiokafka sense — we
    depend on the aiokafka abstractions, not concrete connection strings, so
    tests can substitute fakes (Interface Segregation / Dependency Inversion).
    Dead Letter Queue pattern:
      On unrecoverable Kafka publish failure, the original payload is written to
      the DLQ topic with structured metadata (failure_reason, retry_count,
      first_seen_at, last_attempt_at, original_payload). The message is never
      silently discarded.
    """

    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        producer: AIOKafkaProducer,
        correlator: AlertCorrelator,
        incident_repo: IncidentRepository,
        incidents_topic: str,
        dlq_topic: str,
        max_retries: int = 3,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._correlator = correlator
        self._incident_repo = incident_repo
        self._incidents_topic = incidents_topic
        self._dlq_topic = dlq_topic
        self._max_retries = max_retries

    async def run(self) -> None:
        """Start consuming alerts and processing them until stopped.
        The consumer and producer are used as async context managers so their
        connections are always cleanly closed even if an exception propagates.
        """
        async with self._consumer, self._producer:
            logger.info("kafka_handler_started")
            # Async iteration: aiokafka yields ConsumerRecord objects one by one.
            # Each record is a single Kafka message from the alerts topic.
            async for message in self._consumer:
                await self._process_message(message)

    async def _process_message(self, message: Any) -> None:
        """Process one Kafka message end-to-end.
        Steps:
          1. Parse and validate JSON payload.
          2. Correlate alert → Incident dict (pure in-memory, always succeeds).
          3. Persist Incident to PostgreSQL (best-effort — DB failure does not
             block Kafka publish; Kafka is the source of truth).
          4. Publish Incident to incidents topic (with retry + DLQ fallback).
          5. Commit the Kafka offset after the above steps so the message is not
             reprocessed on restart unless publish and DLQ both fail.
        """
        first_seen_at = datetime.now(timezone.utc).isoformat()
        raw: bytes = message.value

        # --- Step 1: parse and validate ---
        try:
            alert = self._parse_alert(raw)
        except SchemaValidationError as exc:
            logger.error(
                "alert_schema_invalid_sending_to_dlq",
                error=str(exc),
                raw_preview=raw[:200].decode("utf-8", errors="replace"),
            )
            await self._send_to_dlq(
                original_payload=raw,
                failure_reason="schema_validation_error",
                retry_count=0,
                first_seen_at=first_seen_at,
            )
            await self._consumer.commit()
            return

        log = logger.bind(
            alert_id=alert.get("alert_id"),
            tenant_id=alert.get("tenant_id"),
            service=alert.get("service"),
        )

        # --- Step 2: correlate (pure in-memory) ---
        incident = self._correlator.add_alert(alert)
        incident_type = "cascade" if incident["is_cascade"] else "single"
        log.info("incident_created", incident_type=incident_type)

        # --- Step 3: persist to PostgreSQL (best-effort) ---
        await self._save_incident_best_effort(incident, log)

        # --- Step 4: publish to incidents topic ---
        payload = json.dumps(incident).encode("utf-8")
        try:
            await self._publish_with_retry(
                topic=self._incidents_topic,
                key=incident["tenant_id"].encode("utf-8"),
                value=payload,
            )
        except KafkaPublishError:
            log.error(
                "incidents_publish_failed_sending_to_dlq",
                incident_id=incident["incident_id"],
            )
            await self._send_to_dlq(
                original_payload=raw,
                failure_reason="kafka_publish_error",
                retry_count=self._max_retries,
                first_seen_at=first_seen_at,
            )

        # Record Prometheus metrics regardless of publish outcome.
        tenant = alert.get("tenant_id", "unknown")
        if incident["is_cascade"]:
            CASCADE_TOTAL.labels(tenant=tenant).inc()
        else:
            SINGLE_TOTAL.labels(tenant=tenant).inc()

        # --- Step 5: commit Kafka offset ---
        # Manual commit: we only advance the offset after processing is complete
        # so a crash mid-processing re-delivers the message on restart.
        await self._consumer.commit()

    def _parse_alert(self, raw: bytes) -> dict[str, Any]:
        """Decode and validate a raw Kafka message bytes → alert dict.
        Raises SchemaValidationError for malformed JSON or missing required fields.
        A malformed message can never be fixed by retrying — it goes straight to DLQ.
        """
        try:
            data: dict[str, Any] = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise SchemaValidationError(f"JSON decode failed: {exc}") from exc

        missing = _REQUIRED_ALERT_FIELDS - data.keys()
        if missing:
            raise SchemaValidationError(f"Missing required fields: {missing}")

        return data

    async def _save_incident_best_effort(
        self, incident: dict[str, Any], log: Any
    ) -> None:
        """Persist incident to PostgreSQL; log ERROR on failure but do not raise.
        Kafka is the source of truth: the incident is published to the incidents
        topic regardless of whether the DB write succeeded. Downstream services
        (context-compressor, model-router, rca-agent) all consume from Kafka, not
        from the DB, so a transient DB outage does not stall the pipeline.
        """
        try:
            await self._incident_repo.save(incident)
        except DatabaseWriteError as exc:
            log.error(
                "incident_db_save_failed_continuing",
                error=str(exc),
                incident_id=incident["incident_id"],
            )

    async def _publish_with_retry(
        self, topic: str, key: bytes, value: bytes
    ) -> None:
        """Publish a message to a Kafka topic with exponential-backoff retries.
        tenacity AsyncRetrying retries the async callable up to max_retries times.
        On final failure, RetryError is caught and re-raised as KafkaPublishError
        so the caller can write to the DLQ.
        """
        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(self._max_retries),
                wait=wait_exponential(
                    multiplier=_RETRY_MULTIPLIER,
                    min=_RETRY_MIN_WAIT,
                    max=_RETRY_MAX_WAIT,
                ),
                reraise=False,
            ):
                with attempt:
                    await self._producer.send_and_wait(topic, key=key, value=value)
        except RetryError as exc:
            raise KafkaPublishError(
                f"Failed to publish to {topic} after {self._max_retries} retries"
            ) from exc

    async def _send_to_dlq(
        self,
        original_payload: bytes,
        failure_reason: str,
        retry_count: int,
        first_seen_at: str,
    ) -> None:
        """Write a failed message to the dead-letter queue topic.
        DLQ messages always contain: original_topic, failure_reason, retry_count,
        first_seen_at (UTC), last_attempt_at (UTC), original_payload.
        This structure allows operators to replay the message after fixing the
        root cause (Dead Letter Queue Pattern).
        """
        last_attempt_at = datetime.now(timezone.utc).isoformat()
        dlq_payload = json.dumps(
            {
                "original_topic": "alerts",
                "failure_reason": failure_reason,
                "retry_count": retry_count,
                "first_seen_at": first_seen_at,
                "last_attempt_at": last_attempt_at,
                # Decode best-effort for human readability in the DLQ viewer.
                "original_payload": original_payload.decode("utf-8", errors="replace"),
            }
        ).encode("utf-8")
        try:
            await self._producer.send_and_wait(self._dlq_topic, value=dlq_payload)
            logger.error(
                "message_sent_to_dlq",
                failure_reason=failure_reason,
                retry_count=retry_count,
            )
        except Exception as exc:
            # DLQ write failure: log at ERROR but do not raise — we must not
            # crash the consumer loop over an unwritable DLQ.
            logger.error(
                "dlq_write_failed",
                error=str(exc),
                failure_reason=failure_reason,
            )
