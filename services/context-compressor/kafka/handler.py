# --- Kafka consumer / producer handler ---
# Single Responsibility: this module only handles Kafka I/O.
# It delegates compression to ContextCompressor, log-fetching to LogRepository,
# and DB updates to IncidentRepository. None of those classes know about Kafka.
# That is Dependency Inversion: the handler depends on abstractions.
# Dead Letter Queue pattern:
#   Schema-invalid messages go straight to DLQ (can never be fixed by retrying).
#   Kafka publish failures go to DLQ after max_retries exhausted.
#   DB update failures are logged at ERROR but do NOT block the pipeline —
#   compression stats are for display only, not for pipeline correctness.

import json
from datetime import datetime, timezone
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from tenacity import AsyncRetrying, RetryError, stop_after_attempt, wait_exponential

from compressor import ContextCompressor
from exceptions import DatabaseWriteError, KafkaPublishError, SchemaValidationError
from metrics import COMPRESSION_RATIO, COMPRESSION_REQUESTS
from postgres.repository import IncidentRepository, LogRepository

logger = structlog.get_logger()

# Fields every incident message from the alert-correlator must contain.
_REQUIRED_INCIDENT_FIELDS = {"incident_id", "tenant_id", "affected_services"}

# Exponential backoff parameters for Kafka publish retries.
_RETRY_MULTIPLIER = 1
_RETRY_MIN_WAIT = 1   # seconds
_RETRY_MAX_WAIT = 8   # seconds


class KafkaHandler:
    """Consumes incidents, compresses log context, publishes enriched incidents.
    Interface note: AIOKafkaConsumer and AIOKafkaProducer are accepted as-is —
    aiokafka does not expose a formal interface type. In tests, these can be
    replaced with AsyncMock objects that satisfy the duck-typed contract
    (start, stop, __aenter__, __aexit__, __aiter__, send_and_wait).
    That is Interface Segregation: we only call the methods we need.
    """

    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        producer: AIOKafkaProducer,
        compressor: ContextCompressor,
        log_repo: LogRepository,
        incident_repo: IncidentRepository,
        incidents_compressed_topic: str,
        dlq_topic: str,
        max_retries: int = 3,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._compressor = compressor
        self._log_repo = log_repo
        self._incident_repo = incident_repo
        self._incidents_compressed_topic = incidents_compressed_topic
        self._dlq_topic = dlq_topic
        self._max_retries = max_retries

    async def run(self) -> None:
        """Start the consumer loop. Runs until the consumer is stopped externally.
        Using consumer and producer as async context managers ensures their
        connections are always closed cleanly, even if an exception propagates
        out of the loop.
        """
        async with self._consumer, self._producer:
            logger.info("kafka_handler_started")
            # aiokafka yields ConsumerRecord objects from the async iterator.
            # Each record is one Kafka message from the incidents topic.
            async for message in self._consumer:
                await self._process_message(message)

    async def _process_message(self, message: Any) -> None:
        """Process one incident message end-to-end.
        Steps:
          1. Parse and validate the incident JSON payload.
          2. Fetch recent logs for affected services from PostgreSQL.
          3. Compress the logs (fails-open on any error).
          4. Update the incident row in PostgreSQL with compression stats (best-effort).
          5. Enrich the incident payload and publish to incidents.compressed.
          6. Commit the Kafka offset after all steps.
        """
        # Record when we first saw this message for DLQ bookkeeping.
        first_seen_at = datetime.now(timezone.utc).isoformat()
        raw: bytes = message.value

        # --- Step 1: parse and validate ---
        try:
            incident = _parse_incident(raw)
        except SchemaValidationError as exc:
            logger.error(
                "incident_schema_invalid_sending_to_dlq",
                error=str(exc),
                raw_preview=raw[:200].decode("utf-8", errors="replace"),
            )
            await self._send_to_dlq(
                original_payload=raw,
                failure_reason="schema_validation_error",
                retry_count=0,
                first_seen_at=first_seen_at,
            )
            # Commit offset even on DLQ write so this bad message is not reprocessed.
            await self._consumer.commit()
            return

        log = logger.bind(
            incident_id=incident.get("incident_id"),
            tenant_id=incident.get("tenant_id"),
            affected_services=incident.get("affected_services"),
        )

        # --- Step 2: fetch logs (fail-open) ---
        logs = await self._fetch_logs_best_effort(incident, log)

        # --- Step 3: compress (fail-open — always returns a CompressionResult) ---
        result = await self._compressor.compress(
            tenant_id=incident["tenant_id"],
            affected_services=incident.get("affected_services", []),
            logs=logs,
        )

        # --- Step 4: update DB with compression stats (best-effort) ---
        await self._update_db_best_effort(incident, result, log)

        # --- Step 5: enrich payload and publish ---
        # Build enriched payload: original incident fields + compression metadata.
        enriched = {
            **incident,
            # compressed_context: what the RCA agent will receive as log context.
            "compressed_context": result.compressed_text,
            "compression_ratio": result.compression_ratio,
            "original_log_count": result.original_log_count,
            "was_compressed": result.was_compressed,
        }
        payload = json.dumps(enriched).encode("utf-8")

        try:
            await self._publish_with_retry(
                topic=self._incidents_compressed_topic,
                key=incident["tenant_id"].encode("utf-8"),
                value=payload,
            )
        except KafkaPublishError:
            log.error(
                "incidents_compressed_publish_failed_sending_to_dlq",
                incident_id=incident["incident_id"],
            )
            await self._send_to_dlq(
                original_payload=raw,
                failure_reason="kafka_publish_error",
                retry_count=self._max_retries,
                first_seen_at=first_seen_at,
            )

        # Record Prometheus metrics regardless of publish outcome.
        tenant = incident.get("tenant_id", "unknown")
        COMPRESSION_RATIO.labels(tenant=tenant).observe(result.compression_ratio)
        COMPRESSION_REQUESTS.labels(
            compressed=str(result.was_compressed).lower(),
            tenant=tenant,
        ).inc()

        # --- Step 6: commit Kafka offset ---
        # Manual commit: advance the offset only after processing completes.
        # A crash before this point causes the message to be redelivered on
        # restart — at-least-once delivery semantics (acceptable here since
        # compression is idempotent).
        await self._consumer.commit()
        log.info("incident_compressed_and_published", was_compressed=result.was_compressed)

    async def _fetch_logs_best_effort(
        self, incident: dict[str, Any], log: Any
    ) -> list[dict]:
        """Fetch recent logs for affected services; return empty list on failure.
        Fail-open: if PostgreSQL is unavailable or the query fails, we proceed
        with zero logs. The compressor will return was_compressed=False and
        compressed_text="" — the RCA agent gets no context, which is still
        better than the pipeline stalling.
        """
        try:
            return await self._log_repo.fetch_recent_logs(
                tenant_id=incident["tenant_id"],
                services=incident.get("affected_services", []),
            )
        except Exception as exc:
            log.error(
                "log_fetch_failed_proceeding_without_context",
                error=str(exc),
                incident_id=incident.get("incident_id"),
            )
            return []

    async def _update_db_best_effort(
        self, incident: dict[str, Any], result: object, log: Any
    ) -> None:
        """Write compression stats to the incidents table; log on failure but do not raise.
        Why best-effort?
          The DB update is for UI display (AlertDrawer compression stats).
          It does not affect pipeline correctness. A transient DB failure
          should not block the Kafka message from flowing downstream.
        """
        try:
            await self._incident_repo.update_compression_stats(
                incident_id=incident["incident_id"],
                compression_ratio=result.compression_ratio,
                original_log_count=result.original_log_count,
                was_compressed=result.was_compressed,
            )
        except DatabaseWriteError as exc:
            log.error(
                "compression_stats_db_update_failed_continuing",
                error=str(exc),
                incident_id=incident.get("incident_id"),
            )

    async def _publish_with_retry(
        self, topic: str, key: bytes, value: bytes
    ) -> None:
        """Publish a message to Kafka with exponential-backoff retries.
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
        DLQ envelope:
          original_topic, failure_reason, retry_count,
          first_seen_at (UTC ISO), last_attempt_at (UTC ISO), original_payload.
        This structure allows operators to replay the message after fixing the
        root cause.
        """
        last_attempt_at = datetime.now(timezone.utc).isoformat()
        dlq_payload = json.dumps(
            {
                "original_topic": "incidents",
                "failure_reason": failure_reason,
                "retry_count": retry_count,
                "first_seen_at": first_seen_at,
                "last_attempt_at": last_attempt_at,
                "original_payload": original_payload.decode("utf-8", errors="replace"),
            }
        ).encode("utf-8")
        try:
            await self._producer.send_and_wait(self._dlq_topic, value=dlq_payload)
            # Log at ERROR: a DLQ write means data was not processed normally.
            logger.error(
                "message_sent_to_dlq",
                failure_reason=failure_reason,
                retry_count=retry_count,
            )
        except Exception as exc:
            # DLQ write itself failed: log at ERROR but never raise — we must
            # not crash the consumer loop because the DLQ is unavailable.
            logger.error(
                "dlq_write_failed",
                error=str(exc),
                failure_reason=failure_reason,
            )


def _parse_incident(raw: bytes) -> dict[str, Any]:
    """Decode and validate raw Kafka bytes → incident dict.
    Raises SchemaValidationError for malformed JSON or missing required fields.
    A schema-invalid message can never be fixed by retrying — caller sends to DLQ.
    """
    try:
        data: dict[str, Any] = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise SchemaValidationError(f"JSON decode failed: {exc}") from exc

    missing = _REQUIRED_INCIDENT_FIELDS - data.keys()
    if missing:
        raise SchemaValidationError(f"Missing required fields: {missing}")

    return data
