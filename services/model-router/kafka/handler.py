# --- Kafka consumer / producer handler for the Model Router ---
# Single Responsibility: this module handles ONLY Kafka I/O.
# Routing decisions are delegated to ModelRouter. DB queries go to TenantRepository.
# No SQL or routing logic appears in this file.
# Message flow:
#   incidents.routed
#       → parse + validate
#       → ModelRouter.route(tenant_id, severity)
#           → None (LOW + low_skip) : commit + discard
#           → RouterDecision : enrich payload → publish → incidents.ready
#       → on any error : DLQ → commit
# DLQ Pattern:
#   Every unprocessable message is written to logs.dlq with:
#     original_topic, failure_reason, retry_count,
#     first_seen_at (UTC), last_attempt_at (UTC), original_payload.
#   Never silently discard a message. Log every DLQ write at ERROR level.

import json
from datetime import datetime, timezone
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from tenacity import AsyncRetrying, RetryError, stop_after_attempt, wait_exponential

from exceptions import (
    DatabaseQueryError,
    KafkaPublishError,
    SchemaValidationError,
    TenantNotFoundError,
)
from metrics import MODEL_ROUTER_SELECTIONS_TOTAL
from router import ModelRouter, RouterDecision

logger = structlog.get_logger()

# --- Required fields in every incidents.routed message ---
# Absence of any of these makes the message unprocessable — goes to DLQ.
_REQUIRED_FIELDS = frozenset({"incident_id", "tenant_id", "affected_services"})

# --- Retry backoff constants (Circuit Breaker) ---
# Exponential backoff: 1 s → 2 s → 4 s → give up after max_retries attempts.
_RETRY_MULTIPLIER = 1
_RETRY_MIN_WAIT_SECONDS = 1
_RETRY_MAX_WAIT_SECONDS = 4

# --- Default severity when the field is absent from the incident payload ---
# incidents.routed payloads always contain severity (set by the anomaly agent),
# but older versions of the pipeline may omit it. MEDIUM is the safe default:
# it routes standard tenants to gpt-3.5-turbo and premium to gpt-3.5-turbo.
_DEFAULT_SEVERITY = "MEDIUM"


class KafkaHandler:
    """Consumes incidents.routed and publishes enriched incidents to incidents.ready.
    Accepts consumer, producer, and router as constructor arguments
    (Dependency Inversion — ). Tests inject AsyncMock objects;
    production injects real aiokafka clients. KafkaHandler never knows the
    difference.
    """

    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        producer: AIOKafkaProducer,
        router: ModelRouter,
        input_topic: str,
        output_topic: str,
        dlq_topic: str,
        max_retries: int = 3,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._router = router
        self._input_topic = input_topic
        self._output_topic = output_topic
        self._dlq_topic = dlq_topic
        self._max_retries = max_retries

    async def run(self) -> None:
        """Start the consumer loop. Runs until the consumer task is cancelled.
        Using consumer and producer as async context managers guarantees that
        their broker connections are closed cleanly when the coroutine exits,
        even if an unexpected exception escapes the inner loop. Without this,
        the broker holds connection resources open until its own TCP timeout.
        """
        async with self._consumer, self._producer:
            logger.info(
                "model_router_handler_started",
                input_topic=self._input_topic,
                output_topic=self._output_topic,
            )
            # aiokafka yields ConsumerRecord objects one at a time as messages
            # arrive from Kafka. Each record is one incident from incidents.routed.
            async for message in self._consumer:
                await self._process_message(message)

    async def _process_message(self, message: Any) -> None:
        """Process one incident: parse → route → publish to incidents.ready.
        Steps:
          1. Parse and validate the JSON payload — schema errors → DLQ immediately.
          2. Call ModelRouter.route to obtain a RouterDecision (or None).
          3a. None (LOW + low_skip): discard, commit offset.
          3b. RouterDecision: attach model_id + prompt_variant, publish, commit.
          4. On routing or publish failure: DLQ, commit.
        """
        # Record first_seen_at for the DLQ envelope — before any processing.
        # datetime.now(timezone.utc): never use datetime.utcnow.
        first_seen_at = datetime.now(timezone.utc).isoformat()
        raw: bytes = message.value

        # --- Step 1: parse and validate ---
        try:
            incident = _parse_incident(raw)
        except SchemaValidationError as exc:
            logger.error(
                "incident_schema_invalid_sending_to_dlq",
                error=str(exc),
                # Limit raw preview to avoid flooding the log with huge payloads.
                raw_preview=raw[:200].decode("utf-8", errors="replace"),
            )
            # Schema errors cannot be fixed by retrying — send directly to DLQ.
            await self._send_to_dlq(
                original_payload=raw,
                failure_reason="schema_validation_error",
                retry_count=0,
                first_seen_at=first_seen_at,
            )
            # Commit so this bad message is never redelivered to this consumer group.
            await self._consumer.commit()
            return

        tenant_id: str = incident["tenant_id"]
        # severity defaults to MEDIUM for backwards compatibility with older payloads.
        severity: str = incident.get("severity", _DEFAULT_SEVERITY).upper()

        # Bind tenant_id and severity to the logger so every subsequent log line
        # in this message's processing chain includes them automatically.
        log = logger.bind(
            incident_id=incident.get("incident_id"),
            tenant_id=tenant_id,
            severity=severity,
        )

        # --- Step 2: routing decision ---
        try:
            decision: RouterDecision | None = await self._router.route(
                tenant_id=tenant_id,
                severity=severity,
            )
        except TenantNotFoundError as exc:
            log.error("tenant_not_found_sending_to_dlq", error=str(exc))
            await self._send_to_dlq(
                original_payload=raw,
                failure_reason="tenant_not_found",
                retry_count=0,
                first_seen_at=first_seen_at,
            )
            await self._consumer.commit()
            return
        except DatabaseQueryError as exc:
            log.error("database_query_error_sending_to_dlq", error=str(exc))
            await self._send_to_dlq(
                original_payload=raw,
                failure_reason="database_query_error",
                retry_count=0,
                first_seen_at=first_seen_at,
            )
            await self._consumer.commit()
            return

        # --- Step 3a: discard LOW severity when low_skip is configured ---
        # decision is None only when severity=="LOW" and low_skip==True.
        # Commit the offset so this incident is never redelivered.
        if decision is None:
            log.info("incident_discarded_low_severity_skip_enabled")
            await self._consumer.commit()
            return

        # --- Step 3b: enrich payload and publish to incidents.ready ---
        # Spread the original incident and overlay the routing fields.
        # Downstream consumers (RCA Agent) read model_id and prompt_variant
        # directly from the payload — they never call the model router again.
        enriched: dict[str, Any] = {
            **incident,
            "model_id": decision.model_id,
            "prompt_variant": decision.prompt_variant,
            "routing_reason": decision.reason,
        }
        payload = json.dumps(enriched).encode("utf-8")

        try:
            await self._publish_with_retry(
                topic=self._output_topic,
                key=tenant_id.encode("utf-8"),
                value=payload,
            )
            log.info(
                "incident_routed_to_incidents_ready",
                model_id=decision.model_id,
                prompt_variant=decision.prompt_variant,
                routing_reason=decision.reason,
            )
        except KafkaPublishError as exc:
            log.error(
                "publish_to_incidents_ready_failed_sending_to_dlq",
                error=str(exc),
            )
            await self._send_to_dlq(
                original_payload=raw,
                failure_reason="kafka_publish_error",
                retry_count=self._max_retries,
                first_seen_at=first_seen_at,
            )
            await self._consumer.commit()
            return

        # --- Record Prometheus metric (only on successful publish) ---
        # Labels: model, severity, tenant — matches Grafana query variables.
        MODEL_ROUTER_SELECTIONS_TOTAL.labels(
            model=decision.model_id,
            severity=severity,
            tenant=tenant_id,
        ).inc()

        # --- Step 4: commit Kafka offset after successful publish ---
        # Manual commit (enable_auto_commit=False in main.py) gives at-least-once
        # semantics: if the process crashes after publish but before commit, the
        # message is redelivered and the publish is retried. The RCA Agent must
        # handle duplicate incident_ids gracefully as a result.
        await self._consumer.commit()

    async def _publish_with_retry(
        self, topic: str, key: bytes, value: bytes
    ) -> None:
        """Publish one message to Kafka with exponential-backoff retries.
        tenacity AsyncRetrying retries the inner coroutine body up to max_retries
        times. On final failure, RetryError is caught and re-raised as
        KafkaPublishError so the caller can route the message to the DLQ.
        Circuit Breaker pattern for all external calls.
        """
        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(self._max_retries),
                wait=wait_exponential(
                    multiplier=_RETRY_MULTIPLIER,
                    min=_RETRY_MIN_WAIT_SECONDS,
                    max=_RETRY_MAX_WAIT_SECONDS,
                ),
                reraise=False,
            ):
                with attempt:
                    await self._producer.send_and_wait(topic, key=key, value=value)
        except RetryError as exc:
            raise KafkaPublishError(
                f"Failed to publish to {topic!r} after {self._max_retries} retries"
            ) from exc

    async def _send_to_dlq(
        self,
        original_payload: bytes,
        failure_reason: str,
        retry_count: int,
        first_seen_at: str,
    ) -> None:
        """Write a failed message to the dead-letter queue.
        DLQ envelope contains all fields needed for replay:
          original_topic, failure_reason, retry_count,
          first_seen_at (UTC ISO 8601), last_attempt_at (UTC ISO 8601),
          original_payload (decoded to string for readability in Kafka UI).
        Logged at ERROR level — a DLQ write means this incident will NOT be
        processed by the RCA Agent without manual intervention.
        """
        last_attempt_at = datetime.now(timezone.utc).isoformat()
        dlq_envelope: dict[str, Any] = {
            "original_topic": self._input_topic,
            "failure_reason": failure_reason,
            "retry_count": retry_count,
            "first_seen_at": first_seen_at,
            "last_attempt_at": last_attempt_at,
            # Decode to string so the DLQ payload is human-readable in Kafka UI.
            # errors="replace" prevents UnicodeDecodeError on malformed bytes.
            "original_payload": original_payload.decode("utf-8", errors="replace"),
        }
        dlq_payload = json.dumps(dlq_envelope).encode("utf-8")

        try:
            await self._producer.send_and_wait(self._dlq_topic, value=dlq_payload)
            logger.error(
                "message_sent_to_dlq",
                failure_reason=failure_reason,
                retry_count=retry_count,
                original_topic=self._input_topic,
            )
        except Exception as exc:
            # DLQ write itself failed: log at ERROR but never raise.
            # Crashing the consumer because the DLQ is unavailable would make
            # things worse — we would lose the position and stop processing.
            logger.error(
                "dlq_write_failed_message_lost",
                error=str(exc),
                failure_reason=failure_reason,
                original_topic=self._input_topic,
            )


def _parse_incident(raw: bytes) -> dict[str, Any]:
    """Decode raw Kafka bytes and validate that required fields are present.
    Raises SchemaValidationError for malformed JSON or missing required fields.
    Callers must send schema-invalid messages directly to the DLQ — retrying
    a structurally corrupt payload will always fail.
    """
    try:
        data: dict[str, Any] = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise SchemaValidationError(f"JSON decode failed: {exc}") from exc

    missing = _REQUIRED_FIELDS - data.keys()
    if missing:
        raise SchemaValidationError(
            f"Incident payload missing required fields: {missing}"
        )

    return data
