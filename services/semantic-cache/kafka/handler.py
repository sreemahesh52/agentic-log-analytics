# --- Kafka consumer / producer handler for Semantic Cache ---
# Single Responsibility: this module handles ONLY Kafka I/O.
# Cache lookups are delegated to SemanticCache. Metrics updates are
# delegated to metrics.py. No Redis commands appear here.
# Routing logic:
#   HIT → publish enriched rca_result to agent.results (bypasses LLM)
#   MISS → forward incident with description to incidents.routed (model router)
# Dead Letter Queue pattern:
#   Schema-invalid messages: immediate DLQ (retrying a corrupt message is futile).
#   Kafka publish failures: tenacity retries + DLQ on exhaustion.
#   Cache errors: fail-open (cache miss), never send to DLQ for cache-only issues.

import json
from datetime import datetime, timezone
from typing import Any

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from tenacity import AsyncRetrying, RetryError, stop_after_attempt, wait_exponential

from cache import CacheResult, SemanticCache
from exceptions import KafkaPublishError, SchemaValidationError
from metrics import (
    CACHE_COST_SAVED_USD_TOTAL,
    CACHE_HIT_TOTAL,
    CACHE_MISS_TOTAL,
    CACHE_TOKENS_SAVED_TOTAL,
)

logger = structlog.get_logger()

# --- Required fields in every incidents.compressed message ---
# Absence of any of these makes the message unprocessable — goes to DLQ.
# severity is NOT required: it lives on individual alerts, not on incidents.
# The description builder uses .get("severity", "") so missing severity is fine.
_REQUIRED_FIELDS = frozenset({"incident_id", "tenant_id", "affected_services"})

# --- Retry backoff constants for Kafka publish (Circuit Breaker) ---
# Exponential backoff: 1s → 2s → 4s → give up. Jitter is implicit in tenacity.
_RETRY_MULTIPLIER = 1
_RETRY_MIN_WAIT_SECONDS = 1
_RETRY_MAX_WAIT_SECONDS = 4

# --- Token and cost constants for Prometheus (mirrors cache.py constants) ---
# Defined here (not imported from cache.py) to keep the handler independent.
_ESTIMATED_TOKENS_PER_RCA = 2000
_COST_PER_TOKEN_USD = 0.00001


class KafkaHandler:
    """Consumes incidents.compressed, routes to agent.results or incidents.routed.
    Why aiokafka? AIOKafkaConsumer/Producer are async-native (no thread pool),
    which fits naturally into the asyncio event loop used by this service.
    The consumer and producer are accepted as arguments (Dependency Inversion)
    so tests can pass AsyncMock objects without a real Kafka broker.
    """

    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        producer: AIOKafkaProducer,
        cache: SemanticCache,
        input_topic: str,
        output_hit_topic: str,
        output_miss_topic: str,
        dlq_topic: str,
        max_retries: int = 3,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._cache = cache
        self._input_topic = input_topic
        self._output_hit_topic = output_hit_topic
        self._output_miss_topic = output_miss_topic
        self._dlq_topic = dlq_topic
        self._max_retries = max_retries

    async def run(self) -> None:
        """Start the consumer loop. Runs until the consumer is stopped.
        Using consumer and producer as async context managers guarantees
        their connections are closed on exit, even if an exception escapes
        the inner loop. Without this, broker-side resources leak.
        """
        async with self._consumer, self._producer:
            logger.info(
                "semantic_cache_handler_started",
                input_topic=self._input_topic,
            )
            # aiokafka yields ConsumerRecord objects asynchronously.
            # Each record is one message from incidents.compressed.
            async for message in self._consumer:
                await self._process_message(message)

    async def _process_message(self, message: Any) -> None:
        """Process one incident message: cache lookup → route to hit or miss topic.
        Steps:
          1. Parse and validate the incident JSON payload.
          2. Build description string for embedding.
          3. SemanticCache.get — returns hit or miss.
          4. HIT: enrich result with cache_hit=True, publish to agent.results.
          5. MISS: attach description to payload, publish to incidents.routed.
          6. Commit Kafka offset after successful publish.
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
            # Schema errors cannot be fixed by retrying — go straight to DLQ.
            await self._send_to_dlq(
                original_payload=raw,
                failure_reason="schema_validation_error",
                retry_count=0,
                first_seen_at=first_seen_at,
            )
            # Commit so this bad message is never redelivered.
            await self._consumer.commit()
            return

        tenant_id = incident["tenant_id"]
        log = logger.bind(
            incident_id=incident.get("incident_id"),
            tenant_id=tenant_id,
        )

        # --- Step 2: build description for semantic lookup ---
        # Concatenate services + severity + first 200 chars of compressed context.
        # This matches what was stored in the cache by the eval harness's set call.
        services_str = " ".join(incident.get("affected_services", []))
        severity = incident.get("severity", "")
        context_preview = str(incident.get("compressed_context", ""))[:200]
        description = f"{services_str} {severity} {context_preview}".strip()

        # --- Step 3: semantic cache lookup ---
        # SemanticCache.get is fail-open — never raises; always returns CacheResult.
        cache_result: CacheResult = await self._cache.get(tenant_id, description)

        # --- Step 4/5: route based on cache decision ---
        if cache_result.hit:
            await self._handle_hit(incident, cache_result, log, raw, first_seen_at, tenant_id)
        else:
            await self._handle_miss(incident, description, log, raw, first_seen_at, tenant_id)

        # --- Step 6: commit offset ---
        # Manual commit: advance only after the publish succeeds (at-least-once).
        # If the process crashes between publish and commit, the message is
        # redelivered and deduplicated downstream — acceptable here.
        await self._consumer.commit()

    async def _handle_hit(
        self,
        incident: dict[str, Any],
        cache_result: CacheResult,
        log: Any,
        raw: bytes,
        first_seen_at: str,
        tenant_id: str,
    ) -> None:
        """Publish a cached RCA result directly to agent.results.
        cache_hit=True is injected into the rca_result so the eval harness
        knows this result came from cache and can track the hit rate.
        Prometheus counters are incremented regardless of publish outcome.
        """
        # Enrich the cached rca_result with cache provenance fields.
        rca_result = dict(cache_result.rca_result)  # type: ignore[arg-type]
        rca_result["cache_hit"] = True
        rca_result["incident_id"] = incident["incident_id"]
        rca_result["tenant_id"] = tenant_id

        payload = json.dumps(rca_result).encode("utf-8")
        try:
            await self._publish_with_retry(
                topic=self._output_hit_topic,
                key=tenant_id.encode("utf-8"),
                value=payload,
            )
            log.info(
                "cache_hit_published_to_agent_results",
                similarity_score=cache_result.similarity_score,
            )
        except KafkaPublishError:
            log.error(
                "cache_hit_publish_failed_sending_to_dlq",
                incident_id=incident.get("incident_id"),
            )
            await self._send_to_dlq(
                original_payload=raw,
                failure_reason="kafka_publish_error_hit",
                retry_count=self._max_retries,
                first_seen_at=first_seen_at,
            )

        # --- Prometheus metrics: count savings regardless of publish outcome ---
        CACHE_HIT_TOTAL.labels(tenant=tenant_id).inc()
        CACHE_TOKENS_SAVED_TOTAL.labels(tenant=tenant_id).inc(_ESTIMATED_TOKENS_PER_RCA)
        CACHE_COST_SAVED_USD_TOTAL.labels(tenant=tenant_id).inc(
            _ESTIMATED_TOKENS_PER_RCA * _COST_PER_TOKEN_USD
        )

    async def _handle_miss(
        self,
        incident: dict[str, Any],
        description: str,
        log: Any,
        raw: bytes,
        first_seen_at: str,
        tenant_id: str,
    ) -> None:
        """Forward the incident to incidents.routed for the model router.
        Attaches incident_description to the payload so downstream services
        (model router, RCA agent) can use it without re-computing the string.
        """
        # Enrich the original incident with the pre-computed description.
        # The model router and RCA agent use this for context.
        enriched = {**incident, "incident_description": description}
        payload = json.dumps(enriched).encode("utf-8")

        try:
            await self._publish_with_retry(
                topic=self._output_miss_topic,
                key=tenant_id.encode("utf-8"),
                value=payload,
            )
            log.info("cache_miss_forwarded_to_incidents_routed")
        except KafkaPublishError:
            log.error(
                "cache_miss_publish_failed_sending_to_dlq",
                incident_id=incident.get("incident_id"),
            )
            await self._send_to_dlq(
                original_payload=raw,
                failure_reason="kafka_publish_error_miss",
                retry_count=self._max_retries,
                first_seen_at=first_seen_at,
            )

        CACHE_MISS_TOTAL.labels(tenant=tenant_id).inc()

    async def _publish_with_retry(
        self, topic: str, key: bytes, value: bytes
    ) -> None:
        """Publish one message to Kafka with exponential-backoff retries.
        tenacity AsyncRetrying retries the coroutine body up to max_retries.
        On final failure, RetryError is caught and re-raised as KafkaPublishError
        so the caller can write to the DLQ (Circuit Breaker).
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
                f"Failed to publish to {topic} after {self._max_retries} retries"
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
          original_payload (decoded as string for readability).
        """
        last_attempt_at = datetime.now(timezone.utc).isoformat()
        dlq_payload = json.dumps(
            {
                "original_topic": self._input_topic,
                "failure_reason": failure_reason,
                "retry_count": retry_count,
                "first_seen_at": first_seen_at,
                "last_attempt_at": last_attempt_at,
                "original_payload": original_payload.decode("utf-8", errors="replace"),
            }
        ).encode("utf-8")

        try:
            await self._producer.send_and_wait(self._dlq_topic, value=dlq_payload)
            # Log at ERROR: DLQ write means data was not processed normally.
            logger.error(
                "message_sent_to_dlq",
                failure_reason=failure_reason,
                retry_count=retry_count,
            )
        except Exception as exc:
            # DLQ write itself failed: log at ERROR but never raise.
            # Crashing the consumer because the DLQ is down would make things worse.
            logger.error(
                "dlq_write_failed",
                error=str(exc),
                failure_reason=failure_reason,
            )


def _parse_incident(raw: bytes) -> dict[str, Any]:
    """Decode raw Kafka bytes and validate required fields.
    Raises SchemaValidationError for malformed JSON or missing required fields.
    Callers send schema-invalid messages directly to the DLQ — never retry.
    """
    try:
        data: dict[str, Any] = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise SchemaValidationError(f"JSON decode failed: {exc}") from exc

    missing = _REQUIRED_FIELDS - data.keys()
    if missing:
        raise SchemaValidationError(f"Missing required fields: {missing}")

    return data
