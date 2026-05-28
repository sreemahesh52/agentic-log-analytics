"""
KafkaIncidentHandler — Observer pattern Kafka consumer for incidents.ready.
Architecture:
  KafkaIncidentHandler is the orchestrator: it polls incidents.ready, validates
  the payload, runs the RCA Agent, and then fans out the result to all registered
  observers. Each observer independently handles one concern (save to DB, publish
  to agent.results) without knowing about the others.
Observer Pattern:
  RCAEventObserver — abstract base class with one method: on_rca_complete(event).
  KafkaResultPublisher — publishes RCAResult to agent.results topic.
  PostgresResultSaver — persists RCAResult via RCARepository.
  Observers are registered via register_observer. Adding a new concern (e.g.
  SlackNotifier) requires zero changes to KafkaIncidentHandler — just instantiate
  and register. No if/elif chains based on concern type.
DLQ (Dead Letter Queue):
  On any failure, the message is published to rca.dlq with:
    failure_reason, retry_count, first_seen_at, last_attempt_at, original_payload
  The DLQ message is structured JSON so a future DLQ consumer can deserialise
  it without string parsing.
  Routing rules:
    SchemaValidationError → DLQ immediately, no retry.
    LowConfidenceError → DLQ, failure_reason='low_confidence'.
    openai.RateLimitError → tenacity retries (max 3, exponential backoff + jitter).
    Any other Exception → DLQ with failure_reason='unexpected_error'.
Tenacity retries:
  Only openai.RateLimitError is retried. Retrying on other errors (network,
  schema) would re-send identical requests that always fail, wasting tokens and
  blocking the consumer. RateLimitError is the one case where waiting and
  retrying is the correct strategy (the limit resets after a short window).
Async + confluent_kafka:
  confluent_kafka's Consumer.poll is a BLOCKING call from a C extension.
  Running it directly in an async function would block the asyncio event loop,
  preventing all other coroutines from running while waiting for the next message.
  Solution: run consumer.poll via loop.run_in_executor(None, ...) which
  executes it in a thread pool, yielding control back to the event loop while
  waiting. The result is awaitable — the async consumer loop can yield between
  every poll call.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import asyncpg
import structlog
from confluent_kafka import Consumer, KafkaError, Producer
from pydantic import ValidationError
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from exceptions import LowConfidenceError, SchemaValidationError
from metrics import RCAMetrics
from models import IncidentPayload, RCAResult
from streaming import KafkaStreamingCallback

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# RCACompleteEvent — data carrier for the observer fanout.
# ---------------------------------------------------------------------------


@dataclass
class RCACompleteEvent:
    """Carries the completed RCAResult to all registered observers.
    Why a dataclass instead of passing RCAResult directly to observers?
    The event may carry additional context beyond the result itself — e.g.
    the original Kafka message offset for commit acknowledgement. Using a
    dataclass keeps the observer interface stable as context fields are added.
    """

    result: RCAResult
    tenant_id: str
    incident_id: str


# ---------------------------------------------------------------------------
# RCAEventObserver — Abstract Base Class for the Observer pattern.
# ---------------------------------------------------------------------------


class RCAEventObserver(ABC):
    """Abstract base for all post-RCA side effects.
    Every observer implements exactly one method: on_rca_complete.
    The orchestrator calls each observer in sequence after the agent finishes.
    Observers run independently — one observer's failure does not prevent others.
    """

    @abstractmethod
    async def on_rca_complete(self, event: RCACompleteEvent) -> None:
        """Handle a completed RCA investigation event.
        Args:
            event: RCACompleteEvent containing the validated RCAResult
                   and investigation context.
        """
        ...


# ---------------------------------------------------------------------------
# KafkaResultPublisher — publishes RCAResult to agent.results.
# ---------------------------------------------------------------------------


class KafkaResultPublisher(RCAEventObserver):
    """Publishes the RCAResult to the agent.results Kafka topic.
    Downstream consumers (evaluation harness, alerting pipeline) read from
    agent.results to process completed investigations. Publishing here keeps
    the Kafka topology intact: incidents flow in, results flow out.
    Why not publish from the handler directly?
    Single Responsibility: the handler orchestrates; publishing is one concern.
    Using the Observer pattern means this class can be tested in isolation
    with a mock producer, verifying the exact bytes written.
    """

    RESULTS_TOPIC = "agent.results"

    def __init__(self, producer: Producer) -> None:
        """Inject the shared confluent_kafka Producer.
        Args:
            producer: Shared Producer instance. confluent_kafka Producer is
                      thread-safe and reused across all concurrent investigations.
        """
        self._producer = producer

    async def on_rca_complete(self, event: RCACompleteEvent) -> None:
        """Publish the serialised RCAResult to agent.results.
        Uses model_dump_json for Pydantic v2 JSON serialisation — it handles
        nested models (ReasoningStep) and datetime formatting automatically.
        The key is tenant_id so all results for one tenant share a partition.
        Failures are logged and swallowed — the primary concern (DB persistence
        via PostgresResultSaver) must not be blocked by a Kafka publish failure.
        """
        try:
            payload = event.result.model_dump_json()

            self._producer.produce(
                topic=self.RESULTS_TOPIC,
                key=event.tenant_id,
                value=payload,
            )
            # poll(0) drains delivery callbacks without blocking the event loop.
            self._producer.poll(0)

            log.info(
                "rca_result_published",
                rca_id=event.result.rca_id,
                topic=self.RESULTS_TOPIC,
                tenant_id=event.tenant_id,
            )

        except Exception as exc:
            # Swallow: a Kafka publish failure is recoverable. The DB row exists
            # and the UI can read it. The evaluation harness can replay from DB.
            log.error(
                "rca_result_publish_failed",
                rca_id=event.result.rca_id,
                topic=self.RESULTS_TOPIC,
                error=str(exc),
            )


# ---------------------------------------------------------------------------
# PostgresResultSaver — persists RCAResult via the Repository Pattern.
# ---------------------------------------------------------------------------


class PostgresResultSaver(RCAEventObserver):
    """Persists the RCAResult to rca_results via RCARepository.
    The primary concern of the observer fanout: if this observer fails, the
    investigation result is lost. All other observers are secondary.
    Failures are re-raised so the handler can route to DLQ. This is the only
    observer that can cause DLQ routing on post-agent fanout failure.
    """

    def __init__(self, repository: Any) -> None:
        """Inject the RCARepository.
        Args:
            repository: RCARepository instance created at startup.
        """
        self._repo = repository

    async def on_rca_complete(self, event: RCACompleteEvent) -> None:
        """Persist the RCAResult to PostgreSQL via UPSERT.
        Raises:
            RuntimeError: propagated from RCARepository.save on DB failure.
                          The handler catches this and routes to DLQ.
        """
        # UPSERT handles both the trigger-path (placeholder row exists) and
        # the normal path (no existing row). See repository.py for details.
        await self._repo.save(event.result)

        log.info(
            "rca_result_persisted",
            rca_id=event.result.rca_id,
            tenant_id=event.tenant_id,
            status=event.result.status,
        )


# ---------------------------------------------------------------------------
# KafkaIncidentHandler — the main orchestrator.
# ---------------------------------------------------------------------------


class KafkaIncidentHandler:
    """Consumes incidents.ready, runs the RCA Agent, fans out via observers.
    Lifecycle:
      1. consume_loop polls incidents.ready indefinitely.
      2. Each message is validated as IncidentPayload via Pydantic.
      3. RCAAgent.run executes the ReAct loop.
      4. On success: all observers are called in registration order.
      5. On failure: the message is published to rca.dlq.
    Tenancy:
      Every Kafka message carries tenant_id in the payload. The handler passes
      this through to the agent and all observers. No global tenant state exists.
    Graceful shutdown:
      stop sets a flag that causes consume_loop to exit cleanly after the
      current message finishes processing. consumer.close is called to commit
      offsets and disconnect from the broker.
    """

    INCIDENTS_TOPIC = "incidents.ready"
    DLQ_TOPIC = "rca.dlq"

    def __init__(
        self,
        consumer: Consumer,
        producer: Producer,
        agent_factory: Any,
        stream_producer: Producer,
        group_id: str = "rca-agent-group",
        metrics: RCAMetrics | None = None,
        db_pool: asyncpg.Pool | None = None,
    ) -> None:
        """Inject all dependencies.
        Args:
            consumer: confluent_kafka Consumer subscribed to incidents.ready.
            producer: confluent_kafka Producer for agent.results and rca.dlq.
            agent_factory: Callable(tenant_id, rca_id) → RCAAgent with tools registered.
                            Factory pattern: each investigation gets a fresh agent instance
                            with tools bound to the correct tenant_id.
            stream_producer: Separate confluent_kafka Producer for rca.stream.
                             Kept separate from the main producer so streaming failures
                             never interfere with results publishing.
            group_id: Consumer group ID for Kafka offset tracking.
            metrics: RCAMetrics dataclass with all Prometheus counters/gauges.
                            Optional so tests can construct the handler without a
                            live Prometheus registry — Dependency Inversion in practice.
            db_pool: asyncpg pool for tenant name lookups. When provided, metric
                     labels use the human-readable tenant name (e.g. "acme-corp")
                     so Grafana variables match seed data. Defaults to None (tests).
        """
        self._consumer = consumer
        self._producer = producer
        self._agent_factory = agent_factory
        self._stream_producer = stream_producer
        self._observers: list[RCAEventObserver] = []
        self._running = False
        # None is the safe default — _record_success and _record_failure skip
        # all prometheus_client calls when metrics is None (test isolation).
        self._metrics = metrics
        self._db_pool = db_pool
        # Cache UUID→name so each unique tenant triggers at most one DB query.
        self._tenant_name_cache: dict[str, str] = {}

    async def _resolve_tenant_label(self, tenant_id: str) -> str:
        """Return the human-readable tenant name for a UUID, or the UUID itself.
        Result is cached so each unique tenant_id triggers at most one DB query
        for the lifetime of the process. Falls back to the UUID if the pool is
        unavailable or the row does not exist.
        """
        if tenant_id in self._tenant_name_cache:
            return self._tenant_name_cache[tenant_id]
        if self._db_pool is not None:
            try:
                async with self._db_pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT name FROM tenants WHERE tenant_id = $1::uuid",
                        tenant_id,
                    )
                if row:
                    self._tenant_name_cache[tenant_id] = row["name"]
                    return row["name"]
            except Exception:
                pass
        self._tenant_name_cache[tenant_id] = tenant_id
        return tenant_id

    def register_observer(self, observer: RCAEventObserver) -> None:
        """Add an observer to the fanout chain.
        Observers are called in registration order. Order matters: register
        PostgresResultSaver first so the DB row exists when KafkaResultPublisher
        publishes and downstream consumers immediately see the result.
        Args:
            observer: Any RCAEventObserver subclass.
        """
        self._observers.append(observer)
        log.debug("observer_registered", observer_type=type(observer).__name__)

    def stop(self) -> None:
        """Signal the consume loop to exit after the current message."""
        self._running = False

    async def consume_loop(self) -> None:
        """Poll incidents.ready indefinitely, processing each message.
        Uses run_in_executor to call the blocking consumer.poll without
        blocking the asyncio event loop. This allows the event loop to continue
        serving other coroutines (e.g. health check HTTP server) while waiting
        for the next Kafka message.
        The loop exits when stop is called (sets _running=False) or when
        an unhandled exception propagates to main.py's signal handler.
        """
        self._running = True
        loop = asyncio.get_event_loop()

        log.info("kafka_consume_loop_started", topic=self.INCIDENTS_TOPIC)

        while self._running:
            # run_in_executor runs consumer.poll(1.0) in a thread pool so the
            # 1-second timeout does not block the asyncio event loop.
            # poll(1.0): wait up to 1 second for a message before returning None.
            msg = await loop.run_in_executor(None, self._consumer.poll, 1.0)

            if msg is None:
                # No message within the 1-second timeout — continue polling.
                continue

            if msg.error():
                # KafkaError.EOF signals the end of a partition (not an error in
                # normal operation). All other errors are logged at WARNING.
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.debug("kafka_partition_eof")
                else:
                    log.warning(
                        "kafka_consumer_error",
                        error=str(msg.error()),
                    )
                continue

            # --- Decode and process the message ---
            raw_value = msg.value()
            if raw_value is None:
                log.warning("kafka_empty_message")
                continue

            await self._process_message(raw_value.decode("utf-8"))

        # --- Graceful shutdown ---
        # close commits the current offsets and disconnects from the broker.
        # Without this, the broker holds the partition assignment for the full
        # session timeout (default 10 minutes), blocking re-assignment to a new consumer.
        self._consumer.close()
        log.info("kafka_consume_loop_stopped")

    async def _process_message(self, raw: str) -> None:
        """Parse one incidents.ready message and run the agent.
        Validates the payload, constructs a per-investigation streaming callback,
        runs the RCA agent with tenacity retries on RateLimitError, then fans out
        the result to all observers. Any failure routes to the DLQ.
        Args:
            raw: UTF-8 decoded Kafka message value.
        """
        first_seen_at = datetime.now(timezone.utc).isoformat()

        # --- Step 1: Validate payload ---
        try:
            payload_dict = json.loads(raw)
            incident = IncidentPayload.model_validate(payload_dict)
        except (json.JSONDecodeError, ValidationError) as exc:
            # Schema errors are not retryable — the same malformed JSON will
            # fail validation on every attempt. Route directly to DLQ.
            log.error(
                "incident_payload_invalid",
                error=str(exc),
                raw_preview=raw[:200],
            )
            await self._publish_dlq(
                original_payload=raw,
                failure_reason="schema_validation_error",
                retry_count=0,
                first_seen_at=first_seen_at,
            )
            return

        # Use rca_id_hint if provided by the trigger endpoint; otherwise generate fresh.
        # This preserves the rca_id the UI is already polling when triggered manually.
        rca_id = incident.rca_id_hint or str(uuid.uuid4())

        # Resolve the human-readable tenant name for metric labels so Grafana
        # variables (label_values(metric, tenant)) show "acme-corp" rather than UUIDs.
        tenant_label = await self._resolve_tenant_label(incident.tenant_id)

        log.info(
            "incident_processing_start",
            incident_id=incident.incident_id,
            tenant_id=incident.tenant_id,
            rca_id=rca_id,
            severity=incident.severity,
            is_cascade=incident.is_cascade,
        )

        # --- Step 2: Build per-investigation streaming callback ---
        stream_callback = KafkaStreamingCallback(
            producer=self._stream_producer,
            tenant_id=incident.tenant_id,
            rca_id=rca_id,
        )

        # --- Step 3: Create agent via factory (pre-binds tenant_id to all tools) ---
        agent = self._agent_factory(incident.tenant_id, rca_id)
        agent.set_stream_callback(stream_callback)

        # --- Step 4: Run agent with tenacity retry on RateLimitError ---
        start_ms = int(time.monotonic() * 1000)
        result: RCAResult | None = None

        try:
            result = await self._run_with_retry(agent, incident, rca_id)

        except SchemaValidationError as exc:
            # Non-retryable: LLM consistently returned bad JSON.
            log.error(
                "rca_schema_validation_failed",
                rca_id=rca_id,
                incident_id=incident.incident_id,
                error=str(exc),
                context=exc.context,
            )
            await self._publish_dlq(
                original_payload=raw,
                failure_reason="schema_validation_error",
                retry_count=0,
                first_seen_at=first_seen_at,
                rca_id=rca_id,
            )
            # Persist failure row so the UI shows "Investigation Failed".
            await self._save_failure(
                rca_id=rca_id,
                incident=incident,
                failure_reason="schema_validation_error",
            )
            self._record_failure(
                failure_reason="schema_validation_error",
                model=incident.model_id,
                tenant_label=tenant_label,
            )
            return

        except LowConfidenceError as exc:
            log.error(
                "rca_low_confidence",
                rca_id=rca_id,
                incident_id=incident.incident_id,
                final_confidence=exc.final_confidence,
                iterations=exc.iterations,
            )
            await self._publish_dlq(
                original_payload=raw,
                failure_reason="low_confidence",
                retry_count=0,
                first_seen_at=first_seen_at,
                rca_id=rca_id,
            )
            await self._save_failure(
                rca_id=rca_id,
                incident=incident,
                failure_reason="low_confidence",
            )
            self._record_failure(
                failure_reason="low_confidence",
                model=incident.model_id,
                tenant_label=tenant_label,
            )
            return

        except RetryError as exc:
            # tenacity exhausted all retry attempts on RateLimitError.
            log.error(
                "rca_rate_limit_retry_exhausted",
                rca_id=rca_id,
                incident_id=incident.incident_id,
                error=str(exc),
            )
            await self._publish_dlq(
                original_payload=raw,
                failure_reason="api_error",
                retry_count=3,
                first_seen_at=first_seen_at,
                rca_id=rca_id,
            )
            await self._save_failure(
                rca_id=rca_id,
                incident=incident,
                failure_reason="api_error",
            )
            self._record_failure(
                failure_reason="api_error",
                model=incident.model_id,
                tenant_label=tenant_label,
            )
            return

        except Exception as exc:
            log.error(
                "rca_unexpected_error",
                rca_id=rca_id,
                incident_id=incident.incident_id,
                exc_type=type(exc).__name__,
                error=str(exc),
            )
            await self._publish_dlq(
                original_payload=raw,
                failure_reason="unexpected_error",
                retry_count=0,
                first_seen_at=first_seen_at,
                rca_id=rca_id,
            )
            await self._save_failure(
                rca_id=rca_id,
                incident=incident,
                failure_reason="unexpected_error",
            )
            self._record_failure(
                failure_reason="unexpected_error",
                model=incident.model_id,
                tenant_label=tenant_label,
            )
            return

        # --- Step 5: Override rca_id to use the pre-assigned value ---
        # RCAAgent.run generates its own rca_id via default_factory.
        # We replace it with our rca_id so the trigger-endpoint placeholder row
        # is overwritten by the UPSERT in PostgresResultSaver.
        # model_copy(update=...) returns a new instance (Pydantic v2 is immutable).
        result = result.model_copy(update={"rca_id": rca_id})

        # --- Step 6: Publish streaming complete sentinel ---
        stream_callback.publish_complete(
            root_cause=result.root_cause,
            confidence=result.confidence,
            recommendations=result.recommendations,
        )

        # --- Step 7: Fan out to all observers ---
        event = RCACompleteEvent(
            result=result,
            tenant_id=incident.tenant_id,
            incident_id=incident.incident_id,
        )

        for observer in self._observers:
            try:
                await observer.on_rca_complete(event)
            except Exception as exc:
                # One observer failing must not prevent others from running.
                # PostgresResultSaver is registered first, so DB persistence
                # completes before KafkaResultPublisher publishes.
                log.error(
                    "observer_failed",
                    observer_type=type(observer).__name__,
                    rca_id=rca_id,
                    error=str(exc),
                )

        elapsed_ms = int(time.monotonic() * 1000) - start_ms
        log.info(
            "incident_processing_complete",
            rca_id=rca_id,
            incident_id=incident.incident_id,
            tenant_id=incident.tenant_id,
            elapsed_ms=elapsed_ms,
            confidence=result.confidence,
        )

        # --- Record success metrics ---
        # Only recorded here (success path). Failure paths call _record_failure.
        self._record_success(result=result, tenant_label=tenant_label)

    def _record_success(self, result: RCAResult, tenant_label: str) -> None:
        """Increment Prometheus counters/gauges for a successful RCA investigation.
        Called at the end of _process_message after all observers complete.
        Skipped silently when self._metrics is None (test isolation path).
        Args:
            result: The completed RCAResult with model, tokens, and latency fields.
            tenant_label: Human-readable tenant name (e.g. "acme-corp") resolved
                          from the UUID by _resolve_tenant_label.
        """
        # Guard: metrics is None in test environments that did not inject it.
        if self._metrics is None:
            return

        model = result.model_used or "unknown"

        # Counter: total investigations by status=success, model, and tenant.
        self._metrics.investigations_total.labels(
            status="success",
            model=model,
            tenant=tenant_label,
        ).inc()

        # Gauge: overwrite with the latest confidence value for this tenant.
        # set replaces the previous value; this is NOT a cumulative counter.
        self._metrics.confidence_score.labels(tenant=tenant_label).set(result.confidence)

        # Counter: input and output tokens attributed to this tenant + model.
        # Separate type labels allow cost formulas: input_price != output_price.
        if result.input_tokens > 0:
            self._metrics.llm_tokens_total.labels(
                type="input",
                model=model,
                tenant=tenant_label,
            ).inc(result.input_tokens)
        if result.output_tokens > 0:
            self._metrics.llm_tokens_total.labels(
                type="output",
                model=model,
                tenant=tenant_label,
            ).inc(result.output_tokens)

        # Histogram: observe llm_latency in seconds (field is milliseconds).
        # Division by 1000 converts ms → s to match the histogram bucket units.
        if result.llm_latency_ms > 0:
            self._metrics.llm_latency_seconds.labels(tenant=tenant_label).observe(
                result.llm_latency_ms / 1000.0
            )

        # Histogram: observe total tool latency in seconds.
        # tool="all" is an aggregated label — per-tool tracking would require
        # instrumenting each tool function individually in tools/*.py.
        if result.tool_latency_ms > 0:
            self._metrics.tool_latency_seconds.labels(
                tool="all",
                tenant=tenant_label,
            ).observe(result.tool_latency_ms / 1000.0)

        # Counter: per-tool call counts derived from reasoning_steps.
        # Each step.action is the tool name the LLM invoked in that iteration.
        # This drives the "Tool Call Frequency" barchart (Grafana Panel 28).
        for step in result.reasoning_steps:
            if step.action:
                self._metrics.tool_calls_total.labels(
                    tool=step.action,
                    tenant=tenant_label,
                ).inc()

    def _record_failure(
        self,
        failure_reason: str,
        model: str,
        tenant_label: str,
    ) -> None:
        """Increment failure counters after a DLQ routing event.
        Called in each exception handler inside _process_message.
        Skipped silently when self._metrics is None (test isolation path).
        Args:
            failure_reason: The DLQ failure_reason string (one of the four constants).
            model: Model ID from IncidentPayload (may be empty on parse errors).
            tenant_label: Human-readable tenant name resolved by _resolve_tenant_label.
        """
        if self._metrics is None:
            return

        # investigations_total{status="failed"}: count failed investigations separately
        # from successes so Grafana Panel 5 shows success vs failed side by side.
        self._metrics.investigations_total.labels(
            status="failed",
            model=model or "unknown",
            tenant=tenant_label,
        ).inc()

        # failure_reason_total: fine-grained breakdown for Grafana Panel 6.
        # Each unique reason maps to a separate time series, enabling root cause
        # analysis of WHICH failure mode is most common.
        self._metrics.failure_reason_total.labels(
            reason=failure_reason,
            tenant=tenant_label,
        ).inc()

    async def _run_with_retry(
        self,
        agent: Any,
        incident: IncidentPayload,
        rca_id: str,
    ) -> RCAResult:
        """Run the agent with tenacity retries on openai.RateLimitError only.
        Why only retry RateLimitError?
        RateLimitError means the OpenAI API quota was temporarily exhausted —
        waiting and retrying is the correct resolution. Retrying on other errors
        (SchemaValidationError, network errors) would re-send the same failing
        request, wasting tokens and blocking the consumer.
        Tenacity configuration:
          stop_after_attempt(3): at most 3 total attempts (1 original + 2 retries).
          wait_exponential_jitter(initial=2, max=30): starts at ~2 seconds, doubles,
            with random jitter to prevent thundering herd if multiple agents hit
            the rate limit simultaneously.
          retry_if_exception_type(openai.RateLimitError): only catches RateLimitError.
            All other exceptions propagate immediately to the caller.
        Returns:
            RCAResult from a successful agent run.
        Raises:
            tenacity.RetryError: after 3 failed attempts.
            SchemaValidationError, LowConfidenceError: propagated immediately (not retried).
            Any other exception: propagated immediately.
        """
        # Import openai here to keep the module importable even if openai is not
        # installed in test environments (tests mock the agent, not the import).
        try:
            import openai
            rate_limit_error_type = openai.RateLimitError
        except ImportError:
            # Fallback for test environments without openai installed.
            rate_limit_error_type = Exception  # type: ignore[assignment]

        @retry(
            retry=retry_if_exception_type(rate_limit_error_type),
            stop=stop_after_attempt(3),
            wait=wait_exponential_jitter(initial=2, max=30),
            reraise=False,  # reraise=False: raises tenacity.RetryError on exhaustion.
        )
        async def _attempt() -> RCAResult:
            return await agent.run(incident)

        return await _attempt()

    async def _save_failure(
        self,
        rca_id: str,
        incident: IncidentPayload,
        failure_reason: str,
    ) -> None:
        """Persist a failed RCAResult row so the UI shows 'Investigation Failed'.
        Creates a minimal RCAResult with status='failed' and the given failure_reason.
        Uses RCAResult.model_copy to get a valid object, then delegates to the
        first PostgresResultSaver observer.
        Errors during failure persistence are logged but not re-raised — the DLQ
        message already records the failure, and losing the UI update is acceptable.
        """
        failure_result = RCAResult(
            rca_id=rca_id,
            tenant_id=incident.tenant_id,
            incident_id=incident.incident_id,
            root_cause="Investigation failed — see failure_reason for details.",
            confidence=0.0,
            recommendations=["Check the DLQ for failure details."],
            model_used=incident.model_id,
            prompt_version=incident.prompt_variant,
            status="failed",
            failure_reason=failure_reason,
        )

        for observer in self._observers:
            if isinstance(observer, PostgresResultSaver):
                try:
                    event = RCACompleteEvent(
                        result=failure_result,
                        tenant_id=incident.tenant_id,
                        incident_id=incident.incident_id,
                    )
                    await observer.on_rca_complete(event)
                except Exception as exc:
                    log.error(
                        "failure_result_save_error",
                        rca_id=rca_id,
                        error=str(exc),
                    )
                break

    async def _publish_dlq(
        self,
        original_payload: str,
        failure_reason: str,
        retry_count: int,
        first_seen_at: str,
        rca_id: str | None = None,
    ) -> None:
        """Publish a failed message to rca.dlq with structured metadata.
        DLQ message format:
          {
            "rca_id": str | null,
            "failure_reason": str,
            "retry_count": int,
            "first_seen_at": "ISO 8601 UTC",
            "last_attempt_at": "ISO 8601 UTC",
            "original_payload": str (raw Kafka value, possibly truncated)
          }
        original_payload is truncated to 5000 chars to prevent DLQ messages from
        exceeding Kafka's default max.message.bytes (1 MB). Full payloads are
        recoverable from the original topic offsets.
        Args:
            original_payload: Raw Kafka message value (UTF-8 string).
            failure_reason: One of: 'schema_validation_error', 'low_confidence',
                              'api_error', 'unexpected_error'.
            retry_count: Number of retries attempted before DLQ routing.
            first_seen_at: ISO 8601 UTC timestamp of the first processing attempt.
            rca_id: Pre-assigned rca_id if available, else None.
        """
        dlq_message = {
            "rca_id": rca_id,
            "failure_reason": failure_reason,
            "retry_count": retry_count,
            "first_seen_at": first_seen_at,
            "last_attempt_at": datetime.now(timezone.utc).isoformat(),
            # Truncate to prevent exceeding Kafka max.message.bytes.
            "original_payload": original_payload[:5000],
        }

        try:
            self._producer.produce(
                topic=self.DLQ_TOPIC,
                value=json.dumps(dlq_message),
            )
            self._producer.poll(0)

            log.warning(
                "message_sent_to_dlq",
                rca_id=rca_id,
                failure_reason=failure_reason,
                retry_count=retry_count,
            )

        except Exception as exc:
            # DLQ publish failure is catastrophic — the message is truly lost.
            # Log at ERROR so on-call engineers are alerted immediately.
            log.error(
                "dlq_publish_failed",
                rca_id=rca_id,
                failure_reason=failure_reason,
                error=str(exc),
            )
