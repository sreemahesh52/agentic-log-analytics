"""
KafkaStreamingCallback — publish each ReasoningStep to rca.stream in real time.
Why a separate module instead of inlining in the Kafka handler?
KafkaStreamingCallback is a pure I/O adapter: it wraps a confluent_kafka Producer
and knows nothing about the ReAct loop or the RCA Agent. Keeping it isolated makes
it independently testable (mock the producer, verify the correct bytes are written)
and replaceable (swap Kafka for Redis Streams without touching agent.py or handler.py).
this module does one thing — translate a
ReasoningStep Pydantic model into a Kafka produce call. It does not consume,
does not decode, does not touch PostgreSQL.
Why is __call__ synchronous even though the agent uses async/await?
confluent_kafka's Producer is a C-extension library built on librdkafka. It does
NOT integrate with asyncio's event loop. Its produce method enqueues the message
in an internal C-level buffer and returns immediately — the I/O happens on a
background thread managed by librdkafka. This makes produce effectively non-blocking:
the Python thread returns before any network I/O occurs. Therefore __call__ can be
a plain synchronous method that the async ReAct loop calls without await, and without
blocking the event loop.
Why poll(0) after every produce?
poll processes delivery callbacks from librdkafka's internal queue. Calling poll(0)
(zero timeout) drains the callback queue without blocking. Without poll, the callback
queue grows unbounded on long investigations, consuming memory and potentially delaying
delivery reports.
Why flush(timeout=5) only in publish_complete?
flush blocks until all enqueued messages are delivered (or the timeout expires).
Calling it on every step would serialise every produce call — eliminating the
batching benefit of librdkafka's buffer. We call flush exactly once, when the
investigation is complete, to guarantee the type='complete' sentinel is delivered
before the SSE connection is torn down.
"""

from __future__ import annotations

import json

import structlog
from confluent_kafka import Producer

from models import ReasoningStep

log = structlog.get_logger(__name__)


class KafkaStreamingCallback:
    """Callable that publishes each ReasoningStep to rca.stream for SSE delivery.
    Registered as the agent's stream callback via agent.set_stream_callback.
    Called once per completed ReasoningStep — synchronously inside the ReAct loop.
    Thread safety: confluent_kafka Producer is thread-safe. Multiple concurrent
    investigations can share one Producer instance without a lock.
    """

    # Topic that the SSE endpoint (future step) consumes.
    STREAM_TOPIC = "rca.stream"

    def __init__(self, producer: Producer, tenant_id: str, rca_id: str) -> None:
        """Bind the producer and investigation identifiers.
        Args:
            producer: Shared confluent_kafka Producer instance.
            tenant_id: UUID string for the tenant — used as the Kafka message key
                       so all steps for one tenant land on the same partition.
            rca_id: UUID string for this investigation — included in every message
                       so the SSE consumer can filter to a single investigation stream.
        """
        self._producer = producer
        self._tenant_id = tenant_id
        self._rca_id = rca_id

    def __call__(self, step: ReasoningStep) -> None:
        """Publish a completed ReasoningStep to rca.stream.
        Called by agent._emit_step after each ReAct iteration.
        Errors are deliberately NOT raised — see agent._emit_step for the reasoning:
        streaming failure must never abort an ongoing investigation.
        Message format:
            {
              "rca_id": "uuid",
              "step_number": int,
              "thought": str,
              "action": str,
              "action_input": str | dict,
              "observation": str | null,
              "timestamp": "ISO 8601 UTC",
              "type": "step"
            }
        """
        try:
            payload = {
                "rca_id": self._rca_id,
                "step_number": step.step_number,
                "thought": step.thought,
                "action": step.action,
                "action_input": step.action_input,
                "observation": step.observation,
                # Normalise timestamp to explicit UTC for the SSE consumer.
                "timestamp": step.timestamp,
                # type='step' distinguishes intermediate steps from the 'complete' sentinel.
                "type": "step",
            }

            # Key on tenant_id: all messages for a tenant land on the same partition,
            # enabling ordered delivery per tenant without a global ordering guarantee.
            self._producer.produce(
                topic=self.STREAM_TOPIC,
                key=self._tenant_id,
                value=json.dumps(payload),
            )

            # poll(0) drains the delivery callback queue without blocking.
            # Keeps the internal callback buffer from growing unbounded during
            # investigations with many iterations.
            self._producer.poll(0)

            log.debug(
                "stream_step_produced",
                rca_id=self._rca_id,
                step_number=step.step_number,
            )

        except Exception as exc:
            # Swallow — streaming must never abort RCA. agent._emit_step also
            # catches, but logging here provides more specific context (topic, key).
            log.warning(
                "stream_produce_failed",
                rca_id=self._rca_id,
                step_number=step.step_number,
                error=str(exc),
            )

    def publish_complete(
        self,
        root_cause: str,
        confidence: float,
        recommendations: list[str],
    ) -> None:
        """Publish the type='complete' sentinel message after the investigation finishes.
        The SSE consumer (future step) uses this message to signal to the browser
        that the stream is finished and the full RCAResult is ready in rca_results.
        flush(timeout=5) blocks until the sentinel is delivered (or times out).
        This guarantees the UI sees 'complete' before the HTTP response returns,
        preventing a race where the browser polls rca_results before the row lands.
        Args:
            root_cause: Final root cause from RCAOutput.
            confidence: Final confidence score.
            recommendations: List of remediation steps.
        """
        try:
            payload = {
                "rca_id": self._rca_id,
                "root_cause": root_cause,
                "confidence": confidence,
                "recommendations": recommendations,
                # type='complete' is the sentinel the SSE consumer waits for.
                "type": "complete",
            }

            self._producer.produce(
                topic=self.STREAM_TOPIC,
                key=self._tenant_id,
                value=json.dumps(payload),
            )

            # flush blocks until delivery confirmation. timeout=5 prevents hanging
            # forever if the Kafka broker is temporarily unreachable.
            remaining = self._producer.flush(timeout=5)
            if remaining > 0:
                log.warning(
                    "stream_flush_incomplete",
                    rca_id=self._rca_id,
                    remaining_messages=remaining,
                )
            else:
                log.info("stream_complete_published", rca_id=self._rca_id)

        except Exception as exc:
            # Swallow — same rationale as __call__. The investigation already
            # succeeded; losing the 'complete' sentinel is acceptable.
            log.warning(
                "stream_complete_failed",
                rca_id=self._rca_id,
                error=str(exc),
            )
