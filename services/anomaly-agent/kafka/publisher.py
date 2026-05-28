"""Kafka alert publisher — Observer implementation for the anomaly-agent.
Implements AlertPublisher so it can be registered as an Observer in
AnomalyOrchestrator alongside PostgresAlertRepository. The orchestrator
calls publish_alert on both without knowing which is Kafka vs. DB.
Why Kafka + PostgreSQL (not one or the other):
  Kafka (topics): downstream consumers (Alert Correlation Engine, step 8)
                     receive alerts as events. Events are durable, replayable,
                     and decouple producers from consumers.
  PostgreSQL: the API gateway queries the alerts table synchronously.
                     Without the DB write, GET /api/v1/alerts would return nothing
                     even after successful Kafka publishing.
  Both are needed. Neither is redundant.
Producer vs Consumer:
  This module creates a Producer (writes to Kafka). KafkaLogConsumer creates
  a Consumer (reads from Kafka). They use different confluent-kafka classes
  with different configurations and lifecycle methods.
Delivery guarantee:
  flush after each produce call makes publishing synchronous — we wait
  until Kafka acknowledges the message before returning to the orchestrator.
  This trades throughput for reliability: in anomaly detection, alert loss
  is worse than slightly lower publish rate (alerts are rare, not every log).
"""

import json
import structlog

from confluent_kafka import Producer, KafkaException

from orchestrator import AlertPublisher

logger = structlog.get_logger(__name__)

# How long flush waits for delivery confirmation, in seconds.
# 10s is generous — production brokers typically acknowledge in <100ms.
# If flush times out, the message may or may not have been delivered.
_FLUSH_TIMEOUT_SECONDS = 10

# Kafka message value encoding — JSON over UTF-8.
# All downstream consumers (alert correlator, etc.) expect UTF-8 JSON.
_ENCODING = "utf-8"


def _delivery_report(err: object, msg: object) -> None:
    """Callback invoked by confluent-kafka on message delivery or failure.
    This function is called from the Producer's internal delivery thread
    after flush returns. It is a fire-and-forget notification — we log
    the outcome but do not retry here (retry logic lives in the caller).
    Args:
        err: None on success; KafkaError on failure.
        msg: the delivered or failed Message object.
    """
    if err is not None:
        # Log at ERROR — the Kafka message was not delivered.
        # The PostgreSQL publisher may have already succeeded, so the alert is
        # not fully lost, but downstream Kafka consumers won't receive it.
        logger.error(
            "kafka_alert_delivery_failed",
            topic=msg.topic(),
            partition=msg.partition(),
            error=str(err),
        )
    else:
        logger.debug(
            "kafka_alert_delivered",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )


class KafkaAlertPublisher(AlertPublisher):
    """Publishes confirmed alert dicts to the Kafka 'alerts' topic.
    Implements AlertPublisher — the orchestrator depends on that interface,
    not on this class directly. This is Dependency Inversion: the orchestrator
    does not import confluent_kafka.Producer.
    Observer role: registered in main.py alongside PostgresAlertRepository.
    When AnomalyOrchestrator calls publish_alert(alert), this class serialises
    the alert to JSON and sends it to Kafka for the Alert Correlation Engine.
    """

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        """Create the confluent-kafka Producer.
        Args:
            bootstrap_servers: comma-separated "host:port" broker list.
            topic: Kafka topic to publish alerts to ('alerts').
        """
        # acks='all': producer waits for all in-sync replicas to acknowledge.
        # For a single-broker dev setup, this is equivalent to acks=1.
        # In a multi-broker production cluster, this prevents message loss if the
        # leader broker crashes after acknowledgement but before replica sync.
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                # retries: confluent-kafka will retry delivery on transient failures
                # (leader change, network blip) up to this many times.
                "retries": 3,
                # retry.backoff.ms: wait 500ms between retry attempts.
                "retry.backoff.ms": 500,
            }
        )
        self._topic = topic
        logger.info(
            "kafka_alert_publisher_ready",
            topic=topic,
            bootstrap_servers=bootstrap_servers,
        )

    def publish_alert(self, alert: dict) -> None:
        """Serialise the alert dict and publish it to the alerts Kafka topic.
        Per AlertPublisher contract: must not raise. Errors are caught and
        logged at ERROR so PostgresAlertRepository can still run.
        Partition key: alert["tenant_id"] — ensures all alerts for a tenant
        land on the same partition, preserving ordering per tenant.
        Args:
            alert: dict produced by AlertPayload.model_dump(mode='json').
        """
        try:
            # json.dumps serialises the dict to a JSON string.
            # .encode(_ENCODING) converts to bytes — Kafka messages are byte arrays.
            value_bytes = json.dumps(alert).encode(_ENCODING)

            # Partition key = tenant_id: same tenant → same partition → ordered delivery.
            # Without a key, Kafka round-robins across partitions — alerts from the same
            # tenant could arrive at the Alert Correlation Engine out of order.
            key_bytes = alert["tenant_id"].encode(_ENCODING)

            # produce is non-blocking — it enqueues the message internally.
            # The delivery report callback fires after flush confirms delivery.
            self._producer.produce(
                topic=self._topic,
                key=key_bytes,
                value=value_bytes,
                # on_delivery fires asynchronously when the broker acknowledges.
                # We use it for logging only — not for retry logic.
                on_delivery=_delivery_report,
            )

            # flush blocks until all enqueued messages are delivered or the timeout expires.
            # This makes publishing synchronous: we know the alert reached Kafka before
            # returning to the orchestrator and incrementing the Prometheus counter.
            # Trade-off: ~1–5ms latency per alert vs. the risk of silent message loss.
            remaining = self._producer.flush(timeout=_FLUSH_TIMEOUT_SECONDS)
            if remaining > 0:
                # remaining > 0 means flush timed out with messages still in the queue.
                logger.error(
                    "kafka_alert_flush_timeout",
                    messages_in_queue=remaining,
                    topic=self._topic,
                    alert_id=alert.get("alert_id", "unknown"),
                )

        except KafkaException as exc:
            # KafkaException: broker unreachable, authentication failure, etc.
            # Log at ERROR — the alert is NOT in Kafka, but PostgreSQL may have it.
            logger.error(
                "kafka_alert_publish_failed",
                alert_id=alert.get("alert_id", "unknown"),
                topic=self._topic,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            # Per AlertPublisher contract: do not re-raise.

        except Exception as exc:
            # Unexpected errors (serialisation, etc.). Log at ERROR, do not raise.
            logger.error(
                "kafka_alert_publish_unexpected_error",
                alert_id=alert.get("alert_id", "unknown"),
                error=str(exc),
                error_type=type(exc).__name__,
            )
