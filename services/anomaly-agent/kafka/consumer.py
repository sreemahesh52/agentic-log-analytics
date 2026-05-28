"""Kafka consumer for the anomaly-agent service.
Consumes the 'logs.enriched' topic published by the Go log-consumer after
each log batch is inserted into PostgreSQL.
Why confluent-kafka (not kafka-python):
  confluent-kafka is backed by librdkafka, the C implementation of the Kafka
  protocol. It handles consumer group rebalancing, offset commits, and reconnects
  more robustly than the pure-Python kafka-python library. It is the Confluent
  recommendation for production Python Kafka consumers.
Why a consumer class (not a bare Consumer call in main):
  Wrapping Consumer in a class:
    1. Enables mock injection in tests without a real Kafka broker.
    2. Hides confluent-kafka's error handling quirks (KafkaError._PARTITION_EOF
       is not a real error — it signals the consumer is caught up).
    3. Centralises message deserialisation and validation in one place.
    4. Provides a clean close method for graceful shutdown.
Message format (logs.enriched):
  JSON object matching the LogEvent Pydantic model:
    {tenant_id, service, level, message, timestamp, trace_id?, metadata, injection_attempted}
  tenant_id may be at top-level or nested in metadata (depending on log-consumer version).
  Both locations are handled transparently by the consumer.
Dead Letter Queue (DLQ) pattern:
  Messages that fail JSON parsing or Pydantic validation are logged at ERROR
  and skipped — they cannot be processed and retrying would loop forever.
  In production, these would be published to logs.dlq for manual inspection.
  For this step, we log and skip (the DLQ topic exists but writing to it from
  the anomaly-agent is added in a later hardening pass).
"""

import json
import structlog

from confluent_kafka import Consumer, KafkaError, KafkaException
from pydantic import ValidationError

from models import LogEvent

logger = structlog.get_logger(__name__)

# How many milliseconds confluent-kafka's poll waits for a message before
# returning None. Converted from the config's float seconds value.
_MS_PER_SECOND = 1000


class KafkaLogConsumer:
    """Wraps confluent-kafka Consumer with LogEvent deserialisation.
    Interface Segregation: this class only polls and deserialises.
    It does not process messages — that is AnomalyOrchestrator's responsibility.
    Dependency Inversion benefit: tests can inject a MockKafkaLogConsumer
    that yields fixture LogEvent objects without any Kafka broker.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topic: str,
        poll_timeout_seconds: float,
    ) -> None:
        """Create the confluent-kafka Consumer and subscribe to the topic.
        Args:
            bootstrap_servers: comma-separated "host:port" broker list.
            group_id: Kafka consumer group. Kafka tracks committed offsets
                                 per group, so restarting the service resumes from
                                 where it left off rather than replaying all messages.
            topic: topic name to subscribe to (logs.enriched).
            poll_timeout_seconds: how long poll blocks waiting for a message.
        """
        # auto.offset.reset=latest: on first start (no committed offset), skip old messages.
        # This avoids re-processing potentially millions of historical logs on first deploy.
        # Change to 'earliest' to replay from the beginning (e.g., after a bug fix).
        # enable.auto.commit=true: offsets committed automatically after poll.
        # This is the simplest approach — at-least-once delivery is acceptable here
        # because duplicate anomaly detection is harmless (ON CONFLICT DO NOTHING in DB).
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            }
        )

        # subscribe registers the consumer for the topic.
        # Partition assignment happens asynchronously during the first poll call.
        self._consumer.subscribe([topic])

        # Convert seconds to milliseconds — confluent-kafka poll takes float seconds
        self._poll_timeout = poll_timeout_seconds

        logger.info(
            "kafka_consumer_subscribed",
            topic=topic,
            group_id=group_id,
            bootstrap_servers=bootstrap_servers,
        )

    def poll(self) -> LogEvent | None:
        """Poll for one message and deserialise it as a LogEvent.
        Returns None when no message is available or when a non-fatal Kafka
        event occurs (partition EOF, empty poll window, etc.).
        Logs at ERROR and returns None on deserialisation failures (DLQ pattern).
        Returns:
            LogEvent if a valid message was received.
            None if no message, or message was invalid (logged at ERROR).
        Raises:
            KafkaException: on fatal broker errors (connection refused after retries).
                            The main loop catches this for graceful shutdown.
        """
        # confluent-kafka poll: blocks for up to poll_timeout seconds waiting for
        # a message. Returns None if the timeout expires with no message available.
        msg = self._consumer.poll(timeout=self._poll_timeout)

        # None: no message arrived within the timeout — normal, not an error.
        if msg is None:
            return None

        # Check for Kafka-level errors embedded in the message object.
        if msg.error():
            err = msg.error()
            if err.code() == KafkaError._PARTITION_EOF:
                # _PARTITION_EOF: consumer is caught up to the end of the partition.
                # This is informational, not an error — happens when log traffic is low.
                # Return None to continue polling.
                return None
            # Any other Kafka error is fatal — surface it to the main loop.
            raise KafkaException(err)

        # --- Deserialise message bytes → dict ---
        raw_bytes = msg.value()
        try:
            # .decode('utf-8') converts bytes to string; json.loads parses the JSON.
            data = json.loads(raw_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.error(
                "kafka_message_json_parse_failed",
                offset=msg.offset(),
                partition=msg.partition(),
                error=str(exc),
            )
            # DLQ pattern: skip unparseable message, continue consuming.
            # A production implementation would publish to logs.dlq here.
            return None

        # --- Handle tenant_id location ---
        # Go publishes PascalCase keys; TenantID may also be nested in Metadata
        # (injected by the simulate/flood endpoint for testing). Promote it to
        # the top-level PascalCase key so the LogEvent alias resolves correctly.
        if "TenantID" not in data and isinstance(data.get("Metadata"), dict):
            tenant_id = data["Metadata"].get("tenant_id")
            if tenant_id:
                data["TenantID"] = tenant_id

        # --- Validate with Pydantic ---
        # model_validate applies alias resolution (PascalCase → snake_case).
        # LogEvent(**data) bypasses aliases and would fail on PascalCase keys.
        try:
            return LogEvent.model_validate(data)
        except ValidationError as exc:
            logger.error(
                "kafka_message_validation_failed",
                offset=msg.offset(),
                partition=msg.partition(),
                # errors returns a list of dicts with loc, msg, type — safe to log
                validation_errors=exc.errors()[:3],  # first 3 to avoid log spam
            )
            # DLQ pattern: skip invalid message, continue consuming.
            return None

    def close(self) -> None:
        """Gracefully close the consumer, committing pending offsets.
        Must be called on shutdown. Without close:
          1. Uncommitted offsets are lost — messages may be re-consumed after restart.
          2. The consumer group coordinator waits for a heartbeat timeout (30s default)
             before reassigning partitions to another consumer instance.
        """
        # close commits pending offsets, sends a LeaveGroup request to the coordinator,
        # and releases all resources. This triggers immediate partition rebalancing.
        self._consumer.close()
        logger.info("kafka_consumer_closed")
