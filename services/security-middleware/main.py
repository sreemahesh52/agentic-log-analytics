"""Security middleware service entry point.
Startup sequence (fail fast, log first):
  1. Configure structlog JSON output — must be first
  2. Load and validate Settings — fails immediately if required vars are missing
  3. Create asyncpg connection pool with UTC timezone and JSONB codec
  4. Create Kafka producer and consumer via factory functions
  5. Instantiate all components; inject all dependencies (no new-ing inside classes)
  6. Start Prometheus HTTP server on metrics_port (background thread)
  7. Register SIGTERM/SIGINT signal handlers for clean shutdown
  8. Run the Kafka consumer loop until a stop signal is received
  9. Shutdown: flush Kafka producer, close DB pool
"""

import asyncio
import logging
import signal
import sys

import asyncpg
import structlog
from confluent_kafka import Consumer, Producer
from prometheus_client import Counter, start_http_server

from audit import AuditRepository
from config import settings
from detection.injection import InjectionDetector
from detection.pii import PIIDetector
from kafka_handler import SecurityMetrics, SecurityMiddlewareHandler


def _configure_logging(log_level: str) -> None:
    """Configure structlog for JSON output. Must be called before any logger use."""
    # getattr with fallback: a typo in LOG_LEVEL defaults to INFO rather than crash.
    level = getattr(logging, log_level.upper(), logging.INFO)

    structlog.configure(
        processors=[
            # merge_contextvars attaches fields bound via structlog.contextvars
            # (e.g., trace_id set per-message) to every log line automatically.
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    # Configure stdlib logging so confluent-kafka's internal logs respect LOG_LEVEL.
    logging.basicConfig(stream=sys.stdout, level=level, format="%(message)s")


def _create_kafka_producer(bootstrap_servers: str) -> Producer:
    """Factory function: create a confluent-kafka Producer.
    Factory Pattern: complex object creation is isolated here. Callers receive
    the finished object without knowing the config dict format.
    """
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            # socket.keepalive.enable: detect dead TCP connections to the broker.
            "socket.keepalive.enable": True,
        }
    )


def _create_kafka_consumer(
    bootstrap_servers: str, group_id: str, input_topic: str
) -> Consumer:
    """Factory function: create and subscribe a confluent-kafka Consumer.
    enable.auto.commit=false: offsets are committed manually by the handler
    after successful clean-topic publish. This is the root of at-least-once
    delivery — never commit before processing is confirmed complete.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            # earliest: on first run or after a reset, start from the beginning.
            # This ensures no messages are silently skipped on first deploy.
            "auto.offset.reset": "earliest",
            # Manual commit — the handler calls consumer.commit explicitly.
            "enable.auto.commit": False,
        }
    )
    # subscribe registers the consumer with the broker. Messages from all
    # partitions of this topic are distributed across the consumer group.
    consumer.subscribe([input_topic])
    return consumer


async def _create_db_pool(postgres_url: str) -> asyncpg.Pool:
    """Create an asyncpg connection pool with JSONB support."""
    pool = await asyncpg.create_pool(
        postgres_url,
        min_size=2,
        max_size=10,
        server_settings={"timezone": "UTC"},
    )
    return pool


async def _setup_components() -> tuple[
    asyncpg.Pool, Producer, Consumer, SecurityMiddlewareHandler
]:
    """Create all service components and wire up dependency injection.
    Returns a tuple so main can access each component for shutdown.
    All dependencies flow in one direction: main creates everything,
    passes it down — no circular dependencies, no global mutable state.
    """
    pool = await _create_db_pool(settings.postgres_url)
    producer = _create_kafka_producer(settings.kafka_bootstrap_servers)
    consumer = _create_kafka_consumer(
        settings.kafka_bootstrap_servers,
        settings.kafka_consumer_group,
        settings.kafka_input_topic,
    )

    # Counter label lists define the Prometheus label dimensions.
    # Labels must match exactly what the handler uses in .labels calls.
    metrics = SecurityMetrics(
        injection_attempts=Counter(
            "security_injection_attempts_total",
            "Total prompt injection attempts detected, by service and tenant",
            ["service", "tenant"],
        ),
        pii_redactions=Counter(
            "security_pii_redactions_total",
            "Total PII fields redacted, by field type and tenant",
            ["field_type", "tenant"],
        ),
    )

    handler = SecurityMiddlewareHandler(
        producer=producer,
        consumer=consumer,
        injection_detector=InjectionDetector(),
        pii_detector=PIIDetector(),
        audit_repo=AuditRepository(pool),
        metrics=metrics,
        clean_topic=settings.kafka_output_clean_topic,
        security_topic=settings.kafka_security_events_topic,
    )

    return pool, producer, consumer, handler


async def main() -> None:
    """Orchestrate startup, run, and graceful shutdown of the security middleware."""
    # configure logging as the VERY FIRST action.
    _configure_logging(settings.log_level)
    log = structlog.get_logger()

    log.info(
        "security_middleware_starting",
        input_topic=settings.kafka_input_topic,
        clean_topic=settings.kafka_output_clean_topic,
        metrics_port=settings.metrics_port,
    )

    pool, producer, consumer, handler = await _setup_components()

    # start_http_server launches a background thread serving /metrics on metrics_port.
    # This must run before the consumer loop so Prometheus can scrape from the start.
    start_http_server(settings.metrics_port)
    log.info("metrics_server_started", port=settings.metrics_port)

    # asyncio.Event is the idiomatic way to block until a signal arrives in asyncio.
    # It is set by the signal handlers registered below.
    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()

    # add_signal_handler registers a synchronous callback on the loop's signal watcher.
    # When SIGTERM or SIGINT arrives, stop_event.set unblocks the await below.
    loop.add_signal_handler(signal.SIGTERM, stop_event.set)
    loop.add_signal_handler(signal.SIGINT, stop_event.set)

    log.info("consumer_loop_starting")
    # create_task schedules handler.run concurrently — it starts on the next
    # event loop iteration, not immediately. This lets await stop_event.wait
    # run on the same thread without blocking the consumer.
    task = asyncio.create_task(handler.run())
    await stop_event.wait()

    log.info("shutdown_signal_received")
    handler.stop()
    await task

    # --- Cleanup: flush then close in the correct order ---
    # flush blocks until all enqueued Kafka messages are delivered (or timeout).
    # Closing the pool before flush could lose in-flight audit writes.
    await loop.run_in_executor(None, lambda: producer.flush(30))
    consumer.close()
    await pool.close()
    log.info("shutdown_complete")


if __name__ == "__main__":
    # asyncio.run creates a new event loop, runs main, then closes it cleanly.
    # This is the correct entry point for an asyncio application — never call
    # loop.run_forever or loop.run_until_complete directly in production code.
    asyncio.run(main())
