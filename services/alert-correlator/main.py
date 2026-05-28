# --- Alert Correlator service entry point ---
# Wires all dependencies together and starts the Kafka consumer loop.
# No business logic lives here: main.py is a composition root only.
# structured logging is configured first, before any other code runs.

import asyncio
import logging
import signal
import sys

import asyncpg
import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import start_http_server

from config import Settings
from correlator import AlertCorrelator
from kafka.handler import KafkaHandler
from postgres.repository import IncidentRepository

# --- Asyncpg pool configuration ---
_PG_MIN_POOL_SIZE = 2
_PG_MAX_POOL_SIZE = 10


def _configure_logging(log_level: str) -> None:
    """Set up structlog with JSON output.
    every log entry is a JSON object with timestamp, level,
    service_name, and context fields. Never use print or basicConfig.
    Configured before any other code so startup errors are captured.
    """
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper(), logging.INFO),
    )
    structlog.configure(
        processors=[
            # Add log level as a string field.
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            # Add the service name to every log entry for easy Grafana filtering.
            structlog.processors.CallsiteParameterAdder(
                [structlog.processors.CallsiteParameter.FUNC_NAME]
            ),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            # Render as JSON — compatible with Prometheus Loki, Datadog, etc.
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


async def _create_db_pool(postgres_url: str) -> asyncpg.Pool:
    """Create an asyncpg connection pool."""
    return await asyncpg.create_pool(
        postgres_url,
        min_size=_PG_MIN_POOL_SIZE,
        max_size=_PG_MAX_POOL_SIZE,
        server_settings={"timezone": "UTC"},
    )


async def main() -> None:
    """Compose and start the alert-correlator service.
    Execution order:
      1. Load + validate config (fail fast on missing vars).
      2. Configure structured logging.
      3. Start Prometheus HTTP server (metrics + healthcheck endpoint).
      4. Open asyncpg pool.
      5. Create AlertCorrelator, IncidentRepository, KafkaHandler.
      6. Register SIGTERM/SIGINT handlers for graceful shutdown.
      7. Run Kafka consumer loop until shutdown signal.
      8. Close asyncpg pool.
    """
    # --- Step 1: config ---
    # pydantic-settings raises ValidationError if required vars are absent.
    # The process exits here with a clear message rather than failing silently later.
    try:
        settings = Settings()
    except Exception as exc:
        # Print to stderr before structlog is configured.
        print(f"FATAL: configuration error — {exc}", file=sys.stderr)
        sys.exit(1)

    # --- Step 2: logging ---
    _configure_logging(settings.log_level)
    log = structlog.get_logger().bind(service="alert-correlator")
    log.info("service_starting", correlation_window_seconds=settings.correlation_window_seconds)

    # --- Step 3: Prometheus HTTP server ---
    # start_http_server launches a background thread that serves /metrics on the
    # specified port. Docker healthcheck and Prometheus scraping both hit this port.
    start_http_server(settings.metrics_port)
    log.info("metrics_server_started", port=settings.metrics_port)

    # --- Step 4: database pool ---
    db_pool = await _create_db_pool(settings.postgres_url)
    log.info("db_pool_created", min_size=_PG_MIN_POOL_SIZE, max_size=_PG_MAX_POOL_SIZE)

    # --- Step 5: build dependencies ---
    # AlertCorrelator: pure in-memory, no I/O dependencies (testable without infra).
    correlator = AlertCorrelator(window_seconds=settings.correlation_window_seconds)

    # IncidentRepository: injected pool — never creates its own connections.
    incident_repo = IncidentRepository(db_pool=db_pool)

    # AIOKafkaConsumer: enable_auto_commit=False so we commit offsets manually
    # after each message is fully processed (at-least-once delivery guarantee).
    consumer = AIOKafkaConsumer(
        settings.kafka_alerts_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        # Deserialisation: raw bytes are parsed in handler._parse_alert.
        value_deserializer=None,
        enable_auto_commit=False,
        # earliest: on first start (no committed offset), read from beginning
        # so we don't miss alerts that arrived before the service started.
        auto_offset_reset="earliest",
    )

    # AIOKafkaProducer: used for publishing to incidents and DLQ topics.
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
    )

    handler = KafkaHandler(
        consumer=consumer,
        producer=producer,
        correlator=correlator,
        incident_repo=incident_repo,
        incidents_topic=settings.kafka_incidents_topic,
        dlq_topic=settings.kafka_dlq_topic,
        max_retries=settings.kafka_dlq_max_retries,
    )

    # --- Step 6: graceful shutdown ---
    # asyncio.Event that the SIGTERM/SIGINT handlers will set.
    stop_event = asyncio.Event()

    def _on_shutdown() -> None:
        log.info("shutdown_signal_received")
        # set wakes the stop_event.wait below, allowing main to return
        # cleanly instead of being killed mid-message.
        stop_event.set()

    loop = asyncio.get_event_loop()
    # add_signal_handler registers an async-safe callback for OS signals.
    # Without this, Ctrl-C or Docker stop would kill the process immediately,
    # potentially leaving a Kafka offset uncommitted.
    loop.add_signal_handler(signal.SIGTERM, _on_shutdown)
    loop.add_signal_handler(signal.SIGINT, _on_shutdown)

    # --- Step 7: run consumer loop ---
    # asyncio.create_task schedules the handler coroutine concurrently so we can
    # also await stop_event below without blocking the consumer.
    consumer_task = asyncio.create_task(handler.run())

    log.info("service_ready", consuming_topic=settings.kafka_alerts_topic)
    # Wait for shutdown signal. When it arrives, cancel the consumer task.
    await stop_event.wait()

    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

    # --- Step 8: cleanup ---
    await db_pool.close()
    log.info("service_stopped")


if __name__ == "__main__":
    # asyncio.run creates an event loop, runs main, and closes the loop.
    asyncio.run(main())
