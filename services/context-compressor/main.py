# --- Context Compressor service entry point ---
# Wires all dependencies together and starts the Kafka consumer loop.
# No business logic lives here: main.py is a composition root only.
# structured logging is configured as the very first action.
# Execution order:
#   1. Load + validate config (fail fast on missing vars).
#   2. Configure structured logging.
#   3. Start Prometheus HTTP server.
#   4. Open asyncpg pool.
#   5. Create OpenAI client, PromptRegistry, ContextCompressor, repositories.
#   6. Create Kafka consumer and producer.
#   7. Register SIGTERM/SIGINT handlers for graceful shutdown.
#   8. Run Kafka consumer loop until shutdown signal.
#   9. Close asyncpg pool.

import asyncio
import logging
import signal
import sys

import asyncpg
import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from openai import AsyncOpenAI
from prometheus_client import start_http_server

from compressor import ContextCompressor
from config import Settings
from kafka.handler import KafkaHandler
from postgres.repository import IncidentRepository, LogRepository
from prompt_registry import PromptRegistry

# --- asyncpg pool size ---
# min_size=2: pre-create 2 connections so the first requests don't pay the
# TCP handshake cost. max_size=10: cap concurrency to avoid overwhelming PG.
_PG_MIN_POOL_SIZE = 2
_PG_MAX_POOL_SIZE = 10


def _configure_logging(log_level: str) -> None:
    """Set up structlog with JSON output.
    every log entry is a JSON object with timestamp, level,
    service_name, and context fields. Never use print or basicConfig default.
    Configured before any other code runs so startup errors are captured.
    """
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper(), logging.INFO),
    )
    structlog.configure(
        processors=[
            # add_log_level inserts "level": "info" into the JSON output.
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.CallsiteParameterAdder(
                [structlog.processors.CallsiteParameter.FUNC_NAME]
            ),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            # JSONRenderer: emit structured log lines as JSON for log aggregators.
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
    """Compose and start the context-compressor service."""
    # --- Step 1: config ---
    # pydantic-settings raises ValidationError immediately if required vars are
    # absent. The process exits here with a clear message — fail fast.
    try:
        settings = Settings()
    except Exception as exc:
        print(f"FATAL: configuration error — {exc}", file=sys.stderr)
        sys.exit(1)

    # --- Step 2: logging ---
    _configure_logging(settings.log_level)
    log = structlog.get_logger().bind(service="context-compressor")
    log.info("service_starting", token_threshold=settings.compression_token_threshold)

    # --- Step 3: Prometheus HTTP server ---
    # start_http_server launches a background thread serving /metrics on the port.
    # The Docker healthcheck and Prometheus scraper both hit this port.
    start_http_server(settings.metrics_port)
    log.info("metrics_server_started", port=settings.metrics_port)

    # --- Step 4: database pool ---
    db_pool = await _create_db_pool(settings.postgres_url)
    log.info("db_pool_created", min_size=_PG_MIN_POOL_SIZE, max_size=_PG_MAX_POOL_SIZE)

    # --- Step 5: build domain objects ---
    # AsyncOpenAI: uses the injected api_key (never reads os.environ in a loop).
    # Factory pattern: creating the client here, not inside ContextCompressor.
    openai_client = AsyncOpenAI(api_key=settings.openai_api_key)

    # PromptRegistry: reads prompt files from the mounted prompts directory.
    prompt_registry = PromptRegistry(prompts_dir=settings.prompts_dir)

    # ContextCompressor: pure domain logic, no Kafka or DB awareness.
    compressor = ContextCompressor(
        openai_client=openai_client,
        prompt_registry=prompt_registry,
        token_threshold=settings.compression_token_threshold,
    )

    # Repositories: injected with the pool — never create their own connections.
    log_repo = LogRepository(db_pool=db_pool)
    incident_repo = IncidentRepository(db_pool=db_pool)

    # --- Step 6: Kafka consumer + producer ---
    # enable_auto_commit=False: commit offsets manually after each message is
    # fully processed so a crash mid-processing replays the message on restart.
    consumer = AIOKafkaConsumer(
        settings.kafka_incidents_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        value_deserializer=None,
        enable_auto_commit=False,
        # earliest: on first start (no committed offset), read from the beginning
        # so incidents published before the service starts are not lost.
        auto_offset_reset="earliest",
    )

    # AIOKafkaProducer: publishes to incidents.compressed and DLQ topics.
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
    )

    handler = KafkaHandler(
        consumer=consumer,
        producer=producer,
        compressor=compressor,
        log_repo=log_repo,
        incident_repo=incident_repo,
        incidents_compressed_topic=settings.kafka_incidents_compressed_topic,
        dlq_topic=settings.kafka_dlq_topic,
        max_retries=settings.kafka_dlq_max_retries,
    )

    # --- Step 7: graceful shutdown ---
    # asyncio.Event: the SIGTERM/SIGINT handlers set this event. main waits
    # on it and cancels the consumer task when it fires.
    stop_event = asyncio.Event()

    def _on_shutdown() -> None:
        log.info("shutdown_signal_received")
        # set wakes the stop_event.wait call below.
        stop_event.set()

    loop = asyncio.get_event_loop()
    # add_signal_handler registers an async-safe OS signal callback.
    # Without this, Docker stop or Ctrl-C would kill the process mid-message,
    # leaving a Kafka offset uncommitted and the message redelivered on restart.
    loop.add_signal_handler(signal.SIGTERM, _on_shutdown)
    loop.add_signal_handler(signal.SIGINT, _on_shutdown)

    # --- Step 8: run consumer loop ---
    # asyncio.create_task schedules handler.run concurrently. This lets us
    # also await stop_event below without blocking the consumer loop.
    consumer_task = asyncio.create_task(handler.run())

    log.info(
        "service_ready",
        consuming_topic=settings.kafka_incidents_topic,
        publishing_topic=settings.kafka_incidents_compressed_topic,
    )
    # Block until shutdown signal. When it fires, cancel the consumer task.
    await stop_event.wait()

    consumer_task.cancel()
    try:
        # Await the cancelled task so any cleanup in handler.run can finish.
        await consumer_task
    except asyncio.CancelledError:
        pass

    # --- Step 9: cleanup ---
    # Close the pool so PostgreSQL releases the server-side connections.
    await db_pool.close()
    log.info("service_stopped")


if __name__ == "__main__":
    # asyncio.run creates an event loop, runs main to completion, then closes it.
    asyncio.run(main())
