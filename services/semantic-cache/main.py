# --- Semantic Cache service entry point ---
# This file is the composition root: it creates all components and wires them
# together. No business logic lives here — only dependency construction and
# the asyncio event loop lifecycle.
# Startup order:
#   1. Import config (pydantic-settings validates and fails fast if vars missing)
#   2. Configure structlog JSON output (first action)
#   3. Start Prometheus HTTP server (background thread, non-blocking)
#   4. Create Redis client
#   5. Create OpenAI async client
#   6. Create SemanticCache (inject Redis + OpenAI)
#   7. Create aiokafka consumer + producer
#   8. Create KafkaHandler (inject all components)
#   9. Register SIGTERM/SIGINT handlers for graceful shutdown
#  10. Run KafkaHandler.run until shutdown signal

import asyncio
import logging
import signal
import sys

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from openai import AsyncOpenAI
from prometheus_client import start_http_server
from redis.asyncio import Redis

from cache import SemanticCache
from config import settings
from kafka.handler import KafkaHandler


def _configure_logging() -> None:
    """Set up structlog with JSON output. Must run before any logger is used.
    configure logging as the very first action so all subsequent
    code (including library code that calls logging.getLogger) uses the
    correct formatter. A logger created before this runs would use the stdlib
    default formatter and output non-JSON lines.
    """
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    structlog.configure(
        processors=[
            # merge_contextvars injects any bound context (e.g. tenant_id) into
            # every log line automatically — no manual passing through call chains.
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    # Sync the stdlib logging level so aiokafka and other libraries respect LOG_LEVEL.
    logging.basicConfig(stream=sys.stdout, level=log_level, format="%(message)s")


# configure before any module-level logger.get_logger call.
_configure_logging()
logger = structlog.get_logger()


async def _run() -> None:
    """Create all components, run the Kafka consumer loop, then shut down cleanly."""

    # --- Prometheus HTTP server ---
    # start_http_server launches a daemon thread that serves /metrics.
    # It runs independently of the asyncio loop — no await needed.
    # The thread is a daemon so it exits automatically when the process does.
    start_http_server(settings.metrics_port)
    logger.info("prometheus_server_started", port=settings.metrics_port)

    # --- Redis client ---
    # from_url does NOT connect immediately — connection is lazy (on first command).
    # decode_responses=True: all Redis values are returned as strings, not bytes.
    # This avoids manual .decode calls throughout the cache logic.
    redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    logger.info("redis_client_created", url=settings.redis_url.split("@")[-1])

    # --- OpenAI async client ---
    # AsyncOpenAI uses the injected api_key. Never read from environment inside
    # business logic — config.py already validated it at startup.
    openai_client = AsyncOpenAI(api_key=settings.openai_api_key)
    logger.info("openai_client_created")

    # --- SemanticCache ---
    # All external clients injected — no connections created inside SemanticCache.
    cache = SemanticCache(
        redis_client=redis_client,
        openai_client=openai_client,
        similarity_threshold=settings.cache_similarity_threshold,
        ttl_seconds=settings.cache_ttl_seconds,
    )
    logger.info(
        "semantic_cache_created",
        threshold=settings.cache_similarity_threshold,
        ttl_seconds=settings.cache_ttl_seconds,
    )

    # --- Kafka consumer ---
    # enable_auto_commit=False: we commit manually after each successful publish.
    # This gives at-least-once semantics: if the process crashes after publish
    # but before commit, the message is redelivered and processed again.
    # auto_offset_reset="earliest": on first startup (no committed offset), read
    # from the beginning of the topic so no incidents are silently skipped.
    consumer = AIOKafkaConsumer(
        settings.kafka_input_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=None,  # raw bytes — handler.py deserialises
    )

    # --- Kafka producer ---
    # acks="all" waits for all in-sync replicas to acknowledge before returning.
    # For a single-broker dev setup this equals acks=1, but it's production-safe.
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all",
    )

    # --- KafkaHandler ---
    # All dependencies injected. KafkaHandler owns the consumer loop.
    handler = KafkaHandler(
        consumer=consumer,
        producer=producer,
        cache=cache,
        input_topic=settings.kafka_input_topic,
        output_hit_topic=settings.kafka_output_hit_topic,
        output_miss_topic=settings.kafka_output_miss_topic,
        dlq_topic=settings.kafka_dlq_topic,
        max_retries=settings.kafka_max_retries,
    )

    # --- Graceful shutdown via asyncio.Event ---
    # stop_event is set by signal handlers (SIGTERM/SIGINT).
    # We wait for it before cancelling the consumer task — this allows any
    # in-flight message to finish processing before the process exits.
    stop_event = asyncio.Event()

    # asyncio's loop.add_signal_handler registers a callback called from the
    # event loop thread (not a separate OS signal thread), making it safe to
    # set asyncio primitives like Event from within the handler.
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop_event.set)
    loop.add_signal_handler(signal.SIGINT, stop_event.set)
    logger.info("signal_handlers_registered")

    # --- Launch consumer loop as a separate task ---
    # asyncio.create_task schedules handler.run to run concurrently with
    # the stop_event.wait below. Without create_task, handler.run would
    # block here and the signal handlers would never have a chance to fire.
    consumer_task = asyncio.create_task(handler.run())
    logger.info("semantic_cache_service_started")

    # --- Wait for shutdown signal ---
    # stop_event.wait yields control back to the event loop, allowing the
    # consumer_task to run. This coroutine resumes only when a SIGTERM or
    # SIGINT arrives and calls stop_event.set.
    await stop_event.wait()
    logger.info("shutdown_signal_received_stopping_consumer")

    # --- Graceful shutdown ---
    # cancel sends CancelledError into handler.run. The `async with`
    # context manager on the consumer and producer ensures they are closed
    # cleanly even when the task is cancelled mid-iteration.
    consumer_task.cancel()
    try:
        # await lets the cancel propagate and the task's cleanup code run.
        # suppress=True would swallow CancelledError — we use try/except instead
        # to distinguish clean cancel from unexpected failure.
        await consumer_task
    except asyncio.CancelledError:
        # Expected path: task was cancelled cleanly by our stop signal.
        pass
    except Exception as exc:
        logger.error("consumer_task_failed_on_shutdown", error=str(exc))

    # --- Close Redis client ---
    # aclose flushes any pending commands and releases the connection pool.
    # Without this, the Redis server holds the connection open until its own
    # TCP keepalive timeout fires.
    await redis_client.aclose()
    logger.info("semantic_cache_service_stopped")


def main() -> None:
    """Synchronous entry point. Creates the asyncio event loop and runs _run."""
    logger.info(
        "semantic_cache_service_initialising",
        kafka_bootstrap_servers=settings.kafka_bootstrap_servers,
        redis_url=settings.redis_url.split("@")[-1],
        metrics_port=settings.metrics_port,
    )
    # asyncio.run creates a new event loop, runs _run to completion,
    # then closes the loop. This is the recommended entry point for asyncio
    # applications (not get_event_loop.run_until_complete which is deprecated).
    asyncio.run(_run())


if __name__ == "__main__":
    main()
