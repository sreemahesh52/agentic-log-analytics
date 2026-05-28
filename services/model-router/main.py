# --- Model Router service entry point ---
# This file is the composition root: it creates all components and wires them
# together. No business logic lives here — only dependency construction and
# the asyncio event loop lifecycle.
# Startup order:
#   1. Import config (pydantic-settings validates env vars, fails fast if missing)
#   2. Configure structlog JSON output (first action before any logger)
#   3. Start Prometheus HTTP server on metrics_port (daemon thread, non-blocking)
#   4. Create asyncpg connection pool (UTC timezone enforced per-connection)
#   5. Create TenantRepository (inject pool)
#   6. Create RoutingConfig (from settings values)
#   7. Create ModelRouter (inject repository + config)
#   8. Create aiokafka consumer + producer
#   9. Create KafkaHandler (inject all components)
#  10. Register SIGTERM/SIGINT handlers for graceful shutdown
#  11. Launch KafkaHandler.run as a concurrent task
#  12. Wait for shutdown signal, then cancel task and close pool

import asyncio
import logging
import signal
import sys

import asyncpg
import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from prometheus_client import start_http_server

from config import settings
from kafka.handler import KafkaHandler
from postgres.repository import TenantRepository
from router import ModelRouter, RoutingConfig


def _configure_logging() -> None:
    """Set up structlog with JSON output. Must run before any logger is used.
    configure logging as the very first action so all subsequent
    code (including library code from aiokafka and asyncpg) uses the correct
    JSON formatter. A logger created before this call would emit plain-text
    lines and break log aggregation pipelines that expect JSON.
    """
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    structlog.configure(
        processors=[
            # merge_contextvars injects any bound context (e.g. tenant_id) from
            # structlog.contextvars.bind_contextvars into every log line.
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(),
        # cache_logger_on_first_use: avoids processor chain re-evaluation per call.
        cache_logger_on_first_use=True,
    )
    # Sync the stdlib logging level so aiokafka and asyncpg emit at the
    # correct level and their output passes through the structlog chain.
    logging.basicConfig(stream=sys.stdout, level=log_level, format="%(message)s")


# configure before any module-level logger.get_logger call.
_configure_logging()
logger = structlog.get_logger()


async def _run() -> None:
    """Create all service components, run the consumer loop, shut down cleanly."""

    # --- Prometheus HTTP server ---
    # start_http_server spawns a daemon thread that serves /metrics forever.
    # It runs independently of the asyncio loop — no await needed.
    # Daemon threads exit automatically when the main process exits.
    start_http_server(settings.metrics_port)
    logger.info("prometheus_server_started", port=settings.metrics_port)

    # --- PostgreSQL connection pool ---
    # connection pool with explicit min/max sizes.
    # server_settings={"timezone": "UTC"} enforces UTC on every connection.
    # This is belt-and-suspenders alongside PGTZ=UTC in the container env:
    # if a connection is somehow made with a different environment, the
    # per-connection override still ensures correct timezone behaviour.
    db_pool = await asyncpg.create_pool(
        dsn=settings.postgres_url,
        min_size=2,
        max_size=10,
        server_settings={"timezone": "UTC"},
    )
    logger.info("postgres_pool_created", min_size=2, max_size=10)

    # --- TenantRepository ---
    # Inject the pool into the repository. ModelRouter never touches asyncpg.
    # This is the Repository Pattern: all SQL in one place.
    tenant_repo = TenantRepository(
        db_pool=db_pool,
        cache_ttl_seconds=settings.tenant_cache_ttl_seconds,
    )

    # --- RoutingConfig ---
    # All model names come from settings (environment variables).
    # RoutingConfig is a plain object — not a pydantic model — so it can be
    # swapped for a custom config in tests without touching the environment.
    routing_config = RoutingConfig(
        critical_premium=settings.model_critical_premium,
        high_premium=settings.model_high_premium,
        medium_premium=settings.model_medium_premium,
        low_premium=settings.model_low_premium,
        any_standard=settings.model_any_standard,
        budget_exceeded_fallback=settings.daily_budget_override_model,
        low_skip=settings.low_skip,
    )

    # --- ModelRouter ---
    # Inject repository and config. ModelRouter contains only routing logic.
    router = ModelRouter(
        tenant_repository=tenant_repo,
        routing_config=routing_config,
    )

    # --- Kafka consumer ---
    # enable_auto_commit=False: we commit manually after each successful publish
    # to provide at-least-once delivery semantics. If the process crashes after
    # publish but before commit, the message is redelivered and reprocessed.
    # auto_offset_reset="earliest": on first startup (no committed offset),
    # read from the beginning of incidents.routed so no incidents are skipped.
    consumer = AIOKafkaConsumer(
        settings.kafka_input_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        # value_deserializer=None: receive raw bytes — handler.py deserialises.
        value_deserializer=None,
    )

    # --- Kafka producer ---
    # acks="all": wait for all in-sync replicas to acknowledge the write.
    # For single-broker dev setups this equals acks=1, but the setting is
    # production-safe and requires no change when scaling to multi-broker.
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all",
    )

    # --- KafkaHandler ---
    # All dependencies injected via constructor (Dependency Inversion).
    handler = KafkaHandler(
        consumer=consumer,
        producer=producer,
        router=router,
        input_topic=settings.kafka_input_topic,
        output_topic=settings.kafka_output_topic,
        dlq_topic=settings.kafka_dlq_topic,
        max_retries=settings.kafka_max_retries,
    )

    # --- Graceful shutdown via asyncio.Event ---
    # stop_event is set by SIGTERM/SIGINT signal handlers.
    # We wait for it before cancelling the consumer task so any in-flight
    # message finishes processing before the process exits.
    stop_event = asyncio.Event()

    # asyncio's loop.add_signal_handler registers callbacks that run inside
    # the event loop thread (not a separate OS signal thread). This makes it
    # safe to set asyncio primitives (like Event) from within the handler —
    # unlike signal.signal which runs in an arbitrary thread.
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop_event.set)
    loop.add_signal_handler(signal.SIGINT, stop_event.set)
    logger.info("signal_handlers_registered", signals=["SIGTERM", "SIGINT"])

    # --- Launch consumer loop as a concurrent asyncio task ---
    # asyncio.create_task schedules handler.run to run concurrently with
    # the stop_event.wait below. Without create_task, handler.run would
    # block here and the signal handlers would never get a chance to fire —
    # the service would be impossible to shut down gracefully.
    consumer_task = asyncio.create_task(handler.run())
    logger.info(
        "model_router_service_started",
        input_topic=settings.kafka_input_topic,
        output_topic=settings.kafka_output_topic,
        low_skip=settings.low_skip,
    )

    # --- Wait for shutdown signal ---
    # stop_event.wait yields control back to the event loop, allowing
    # consumer_task to run. This coroutine resumes only when SIGTERM or SIGINT
    # arrives and calls stop_event.set.
    await stop_event.wait()
    logger.info("shutdown_signal_received_stopping_consumer")

    # --- Graceful shutdown ---
    # cancel injects CancelledError into handler.run. The `async with`
    # context manager on consumer and producer in handler.run ensures they
    # are closed cleanly even when the task is cancelled mid-message.
    consumer_task.cancel()
    try:
        # await lets the cancellation propagate and the task's cleanup code run.
        await consumer_task
    except asyncio.CancelledError:
        # Expected path: the task was cancelled cleanly by our stop signal.
        pass
    except Exception as exc:
        logger.error("consumer_task_failed_on_shutdown", error=str(exc))

    # --- Close PostgreSQL pool ---
    # close waits for all in-use connections to return to the pool, then
    # terminates all connections. Without this, the PostgreSQL server holds
    # connections open until its own idle timeout fires.
    await db_pool.close()
    logger.info("model_router_service_stopped")


def main() -> None:
    """Synchronous entry point. Creates the asyncio event loop and runs _run."""
    logger.info(
        "model_router_service_initialising",
        kafka_bootstrap_servers=settings.kafka_bootstrap_servers,
        kafka_input_topic=settings.kafka_input_topic,
        kafka_output_topic=settings.kafka_output_topic,
        metrics_port=settings.metrics_port,
        low_skip=settings.low_skip,
    )
    # asyncio.run creates a new event loop, runs _run to completion, then
    # closes the loop and finalises all async generators. This is the recommended
    # entry point for asyncio applications (not get_event_loop.run_until_complete
    # which is deprecated in Python 3.10+).
    asyncio.run(_run())


if __name__ == "__main__":
    main()
