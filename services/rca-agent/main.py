"""
RCA Agent service entry point — composition root and startup orchestrator.
This file is the composition root: it creates all components and wires them
together. No business logic lives here — only dependency construction, tool
registration, and the asyncio event loop lifecycle.
Startup order:
  1. Import config (pydantic-settings validates env vars, fails fast if missing)
  2. Configure structlog JSON output (first action before any logger)
  3. Start Prometheus HTTP server on metrics_port (daemon thread, non-blocking)
  4. Load CrossEncoderReranker singleton (100 MB model — do this once at startup)
  5. Create asyncpg connection pool (UTC timezone enforced per-connection)
  6. Create RCARepository (inject pool)
  7. Create ChromaDB client (for SearchKnowledgeBase tool)
  8. Create confluent_kafka Consumer and Producer instances
  9. Create OpenAI async client
 10. Create PromptRegistry
 11. Define agent_factory — closure that creates a fresh RCAAgent per investigation
     with all tools pre-bound to the correct tenant_id via functools.partial
 12. Create KafkaIncidentHandler and register observers
 13. Register SIGTERM/SIGINT handlers for graceful shutdown
 14. Launch consume_loop as a concurrent asyncio task
 15. Wait for shutdown signal, clean up, exit
Why agent_factory instead of a shared RCAAgent instance?
Each investigation needs tools bound to the SPECIFIC tenant_id of that message.
Using a shared agent would require resetting tool bindings on every message —
a race condition in concurrent processing. A factory closure creates a fresh
agent per investigation, each with tools correctly pre-bound to its tenant_id.
Why functools.partial for tool binding?
partial(query_logs, tenant_id=t, db_pool=pool) returns a new callable that
already has tenant_id and db_pool bound. When the LLM calls QueryLogs with
{"service": "payment-service"}, the agent executes:
    partial_func(service="payment-service")
    → query_logs(tenant_id="abc", db_pool=pool, service="payment-service")
The LLM never sees tenant_id or db_pool — it only provides the business args.
This is applied to tool registration.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import signal
import sys

import asyncpg
import structlog
from confluent_kafka import Consumer, Producer
from prometheus_client import start_http_server

from agent import RCAAgent
from config import settings
from hybrid_rag.bm25_index import BM25Index
from hybrid_rag.reranker import CrossEncoderReranker
from hybrid_rag.vector_search import VectorSearch
from kafka.handler import (
    KafkaIncidentHandler,
    KafkaResultPublisher,
    PostgresResultSaver,
)
from metrics import create_metrics
from postgres.repository import RCARepository
from prompt_registry import PromptRegistry
from tools.build_timeline import BUILD_TIMELINE_SCHEMA, build_timeline
from tools.get_dependencies import GET_DEPENDENCIES_SCHEMA, get_dependencies
from tools.query_logs import QUERY_LOGS_SCHEMA, query_logs
from tools.search_knowledge_base import (
    SEARCH_KNOWLEDGE_BASE_SCHEMA,
    search_knowledge_base,
)


def _configure_logging() -> None:
    """Set up structlog JSON output. Must run before any logger is used.
    configure logging as the very first action in the process.
    Any logger created before this call emits unstructured plain-text output
    that breaks log aggregation pipelines expecting JSON.
    """
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    structlog.configure(
        processors=[
            # merge_contextvars attaches any structlog.contextvars.bind_contextvars
            # context (e.g. tenant_id, rca_id bound per-investigation) to every line.
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    # Sync stdlib logging so confluent_kafka and asyncpg emit at the same level.
    logging.basicConfig(stream=sys.stdout, level=log_level, format="%(message)s")


# configure before any module-level logger is created.
_configure_logging()
logger = structlog.get_logger()


async def _run() -> None:
    """Create all service components, run the consumer loop, shut down cleanly."""

    # --- Prometheus metrics registration ---
    # create_metrics registers all Counter/Gauge/Histogram objects with the
    # global CollectorRegistry. Must run before start_http_server so the
    # /metrics endpoint immediately returns all metric families (even at zero).
    rca_metrics = create_metrics()
    logger.info("prometheus_metrics_registered")

    # --- Prometheus HTTP server ---
    # start_http_server spawns a daemon thread — non-blocking, runs forever.
    # Must start AFTER create_metrics so the endpoint is never transiently empty.
    start_http_server(settings.metrics_port)
    logger.info("prometheus_server_started", port=settings.metrics_port)

    # --- CrossEncoderReranker singleton ---
    # The cross-encoder model is ~85MB. load is a no-op if sentence-transformers
    # is not installed — the reranker falls back to RRF ordering automatically.
    # This is the Singleton pattern: one model instance shared across all agents.
    logger.info("loading_cross_encoder_reranker")
    reranker = CrossEncoderReranker()
    try:
        reranker.load()
        logger.info("cross_encoder_reranker_loaded")
    except ImportError:
        logger.warning(
            "cross_encoder_reranker_skipped",
            reason="sentence-transformers not installed; reranker falls back to RRF order",
        )

    # --- PostgreSQL connection pool ---
    # server_settings={"timezone": "UTC"} enforces UTC on every
    # new connection — belt-and-suspenders alongside TZ=UTC in the container.
    db_pool = await asyncpg.create_pool(
        dsn=settings.postgres_url,
        min_size=2,
        max_size=10,
        server_settings={"timezone": "UTC"},
    )
    logger.info("postgres_pool_created", min_size=2, max_size=10)

    # --- RCARepository ---
    # Repository Pattern: all SQL for rca_results lives in repository.py.
    # The handler and agent never write SQL directly.
    rca_repository = RCARepository(db_pool=db_pool)

    # --- BM25Index ---
    # BM25 sparse retrieval stage — reads from past_incidents PostgreSQL table.
    bm25_index = BM25Index(db_pool=db_pool)

    # --- confluent_kafka Consumer ---
    # auto.offset.reset=earliest: if no committed offset exists (first start or
    # new partition), begin consuming from the oldest available message.
    # enable.auto.commit=true: confluent_kafka commits offsets automatically after
    # each poll call. For production, consider manual commit for exactly-once
    # semantics, but auto-commit is correct for this at-least-once pipeline.
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "true",
        }
    )
    consumer.subscribe([settings.kafka_incidents_topic])
    logger.info(
        "kafka_consumer_created",
        topic=settings.kafka_incidents_topic,
        group_id=settings.kafka_consumer_group,
    )

    # --- confluent_kafka Producer (for agent.results and rca.dlq) ---
    # acks=all: the broker waits for all in-sync replicas to acknowledge.
    # This is the safest delivery guarantee — correct for result publishing
    # where losing a message is not acceptable.
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "all",
        }
    )

    # --- Separate stream producer (for rca.stream) ---
    # Kept separate from the main producer so streaming failures (Kafka brokers
    # rejecting the stream topic) never interfere with result publishing.
    # acks=0 for stream: we prefer low latency over delivery guarantee for
    # per-step streaming — losing one step is acceptable; losing the final
    # result is not.
    stream_producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "0",
        }
    )
    logger.info("kafka_producers_created")

    # --- ChromaDB client + VectorSearch ---
    # Local imports: both are heavy packages; importing them after the Kafka
    # consumer is created ensures startup log ordering is clear.
    import chromadb as _chromadb
    import openai as _openai

    chroma_client = _chromadb.HttpClient(
        host=settings.chromadb_host,
        port=settings.chromadb_port,
    )
    # VectorSearch.search calls openai.embeddings.create synchronously inside
    # the async coroutine. A sync openai.OpenAI client is required here — passing
    # AsyncOpenAI would return a coroutine instead of a response object.
    sync_openai = _openai.OpenAI(
        api_key=settings.openai_api_key,
        base_url=settings.openai_base_url,
    )
    vector_search = VectorSearch(
        chroma_client=chroma_client,
        openai_client=sync_openai,
    )
    logger.info(
        "chromadb_client_created",
        host=settings.chromadb_host,
        port=settings.chromadb_port,
    )

    # --- OpenAI async client ---
    # AsyncOpenAI is fully async — all API calls are coroutines compatible with
    # the asyncio event loop. Kept separate from sync_openai above because the
    # ReAct loop awaits each chat.completions call.
    # The base_url override allows pointing at a mock server or LiteLLM proxy.
    openai_client = _openai.AsyncOpenAI(
        api_key=settings.openai_api_key,
        base_url=settings.openai_base_url,  # None = use default OpenAI endpoint.
    )
    logger.info("openai_client_created")

    # --- PromptRegistry ---
    # Loads prompt templates from the prompts/ directory at startup.
    # Caches them in memory — prompts are static files, not database rows.
    prompt_registry = PromptRegistry(prompts_dir=settings.prompts_dir)
    logger.info("prompt_registry_loaded", prompts_dir=settings.prompts_dir)

    # --- agent_factory closure ---
    # Returns a fresh RCAAgent for each investigation. The factory pattern is
    # essential here: tools must be bound to the tenant_id of the CURRENT message.
    # A shared agent would require resetting tool bindings between messages —
    # a race condition if two messages arrive concurrently.
    # Captured variables (db_pool, bm25_index, vector_search, reranker,
    # openai_client, prompt_registry) are shared safely across investigations
    # because they are thread/coroutine-safe (asyncpg pool manages connections,
    # the reranker is read-only inference).
    def agent_factory(tenant_id: str, rca_id: str) -> RCAAgent:
        """Create a fresh RCAAgent with tools pre-bound to tenant_id.
        Called once per Kafka message. Each investigation gets its own agent
        instance to avoid shared mutable state between concurrent runs.
        Args:
            tenant_id: UUID string from the IncidentPayload — pre-bound to tools.
            rca_id: UUID string for this investigation (unused in tool binding
                       but available for future tool extensions that need it).
        Returns:
            RCAAgent: configured agent with all four tools registered.
        """
        agent = RCAAgent(
            openai_client=openai_client,
            prompt_registry=prompt_registry,
            max_iterations=settings.max_iterations,
            confidence_threshold=settings.confidence_threshold,
        )

        # --- Register tools with tenant_id pre-bound ---
        # functools.partial binds tenant_id and infrastructure args.
        # The LLM only provides the business-level args in the schema.
        agent.register_tool(
            name="QueryLogs",
            func=functools.partial(
                query_logs,
                tenant_id=tenant_id,
                db_pool=db_pool,
            ),
            schema=QUERY_LOGS_SCHEMA,
        )

        agent.register_tool(
            name="GetDependencies",
            func=functools.partial(
                get_dependencies,
                tenant_id=tenant_id,
                db_pool=db_pool,
            ),
            schema=GET_DEPENDENCIES_SCHEMA,
        )

        agent.register_tool(
            name="BuildTimeline",
            func=functools.partial(
                build_timeline,
                tenant_id=tenant_id,
                db_pool=db_pool,
            ),
            schema=BUILD_TIMELINE_SCHEMA,
        )

        agent.register_tool(
            name="SearchKnowledgeBase",
            func=functools.partial(
                search_knowledge_base,
                tenant_id=tenant_id,
                bm25_index=bm25_index,
                vector_search=vector_search,
                reranker=reranker,
            ),
            schema=SEARCH_KNOWLEDGE_BASE_SCHEMA,
        )

        return agent

    # --- KafkaIncidentHandler ---
    # The orchestrator: polls incidents.ready, runs the agent, fans out results.
    # metrics is injected here (Dependency Inversion) — the handler does not
    # create or import prometheus_client types; it only observes via the interface.
    handler = KafkaIncidentHandler(
        consumer=consumer,
        producer=producer,
        agent_factory=agent_factory,
        stream_producer=stream_producer,
        metrics=rca_metrics,
        db_pool=db_pool,
    )

    # --- Register observers (Observer Pattern) ---
    # Order matters: PostgresResultSaver FIRST so the DB row exists before
    # KafkaResultPublisher signals downstream consumers.
    handler.register_observer(PostgresResultSaver(repository=rca_repository))
    handler.register_observer(KafkaResultPublisher(producer=producer))
    logger.info("observers_registered")

    # --- Graceful shutdown via asyncio.Event ---
    # stop_event is set by SIGTERM/SIGINT signal handlers registered below.
    stop_event = asyncio.Event()

    # loop.add_signal_handler registers callbacks that run inside the event loop
    # thread — safe for setting asyncio primitives (unlike signal.signal which
    # runs in an arbitrary OS signal thread and cannot safely call asyncio APIs).
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop_event.set)
    loop.add_signal_handler(signal.SIGINT, stop_event.set)
    logger.info("signal_handlers_registered", signals=["SIGTERM", "SIGINT"])

    # --- Launch consumer loop as a concurrent asyncio task ---
    # create_task schedules consume_loop to run concurrently with
    # stop_event.wait below. Without create_task, consume_loop would
    # block here and signals would never be delivered.
    consumer_task = asyncio.create_task(handler.consume_loop())
    logger.info(
        "rca_agent_service_started",
        kafka_topic=settings.kafka_incidents_topic,
        metrics_port=settings.metrics_port,
        max_iterations=settings.max_iterations,
        confidence_threshold=settings.confidence_threshold,
    )

    # --- Wait for shutdown signal ---
    # stop_event.wait yields to the event loop — consume_loop runs here.
    # This coroutine resumes only when SIGTERM or SIGINT fires.
    await stop_event.wait()
    logger.info("shutdown_signal_received")

    # --- Graceful shutdown ---
    # Signal the consumer loop to exit after the current message.
    handler.stop()

    try:
        # Wait for consume_loop to exit cleanly. Use a generous timeout so
        # an in-progress investigation can finish before the process exits.
        await asyncio.wait_for(consumer_task, timeout=30.0)
    except asyncio.TimeoutError:
        logger.warning("consumer_task_shutdown_timeout_cancelling")
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        logger.error("consumer_task_failed_on_shutdown", error=str(exc))

    # Close PostgreSQL pool last — the observer (PostgresResultSaver) may still
    # be writing the result of the final in-progress investigation.
    await db_pool.close()
    logger.info("rca_agent_service_stopped")


def main() -> None:
    """Synchronous entry point. Creates the asyncio event loop and runs _run."""
    logger.info(
        "rca_agent_service_initialising",
        kafka_bootstrap_servers=settings.kafka_bootstrap_servers,
        kafka_topic=settings.kafka_incidents_topic,
        metrics_port=settings.metrics_port,
    )
    # asyncio.run creates a new event loop, runs _run to completion, then
    # closes the loop and finalises all async generators. Preferred over the
    # deprecated loop.run_until_complete pattern.
    asyncio.run(_run())


if __name__ == "__main__":
    main()
