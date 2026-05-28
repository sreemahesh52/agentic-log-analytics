"""Eval Harness service entry point — composition root and startup orchestrator.
This file wires all dependencies together and starts the Kafka consumer loop.
No business logic lives here — only dependency construction and lifecycle management.
Startup order:
  1. config (pydantic-settings validates all env vars, fails fast if missing)
  2. _configure_logging — must run before any other import uses logging
  3. Start Prometheus metrics server on port 8091
  4. Create asyncpg connection pool (UTC timezone enforced per-connection)
  5. Create Redis async client
  6. Create ChromaDB HTTP client
  7. Create OpenAI AsyncClient
  8. Create httpx AsyncClient (for Slack webhook)
  9. Create PromptRegistry
 10. Create EvaluatorFactory faithfulness pipeline
 11. Create HallucinationEvaluator
 12. Create EvalRepository
 13. Create SelfLearner
 14. Create SlackNotifier
 15. Create SemanticCacheWriter
 16. Create EvalKafkaHandler and start consumer loop
 17. Handle SIGTERM/SIGINT for graceful shutdown
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

import asyncpg
import chromadb
import httpx
import structlog
from openai import AsyncOpenAI
from redis.asyncio import Redis

from cache_writer import SemanticCacheWriter
from config import settings
from evaluation.factory import EvaluatorFactory
from evaluation.hallucination import HallucinationEvaluator
from kafka.handler import EvalKafkaHandler
from metrics import KNOWLEDGE_BASE_SIZE, start_metrics_server
from postgres.repository import EvalRepository
from prompt_registry import PromptRegistry
from self_learner import SelfLearner
from slack_notifier import SlackNotifier


def _configure_logging() -> None:
    """Configure structlog JSON output. must run first.
    Any module that calls logging.getLogger before this function runs will
    capture a pre-configuration logger that emits unstructured text — breaking
    log aggregation pipelines that expect JSON.
    """
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    # Sync stdlib logging level so aiokafka, asyncpg, and httpx use the same level.
    logging.basicConfig(stream=sys.stdout, level=log_level, format="%(message)s")


# configure before any module-level logger is used.
_configure_logging()
logger = structlog.get_logger()


def _create_chromadb_client() -> chromadb.HttpClient:
    """Create a ChromaDB HTTP client from CHROMADB_URL.
    chromadb.HttpClient takes separate host and port parameters, not a URL.
    This helper parses the URL string (e.g. "http://chromadb:8000") into components.
    """
    url = settings.chromadb_url
    host_port = url.replace("https://", "").replace("http://", "")
    if ":" in host_port:
        host, port_str = host_port.rsplit(":", 1)
        port = int(port_str)
    else:
        host = host_port
        port = 8000  # ChromaDB default port
    return chromadb.HttpClient(host=host, port=port)


async def _run() -> None:
    """Create all service components, start the consumer loop, and shut down cleanly."""

    # --- Prometheus metrics server ---
    # Spawns a daemon thread — non-blocking. Must start before any metric is observed.
    start_metrics_server(settings.metrics_port)
    logger.info("prometheus_server_started", port=settings.metrics_port)

    # --- PostgreSQL connection pool ---
    db_pool = await asyncpg.create_pool(
        dsn=settings.postgres_url,
        min_size=2,
        max_size=10,
        server_settings={"timezone": "UTC"},
    )
    logger.info("postgres_pool_created", min_size=2, max_size=10)

    # --- Redis async client ---
    # decode_responses=True: all values returned as str, not bytes.
    # Consistent with the semantic-cache service client configuration.
    redis_client = Redis.from_url(
        settings.redis_url,
        decode_responses=True,
    )
    logger.info("redis_client_created")

    # --- ChromaDB client ---
    chroma_client = _create_chromadb_client()
    logger.info("chromadb_client_created", url=settings.chromadb_url)

    # --- OpenAI async client ---
    # api_key is read from settings — never hardcoded, never logged.
    openai_client = AsyncOpenAI(api_key=settings.openai_api_key)
    logger.info("openai_client_created")

    # --- httpx async client (for Slack webhook) ---
    # A single AsyncClient reuses TCP connections across all Slack POST requests.
    # Timeout(5.0, connect=3.0): 3 s to establish TCP, 5 s total per request.
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(5.0, connect=3.0),
    )
    logger.info("httpx_client_created")

    # --- PromptRegistry ---
    # Loads prompt templates from the prompts/ directory (volume-mounted at /app/prompts).
    # Shared across the faithfulness and hallucination evaluators.
    prompt_registry = PromptRegistry(prompts_dir=settings.prompts_dir)
    logger.info("prompt_registry_created", prompts_dir=settings.prompts_dir)

    # --- Faithfulness evaluation pipeline ---
    # EvaluatorFactory returns the ordered list: [GroundTruthStrategy, SimilarityStrategy, HeuristicStrategy].
    # The order is fixed — changing it without updating the factory docstring is a bug.
    faithfulness_strategies = EvaluatorFactory.create_faithfulness_pipeline(
        openai_client=openai_client,
        prompt_registry=prompt_registry,
        db_pool=db_pool,
        chroma_client=chroma_client,
    )
    logger.info("faithfulness_pipeline_created", strategy_count=len(faithfulness_strategies))

    # --- Hallucination evaluator ---
    hallucination_evaluator = HallucinationEvaluator(
        openai_client=openai_client,
        prompt_registry=prompt_registry,
        db_pool=db_pool,
    )

    # --- EvalRepository ---
    eval_repository = EvalRepository(db_pool=db_pool)

    # --- SelfLearner ---
    self_learner = SelfLearner(
        db_pool=db_pool,
        chroma_client=chroma_client,
        openai_client=openai_client,
        auto_learn=settings.auto_learn,
        faithfulness_threshold=settings.learn_faithfulness_threshold,
        hallucination_threshold=settings.learn_hallucination_threshold,
    )

    # --- Initialize KB size gauge from current past_incidents counts ---
    # Without this, the Grafana KB Size panel shows "No data" until the first
    # auto-learn event fires, even when seed_incidents.py has populated the table.
    try:
        async with db_pool.acquire() as _conn:
            rows = await _conn.fetch(
                "SELECT tenant_id::text, COUNT(*) AS cnt "
                "FROM past_incidents GROUP BY tenant_id"
            )
        for row in rows:
            KNOWLEDGE_BASE_SIZE.labels(tenant=row["tenant_id"]).set(int(row["cnt"]))
        logger.info("kb_size_gauge_initialized", tenant_count=len(rows))
    except Exception as _exc:
        logger.warning("kb_size_gauge_init_failed", error=str(_exc))

    # --- SlackNotifier ---
    # webhook_url is injected — never logged by SlackNotifier or by this module.
    slack_notifier = SlackNotifier(
        http_client=http_client,
        webhook_url=settings.slack_webhook_url,
        faithfulness_threshold=settings.slack_faithfulness_threshold,
    )

    # --- SemanticCacheWriter ---
    cache_writer = SemanticCacheWriter(
        redis_client=redis_client,
        openai_client=openai_client,
        ttl_seconds=settings.cache_ttl_seconds,
    )

    # --- EvalKafkaHandler ---
    handler = EvalKafkaHandler(
        db_pool=db_pool,
        eval_repository=eval_repository,
        faithfulness_strategies=faithfulness_strategies,
        hallucination_evaluator=hallucination_evaluator,
        self_learner=self_learner,
        slack_notifier=slack_notifier,
        cache_writer=cache_writer,
    )

    # --- Graceful shutdown on SIGTERM / SIGINT ---
    # asyncio.Event allows the SIGTERM handler (a sync function) to signal
    # the async consumer loop without blocking the event loop.
    shutdown_event = asyncio.Event()

    def _handle_signal(signum: int, frame: object) -> None:
        logger.info("shutdown_signal_received", signal=signum)
        shutdown_event.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # --- Start consumer loop as a concurrent asyncio Task ---
    # asyncio.create_task schedules the consumer loop without blocking.
    # The main coroutine waits on the shutdown_event.
    consumer_task = asyncio.create_task(handler.run())

    logger.info(
        "eval_harness_started",
        topic=settings.kafka_input_topic,
        group_id=settings.kafka_consumer_group,
        auto_learn=settings.auto_learn,
    )

    # --- Wait for shutdown signal or consumer task completion ---
    done, pending = await asyncio.wait(
        [consumer_task, asyncio.ensure_future(shutdown_event.wait())],
        return_when=asyncio.FIRST_COMPLETED,
    )

    # --- Cancel the consumer task if still running ---
    if not consumer_task.done():
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

    # --- Clean up shared resources ---
    await http_client.aclose()
    await redis_client.aclose()
    await db_pool.close()

    logger.info("eval_harness_shutdown_complete")


if __name__ == "__main__":
    asyncio.run(_run())
