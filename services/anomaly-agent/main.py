"""Anomaly-agent service entry point.
Startup sequence:
  1. Configure structlog (logging first, before any import side effects)
  2. Load and validate config (fail fast on missing env vars)
  3. Create all external service clients (Redis, ChromaDB, OpenAI, psycopg2, Kafka)
  4. Instantiate all components (detectors, verifier, repositories, publishers)
  5. Wire components into AnomalyOrchestrator (Dependency Inversion)
  6. Start Prometheus HTTP server on metrics_port (background thread)
  7. Run Kafka consumer loop until SIGTERM/SIGINT
  8. Graceful shutdown: close Kafka consumer, flush PostgreSQL connection
Why a single process (not asyncio):
  All dependencies — confluent-kafka, psycopg2, redis-py, chromadb, openai —
  are synchronous libraries. Using asyncio would require wrapping every call in
  asyncio.to_thread, adding complexity with no benefit. The consumer loop is
  single-threaded and synchronous: one message processed at a time, which is
  appropriate for a stateful detector that maintains Redis baselines per service.
"""

import logging
import signal
import sys
import threading

import psycopg2
import psycopg2.extras
import redis
import structlog
from openai import OpenAI
from prometheus_client import start_http_server

import chromadb

from config import config
from detection.llm_verifier import LLMVerifier
from detection.semantic import SemanticDetector
from detection.statistical import StatisticalDetector
from kafka.consumer import KafkaLogConsumer
from kafka.publisher import KafkaAlertPublisher
from metrics import create_metrics
from orchestrator import AnomalyOrchestrator
from postgres.repository import LogRepository, PostgresAlertRepository
from prompt_registry import PromptRegistry


def _configure_logging() -> None:
    """Configure structlog JSON output. Must run before any logger is used.
    structured logging as the VERY FIRST action in main.
    Called at module top-level before any other function.
    """
    log_level = getattr(logging, config.log_level.upper(), logging.INFO)

    structlog.configure(
        processors=[
            # merge_contextvars attaches any context bound with structlog.contextvars
            # to every log line in the current thread automatically.
            structlog.contextvars.merge_contextvars,
            # add_log_level inserts "level": "info" into every JSON log line.
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            # JSONRenderer produces one JSON object per line — machine-parseable.
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    logging.basicConfig(stream=sys.stdout, level=log_level, format="%(message)s")


# configure logging as the VERY FIRST action at module level.
# This runs before any import that might capture a pre-configuration logger.
_configure_logging()
logger = structlog.get_logger()

# --- Shutdown coordination ---
# threading.Event is used instead of a boolean flag because .wait is
# interruptible by signals — the main loop can block on .wait(0) and still
# receive SIGTERM cleanly. A plain bool would require a busy loop.
_shutdown = threading.Event()


def _handle_signal(signum: int, frame: object) -> None:
    """Signal handler: set the shutdown event to exit the consumer loop.
    SIGTERM is sent by Docker on 'docker stop' and Kubernetes pod eviction.
    SIGINT is sent by Ctrl+C in development.
    Both trigger the same graceful shutdown path.
    """
    logger.info("shutdown_signal_received", signal=signum)
    # .set makes _shutdown.is_set return True, causing the consumer loop to exit.
    _shutdown.set()


def _create_postgres_connection() -> psycopg2.extensions.connection:
    """Create and return a psycopg2 connection with UTC timezone set.
    The connection is long-lived — shared across LogRepository and
    PostgresAlertRepository for the lifetime of the process.
    server_settings sets timezone=UTC on every connection,
    regardless of the PostgreSQL server's default timezone setting.
    """
    conn = psycopg2.connect(
        config.postgres_url,
        # options: SET commands sent on every new connection.
        options="-c timezone=UTC",
    )
    # autocommit=False (default): explicit commit required per INSERT.
    # PostgresAlertRepository.publish_alert calls conn.commit after each INSERT.
    conn.autocommit = False
    return conn


def _create_redis_client() -> redis.Redis:
    """Create and return a Redis client from REDIS_URL.
    decode_responses=False: keys and values are returned as bytes.
    StatisticalDetector.py handles both bytes and str keys explicitly.
    """
    # redis.from_url parses "redis://host:port/db" — safer than manual host/port split.
    return redis.from_url(config.redis_url, decode_responses=False)


def _create_chromadb_client() -> chromadb.HttpClient:
    """Create and return a ChromaDB HTTP client from CHROMADB_URL.
    The URL is split into host and port because chromadb.HttpClient takes
    separate host/port parameters, not a full URL string.
    """
    # Parse the URL to extract host and port.
    # Expected format: "http://hostname:port" — strip the scheme first.
    url = config.chromadb_url
    # Strip "http://" or "https://" prefix to get "hostname:port"
    host_port = url.replace("https://", "").replace("http://", "")
    if ":" in host_port:
        host, port_str = host_port.rsplit(":", 1)
        port = int(port_str)
    else:
        host = host_port
        # chromadb default port
        port = 8000

    return chromadb.HttpClient(host=host, port=port)


def main() -> None:
    """Wire all dependencies and run the Kafka consumer loop.
    This function:
      1. Creates all external service clients.
      2. Instantiates all components with injected dependencies.
      3. Starts the Prometheus HTTP server.
      4. Runs the consumer loop until _shutdown is set.
      5. Performs graceful shutdown.
    """
    logger.info(
        "anomaly_agent_starting",
        kafka_topic=config.kafka_input_topic,
        metrics_port=config.metrics_port,
    )

    # --- Step 1: Create external service clients ---
    # These are the only places in the service where connections are opened.
    # All other code receives them via injection.

    # Redis: used by StatisticalDetector for sliding-window baseline storage.
    redis_client = _create_redis_client()
    logger.info("redis_client_ready", url=config.redis_url.split("@")[-1])

    # ChromaDB: used by SemanticDetector for embedding storage and similarity queries.
    chroma_client = _create_chromadb_client()
    logger.info("chromadb_client_ready", url=config.chromadb_url)

    # OpenAI: used by SemanticDetector (embeddings) and LLMVerifier (classification).
    # api_key is read from environment — never hardcoded or logged.
    openai_client = OpenAI(api_key=config.openai_api_key)
    logger.info("openai_client_ready")

    # PostgreSQL: used by LogRepository (read) and PostgresAlertRepository (write).
    db_conn = _create_postgres_connection()
    db_host = config.postgres_url.split("@")[-1]  # log host only, not credentials
    logger.info("postgres_connection_ready", host=db_host)

    # --- Step 2: Create Prometheus metrics ---
    # create_metrics registers all Counters on the global CollectorRegistry.
    # Must be called before start_http_server so all metrics are visible.
    metrics = create_metrics()

    # --- Step 3: Start Prometheus HTTP server ---
    # start_http_server launches a background thread serving /metrics on metrics_port.
    # The docker-compose healthcheck curls this endpoint to verify the service is up.
    # This thread runs for the lifetime of the process — no cleanup needed.
    start_http_server(config.metrics_port)
    logger.info("prometheus_metrics_server_started", port=config.metrics_port)

    # --- Step 4: Instantiate all components (Dependency Inversion) ---
    # Every component receives its dependencies via constructor injection.
    # main is the only place where concrete types are named — all other
    # code depends on abstractions (BaseAnomalyDetector, AlertPublisher, etc.).

    prompt_registry = PromptRegistry(prompts_dir=config.prompts_dir)

    statistical_detector = StatisticalDetector(
        redis_client=redis_client,
        window_seconds=config.zscore_window_seconds,
        z_score_threshold=config.zscore_threshold,
        min_data_points=config.zscore_min_data_points,
        bucket_size_seconds=config.zscore_bucket_size_seconds,
    )

    semantic_detector = SemanticDetector(
        chroma_client=chroma_client,
        openai_client=openai_client,
        similarity_threshold=config.similarity_threshold,
        max_collection_size=config.max_collection_size,
        eviction_batch_size=config.eviction_batch_size,
    )

    llm_verifier = LLMVerifier(
        openai_client=openai_client,
        prompt_registry=prompt_registry,
    )

    log_repository = LogRepository(connection=db_conn)

    # --- Step 5: Create alert publishers (Observer list) ---
    # Both publishers receive every confirmed alert independently.
    # Order: Postgres first (durable record), then Kafka (event stream).
    # If Kafka fails, the alert is still in PostgreSQL — not lost entirely.
    postgres_publisher = PostgresAlertRepository(connection=db_conn)
    kafka_publisher = KafkaAlertPublisher(
        bootstrap_servers=config.kafka_bootstrap_servers,
        topic=config.kafka_output_topic,
    )
    # alert_publishers list = the Observer registry.
    # Adding a new publisher (e.g., SlackNotifier) requires only appending here.
    alert_publishers = [postgres_publisher, kafka_publisher]

    # --- Step 6: Wire the orchestrator ---
    orchestrator = AnomalyOrchestrator(
        statistical_detector=statistical_detector,
        semantic_detector=semantic_detector,
        llm_verifier=llm_verifier,
        alert_publishers=alert_publishers,
        log_repository=log_repository,
        metrics=metrics,
        alert_cooldown_seconds=config.alert_cooldown_seconds,
    )

    # --- Step 7: Create Kafka consumer ---
    consumer = KafkaLogConsumer(
        bootstrap_servers=config.kafka_bootstrap_servers,
        group_id=config.kafka_consumer_group,
        topic=config.kafka_input_topic,
        poll_timeout_seconds=config.kafka_poll_timeout_seconds,
    )

    # --- Step 8: Register signal handlers for graceful shutdown ---
    # signal.signal must be called from the main thread — Python restriction.
    # SIGTERM: Docker/Kubernetes stop. SIGINT: Ctrl+C in development.
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    logger.info("anomaly_agent_ready", topic=config.kafka_input_topic)

    # --- Step 9: Consumer loop ---
    # The loop runs until _shutdown is set by a signal handler.
    # Each iteration:
    #   1. poll for up to poll_timeout_seconds
    #   2. If a valid LogEvent arrives, pass it to the orchestrator
    #   3. The orchestrator runs detection, verification, and publishing
    try:
        while not _shutdown.is_set():
            # consumer.poll returns a LogEvent or None.
            # None = no message in the poll window OR invalid message (logged separately).
            log_event = consumer.poll()
            if log_event is None:
                # No message this tick — loop back and poll again.
                continue

            # process_log runs the full detection pipeline.
            # It catches all internal errors — the consumer loop never crashes on
            # a single bad message.
            orchestrator.process_log(log_event)

    except Exception as exc:
        # Unexpected error outside the orchestrator (e.g., Kafka broker failure
        # that KafkaException surfaces from consumer.poll).
        # Log at ERROR and exit — Docker will restart the container.
        logger.error(
            "anomaly_agent_fatal_error",
            error=str(exc),
            error_type=type(exc).__name__,
        )
        sys.exit(1)

    finally:
        # --- Graceful shutdown ---
        # Always runs, whether the loop exited cleanly or via exception.
        logger.info("anomaly_agent_shutting_down")

        # Close the Kafka consumer: commits pending offsets, sends LeaveGroup.
        # Without this, the broker waits for a heartbeat timeout (~30s) before
        # reassigning partitions to another consumer in the group.
        try:
            consumer.close()
        except Exception as exc:
            logger.warning("consumer_close_error", error=str(exc))

        # Close the PostgreSQL connection cleanly.
        # Without this, PostgreSQL holds the connection open until its idle timeout.
        try:
            db_conn.close()
        except Exception as exc:
            logger.warning("db_connection_close_error", error=str(exc))

        logger.info("anomaly_agent_stopped")


# Standard entry point guard: prevents main from running on module import.
# Without this, importing main.py in tests would start the Kafka consumer loop.
if __name__ == "__main__":
    main()
