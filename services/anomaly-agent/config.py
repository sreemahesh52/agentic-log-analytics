"""Configuration for the anomaly-agent service.
pydantic-settings reads values from environment variables at import time.
If a required field has no default and the env var is absent, it raises
ValidationError immediately — the service never starts with bad config.
This is fail fast with a clear message rather than crashing later
inside a Kafka poll loop with a cryptic AttributeError.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class AnomalyAgentConfig(BaseSettings):
    """All configuration for the anomaly-agent, loaded from environment variables.
    Fields without defaults are required — missing them causes startup failure.
    Fields with defaults work out of the box inside Docker Compose where service
    names resolve via the agentic-network bridge network.
    """

    # --- Required external services ---
    # kafka_bootstrap_servers: comma-separated list, e.g. "kafka:9092"
    kafka_bootstrap_servers: str
    # postgres_url: full DSN including credentials, e.g. "postgresql://admin:admin@postgres:5432/loganalytics"
    postgres_url: str
    # redis_url: e.g. "redis://redis:6379"
    redis_url: str
    # chromadb_url: e.g. "http://chromadb:8081"
    chromadb_url: str
    # openai_api_key: never hardcoded — loaded from env, never logged
    openai_api_key: str

    # --- Kafka topic configuration ---
    # Consumer reads logs.enriched (published by the Go log-consumer after DB insert).
    kafka_input_topic: str = "logs.enriched"
    # Publisher writes confirmed anomaly alerts to the alerts topic.
    kafka_output_topic: str = "alerts"
    # Consumer group ID — Kafka tracks offsets per group, enabling replay on restart.
    kafka_consumer_group: str = "anomaly-agent"
    # How long poll blocks waiting for a message before returning None.
    # 1.0s: short enough for responsive shutdown, long enough to not busy-spin.
    kafka_poll_timeout_seconds: float = 1.0

    # --- Statistical detector parameters ---
    # z_score_threshold: how many standard deviations above the mean triggers detection.
    # 3.0 = 99.7th percentile (3-sigma rule). Lower = more sensitive, more false positives.
    zscore_threshold: float = 3.0
    # zscore_window_seconds: how far back the sliding window looks.
    # 3600s = 1 hour of baseline data used to compute the mean and std.
    zscore_window_seconds: int = 3600
    # min_data_points: minimum baseline buckets required before firing alerts.
    # Guards against false positives at startup when the baseline is too small.
    zscore_min_data_points: int = 5
    # bucket_size_seconds: each Redis key covers this many seconds of events.
    # 60s = one bucket per minute — balances granularity vs. Redis key count.
    zscore_bucket_size_seconds: int = 60

    # --- Semantic detector parameters ---
    # similarity_threshold: cosine similarity below which a message is anomalous.
    # 0.7 per spec. Lower = more sensitive (flags more messages as new patterns).
    similarity_threshold: float = 0.7
    # max_collection_size: evict oldest embeddings when ChromaDB collection exceeds this.
    max_collection_size: int = 10000
    # eviction_batch_size: how many oldest embeddings to delete per eviction cycle.
    eviction_batch_size: int = 1000

    # --- Alert deduplication ---
    # alert_cooldown_seconds: minimum seconds between two alerts for the same
    # (tenant, service) pair. Prevents the anomaly-agent from publishing hundreds
    # of identical alerts when a Z-score spike persists across many log messages,
    # which would exhaust the LLM API budget and delay cross-service CASCADE detection.
    # 60s = at most one alert per service per minute during a sustained spike.
    alert_cooldown_seconds: int = 60

    # --- Observability ---
    # metrics_port: Prometheus scrape endpoint served by prometheus_client.start_http_server.
    # Port 8085 per the project spec — docker-compose healthcheck curls /metrics on this port.
    metrics_port: int = 8085

    # --- Logging ---
    log_level: str = "INFO"

    # --- Prompts ---
    # prompts_dir: absolute path to the prompts directory inside the container.
    # Mounted as a read-only volume from the repo root prompts/ directory.
    # In local development outside Docker, set PROMPTS_DIR=../../prompts.
    prompts_dir: str = "/app/prompts"

    # extra="ignore" silences warnings about unrecognised env vars in the container.
    # This avoids noise from Docker Compose injecting internal env vars (e.g. PATH).
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


# Module-level singleton — created once at import time.
# pydantic-settings validates all fields here, so misconfiguration surfaces
# before any Kafka consumer or Redis client is created.
config = AnomalyAgentConfig()
