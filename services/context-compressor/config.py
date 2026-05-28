# --- Service configuration ---
# pydantic-settings validates every value at import time.
# The service exits immediately with a clear message if a required variable
# is missing — no silent defaults that mask misconfiguration in production.
# Every configurable value is defined here. No magic numbers exist anywhere
# else in the service: business logic reads from a Settings instance, never
# from os.environ directly.

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All runtime configuration for the context-compressor service.
    Populated from environment variables (or an optional .env file for local dev).
    Every variable with no default MUST be present in the environment or the
    process raises ValidationError and exits — fail fast.
    """

    # --- Kafka ---
    # KAFKA_BOOTSTRAP_SERVERS: comma-separated broker list, e.g. "kafka:9092"
    kafka_bootstrap_servers: str

    # Consumer group ID: stable across restarts so Kafka tracks our read offset.
    kafka_consumer_group: str = "context-compressor-group"

    # Input topic: incidents published by the alert-correlator.
    kafka_incidents_topic: str = "incidents"

    # Output topic: incidents enriched with compressed log context.
    kafka_incidents_compressed_topic: str = "incidents.compressed"

    # DLQ topic: messages that fail validation or exhausted retries.
    kafka_dlq_topic: str = "logs.dlq"

    # Maximum Kafka publish retries before writing to DLQ.
    kafka_dlq_max_retries: int = 3

    # --- PostgreSQL ---
    # Full DSN, e.g. "postgresql://admin:admin@postgres:5432/loganalytics"
    postgres_url: str

    # --- OpenAI ---
    # Required: the service calls GPT-3.5-turbo for log compression.
    openai_api_key: str

    # --- Compression logic ---
    # Token threshold: if the assembled log text has more tokens than this,
    # send to GPT-3.5-turbo for compression before the RCA agent sees it.
    # 6000 leaves headroom for the system prompt + tool results within the
    # GPT-4 context window (~8192 tokens for GPT-4, ~16k for gpt-3.5-turbo-16k).
    compression_token_threshold: int = 6000

    # Maximum log lines to fetch per affected service.
    # 500 lines × N services is the raw input; tiktoken then decides if compression
    # is needed. Setting this too high raises DB query cost; too low loses context.
    logs_limit_per_service: int = 500

    # --- Observability ---
    # Port where the prometheus_client HTTP server serves /metrics.
    # Docker healthcheck and Prometheus scraper both target this port.
    metrics_port: int = 8087
    log_level: str = "INFO"

    # --- Prompt registry ---
    # Directory mounted from the repo root prompts/ folder at runtime.
    prompts_dir: str = "/app/prompts"

    # pydantic-settings loads .env if present; env vars always take precedence.
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
