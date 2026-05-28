# --- Security Middleware configuration ---
# pydantic-settings reads every field from environment variables at import time.
# Fields with no default are REQUIRED: if the env var is absent, pydantic raises
# a ValidationError immediately with a clear message listing the missing variable.
# This is the "fail fast" principle — the service never starts with incomplete config.

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All configuration for the security-middleware, loaded from environment variables.
    Required fields (no default) cause an immediate startup failure if absent.
    Optional fields have production-safe defaults matching the Docker Compose setup.
    """

    # Required — service cannot run without a Kafka broker.
    # Expected format: "kafka:9092" (Docker Compose) or "localhost:29092" (local dev).
    kafka_bootstrap_servers: str

    # Required — service cannot write audit records without a DB connection.
    # Expected format: "postgresql://user:pass@host:port/dbname"
    postgres_url: str

    # --- Kafka topic names ---
    # These must match the topics created by kafka-init in docker-compose.yml.
    # Changing them here without also changing kafka-init breaks the pipeline.
    kafka_input_topic: str = "logs.raw"
    kafka_output_clean_topic: str = "logs.raw.clean"
    kafka_security_events_topic: str = "security.events"

    # Consumer group ID — all replicas of this service share a group so Kafka
    # partitions messages across them rather than delivering each to all replicas.
    kafka_consumer_group: str = "security-middleware-group"

    # Port for the Prometheus metrics HTTP server (not the main Kafka service port).
    # Scraped by Prometheus at http://security-middleware:8083/metrics.
    metrics_port: int = 8083

    # Minimum log level. Accepted values: DEBUG, INFO, WARN, ERROR.
    log_level: str = "INFO"

    # env_file=".env" allows local development overrides without touching the
    # system environment. Environment variables always take precedence over .env.
    model_config = SettingsConfigDict(env_file=".env")


# Module-level singleton — validated once at import time, reused everywhere.
# Any module that imports `settings` gets the same validated instance.
settings = Settings()
