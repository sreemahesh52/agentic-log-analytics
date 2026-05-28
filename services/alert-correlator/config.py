# --- Service configuration ---
# pydantic-settings validates every value at import time.
# The service exits immediately with a clear message if a required variable
# is missing — no silent defaults that mask misconfiguration in production.

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All runtime configuration for the alert-correlator service.
    Populated from environment variables (or an optional .env file for local dev).
    Every variable that has no default MUST be present in the environment or the
    process will raise ValidationError and refuse to start.
    """

    # --- Kafka ---
    # KAFKA_BOOTSTRAP_SERVERS: comma-separated list, e.g. "kafka:9092"
    kafka_bootstrap_servers: str

    # Consumer group ID: a stable ID means Kafka tracks our read offset so
    # the service can resume from where it left off after a restart.
    kafka_consumer_group: str = "alert-correlator-group"

    # Topics: explicit names prevent hard-coding in business logic.
    kafka_alerts_topic: str = "alerts"
    kafka_incidents_topic: str = "incidents"
    kafka_dlq_topic: str = "logs.dlq"

    # --- PostgreSQL ---
    # Full DSN, e.g. "postgresql://admin:admin@postgres:5432/loganalytics"
    postgres_url: str

    # --- Correlation logic ---
    # Window in seconds: alerts from distinct services within this window are
    # grouped into a CascadeIncident. 60s is wide enough to catch cascades from
    # a single root cause (e.g. DB down → all services alert within seconds)
    # but narrow enough to avoid grouping unrelated incidents.
    correlation_window_seconds: int = 60

    # --- Reliability ---
    # Maximum Kafka publish retries before writing to DLQ.
    kafka_dlq_max_retries: int = 3

    # --- Observability ---
    # Port where Prometheus metrics and the healthcheck endpoint are served.
    metrics_port: int = 8086
    log_level: str = "INFO"

    # pydantic-settings loads .env if present; env vars always take precedence.
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
