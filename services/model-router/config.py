# --- Model Router service configuration ---
# pydantic-settings reads every field from the process environment (or .env).
# Fields with no default are REQUIRED — the process exits at import time with
# a clear ValidationError if they are missing. This is fail fast,
# never silently run with missing configuration.
# Why pydantic-settings over os.getenv?
# pydantic-settings gives us type coercion (str→int, str→bool), a single
# explicit schema for all config, and early failure on missing vars.
# os.getenv scatters defaults across the codebase and silently returns None.

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Model Router service settings. All values from environment variables."""

    # --- Kafka ---
    # Required: no default forces explicit configuration at deploy time.
    kafka_bootstrap_servers: str
    kafka_consumer_group: str = "model-router-group"
    # Topic consumed by this service (produced by semantic-cache on cache MISS).
    kafka_input_topic: str = "incidents.routed"
    # Incidents enriched with model_id + prompt_variant published here.
    kafka_output_topic: str = "incidents.ready"
    # Dead-letter queue for messages that exhaust all retry attempts.
    kafka_dlq_topic: str = "logs.dlq"
    # How many Kafka publish retries before a message goes to the DLQ.
    kafka_max_retries: int = 3

    # --- PostgreSQL ---
    # Required: used to look up tenant model tier and daily spend.
    postgres_url: str

    # --- Routing model targets ---
    # All model names are configurable via environment variables.
    # Why? When GPT-5 releases, the operator updates the env var and restarts
    # the container — no code change, no redeploy of a new image required.
    # Hardcoding model names creates a mandatory code change + deploy gate on
    # every model upgrade, which is an unnecessary operational burden.
    model_critical_premium: str = "gpt-4-turbo"
    model_high_premium: str = "gpt-4-turbo"
    model_medium_premium: str = "gpt-3.5-turbo"
    model_low_premium: str = "gpt-3.5-turbo"
    model_any_standard: str = "gpt-3.5-turbo"
    # DAILY_BUDGET_OVERRIDE_MODEL: the model used when a tenant has exhausted
    # their daily token budget. Always the cheapest option so cost stays bounded.
    daily_budget_override_model: str = "gpt-3.5-turbo"

    # --- Routing behaviour ---
    # LOW_SKIP: when true, LOW severity incidents are discarded without calling
    # any LLM. Default false — all severities are routed. Operators set
    # LOW_SKIP=true in environments where LOW alerts are treated as noise.
    low_skip: bool = False

    # --- Tenant cache ---
    # Tenant rows (model_tier, budget) are cached in memory to reduce DB load.
    # 60 seconds: a tier change (standard→premium) propagates within one minute.
    tenant_cache_ttl_seconds: int = 60

    # --- Service ---
    # Port on which the Prometheus HTTP server listens for scraping.
    metrics_port: int = 8089
    log_level: str = "INFO"

    # env_file=".env" allows local overrides without modifying compose files.
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )


# Module-level singleton — created once at import, validated immediately.
# main.py imports this and injects it into all components that need config.
# Never instantiate Settings inside business logic classes.
settings = Settings()
