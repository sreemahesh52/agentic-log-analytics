# --- Settings module ---
# pydantic-settings reads environment variables and validates types at import time.
# If a required field has no default and the env var is missing, it raises
# ValidationError immediately — the service never starts with bad config.
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All configuration for the API gateway, loaded from environment variables."""

    # Required — no default. pydantic-settings raises ValidationError at startup
    # if POSTGRES_URL is not set, printing exactly which variable is missing.
    postgres_url: str

    # Defaults point to Docker Compose service names — work out of the box
    # when all containers share the agentic-network bridge network.
    log_ingestion_url: str = "http://log-ingestion:8082"
    kafka_bootstrap_servers: str = "kafka:9092"
    # incidents.ready: the gateway publishes to this topic when triggering an RCA
    # manually via POST /api/v1/investigations/trigger.
    kafka_incidents_topic: str = "incidents.ready"
    redis_url: str = "redis://redis:6379"
    log_level: str = "INFO"

    # How long a verified tenant dict stays in the in-memory cache before
    # the next request re-checks the database. 60 s balances DB load vs
    # how quickly a revoked key stops working.
    api_key_cache_ttl_seconds: int = 60

    # env_file=".env" lets developers override values locally without
    # touching the environment. env vars always take precedence over .env.
    model_config = SettingsConfigDict(env_file=".env")


# Module-level singleton — imported by every module that needs config.
# pydantic-settings validates all fields here, at import time, so startup
# fails immediately with a clear message if any required var is absent.
settings = Settings()
