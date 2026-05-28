"""
Configuration for the RCA Agent service.
all configuration is loaded from environment
variables at import time. If a required variable is absent or the wrong type,
pydantic-settings raises ValidationError before any Kafka consumer or DB pool
is created — fail fast with a precise error message, not a cryptic runtime crash.
Why separate config.py instead of reading os.environ directly?
Type coercion: os.environ always returns strings. pydantic-settings converts
CONFIDENCE_THRESHOLD="0.85" to float(0.85) automatically. Without it, every
consumer of an env var must call float/int/bool(os.getenv(...)) manually, with
no validation of what happens if the value is missing or malformed.
Why port 8090?
Each service in the docker-compose stack exposes Prometheus metrics on a unique
port. 8090 is the RCA Agent's designated scrape port, matching the docker-compose
healthcheck and the Prometheus scrape config.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All configuration for the RCA Agent, loaded from environment variables."""

    # --- Database ---
    # Required — no default. Missing POSTGRES_URL causes immediate ValidationError.
    postgres_url: str

    # --- Kafka ---
    kafka_bootstrap_servers: str = "kafka:9092"

    # Consumer group ID — all rca-agent replicas share one group so each
    # incidents.ready message is processed by exactly one instance.
    kafka_consumer_group: str = "rca-agent-group"

    # incidents.ready: the model-router publishes enriched incidents here.
    kafka_incidents_topic: str = "incidents.ready"

    # --- OpenAI ---
    # Required — no default. The RCA Agent cannot run without an API key.
    openai_api_key: str

    # Base URL override for test environments (e.g. LiteLLM proxy, mock server).
    # None means use the default OpenAI API endpoint.
    openai_base_url: str | None = None

    # --- Agent tuning ---
    # Maximum ReAct loop iterations before LowConfidenceError is raised.
    # 15 iterations is enough for complex cascade incidents while capping
    # token cost. Increase with caution — each iteration adds input_tokens.
    max_iterations: int = 15

    # Minimum confidence required for the agent to accept a stop response.
    # 0.8 = 80% confident. Values below this prompt the agent to gather more evidence.
    confidence_threshold: float = 0.8

    # --- ChromaDB (vector store for SearchKnowledgeBase tool) ---
    chromadb_host: str = "chromadb"
    chromadb_port: int = 8000

    # --- Prometheus metrics ---
    # Port for the metrics HTTP server. Exposed in the Dockerfile and docker-compose.
    metrics_port: int = 8090

    # --- Prompts ---
    # Directory containing versioned prompt templates (prompts/rca_agent/v1.txt etc).
    # Mounted as a read-only volume from the repo-root prompts/ directory so prompt
    # changes take effect without rebuilding the Docker image.
    prompts_dir: str = "/app/prompts"

    # --- Logging ---
    log_level: str = "INFO"

    # env_file=".env" lets developers override values locally without touching
    # the environment. Environment variables always take precedence over .env.
    model_config = SettingsConfigDict(env_file=".env")


# Module-level singleton — imported by every module that needs configuration.
# Validation runs here at import time so the service fails fast on bad config.
settings = Settings()
