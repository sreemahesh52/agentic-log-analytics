# --- Semantic Cache service configuration ---
# pydantic-settings reads every field from the process environment (or .env).
# Fields with no default are REQUIRED — the process exits at import time
# with a clear ValidationError if they are missing. This is
# fail fast, never silently run with missing configuration.
# Why pydantic-settings over os.getenv?
# pydantic-settings gives us type coercion (str→int, str→float) and a
# single explicit schema for all config. os.getenv scatters defaults
# across the codebase and silently returns None for missing vars.

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Semantic Cache service settings. All values from environment variables."""

    # --- Kafka ---
    # Required: no default forces explicit configuration at deploy time.
    kafka_bootstrap_servers: str
    kafka_consumer_group: str = "semantic-cache-group"
    # Topic consumed by this service (produced by context-compressor).
    kafka_input_topic: str = "incidents.compressed"
    # On cache MISS: incident forwarded here for the model router to handle.
    kafka_output_miss_topic: str = "incidents.routed"
    # On cache HIT: cached RCAResult published directly — LLM call skipped.
    kafka_output_hit_topic: str = "agent.results"
    # Dead-letter queue for messages that fail all retry attempts.
    kafka_dlq_topic: str = "logs.dlq"
    # How many Kafka publish retries before a message goes to the DLQ.
    kafka_max_retries: int = 3

    # --- Redis ---
    # Required: Redis is the backing store for all cache entries.
    redis_url: str

    # --- OpenAI ---
    # Required: embeddings are used to compute semantic similarity between
    # incident descriptions. Without a key the service cannot function.
    openai_api_key: str

    # --- Cache behaviour ---
    # Cosine similarity must exceed this value to count as a cache hit.
    # 0.92 is intentionally high: a wrong cache hit (returning incorrect
    # root cause) is worse than a miss (paying for a fresh LLM call).
    cache_similarity_threshold: float = 0.92
    # TTL per cache entry in seconds (default = 24 hours).
    # After TTL, Redis auto-deletes the key. Stale incident patterns
    # are not kept indefinitely — system behaviour evolves over time.
    cache_ttl_seconds: int = 86400

    # --- Service ---
    # Port on which the Prometheus HTTP server listens for scraping.
    metrics_port: int = 8088
    log_level: str = "INFO"

    # env_file=".env" allows local overrides without changing compose files.
    # env_file_encoding ensures non-ASCII characters in .env are handled.
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )


# Module-level singleton — created once at import, validated immediately.
# main.py imports this and injects it into all components that need config.
# Never instantiate Settings inside business logic classes.
settings = Settings()
