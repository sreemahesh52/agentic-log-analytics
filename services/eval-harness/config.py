# --- Eval Harness settings ---
# pydantic-settings reads environment variables and validates types at import time.
# Missing required fields raise ValidationError immediately — the service never
# starts with incomplete configuration (fail-fast config).

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All configuration for the eval harness, loaded from environment variables."""

    # --- Required — no defaults. Missing env vars cause immediate startup failure. ---

    # Kafka broker address: e.g. "kafka:9092"
    kafka_bootstrap_servers: str

    # PostgreSQL connection string — must include user, password, host, db name.
    postgres_url: str

    # Redis connection string — e.g. "redis://redis:6379"
    redis_url: str

    # ChromaDB HTTP API base URL — e.g. "http://chromadb:8000"
    chromadb_url: str

    # OpenAI API key — never logged, never committed to git.
    openai_api_key: str

    # Slack incoming webhook URL — never logged, never committed to git.
    # Required so operators get CRITICAL + failing alerts immediately.
    slack_webhook_url: str

    # --- Kafka topic / consumer config ---
    # agent.results: published by both the RCA Agent (fresh investigations) and
    # the Semantic Cache service (cache-hit results). Eval harness consumes from here.
    kafka_input_topic: str = "agent.results"

    # eval-harness-group: consumer group ID. Every eval-harness replica in the same
    # group shares the partition load — only one replica processes each message.
    kafka_consumer_group: str = "eval-harness-group"

    # eval.dlq: dead-letter queue for messages the handler cannot process after
    # all retries. Operators can replay from this topic after fixing the root cause.
    kafka_dlq_topic: str = "eval.dlq"

    # --- Evaluation thresholds ---
    # auto_learn: "true" enables the Self-Learning Indexer. Stored as a string
    # because environment variables are always strings — never a Python bool.
    # Comparison: settings.auto_learn == "true" (not `is True`).
    auto_learn: str = "false"

    # similarity_threshold: minimum cosine similarity for SemanticCache.get to
    # return a hit. 0.85 is the same threshold used by SimilarityStrategy.
    similarity_threshold: float = 0.85

    # cache_ttl_seconds: how long a cached RCA result lives in Redis before expiry.
    # 3600 s (1 hour) matches the semantic-cache service default.
    cache_ttl_seconds: int = 3600

    # --- Self-learning thresholds ---
    # Only auto-index results where faithfulness > learn_faithfulness_threshold.
    # 0.8 per PROJECT-SPEC: "faithfulness>0.8" for auto-learn.
    learn_faithfulness_threshold: float = 0.8

    # Only auto-index results where hallucination > learn_hallucination_threshold.
    # 0.7 per PROJECT-SPEC: "hallucination>0.7" for auto-learn.
    learn_hallucination_threshold: float = 0.7

    # --- Slack notification threshold ---
    # Notify on CRITICAL severity + faithfulness above this threshold.
    # 0.7 per PROJECT-SPEC: "faithfulness>0.7: triggers Slack Notifier".
    slack_faithfulness_threshold: float = 0.7

    # --- Prometheus ---
    # metrics_port: HTTP port for the Prometheus /metrics scrape endpoint.
    # 8091 is the eval-harness reserved port (see PROJECT-SPEC service table).
    metrics_port: int = 8091

    # --- Prompts ---
    # prompts_dir: path to the repo-root prompts/ directory.
    # Mounted as a read-only volume from ../prompts:/app/prompts:ro in docker-compose.
    # The PromptRegistry loads faithfulness_v1.txt and hallucination_v1.txt from here.
    prompts_dir: str = "/app/prompts"

    # --- General ---
    log_level: str = "INFO"

    # env_file=".env" lets local development override values without touching
    # the shell environment. Container env vars always take precedence over .env.
    model_config = SettingsConfigDict(env_file=".env")


# Module-level singleton — imported by every module that needs config.
# Validation runs here, at import time, so startup fails immediately with a
# clear message identifying exactly which required variable is absent.
settings = Settings()
