"""Semantic cache writer for the eval harness.
Stores quality-checked RCA results in the shared Redis semantic cache.
Uses the same Redis key format as the semantic-cache service:
  cache:{tenant_id}:{uuid4}
This class reimplements only the write path (set) to avoid cross-service
coupling — services cannot import from sibling service directories in Docker.
The key format is intentionally identical so SemanticCache.get in the
semantic-cache service can find entries written here.
Why the eval harness owns cache writes (not the RCA agent)?
Only evaluations that PASS quality checks should enter the cache. If the RCA
agent cached every result immediately, low-quality outputs (faithfulness < 0.7)
would pollute future lookups. The eval harness gates cache writes on pass status.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

import structlog

log = structlog.get_logger(__name__)

# Redis key formats — must match the semantic-cache service exactly.
# Any change here requires a matching change in services/semantic-cache/cache.py.
_ENTRY_KEY_FMT = "cache:{tenant_id}:{uid}"

# OpenAI model used for embedding incident descriptions.
# Must match the model used by SemanticCache._embed — changing this breaks
# cosine similarity comparisons between entries from different services.
_EMBEDDING_MODEL = "text-embedding-3-small"


class SemanticCacheWriter:
    """Write-only semantic cache client for the eval harness.
    Embeds the incident description using text-embedding-3-small and stores the
    RCA result as a Redis HASH with embedding, rca_result JSON, and created_at.
    The entry expires after ttl_seconds so stale patterns are auto-evicted.
    Fail-open: any error (OpenAI unavailable, Redis down) is logged and
    silently swallowed — the eval pipeline must never be blocked by cache.
    """

    def __init__(
        self,
        redis_client: object,
        openai_client: object,
        ttl_seconds: int,
    ) -> None:
        """Inject all dependencies.
        Args:
            redis_client: Async Redis client (from redis.asyncio).
            openai_client: AsyncOpenAI client for text-embedding-3-small.
            ttl_seconds: Redis entry TTL — entries expire automatically.
        """
        self._redis = redis_client
        self._openai = openai_client
        self._ttl = ttl_seconds

    async def set(
        self,
        tenant_id: str,
        incident_description: str,
        rca_result: dict,
    ) -> None:
        """Store an RCA result in Redis with a semantic embedding key.
        Args:
            tenant_id: Tenant namespace — entries are tenant-scoped.
            incident_description: Text used to generate the embedding lookup key.
            rca_result: Full RCA result dict to cache for future hits.
        """
        # --- Embed the incident description ---
        try:
            response = await self._openai.embeddings.create(
                model=_EMBEDDING_MODEL,
                input=incident_description,
            )
            embedding = response.data[0].embedding
        except Exception as exc:
            # OpenAI unavailable or rate-limited — skip this cache write.
            # The RCA result is still saved to PostgreSQL; only caching is skipped.
            log.warning(
                "cache_write_embed_failed_skipping",
                error=str(exc),
                tenant_id=tenant_id,
            )
            return

        # --- Build the Redis key ---
        # uuid4 suffix ensures concurrent writes from multiple replicas do not
        # overwrite each other. Each evaluation produces a distinct Redis entry.
        uid = str(uuid4())
        entry_key = _ENTRY_KEY_FMT.format(tenant_id=tenant_id, uid=uid)

        # --- Store the entry ---
        try:
            await self._redis.hset(
                entry_key,
                mapping={
                    "embedding": json.dumps(embedding),
                    "rca_result": json.dumps(rca_result),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            # EXPIRE sets the TTL. Without it, the key lives forever — stale
            # incident patterns would never be evicted after system changes.
            await self._redis.expire(entry_key, self._ttl)

            log.info(
                "cache_entry_written",
                tenant_id=tenant_id,
                key=entry_key,
                ttl_seconds=self._ttl,
            )
        except Exception as exc:
            log.warning(
                "cache_write_redis_failed_skipping",
                error=str(exc),
                tenant_id=tenant_id,
            )
