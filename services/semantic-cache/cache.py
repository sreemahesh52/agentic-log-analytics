# --- Semantic Cache: Redis-backed cache keyed by embedding similarity ---
# SemanticCache owns ALL Redis operations
# for the cache. No Redis commands appear in the Kafka handler or main.py.
# Why cosine similarity for cache lookup?
# Traditional caches use exact key matching. Two incidents like "payment-service
# DB connection pool exhausted on 2024-01-15" and "payment-service DB connection
# pool full on 2024-01-16" have different strings but the same root cause. An
# exact-match cache misses these; a semantic cache hits them correctly.
# Why SCAN not KEYS?
# KEYS is O(N) and blocks the Redis event loop for the entire scan duration —
# in production with thousands of keys this causes visible latency spikes for
# all other Redis clients. SCAN is cursor-based and yields results
# incrementally without blocking, making it safe for production use.

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import numpy as np
import structlog
from openai import AsyncOpenAI
from redis.asyncio import Redis

logger = structlog.get_logger()

# --- Named constants (no magic numbers) ---

# Embedding model: text-embedding-3-small gives 1536-dimensional vectors.
# Cheaper and faster than text-embedding-3-large with minimal quality loss
# for incident description matching.
_EMBEDDING_MODEL = "text-embedding-3-small"

# Estimated tokens per RCA investigation. Used to approximate cost savings.
# The real count varies (500–4000 tokens) but 2000 is a representative median.
_ESTIMATED_TOKENS_PER_RCA = 2000

# Approximate USD cost per input token for GPT-4-turbo ($0.01 per 1K tokens).
# This is a conservative estimate — actual savings are higher for GPT-4o.
_COST_PER_TOKEN_USD = 0.00001

# Redis SCAN hint: how many keys to return per SCAN iteration.
# COUNT is advisory, not exact — Redis may return more or fewer. Higher values
# reduce round-trips; 100 is a safe default for dev-scale datasets.
_SCAN_COUNT_HINT = 100

# Key format used for individual cache entries.
# Pattern: cache:{tenant_id}:{uid}
# The uid portion is a UUID4 so we can store multiple entries per tenant.
_ENTRY_KEY_FMT = "cache:{tenant_id}:{uid}"

# Counter keys: simple Redis STRINGs (not HASHes) that track hits and misses.
# Stored under the same tenant-namespaced prefix for easy SCAN exclusion.
_HITS_KEY_FMT = "cache:{tenant_id}:hits"
_MISSES_KEY_FMT = "cache:{tenant_id}:misses"

# Suffixes that mark counter keys — excluded from the entry scan.
# Without filtering these out, HGETALL on the counter keys returns empty dict
# (they are STRING keys, not HASHes) and skews the similarity search.
_COUNTER_SUFFIXES = frozenset(("hits", "misses"))


@dataclass
class CacheResult:
    """Result of a SemanticCache.get lookup.
    hit=True means a cached RCA result was found with similarity >= threshold.
    hit=False means the incident must be investigated fresh by the RCA agent.
    """

    hit: bool
    rca_result: dict[str, Any] | None
    # similarity_score: cosine similarity of the best matching cached embedding.
    # None when hit=False (no match found or cache empty or error).
    similarity_score: float | None


class SemanticCache:
    """Redis-backed semantic cache for RCA results.
    Embeds incoming incident descriptions and compares them against all
    stored embeddings for the same tenant using cosine similarity. When the
    best match exceeds similarity_threshold, the cached result is returned
    instead of invoking the LLM (zero token cost).
    SemanticCache depends on abstractions (Redis, AsyncOpenAI) injected at
    construction time — never created internally. This is Dependency Inversion:
    tests can inject a FakeRedis and a mock OpenAI client without any patching.
    """

    def __init__(
        self,
        redis_client: Redis,
        openai_client: AsyncOpenAI,
        similarity_threshold: float,
        ttl_seconds: int,
    ) -> None:
        # All dependencies injected — SemanticCache never creates connections.
        self._redis = redis_client
        self._openai = openai_client
        self._threshold = similarity_threshold
        self._ttl = ttl_seconds

    async def get(self, tenant_id: str, incident_description: str) -> CacheResult:
        """Look up a cached RCA result by semantic similarity.
        Returns a cache hit (with rca_result) if any stored embedding has
        cosine similarity >= threshold. Returns a miss otherwise.
        Fail-open: any Redis or OpenAI error returns CacheResult(hit=False)
        so the incident pipeline is never blocked by cache infrastructure.
        """
        # --- Embed the query ---
        try:
            query_embedding = await self._embed(incident_description)
        except Exception as exc:
            # OpenAI is down or rate-limited: fail-open, proceed to RCA.
            # Log at WARN (recoverable — next incident will try again).
            logger.warning(
                "cache_embed_failed_returning_miss",
                error=str(exc),
                tenant_id=tenant_id,
            )
            return CacheResult(hit=False, rca_result=None, similarity_score=None)

        # --- Scan Redis for all entry keys belonging to this tenant ---
        try:
            entry_keys = await self._scan_entry_keys(tenant_id)
        except Exception as exc:
            logger.error(
                "cache_scan_failed_returning_miss",
                error=str(exc),
                tenant_id=tenant_id,
            )
            return CacheResult(hit=False, rca_result=None, similarity_score=None)

        # --- Compare similarity against every stored embedding ---
        best_score: float = -1.0
        best_result: dict[str, Any] | None = None

        for key in entry_keys:
            try:
                # HGETALL returns all field-value pairs for the hash key.
                # With decode_responses=True, all values are strings.
                entry = await self._redis.hgetall(key)
                if not entry or "embedding" not in entry or "rca_result" not in entry:
                    # Corrupted or partially-written entry — skip it.
                    continue

                stored_embedding: list[float] = json.loads(entry["embedding"])
                score = _cosine_similarity(query_embedding, stored_embedding)

                if score > best_score:
                    best_score = score
                    best_result = json.loads(entry["rca_result"])

            except Exception as exc:
                # Single-key read failure: log and continue to next key.
                # We never abort the whole scan because one key is corrupt.
                logger.warning(
                    "cache_entry_read_failed_skipping",
                    key=key,
                    error=str(exc),
                    tenant_id=tenant_id,
                )
                continue

        # --- Determine hit or miss ---
        hits_key = _HITS_KEY_FMT.format(tenant_id=tenant_id)
        misses_key = _MISSES_KEY_FMT.format(tenant_id=tenant_id)

        if best_score >= self._threshold and best_result is not None:
            # Cache hit: increment counter and return the cached result.
            try:
                await self._redis.incr(hits_key)
            except Exception:
                # Counter failure is non-critical — log and continue.
                logger.warning("cache_hits_counter_incr_failed", tenant_id=tenant_id)
            logger.info(
                "cache_hit",
                tenant_id=tenant_id,
                similarity_score=round(best_score, 4),
            )
            return CacheResult(hit=True, rca_result=best_result, similarity_score=best_score)

        # Cache miss: increment miss counter.
        try:
            await self._redis.incr(misses_key)
        except Exception:
            logger.warning("cache_misses_counter_incr_failed", tenant_id=tenant_id)

        logger.info(
            "cache_miss",
            tenant_id=tenant_id,
            best_score=round(best_score, 4) if best_score >= 0 else None,
            threshold=self._threshold,
        )
        return CacheResult(hit=False, rca_result=None, similarity_score=None)

    async def set(
        self,
        tenant_id: str,
        incident_description: str,
        rca_result: dict[str, Any],
    ) -> None:
        """Store a new RCA result in the cache with a TTL.
        Uses a UUID as the key suffix so multiple results can coexist per
        tenant without overwriting each other. EXPIRE sets the TTL so stale
        results are automatically purged without manual maintenance.
        Fail-open on any error: the calling pipeline continues unblocked
        even if the cache write fails. The result is simply not cached.
        """
        try:
            embedding = await self._embed(incident_description)
        except Exception as exc:
            # Embedding failure means we cannot store this result.
            # Log at WARN — not ERROR, because RCA succeeded; only caching failed.
            logger.warning(
                "cache_set_embed_failed_skipping",
                error=str(exc),
                tenant_id=tenant_id,
            )
            return

        # Build the per-entry key: cache:{tenant_id}:{uuid4}
        # uuid4 guarantees uniqueness — no collision risk even at high throughput.
        uid = str(uuid4())
        entry_key = _ENTRY_KEY_FMT.format(tenant_id=tenant_id, uid=uid)

        try:
            # HSET stores embedding, result, and creation timestamp atomically.
            # json.dumps(embedding) converts the float list to a JSON string
            # because Redis hash values must be strings, not Python lists.
            await self._redis.hset(
                entry_key,
                mapping={
                    "embedding": json.dumps(embedding),
                    "rca_result": json.dumps(rca_result),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            # EXPIRE sets the TTL. Without it, the key lives forever — old
            # incident patterns would never be evicted even after system changes.
            await self._redis.expire(entry_key, self._ttl)
            logger.info(
                "cache_entry_stored",
                tenant_id=tenant_id,
                key=entry_key,
                ttl_seconds=self._ttl,
            )
        except Exception as exc:
            logger.warning(
                "cache_set_redis_failed_skipping",
                error=str(exc),
                tenant_id=tenant_id,
            )

    async def get_stats(self, tenant_id: str) -> dict[str, Any]:
        """Compute cache statistics for the given tenant.
        Reads the hit/miss counters and scans for the current number of
        stored entries. Returns zeroed stats on any Redis error so the
        API endpoint always returns a valid response.
        """
        try:
            hits_key = _HITS_KEY_FMT.format(tenant_id=tenant_id)
            misses_key = _MISSES_KEY_FMT.format(tenant_id=tenant_id)

            # GET returns None if the key doesn't exist (no hits/misses yet).
            raw_hits = await self._redis.get(hits_key)
            raw_misses = await self._redis.get(misses_key)

            # Cast to int; default to 0 if key is missing (None).
            hit_count = int(raw_hits) if raw_hits is not None else 0
            miss_count = int(raw_misses) if raw_misses is not None else 0

            total = hit_count + miss_count
            # Avoid division by zero on a cold cache.
            hit_rate = hit_count / total if total > 0 else 0.0

            # Count stored entry keys (excluding the counters themselves).
            entry_keys = await self._scan_entry_keys(tenant_id)
            keys_stored = len(entry_keys)

            # Estimated token and cost savings using conservative constants.
            estimated_tokens_saved = hit_count * _ESTIMATED_TOKENS_PER_RCA
            estimated_cost_saved_usd = estimated_tokens_saved * _COST_PER_TOKEN_USD

            return {
                "hit_count": hit_count,
                "miss_count": miss_count,
                "hit_rate": round(hit_rate, 4),
                "keys_stored": keys_stored,
                "estimated_tokens_saved": estimated_tokens_saved,
                "estimated_cost_saved_usd": round(estimated_cost_saved_usd, 6),
            }

        except Exception as exc:
            logger.error(
                "cache_stats_failed_returning_zeros",
                error=str(exc),
                tenant_id=tenant_id,
            )
            # Return zeroed stats — the API endpoint must always return 200.
            return {
                "hit_count": 0,
                "miss_count": 0,
                "hit_rate": 0.0,
                "keys_stored": 0,
                "estimated_tokens_saved": 0,
                "estimated_cost_saved_usd": 0.0,
            }

    async def _scan_entry_keys(self, tenant_id: str) -> list[str]:
        """Return all Redis keys for this tenant that are cache entries.
        Filters out the :hits and :misses counter keys so only actual
        embedding hashes are returned. Uses SCAN (not KEYS) to avoid
        blocking the Redis event loop.
        cursor=0 is the starting cursor for a new SCAN. Redis returns
        cursor=0 when the full keyspace has been traversed.
        """
        pattern = f"cache:{tenant_id}:*"
        entry_keys: list[str] = []
        cursor = 0

        while True:
            # scan returns (next_cursor, [keys]).
            # count is advisory — Redis may return more or fewer per call.
            cursor, batch = await self._redis.scan(
                cursor=cursor, match=pattern, count=_SCAN_COUNT_HINT
            )
            for key in batch:
                # The suffix after the last colon identifies counters.
                # Entry keys end with a UUID4; counter keys end with "hits"/"misses".
                suffix = key.rsplit(":", 1)[-1]
                if suffix not in _COUNTER_SUFFIXES:
                    entry_keys.append(key)

            # cursor == 0 signals the end of the full scan cycle.
            if cursor == 0:
                break

        return entry_keys

    async def _embed(self, text: str) -> list[float]:
        """Embed text using OpenAI text-embedding-3-small.
        Returns a list of floats (1536 dimensions). The caller handles
        exceptions — this method propagates OpenAI errors unchanged.
        """
        response = await self._openai.embeddings.create(
            model=_EMBEDDING_MODEL,
            input=text,
        )
        # response.data[0].embedding is a list[float] from the OpenAI SDK.
        # No .tolist needed — it is already a plain Python list.
        return response.data[0].embedding


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two embedding vectors.
    Cosine similarity = dot(a, b) / (||a|| * ||b||).
    Range is [-1, 1]; OpenAI embeddings are normalised unit vectors so
    in practice the range is [0, 1] for semantically related texts.
    Returns 0.0 for zero-norm vectors to prevent NaN propagation.
    Using float32 reduces memory usage; the small precision loss does not
    affect threshold comparisons at 2 decimal places.
    """
    vec_a = np.array(a, dtype=np.float32)
    vec_b = np.array(b, dtype=np.float32)
    norm_a = float(np.linalg.norm(vec_a))
    norm_b = float(np.linalg.norm(vec_b))
    if norm_a == 0.0 or norm_b == 0.0:
        # Zero-norm vector has no direction — similarity is undefined, treat as 0.
        return 0.0
    # Clamp to [0, 1]: float32 arithmetic can produce values like 1.0000001 for
    # identical vectors, which would break callers that assert score <= 1.0.
    return float(np.clip(np.dot(vec_a, vec_b) / (norm_a * norm_b), 0.0, 1.0))
