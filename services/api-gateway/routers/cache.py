# --- API Gateway: Semantic Cache stats endpoint ---
# Single Responsibility: this router only handles GET /api/v1/cache/stats.
# Why does the gateway read cache stats from Redis directly instead of calling
# the semantic-cache service over HTTP?
# The gateway already has a Redis connection (created in lifespan for auth caching
# and future use). Adding an HTTP hop to the semantic-cache service would introduce
# a circular dependency and a new failure mode. Reading Redis counters directly
# is simpler, lower latency, and requires no new inter-service protocol.
# Repository pattern:
# CacheStatsRepository owns all Redis operations for stats. The route handler
# depends on the repository abstraction, not raw Redis commands.

import structlog
from fastapi import APIRouter, Depends, Request
from redis.asyncio import Redis

from auth import verify_api_key

logger = structlog.get_logger()

router = APIRouter()

# --- Named constants ---
# Must match the key format in services/semantic-cache/cache.py exactly.
# If the semantic-cache service changes its key format, update both files.
_HITS_KEY_FMT = "cache:{tenant_id}:hits"
_MISSES_KEY_FMT = "cache:{tenant_id}:misses"
_CACHE_SCAN_PATTERN = "cache:{tenant_id}:*"

# Suffixes identifying Redis counter keys (not cache entry hashes).
_COUNTER_SUFFIXES = frozenset(("hits", "misses"))

# Mirrors the constant in semantic-cache/cache.py.
# 2000 tokens is the estimated cost per RCA investigation (median).
_ESTIMATED_TOKENS_PER_RCA = 2000

# Approximate USD cost per input token for GPT-4-turbo.
_COST_PER_TOKEN_USD = 0.00001

# SCAN COUNT hint: advisory number of keys per iteration.
_SCAN_COUNT_HINT = 100


def _get_redis_client(request: Request) -> Redis:
    """FastAPI dependency: return the shared Redis client from app.state.
    The Redis client is created once in the lifespan context manager and stored
    on app.state. Returning it here avoids creating a new connection per request
    (connection pooling).
    """
    return request.app.state.redis_client  # type: ignore[return-value]


class CacheStatsRepository:
    """Read semantic cache statistics from Redis.
    Reads the per-tenant hit/miss counters and scans for the count of stored
    cache entry keys. Uses the same key format as the semantic-cache service
    (cache:{tenant_id}:{uid} for entries, cache:{tenant_id}:hits/misses for
    counters) so both processes share the same Redis namespace.
    """

    def __init__(self, redis_client: Redis) -> None:
        # Redis client injected — CacheStatsRepository does not create connections.
        self._redis = redis_client

    async def get_stats(self, tenant_id: str) -> dict:
        """Compute and return cache statistics for the given tenant.
        Returns zeroed stats on any Redis error — the endpoint must always
        return 200 rather than 503 for a non-critical stats endpoint.
        """
        try:
            hits_key = _HITS_KEY_FMT.format(tenant_id=tenant_id)
            misses_key = _MISSES_KEY_FMT.format(tenant_id=tenant_id)

            # GET returns None for keys that have never been set (cold cache).
            raw_hits = await self._redis.get(hits_key)
            raw_misses = await self._redis.get(misses_key)

            hit_count = int(raw_hits) if raw_hits is not None else 0
            miss_count = int(raw_misses) if raw_misses is not None else 0

            total = hit_count + miss_count
            # hit_rate: fraction of cache lookups that returned a hit.
            # Avoid division by zero on a cold cache (zero total lookups).
            hit_rate = hit_count / total if total > 0 else 0.0

            # Scan for the count of stored entry keys (exclude counter keys).
            keys_stored = await self._count_entry_keys(tenant_id)

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
            # Redis unavailable: log at ERROR but return zeroed stats.
            # This endpoint is informational — do not return 503 for stats failures.
            logger.error(
                "cache_stats_redis_error",
                tenant_id=tenant_id,
                error=str(exc),
            )
            return {
                "hit_count": 0,
                "miss_count": 0,
                "hit_rate": 0.0,
                "keys_stored": 0,
                "estimated_tokens_saved": 0,
                "estimated_cost_saved_usd": 0.0,
            }

    async def _count_entry_keys(self, tenant_id: str) -> int:
        """Count cache entry keys for the tenant, excluding counter keys.
        Uses SCAN (not KEYS) to avoid blocking the Redis event loop.
        Each SCAN iteration returns a batch of keys; cursor=0 signals end.
        """
        pattern = _CACHE_SCAN_PATTERN.format(tenant_id=tenant_id)
        count = 0
        cursor = 0

        while True:
            cursor, batch = await self._redis.scan(
                cursor=cursor, match=pattern, count=_SCAN_COUNT_HINT
            )
            for key in batch:
                # Exclude counter keys by checking the last segment.
                suffix = key.rsplit(":", 1)[-1]
                if suffix not in _COUNTER_SUFFIXES:
                    count += 1
            # cursor == 0 means the scan is complete.
            if cursor == 0:
                break

        return count


@router.get("/api/v1/cache/stats")
async def get_cache_stats(
    tenant: dict = Depends(verify_api_key),
    redis_client: Redis = Depends(_get_redis_client),
) -> dict:
    """Return semantic cache statistics for the authenticated tenant.
    Statistics include the raw hit/miss counts, hit rate as a fraction,
    number of currently stored cache entries, and estimated cost savings
    from cache hits (approximate, based on 2000 tokens per RCA at GPT-4 pricing).
    Returns zeroed values if Redis is unavailable — never 503 for a stats endpoint.
    """
    tenant_id: str = tenant["tenant_id"]
    repo = CacheStatsRepository(redis_client)
    stats = await repo.get_stats(tenant_id)
    logger.info(
        "cache_stats_fetched",
        tenant_id=tenant_id,
        hit_rate=stats["hit_rate"],
        keys_stored=stats["keys_stored"],
    )
    return stats
