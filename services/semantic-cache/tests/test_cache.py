# --- Unit tests for SemanticCache ---
# all tests run with zero external services.
#   - fakeredis.FakeAsyncRedis replaces a real Redis instance.
#   - _MockOpenAI replaces the real OpenAI API with deterministic vectors.
# Every test asserts a specific outcome — no "just check it doesn't raise".
# Test naming convention:
#   test_<subject>_<condition>_<expected_outcome>
# This makes failures self-explanatory without reading the test body.

import json
import sys
import os

# Ensure the service root is on sys.path so `from cache import ...` works
# when tests are run with: cd services/semantic-cache && python -m pytest tests/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import fakeredis

from cache import SemanticCache, _cosine_similarity

# ---------------------------------------------------------------------------
# Test doubles (mock all external dependencies)
# ---------------------------------------------------------------------------

class _MockEmbeddingData:
    """Mimics openai.types.CreateEmbeddingResponse.data[0]."""

    def __init__(self, embedding: list[float]) -> None:
        self.embedding = embedding


class _MockEmbeddingResponse:
    """Mimics openai.types.CreateEmbeddingResponse."""

    def __init__(self, embedding: list[float]) -> None:
        self.data = [_MockEmbeddingData(embedding)]


class _MockEmbeddingsCreate:
    """Async callable that returns deterministic embeddings keyed by input text.
    If the input text is not in the map, returns the default vector.
    This lets individual tests control exactly which embedding a given
    description produces, enabling precise similarity threshold tests.
    """

    def __init__(
        self,
        embedding_map: dict[str, list[float]],
        default: list[float],
    ) -> None:
        self._map = embedding_map
        self._default = default
        # call_count lets tests verify embed was called (or not).
        self.call_count = 0

    async def __call__(self, model: str, input: str) -> _MockEmbeddingResponse:
        self.call_count += 1
        return _MockEmbeddingResponse(self._map.get(input, self._default))


class _MockEmbeddings:
    """Mimics openai_client.embeddings — exposes a .create async callable."""

    def __init__(self, create_fn: _MockEmbeddingsCreate) -> None:
        self.create = create_fn


class _MockOpenAI:
    """Deterministic OpenAI client for testing SemanticCache.
    Usage:
        client = _MockOpenAI({"incident A": [1.0, 0.0]}, default=[0.0, 1.0])
        # client.embeddings.create(model=..., input="incident A")
        # → returns embedding [1.0, 0.0]
    """

    def __init__(
        self,
        embedding_map: dict[str, list[float]] | None = None,
        default: list[float] | None = None,
    ) -> None:
        _map = embedding_map or {}
        _default = default or [1.0, 0.0, 0.0]
        _create = _MockEmbeddingsCreate(_map, _default)
        self.embeddings = _MockEmbeddings(_create)

    @property
    def create_fn(self) -> _MockEmbeddingsCreate:
        """Expose the create callable so tests can inspect call_count."""
        return self.embeddings.create  # type: ignore[return-value]


class _RaisingOpenAI:
    """OpenAI client that always raises — used to test fail-open behaviour."""

    class _RaisingCreate:
        async def __call__(self, model: str, input: str) -> None:
            raise RuntimeError("OpenAI unavailable")

    class _RaisingEmbeddings:
        def __init__(self) -> None:
            self.create = _RaisingOpenAI._RaisingCreate()

    def __init__(self) -> None:
        self.embeddings = self._RaisingEmbeddings()


# --- Standard test vectors ---
# Two orthogonal unit vectors — cosine similarity = 0.0 (guaranteed miss).
VEC_A = [1.0, 0.0, 0.0]
VEC_B = [0.0, 1.0, 0.0]

# Two identical vectors — cosine similarity = 1.0 (guaranteed hit above any threshold).
VEC_SAME = [0.5, 0.5, 0.707]

TENANT_ID = "tenant-test-uuid-1234"
OTHER_TENANT_ID = "tenant-other-uuid-5678"
THRESHOLD = 0.92
TTL = 86400

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cache(
    openai_client: object,
    threshold: float = THRESHOLD,
    ttl: int = TTL,
    decode_responses: bool = True,
) -> tuple[SemanticCache, fakeredis.FakeAsyncRedis]:
    """Create a SemanticCache backed by an in-memory FakeAsyncRedis."""
    redis_client = fakeredis.FakeAsyncRedis(decode_responses=decode_responses)
    cache = SemanticCache(
        redis_client=redis_client,
        openai_client=openai_client,  # type: ignore[arg-type]
        similarity_threshold=threshold,
        ttl_seconds=ttl,
    )
    return cache, redis_client


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_returns_miss_when_cache_empty() -> None:
    """get on an empty Redis returns a cache miss with no rca_result."""
    openai_client = _MockOpenAI(default=VEC_A)
    cache, _ = _make_cache(openai_client)

    result = await cache.get(TENANT_ID, "payment service database error")

    assert result.hit is False
    assert result.rca_result is None
    assert result.similarity_score is None


@pytest.mark.asyncio
async def test_set_stores_embedding_and_result() -> None:
    """set writes an HSET key with embedding, rca_result, and created_at fields."""
    openai_client = _MockOpenAI(default=VEC_A)
    cache, redis_client = _make_cache(openai_client)

    rca = {"root_cause": "DB connection pool exhausted", "confidence": 0.9}
    await cache.set(TENANT_ID, "payment service error", rca)

    # Scan for the stored key — there should be exactly one entry key.
    cursor, keys = await redis_client.scan(0, match=f"cache:{TENANT_ID}:*", count=100)
    entry_keys = [k for k in keys if not k.endswith((":hits", ":misses"))]
    assert len(entry_keys) == 1

    # Verify all three hash fields were written.
    entry = await redis_client.hgetall(entry_keys[0])
    assert "embedding" in entry
    assert "rca_result" in entry
    assert "created_at" in entry

    # Verify the rca_result round-trips correctly.
    stored_rca = json.loads(entry["rca_result"])
    assert stored_rca["root_cause"] == rca["root_cause"]
    assert stored_rca["confidence"] == rca["confidence"]

    # Verify the embedding is stored as a JSON-serialisable list.
    stored_embedding = json.loads(entry["embedding"])
    assert isinstance(stored_embedding, list)
    assert len(stored_embedding) == len(VEC_A)


@pytest.mark.asyncio
async def test_get_returns_hit_when_similarity_above_threshold() -> None:
    """get returns a cache hit when the stored embedding matches above threshold."""
    # Both set and get embed the same text → same vector → similarity = 1.0.
    openai_client = _MockOpenAI(default=VEC_SAME)
    # Low threshold so the identical vector definitely hits.
    cache, _ = _make_cache(openai_client, threshold=0.5)

    rca = {"root_cause": "Redis OOM eviction", "confidence": 0.85}
    await cache.set(TENANT_ID, "auth service cache miss", rca)

    result = await cache.get(TENANT_ID, "auth service cache miss")

    assert result.hit is True
    assert result.rca_result is not None
    assert result.rca_result["root_cause"] == rca["root_cause"]
    # similarity_score must be a float in [0, 1].
    assert result.similarity_score is not None
    assert 0.0 <= result.similarity_score <= 1.0


@pytest.mark.asyncio
async def test_get_returns_miss_when_similarity_below_threshold() -> None:
    """get returns a miss when the best similarity is below the threshold."""
    # set with VEC_A, get with VEC_B — orthogonal vectors → similarity = 0.
    openai_client = _MockOpenAI(
        embedding_map={
            "stored incident": VEC_A,
            "very different incident": VEC_B,
        }
    )
    cache, _ = _make_cache(openai_client, threshold=THRESHOLD)

    rca = {"root_cause": "Deadlock in inventory updates", "confidence": 0.8}
    await cache.set(TENANT_ID, "stored incident", rca)

    result = await cache.get(TENANT_ID, "very different incident")

    # Orthogonal vectors → cosine similarity = 0.0 < 0.92 → miss.
    assert result.hit is False
    assert result.rca_result is None


@pytest.mark.asyncio
async def test_uses_scan_not_keys() -> None:
    """get must use SCAN, never KEYS — KEYS blocks the Redis event loop.
    KEYS is O(N) and pauses all Redis operations during the scan. In production
    with thousands of cache entries this causes visible latency for all clients.
    SCAN yields results incrementally without blocking.
    This test patches redis_client.keys to raise if called, then verifies
    that get completes successfully without triggering the assertion.
    """
    openai_client = _MockOpenAI(default=VEC_A)
    cache, redis_client = _make_cache(openai_client)

    # Patch .keys to raise if called — get should never call it.
    async def _keys_must_not_be_called(pattern: str = "*") -> list:
        raise AssertionError(
            "keys() was called — use scan() instead. "
            "KEYS blocks the Redis event loop and must never be used in production."
        )

    redis_client.keys = _keys_must_not_be_called  # type: ignore[method-assign]

    # This must NOT raise AssertionError — it should use scan not keys.
    result = await cache.get(TENANT_ID, "some incident description")
    # get on empty cache returns miss regardless.
    assert result.hit is False


@pytest.mark.asyncio
async def test_tenant_isolation() -> None:
    """Cache entries for tenant A must not be visible to tenant B.
    Redis keys are namespaced as cache:{tenant_id}:* — the SCAN pattern
    includes tenant_id so tenant A's entries never appear in tenant B's scan.
    """
    openai_client = _MockOpenAI(default=VEC_SAME)
    cache, _ = _make_cache(openai_client, threshold=0.5)

    # Store a result under TENANT_ID.
    rca = {"root_cause": "TLS certificate expired", "confidence": 0.95}
    await cache.set(TENANT_ID, "auth TLS failure", rca)

    # Query with the same description but a DIFFERENT tenant_id.
    result = await cache.get(OTHER_TENANT_ID, "auth TLS failure")

    # OTHER_TENANT_ID should not see TENANT_ID's entry.
    assert result.hit is False
    assert result.rca_result is None


@pytest.mark.asyncio
async def test_ttl_set_on_cache_keys() -> None:
    """set must call EXPIRE on the entry key with the configured TTL.
    Without a TTL, cache entries accumulate forever. Old incident patterns
    from six months ago would pollute similarity searches for current incidents.
    """
    openai_client = _MockOpenAI(default=VEC_A)
    ttl = 3600  # 1 hour for this test
    cache, redis_client = _make_cache(openai_client, ttl=ttl)

    await cache.set(TENANT_ID, "order processing delay", {"root_cause": "Kafka lag"})

    # Find the stored key.
    cursor, keys = await redis_client.scan(0, match=f"cache:{TENANT_ID}:*", count=100)
    entry_keys = [k for k in keys if not k.endswith((":hits", ":misses"))]
    assert len(entry_keys) == 1

    # TTL must be set and within the configured window.
    # fakeredis.FakeAsyncRedis supports TTL inspection via .ttl.
    remaining_ttl = await redis_client.ttl(entry_keys[0])
    # Remaining TTL must be positive and not exceed configured TTL.
    assert 0 < remaining_ttl <= ttl


@pytest.mark.asyncio
async def test_hit_increments_hits_counter() -> None:
    """A successful cache hit must increment cache:{tenant_id}:hits by 1."""
    openai_client = _MockOpenAI(default=VEC_SAME)
    cache, redis_client = _make_cache(openai_client, threshold=0.5)

    rca = {"root_cause": "Memory leak in goroutine", "confidence": 0.88}
    await cache.set(TENANT_ID, "notification OOM", rca)

    # Confirm hit.
    result = await cache.get(TENANT_ID, "notification OOM")
    assert result.hit is True

    # Verify counter incremented.
    hits_key = f"cache:{TENANT_ID}:hits"
    raw = await redis_client.get(hits_key)
    assert raw is not None
    assert int(raw) == 1


@pytest.mark.asyncio
async def test_miss_increments_misses_counter() -> None:
    """A cache miss must increment cache:{tenant_id}:misses by 1."""
    openai_client = _MockOpenAI(default=VEC_A)
    cache, redis_client = _make_cache(openai_client)

    # Empty cache → guaranteed miss.
    result = await cache.get(TENANT_ID, "inventory deadlock")
    assert result.hit is False

    misses_key = f"cache:{TENANT_ID}:misses"
    raw = await redis_client.get(misses_key)
    assert raw is not None
    assert int(raw) == 1


@pytest.mark.asyncio
async def test_stats_returns_correct_hit_rate() -> None:
    """get_stats computes hit_rate = hits / (hits + misses) correctly."""
    openai_client = _MockOpenAI(default=VEC_SAME)
    # Low threshold so the identical-vector lookup always hits.
    cache, redis_client = _make_cache(openai_client, threshold=0.5)

    rca = {"root_cause": "Rate limiter misconfiguration", "confidence": 0.9}
    await cache.set(TENANT_ID, "auth rate limit", rca)

    # 1 hit.
    await cache.get(TENANT_ID, "auth rate limit")
    # 1 miss (empty cache with a fresh tenant).
    await cache.get(OTHER_TENANT_ID, "completely different incident")

    stats = await cache.get_stats(TENANT_ID)

    assert stats["hit_count"] == 1
    assert stats["miss_count"] == 0  # miss was under OTHER_TENANT_ID
    assert stats["hit_rate"] == 1.0
    # keys_stored: 1 entry (not counting counters).
    assert stats["keys_stored"] == 1
    # estimated_tokens_saved: 1 hit * 2000.
    assert stats["estimated_tokens_saved"] == 2000
    # estimated_cost_saved_usd must be positive.
    assert stats["estimated_cost_saved_usd"] > 0.0


@pytest.mark.asyncio
async def test_redis_error_returns_miss_gracefully() -> None:
    """get must return a cache miss (not raise) if Redis is unavailable.
    Fail-open: the incident pipeline continues to the LLM rather than stalling
    because the cache infrastructure has an error. The cache is a performance
    optimisation, not a correctness requirement.
    """
    openai_client = _MockOpenAI(default=VEC_A)
    # Use a closed/broken Redis client by closing it after creation.
    cache, redis_client = _make_cache(openai_client)
    # Close the Redis connection to simulate a Redis failure.
    await redis_client.aclose()

    # get must not raise — it must return a miss result.
    result = await cache.get(TENANT_ID, "payment service error")

    assert result.hit is False
    assert result.rca_result is None
    assert result.similarity_score is None


@pytest.mark.asyncio
async def test_openai_error_returns_miss_gracefully() -> None:
    """get must return a cache miss (not raise) if the OpenAI API fails.
    If we cannot embed the query description, we cannot perform similarity
    search. Fail-open: proceed to fresh RCA rather than blocking the pipeline.
    """
    # _RaisingOpenAI always raises RuntimeError from embeddings.create.
    cache, _ = _make_cache(_RaisingOpenAI())

    result = await cache.get(TENANT_ID, "gateway 503 cascade")

    assert result.hit is False
    assert result.rca_result is None
    assert result.similarity_score is None


# ---------------------------------------------------------------------------
# Unit test for _cosine_similarity helper
# ---------------------------------------------------------------------------

def test_cosine_similarity_identical_vectors_returns_one() -> None:
    """Identical unit vectors have cosine similarity = 1.0."""
    vec = [1.0, 0.0, 0.0]
    score = _cosine_similarity(vec, vec)
    assert abs(score - 1.0) < 1e-6


def test_cosine_similarity_orthogonal_vectors_returns_zero() -> None:
    """Orthogonal vectors have cosine similarity = 0.0."""
    score = _cosine_similarity([1.0, 0.0], [0.0, 1.0])
    assert abs(score) < 1e-6


def test_cosine_similarity_zero_vector_returns_zero() -> None:
    """Zero-norm vector returns 0.0 without NaN to prevent propagation."""
    score = _cosine_similarity([0.0, 0.0, 0.0], [1.0, 0.0, 0.0])
    assert score == 0.0
