"""
Integration and unit tests for the hybrid RAG pipeline.
Test structure:
  TestBM25Index — keyword search over real PostgreSQL (live DB required)
  TestVectorSearch — semantic search over EphemeralClient ChromaDB
  TestRRF — pure-Python RRF function (no external deps)
  TestCrossEncoderReranker — reranker unit tests (no model download required)
  TestSearchKnowledgeBase — full pipeline with mocked infrastructure
Run all tests:
  pytest tests/test_hybrid_rag.py -v --tb=short
BM25 tests require PostgreSQL:
  export POSTGRES_URL=postgresql://admin:admin@localhost:5432/loganalytics
  docker compose -f infra/docker-compose.yml up -d postgres
All other test classes require zero external services.
every function has happy path + at least two failure
  paths. External services mocked except where explicitly integration-testing
  the SQL layer (TestBM25Index) or the ChromaDB layer (TestVectorSearch).
pytest-asyncio strict mode notes:
  - All async fixtures use @pytest_asyncio.fixture (not @pytest.fixture)
  - Module-level pytestmark applies @pytest.mark.asyncio to all async tests
    in this module automatically
  - This avoids the "coroutine was never awaited" warning from strict mode
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from hybrid_rag.bm25_index import BM25Index, BM25Result
from hybrid_rag.fusion import FusedResult, reciprocal_rank_fusion
from hybrid_rag.reranker import CrossEncoderReranker
from hybrid_rag.vector_search import VectorResult, VectorSearch
from tools.search_knowledge_base import SEARCH_KNOWLEDGE_BASE_SCHEMA, search_knowledge_base

# Apply @pytest.mark.asyncio to every async test function in this module.
# This is the recommended pattern for pytest-asyncio >= 0.21 strict mode —
# it avoids decorating each individual async test with @pytest.mark.asyncio.
pytestmark = pytest.mark.asyncio

# ---------------------------------------------------------------------------
# PostgreSQL connection helper — defined BEFORE requires_postgres uses it.
# ---------------------------------------------------------------------------

# POSTGRES_URL loaded from environment. Tests that need it are skipped if absent.
_POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://admin:admin@localhost:5432/loganalytics",
)


def _db_reachable() -> bool:
    """Return True if PostgreSQL is reachable at _POSTGRES_URL.
    Called at import time by the requires_postgres mark. Uses a 2-second
    timeout so the test collection phase does not hang if PostgreSQL is down.
    """
    try:
        import asyncpg  # type: ignore[import]

        async def _check() -> bool:
            try:
                conn = await asyncpg.connect(_POSTGRES_URL, timeout=2)
                await conn.close()
                return True
            except Exception:
                return False

        return asyncio.run(_check())
    except Exception:
        return False


# Pytest mark that skips tests when PostgreSQL is unavailable.
# _db_reachable is evaluated once at import time — not on every test run.
requires_postgres = pytest.mark.skipif(
    not _db_reachable(),
    reason="PostgreSQL not reachable. Set POSTGRES_URL and start postgres container.",
)

# ---------------------------------------------------------------------------
# Shared test data: 10 diverse past incidents seeded before BM25/Vector tests.
# ---------------------------------------------------------------------------

# Each incident covers a distinct failure mode. The diversity ensures BM25
# relevance tests can verify that querying "connection pool" surfaces incident 0
# rather than incident 3 (TLS) or incident 4 (goroutine leak).
_TEST_INCIDENTS = [
    {
        "service": "payment-service",
        "description": (
            "Database connection pool exhausted on payment-service. All 10 connections "
            "held by slow queries causing request queuing and eventual timeout failures."
        ),
        "root_cause": (
            "Missing statement_timeout on payment database connection pool. "
            "Long-running queries held connections indefinitely."
        ),
        "resolution": "Set statement_timeout = 30s. Increased pool max_size from 10 to 25.",
        "tags": ["connection-pool", "database", "timeout", "payment"],
    },
    {
        "service": "auth-service",
        "description": (
            "Redis OOM eviction causing cache stampede in auth-service. "
            "All sessions invalidated simultaneously leading to thundering herd on the DB."
        ),
        "root_cause": (
            "Redis maxmemory-policy was allkeys-lru. High memory pressure caused "
            "mass eviction of session keys."
        ),
        "resolution": (
            "Set maxmemory-policy to volatile-lru. Moved session keys to dedicated "
            "Redis instance with reserved memory."
        ),
        "tags": ["redis", "oom", "cache", "auth"],
    },
    {
        "service": "order-service",
        "description": (
            "Kafka consumer group rebalance in order-service causing 90-second processing "
            "pause while partitions were redistributed across consumers."
        ),
        "root_cause": (
            "Consumer pod restart during high-load period triggered group rebalance. "
            "max.poll.interval.ms was too short for slow batch processing."
        ),
        "resolution": (
            "Increased max.poll.interval.ms to 300000. Added consumer group "
            "monitoring alert for rebalance events."
        ),
        "tags": ["kafka", "consumer", "rebalance", "order"],
    },
    {
        "service": "auth-service",
        "description": (
            "TLS certificate for auth-service expired at 03:00 UTC causing all HTTPS "
            "connections to be rejected with SSL handshake failure."
        ),
        "root_cause": (
            "Certificate renewal cron job failed silently 30 days prior. "
            "No alert was configured for certificate expiry."
        ),
        "resolution": (
            "Renewed certificate manually. Added cert-manager for automatic renewal. "
            "Configured Prometheus alert for certificate expiry < 14 days."
        ),
        "tags": ["tls", "certificate", "ssl", "auth"],
    },
    {
        "service": "notification-service",
        "description": (
            "Memory leak in notification-service goroutines causing gradual RSS growth "
            "from 200MB to 2GB over 6 hours, culminating in OOM kill by the kernel."
        ),
        "root_cause": (
            "Goroutine leak — HTTP client connection not closed when upstream "
            "webhook returned non-200 response. Goroutines accumulated unbounded."
        ),
        "resolution": (
            "Added defer resp.Body.Close() and timeout on all HTTP client calls. "
            "Deployed goroutine leak detector (pprof) to staging."
        ),
        "tags": ["goroutine-leak", "oom", "memory", "notification"],
    },
    {
        "service": "order-service",
        "description": (
            "Slow query on orders table in order-service causing p99 latency to spike "
            "from 50ms to 8s during high-traffic periods."
        ),
        "root_cause": (
            "Missing index on orders.customer_id column. Full table scan on a "
            "25M-row table for every order lookup by customer."
        ),
        "resolution": (
            "CREATE INDEX CONCURRENTLY idx_orders_customer_id ON orders(customer_id). "
            "Query time dropped from 8s to less than 10ms."
        ),
        "tags": ["slow-query", "index", "postgres", "order"],
    },
    {
        "service": "payment-service",
        "description": (
            "Network timeout between payment-service and bank payment gateway API. "
            "All transactions failing with connection timeout after 30 seconds."
        ),
        "root_cause": (
            "Bank API experiencing DDoS attack causing their load balancer to "
            "drop connections. No circuit breaker configured on payment-service."
        ),
        "resolution": (
            "Added circuit breaker with 5 failure threshold, 60s open window. "
            "Configured fallback to queue transactions for retry."
        ),
        "tags": ["network", "timeout", "circuit-breaker", "payment"],
    },
    {
        "service": "gateway-service",
        "description": (
            "HTTP 503 cascade in gateway-service as downstream database connection "
            "failures propagated upstream through the service mesh."
        ),
        "root_cause": (
            "Database primary failed over to replica. Replica read-only mode "
            "rejected write queries with error 1290, causing gateway to return 503."
        ),
        "resolution": (
            "Added write/read split at application layer. Updated connection strings "
            "to use RDS proxy which handles failover transparently."
        ),
        "tags": ["cascade", "failover", "database", "gateway"],
    },
    {
        "service": "inventory-service",
        "description": (
            "Disk space exhaustion on inventory-service nodes due to log files filling "
            "the partition. Service crashed when it could not write new log entries."
        ),
        "root_cause": (
            "Log rotation was configured but logrotate cron job was not running "
            "because systemd timer had been accidentally disabled."
        ),
        "resolution": (
            "Re-enabled systemd timer. Implemented log shipping to S3 for long-term "
            "retention. Added disk usage Prometheus alert at 80% threshold."
        ),
        "tags": ["disk", "logs", "rotation", "inventory"],
    },
    {
        "service": "payment-service",
        "description": (
            "Race condition in concurrent payment transaction updates causing double-charge "
            "for some customers due to lost updates on the transactions table."
        ),
        "root_cause": (
            "Optimistic locking implemented with SELECT then UPDATE without a version "
            "column. Two concurrent requests read the same row and both proceeded to update."
        ),
        "resolution": (
            "Added version column with CHECK constraint. Implemented pessimistic "
            "locking (SELECT FOR UPDATE) for high-value transactions."
        ),
        "tags": ["race-condition", "locking", "transactions", "payment"],
    },
]


# ---------------------------------------------------------------------------
# Module-level async fixture for BM25 tests — seeds PostgreSQL test tenant.
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="module")
async def bm25_db_fixture():
    """Create unique test tenant, insert 10 incidents, yield (tenant_id, pool), cleanup.
    scope="module" means this fixture runs ONCE before all tests in this file
    and tears down ONCE after all tests complete. The unique UUID tenant ensures
    no collision with seed data from scripts/seed_tenants.py.
    Why module scope instead of function scope?
    Inserting 10 rows and creating the pool takes ~200ms. Running it per-function
    would add 10s to a 50-test suite. Module scope amortises the setup cost.
    The test data is read-only (no test modifies past_incidents), so sharing is safe.
    """
    import asyncpg  # type: ignore[import]
    import hashlib

    # server_settings={"timezone": "UTC"} ensures UTC on every connection.
    pool = await asyncpg.create_pool(
        _POSTGRES_URL,
        min_size=1,
        max_size=3,
        server_settings={"timezone": "UTC"},
    )

    test_tenant_id = str(uuid.uuid4())
    api_key_hash = hashlib.sha256(
        f"test-key-{test_tenant_id}".encode()
    ).hexdigest()

    try:
        # Insert a unique test tenant. UUID ensures no conflict with seed data.
        await pool.execute(
            """
            INSERT INTO tenants (tenant_id, name, api_key_hash, model_tier)
            VALUES ($1, $2, $3, 'standard')
    """,
            test_tenant_id,
            f"test-bm25-{test_tenant_id[:8]}",
            api_key_hash,
        )

        # Insert all 10 test incidents — each tagged with the test tenant_id.
        for incident in _TEST_INCIDENTS:
            await pool.execute(
                """
                INSERT INTO past_incidents
                  (incident_id, tenant_id, source, service, description,
                   root_cause, resolution, tags)
                VALUES
                  (gen_random_uuid, $1, 'seed', $2, $3, $4, $5, $6)
    """,
                test_tenant_id,
                incident["service"],
                incident["description"],
                incident["root_cause"],
                incident["resolution"],
                incident["tags"],
            )

        # yield: everything before yield is setup, everything after is teardown.
        yield test_tenant_id, pool

    finally:
        # Always clean up test data, even if a test raised an exception.
        # Order matters: past_incidents references tenants via FK, so delete
        # past_incidents first to avoid FK constraint violation.
        await pool.execute(
            "DELETE FROM past_incidents WHERE tenant_id = $1",
            test_tenant_id,
        )
        await pool.execute(
            "DELETE FROM tenants WHERE tenant_id = $1",
            test_tenant_id,
        )
        await pool.close()


# ---------------------------------------------------------------------------
# TestBM25Index — requires live PostgreSQL, skipped if unavailable.
# ---------------------------------------------------------------------------


@requires_postgres
class TestBM25Index:
    """Integration tests for BM25Index against real PostgreSQL."""

    async def test_returns_empty_when_no_incidents_in_db(self, bm25_db_fixture):
        """BM25Index returns [] for a tenant UUID that has no past incidents."""
        _, pool = bm25_db_fixture
        idx = BM25Index(pool)

        # Use a completely different tenant UUID — no rows in past_incidents.
        nonexistent_tenant = str(uuid.uuid4())
        results = await idx.search(nonexistent_tenant, "database connection pool")

        # No incidents for this tenant → empty list, not an exception.
        assert results == [], f"Expected [], got {results}"

    async def test_returns_results_ranked_by_relevance(self, bm25_db_fixture):
        """BM25 ranks incidents containing query keywords above unrelated ones."""
        tenant_id, pool = bm25_db_fixture
        idx = BM25Index(pool)

        # "database connection pool exhausted" closely matches _TEST_INCIDENTS[0].
        # Incidents about Redis OOM, TLS certs, or goroutine leaks should score lower.
        results = await idx.search(
            tenant_id,
            "database connection pool exhausted payment service",
            top_k=10,
        )

        assert len(results) > 0, "Expected at least one result for connection pool query"

        top_result = results[0]
        assert top_result.rank == 1, "First result must have rank=1"
        assert top_result.bm25_score > 0.0, "Top result must have positive BM25 score"

        # The top result should be about connection pool (from _TEST_INCIDENTS[0]).
        combined = (top_result.description + top_result.root_cause).lower()
        assert "connection pool" in combined, (
            f"Top BM25 result should mention 'connection pool'. Got: "
            f"{top_result.description[:80]}"
        )

        # Verify results are sorted by score descending (rank ascending).
        for i in range(len(results) - 1):
            assert results[i].bm25_score >= results[i + 1].bm25_score, (
                f"Results not sorted: rank {i + 1} score {results[i].bm25_score:.4f} "
                f"< rank {i + 2} score {results[i + 1].bm25_score:.4f}"
            )

    async def test_corpus_cached_within_ttl(self, bm25_db_fixture):
        """Second BM25 search within TTL uses cached corpus, not a fresh DB query."""
        tenant_id, pool = bm25_db_fixture
        idx = BM25Index(pool)
        # Long TTL so the cache doesn't expire during this test.
        idx._cache_ttl_seconds = 600

        # First search: queries PostgreSQL and populates the cache.
        results_first = await idx.search(tenant_id, "connection pool", top_k=5)
        assert tenant_id in idx._cache, "Cache should be populated after first search"

        cached_corpus, cached_incidents, cached_at = idx._cache[tenant_id]
        assert len(cached_corpus) == 10, (
            f"Expected 10 incidents in corpus, got {len(cached_corpus)}"
        )

        # To verify the cache is used on the second call, we set the pool to None.
        # If the code tries to query PostgreSQL, it will fail with AttributeError.
        # If it uses the cache, it succeeds.
        original_pool = idx._db_pool
        idx._db_pool = None  # type: ignore[assignment]

        try:
            results_second = await idx.search(tenant_id, "connection pool", top_k=5)
        finally:
            # Always restore pool so other tests are not affected.
            idx._db_pool = original_pool

        # Same query, same cached corpus → same result count.
        assert len(results_first) == len(results_second), (
            f"Cached results differ: {len(results_first)} vs {len(results_second)}"
        )

    async def test_cache_refreshed_after_ttl_expires(self, bm25_db_fixture):
        """BM25Index re-queries PostgreSQL once the cache entry expires."""
        tenant_id, pool = bm25_db_fixture
        idx = BM25Index(pool)
        idx._cache_ttl_seconds = 600

        # Populate the cache with a first search.
        await idx.search(tenant_id, "connection pool", top_k=3)
        assert tenant_id in idx._cache, "Cache must be populated after first search"

        # Forcibly expire the cache by backdating the timestamp.
        # time.monotonic - 700 simulates a cache entry that is 700 seconds old
        # (older than the 600-second TTL), so the next search must refresh.
        corpus, incidents, _ = idx._cache[tenant_id]
        idx._cache[tenant_id] = (corpus, incidents, time.monotonic() - 700.0)

        before_refresh = time.monotonic()
        results = await idx.search(tenant_id, "connection pool", top_k=3)
        after_refresh = time.monotonic()

        # After cache expiry, the timestamp should have been updated to now.
        _, _, refreshed_at = idx._cache[tenant_id]
        assert refreshed_at >= before_refresh, "Cache timestamp should be updated after expiry"
        assert refreshed_at <= after_refresh, "Cache timestamp should not be in the future"

        # PostgreSQL still has the test incidents — results should be non-empty.
        assert len(results) > 0, "Expected results after cache refresh from DB"


# ---------------------------------------------------------------------------
# TestVectorSearch — uses EphemeralClient, no persistent ChromaDB needed.
# ---------------------------------------------------------------------------


class TestVectorSearch:
    """Semantic search tests using chromadb.EphemeralClient.
    EphemeralClient creates an in-memory ChromaDB. No server or docker needed.
    The client is discarded when the test process exits.
    """

    @pytest_asyncio.fixture
    async def chroma_with_incidents(self):
        """Return (EphemeralClient, tenant_id) with 3 incidents added to collection."""
        import chromadb  # type: ignore[import]

        client = chromadb.EphemeralClient()
        tenant_id = str(uuid.uuid4())

        # Create collection with cosine distance space.
        # hnsw:space=cosine → distance = 1 - cosine_similarity → sim = 1 - distance.
        collection = client.create_collection(
            name=f"past_incidents_{tenant_id}",
            metadata={"hnsw:space": "cosine"},
        )

        # Add 3 incidents with deterministic unit vectors (L2-norm = 1).
        # Unit vectors: cosine similarity = dot product (since |A|=|B|=1).
        # Doc 0 embedding: [1, 0, 0, ...] → dimension 0 is "connection pool"
        # Doc 1 embedding: [0, 1, 0, ...] → dimension 1 is "redis oom"
        # Doc 2 embedding: [0, 0, 1, ...] → dimension 2 is "tls certificate"
        collection.add(
            documents=[
                "Database connection pool exhausted on payment service",
                "Redis OOM eviction causing cache stampede in auth service",
                "TLS certificate expired causing authentication failures",
            ],
            metadatas=[
                {
                    "incident_id": "inc-001",
                    "service": "payment-service",
                    "root_cause": "Missing statement timeout",
                    "resolution": "Increased pool size",
                    "tenant_id": tenant_id,
                },
                {
                    "incident_id": "inc-002",
                    "service": "auth-service",
                    "root_cause": "allkeys-lru eviction policy",
                    "resolution": "volatile-lru policy applied",
                    "tenant_id": tenant_id,
                },
                {
                    "incident_id": "inc-003",
                    "service": "auth-service",
                    "root_cause": "Cert renewal cron job failed",
                    "resolution": "cert-manager deployed",
                    "tenant_id": tenant_id,
                },
            ],
            embeddings=[
                # Each embedding is a 1536-dimensional unit vector.
                [1.0] + [0.0] * 1535,         # doc 0: aligned with dimension 0
                [0.0, 1.0] + [0.0] * 1534,    # doc 1: aligned with dimension 1
                [0.0, 0.0, 1.0] + [0.0] * 1533,  # doc 2: aligned with dimension 2
            ],
            ids=["inc-001", "inc-002", "inc-003"],
        )
        return client, tenant_id

    def _make_openai_mock(self, embedding: list[float]) -> MagicMock:
        """Return a mock OpenAI client that returns the given embedding."""
        mock_data = MagicMock()
        mock_data.embedding = embedding

        mock_response = MagicMock()
        mock_response.data = [mock_data]

        mock_client = MagicMock()
        mock_client.embeddings.create.return_value = mock_response
        return mock_client

    async def test_returns_empty_when_collection_not_found(self):
        """VectorSearch returns [] when the tenant's ChromaDB collection does not exist."""
        import chromadb  # type: ignore[import]

        client = chromadb.EphemeralClient()
        vs = VectorSearch(client, self._make_openai_mock([0.1] * 1536))

        # Nonexistent tenant → no collection → get_collection raises → return [].
        results = await vs.search(str(uuid.uuid4()), "any query", top_k=3)

        assert results == [], f"Expected [] for missing collection, got {results}"

    async def test_returns_results_for_known_query(self, chroma_with_incidents):
        """VectorSearch returns the closest incident when query aligns with a known embedding."""
        client, tenant_id = chroma_with_incidents

        # Query embedding aligned with doc 0 ([1, 0, 0, ...]).
        # cosine_similarity([1,0,...], [1,0,...]) = 1.0 (identical).
        # cosine_similarity([1,0,...], [0,1,...]) = 0.0 (orthogonal).
        # So inc-001 should be the top result.
        vs = VectorSearch(client, self._make_openai_mock([1.0] + [0.0] * 1535))
        results = await vs.search(tenant_id, "connection pool exhausted", top_k=3)

        assert len(results) > 0, "Expected results from populated collection"
        assert results[0].incident_id == "inc-001", (
            f"Expected inc-001 (aligned embedding) as top result, got {results[0].incident_id}"
        )
        assert results[0].rank == 1
        # Similarity should be near 1.0 for a perfectly aligned embedding.
        assert results[0].similarity_score > 0.9, (
            f"Expected similarity > 0.9, got {results[0].similarity_score:.4f}"
        )

    async def test_handles_openai_error_gracefully(self, chroma_with_incidents):
        """VectorSearch returns [] when OpenAI embeddings.create raises."""
        client, tenant_id = chroma_with_incidents

        # Mock OpenAI client that raises on embeddings.create.
        mock_client = MagicMock()
        mock_client.embeddings.create.side_effect = ConnectionError("OpenAI API unreachable")

        vs = VectorSearch(client, mock_client)
        results = await vs.search(tenant_id, "some query", top_k=3)

        # OpenAI error → graceful empty return, no exception propagated to caller.
        assert results == [], f"Expected [] on OpenAI error, got {results}"

    async def test_returns_empty_for_empty_collection(self):
        """VectorSearch returns [] when the collection exists but has no documents."""
        import chromadb  # type: ignore[import]

        client = chromadb.EphemeralClient()
        tenant_id = str(uuid.uuid4())

        # Create collection with zero documents.
        client.create_collection(name=f"past_incidents_{tenant_id}")

        vs = VectorSearch(client, self._make_openai_mock([0.1] * 1536))
        results = await vs.search(tenant_id, "any query", top_k=3)

        assert results == [], f"Expected [] for empty collection, got {results}"


# ---------------------------------------------------------------------------
# TestRRF — pure Python, zero external dependencies.
# ---------------------------------------------------------------------------


class TestRRF:
    """Unit tests for reciprocal_rank_fusion. No DB, no embeddings, no model."""

    def _bm25(
        self,
        incident_id: str,
        rank: int,
        score: float = 1.0,
    ) -> BM25Result:
        """Minimal BM25Result test helper."""
        return BM25Result(
            incident_id=incident_id,
            description=f"Description for {incident_id}",
            root_cause=f"Root cause for {incident_id}",
            resolution=f"Resolution for {incident_id}",
            service="test-service",
            bm25_score=score,
            rank=rank,
        )

    def _vec(
        self,
        incident_id: str,
        rank: int,
        similarity: float = 0.9,
    ) -> VectorResult:
        """Minimal VectorResult test helper."""
        return VectorResult(
            incident_id=incident_id,
            description=f"Description for {incident_id}",
            root_cause=f"Root cause for {incident_id}",
            resolution=f"Resolution for {incident_id}",
            service="test-service",
            similarity_score=similarity,
            rank=rank,
        )

    def test_incident_in_both_lists_scores_higher(self):
        """Incident ranked in both BM25 and vector outscore one present in only one list."""
        # A appears in both lists at rank 1.
        # B appears only in BM25 at rank 2.
        # Despite B being rank-2 in BM25, A should rank first overall.
        bm25 = [self._bm25("A", rank=1), self._bm25("B", rank=2)]
        vector = [self._vec("A", rank=1)]

        fused = reciprocal_rank_fusion(bm25, vector)

        assert fused[0].incident_id == "A", (
            f"A (in both lists) should rank first. Got: {fused[0].incident_id}"
        )
        assert fused[0].rrf_score > fused[1].rrf_score, "A must outscore B"

    def test_incident_only_in_bm25_included(self):
        """An incident present only in BM25 still appears in fused results."""
        bm25 = [self._bm25("BM25-only", rank=1)]
        vector = [self._vec("Vector-only", rank=1)]

        fused = reciprocal_rank_fusion(bm25, vector)
        ids = {r.incident_id for r in fused}

        assert "BM25-only" in ids, "BM25-only incident must be included in fused results"
        assert len(fused) == 2

    def test_incident_only_in_vector_included(self):
        """An incident present only in vector results still appears in fused results."""
        # BM25 is empty — only vector has results.
        vector = [self._vec("vec-1", rank=1), self._vec("vec-2", rank=2)]
        fused = reciprocal_rank_fusion([], vector)

        assert len(fused) == 2
        assert fused[0].incident_id == "vec-1", (
            "vec-1 (rank 1 in vector) should score higher than vec-2 (rank 2)"
        )
        # Both have None bm25_rank because no BM25 results were provided.
        assert fused[0].bm25_rank is None
        assert fused[0].vector_rank == 1

    def test_empty_inputs_returns_empty(self):
        """Both inputs empty returns [], not an exception."""
        fused = reciprocal_rank_fusion([], [])
        assert fused == [], f"Expected [], got {fused}"

    def test_rrf_formula_correct(self):
        """RRF score equals 1/(k + rank), verified manually against k=60."""
        # Single incident at rank 1 in BM25 only.
        fused = reciprocal_rank_fusion([self._bm25("X", rank=1)], [], k=60)

        assert len(fused) == 1
        # Expected: 1 / (60 + 1) = 1/61
        expected = 1.0 / 61.0
        assert abs(fused[0].rrf_score - expected) < 1e-9, (
            f"Expected {expected:.10f}, got {fused[0].rrf_score:.10f}"
        )

        # Incident in both lists at rank 1: score = 1/(60+1) + 1/(60+1) = 2/61
        fused_double = reciprocal_rank_fusion(
            [self._bm25("D", rank=1)],
            [self._vec("D", rank=1)],
            k=60,
        )
        expected_double = 2.0 / 61.0
        assert abs(fused_double[0].rrf_score - expected_double) < 1e-9, (
            f"Double list: expected {expected_double:.10f}, "
            f"got {fused_double[0].rrf_score:.10f}"
        )

    def test_bm25_rank_and_vector_rank_stored_in_fused_result(self):
        """FusedResult stores bm25_rank and vector_rank provenance for both sources."""
        bm25 = [self._bm25("shared", rank=2)]
        vector = [self._vec("shared", rank=3)]

        fused = reciprocal_rank_fusion(bm25, vector)

        assert fused[0].incident_id == "shared"
        assert fused[0].bm25_rank == 2, f"Expected bm25_rank=2, got {fused[0].bm25_rank}"
        assert fused[0].vector_rank == 3, f"Expected vector_rank=3, got {fused[0].vector_rank}"


# ---------------------------------------------------------------------------
# TestCrossEncoderReranker — no model download required.
# ---------------------------------------------------------------------------


class TestCrossEncoderReranker:
    """Tests for CrossEncoderReranker using mocked model — no 85MB download needed.
    Each test that needs a specific model state saves and restores _model via
    try/finally to ensure test isolation even on failure.
    """

    def _fused(self, incident_id: str, rrf_score: float) -> FusedResult:
        """Minimal FusedResult helper."""
        return FusedResult(
            incident_id=incident_id,
            description=f"Description for {incident_id}",
            root_cause=f"Root cause for {incident_id}",
            resolution=f"Resolution for {incident_id}",
            service="test-service",
            rrf_score=rrf_score,
            bm25_rank=1,
            vector_rank=1,
        )

    def test_singleton_pattern(self):
        """Two CrossEncoderReranker calls return the exact same object (identity check)."""
        r1 = CrossEncoderReranker()
        r2 = CrossEncoderReranker()

        # `is` checks memory identity — not just equality — the true Singleton test.
        assert r1 is r2, (
            "CrossEncoderReranker must return the same instance on every call. "
            "Two separate CrossEncoderReranker() calls returned different objects."
        )

    def test_returns_graceful_fallback_when_model_not_loaded(self):
        """rerank falls back to RRF order when _model is None (load not called)."""
        original_model = CrossEncoderReranker._model

        try:
            # Reset to None to simulate a fresh service startup before load.
            CrossEncoderReranker._model = None
            reranker = CrossEncoderReranker()

            candidates = [
                self._fused("A", rrf_score=0.032),
                self._fused("B", rrf_score=0.020),
                self._fused("C", rrf_score=0.015),
            ]
            results = reranker.rerank("test query about database failure", candidates, top_k=2)

            # Fallback: returns top 2 in RRF order (A has highest rrf_score).
            assert len(results) == 2, f"Expected 2 results, got {len(results)}"
            assert results[0].incident_id == "A", (
                f"A has highest RRF score → should be rank 1. Got: {results[0].incident_id}"
            )
            assert results[0].final_rank == 1
            assert results[1].incident_id == "B"
            assert results[1].final_rank == 2

            # Fallback uses rrf_score as proxy rerank_score.
            assert abs(results[0].rerank_score - 0.032) < 1e-9, (
                f"Fallback rerank_score should be rrf_score 0.032, "
                f"got {results[0].rerank_score}"
            )
        finally:
            # Restore model state — critical to avoid affecting other tests.
            CrossEncoderReranker._model = original_model

    def test_rerank_returns_top_k_results(self):
        """rerank returns exactly top_k results with correct final_rank values."""
        original_model = CrossEncoderReranker._model

        try:
            # Mock cross-encoder: assigns score = index + 1 (E=5 > D=4 > C=3 > B=2 > A=1).
            # After reranking by score descending: E, D, C — top 3.
            class MockCrossEncoder:
                """Mock model that scores candidates by input order (ascending)."""

                def predict(self, pairs: list) -> list[float]:
                    # pairs[0] → score 1.0, pairs[4] → score 5.0
                    return [float(i + 1) for i in range(len(pairs))]

            CrossEncoderReranker._model = MockCrossEncoder()  # type: ignore[assignment]
            reranker = CrossEncoderReranker()

            candidates = [
                self._fused("A", rrf_score=0.05),
                self._fused("B", rrf_score=0.04),
                self._fused("C", rrf_score=0.03),
                self._fused("D", rrf_score=0.02),
                self._fused("E", rrf_score=0.01),
            ]
            results = reranker.rerank("test query", candidates, top_k=3)

            # Must return exactly top_k=3 results.
            assert len(results) == 3, f"Expected 3 results, got {len(results)}"

            # final_rank is 1-based.
            assert results[0].final_rank == 1
            assert results[1].final_rank == 2
            assert results[2].final_rank == 3

            # MockCrossEncoder: score[4] = 5.0 (E) > score[3] = 4.0 (D) > score[2] = 3.0 (C)
            assert results[0].incident_id == "E", (
                f"E has score 5.0 → should be rank 1. Got: {results[0].incident_id}"
            )
            assert results[1].incident_id == "D"
            assert results[2].incident_id == "C"

        finally:
            CrossEncoderReranker._model = original_model

    def test_returns_empty_for_empty_candidates(self):
        """rerank returns [] for empty input without raising."""
        reranker = CrossEncoderReranker()
        results = reranker.rerank("any query", [], top_k=3)
        assert results == [], f"Expected [] for empty input, got {results}"


# ---------------------------------------------------------------------------
# TestSearchKnowledgeBase — full pipeline with mocked infrastructure.
# ---------------------------------------------------------------------------


class TestSearchKnowledgeBase:
    """Integration tests for the search_knowledge_base orchestrator.
    BM25Index and VectorSearch are mocked with AsyncMock.
    CrossEncoderReranker uses the RRF fallback (model intentionally not loaded).
    Zero external services required — tests run in pure memory.
    """

    def _bm25_result(self, incident_id: str, rank: int) -> BM25Result:
        """Minimal BM25Result for pipeline tests."""
        return BM25Result(
            incident_id=incident_id,
            description=f"Database connection pool exhausted — {incident_id}",
            root_cause="Missing connection timeout configuration on the pool",
            resolution="Set connection_timeout=30s. Increased pool_max_size to 25.",
            service="payment-service",
            bm25_score=1.0 - rank * 0.1,
            rank=rank,
        )

    def _vector_result(self, incident_id: str, rank: int) -> VectorResult:
        """Minimal VectorResult for pipeline tests."""
        return VectorResult(
            incident_id=incident_id,
            description=f"Database connection pool exhausted — {incident_id}",
            root_cause="Missing connection timeout configuration on the pool",
            resolution="Set connection_timeout=30s. Increased pool_max_size to 25.",
            service="payment-service",
            similarity_score=1.0 - rank * 0.1,
            rank=rank,
        )

    def _mock_bm25(self, results: list[BM25Result]) -> MagicMock:
        """Mock BM25Index with an awaitable search returning given results."""
        m = MagicMock(spec=BM25Index)
        # AsyncMock makes search awaitable: `await m.search(...)` returns results.
        m.search = AsyncMock(return_value=results)
        return m

    def _mock_vector(self, results: list[VectorResult]) -> MagicMock:
        """Mock VectorSearch with an awaitable search returning given results."""
        m = MagicMock(spec=VectorSearch)
        m.search = AsyncMock(return_value=results)
        return m

    def _reranker_no_model(self) -> CrossEncoderReranker:
        """Return singleton with _model=None so rerank uses RRF fallback."""
        CrossEncoderReranker._model = None
        return CrossEncoderReranker()

    async def test_returns_formatted_string_with_ranked_results(self):
        """Output is structured text containing ranked incidents the LLM can parse."""
        bm25_results = [
            self._bm25_result("inc-A", 1),
            self._bm25_result("inc-B", 2),
        ]
        vector_results = [
            # inc-A in both lists → highest RRF score → should be Rank 1.
            self._vector_result("inc-A", 1),
            self._vector_result("inc-C", 2),
        ]

        result = await search_knowledge_base(
            tenant_id="test-tenant",
            bm25_index=self._mock_bm25(bm25_results),
            vector_search=self._mock_vector(vector_results),
            reranker=self._reranker_no_model(),
            query="database connection pool exhausted payment service",
            top_k=2,
        )

        # Verify all required sections in the formatted output.
        assert "=== Similar Past Incidents (Hybrid RAG) ===" in result, (
            "Output must include the RAG header"
        )
        assert "Rank 1" in result, "Output must show 'Rank 1'"
        assert "Root Cause:" in result, "Output must include 'Root Cause:' label"
        assert "Resolution:" in result, "Output must include 'Resolution:' label"
        assert "Service:" in result, "Output must include 'Service:' label"
        assert "payment-service" in result, "Output must include the service name"

        # Footer must show retrieval counts so the LLM knows how thorough the search was.
        assert "BM25: 2 candidates" in result, "Footer must show BM25 candidate count"
        assert "Vector: 2 candidates" in result, "Footer must show vector candidate count"

    async def test_returns_novel_message_when_db_empty(self):
        """Returns the 'novel incident' message when both BM25 and vector are empty."""
        result = await search_knowledge_base(
            tenant_id="test-tenant",
            bm25_index=self._mock_bm25([]),
            vector_search=self._mock_vector([]),
            reranker=self._reranker_no_model(),
            query="completely unknown failure mode never seen before",
            top_k=3,
        )

        # LLM must receive a clear "no history" signal.
        assert "No similar past incidents found" in result, (
            f"Expected novel incident message, got: {result[:150]}"
        )
        assert "novel incident" in result.lower(), (
            "Message should confirm this is a novel incident"
        )

    async def test_handles_bm25_empty_gracefully(self):
        """Pipeline proceeds with vector results only when BM25 returns empty."""
        vector_results = [
            self._vector_result("vec-only-1", 1),
            self._vector_result("vec-only-2", 2),
        ]

        result = await search_knowledge_base(
            tenant_id="test-tenant",
            bm25_index=self._mock_bm25([]),  # BM25 returns nothing
            vector_search=self._mock_vector(vector_results),
            reranker=self._reranker_no_model(),
            query="semantically similar but no keyword match",
            top_k=2,
        )

        # Should return vector results, not the "novel incident" message.
        assert "=== Similar Past Incidents" in result, (
            "Expected RAG results from vector search. Got: " + result[:150]
        )
        # BM25 count shows 0, vector count shows 2.
        assert "BM25: 0 candidates" in result, "Footer must show 0 BM25 candidates"

    async def test_handles_vector_empty_gracefully(self):
        """Pipeline proceeds with BM25 results only when vector returns empty."""
        bm25_results = [
            self._bm25_result("bm25-only-1", 1),
            self._bm25_result("bm25-only-2", 2),
        ]

        result = await search_knowledge_base(
            tenant_id="test-tenant",
            bm25_index=self._mock_bm25(bm25_results),
            vector_search=self._mock_vector([]),  # vector returns nothing
            reranker=self._reranker_no_model(),
            query="connection pool exhausted exact keyword match",
            top_k=2,
        )

        # Should return BM25 results, not the "novel incident" message.
        assert "=== Similar Past Incidents" in result, (
            "Expected RAG results from BM25 search. Got: " + result[:150]
        )
        assert "Vector: 0 candidates" in result, "Footer must show 0 vector candidates"

    async def test_search_kb_schema_is_valid_openai_format(self):
        """SEARCH_KNOWLEDGE_BASE_SCHEMA passes JSON serialisation and has all required OpenAI fields."""
        import json

        # OpenAI wraps each schema as {"type": "function", "function": <schema>}.
        openai_tool = {"type": "function", "function": SEARCH_KNOWLEDGE_BASE_SCHEMA}

        # Must be fully JSON-serialisable (no datetime, no numpy, no custom types).
        serialised = json.dumps(openai_tool)
        parsed = json.loads(serialised)

        assert parsed["type"] == "function"
        assert parsed["function"]["name"] == "SearchKnowledgeBase"
        assert "description" in parsed["function"]
        assert "parameters" in parsed["function"]

        # "query" must be required so the LLM always provides a search string.
        assert "query" in parsed["function"]["parameters"]["required"], (
            "'query' must be in the required parameters list"
        )
        # Description must tell the LLM to call this tool FIRST.
        assert "FIRST" in parsed["function"]["description"], (
            "Schema description must instruct the LLM to call SearchKnowledgeBase first"
        )
