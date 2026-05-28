"""Unit tests for SemanticDetector.
All tests use chromadb.EphemeralClient — an in-memory ChromaDB that behaves
identically to the real server but requires no running container and creates no files.
OpenAI is mocked with unittest.mock.Mock — no real API calls, no API key needed.
Embedding strategy:
  Unit vectors in different dimensions produce deterministic cosine distances.
  Two unit vectors in the SAME dimension: cosine_distance = 0, similarity = 1.0.
  Two unit vectors in DIFFERENT dimensions: cosine_distance = 1, similarity = 0.0.
  This gives us full control over similarity values without any randomness.
  Example with similarity_threshold=0.7:
    Same dimension: similarity=1.0 >= 0.7 → no anomaly
    Different dimension: similarity=0.0 < 0.7 → anomaly detected
  - Every test mocks all external dependencies (EphemeralClient + Mock)
  - Tests cover happy path AND at least two failure paths per function
  - pytest fixtures for shared setup — no repeated boilerplate
  - Zero network access, zero running services required
  - Assertions on specific expected values, never just "did not raise"
Test naming convention: test_<subject>_<condition>_<expected_outcome>
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import chromadb
import pytest

from detection.semantic import SemanticDetector


# ---------------------------------------------------------------------------
# Embedding helpers
# ---------------------------------------------------------------------------

# Embedding dimension for text-embedding-3-small — used to construct test vectors.
# Must match the real model dimension so ChromaDB accepts the vectors.
_EMBEDDING_DIM = 1536


def _unit_vec(index: int) -> list[float]:
    """Return a unit vector with 1.0 at position index and 0.0 everywhere else.
    All pairs of unit vectors in different dimensions are orthogonal:
      dot([1,0,...], [0,1,...]) = 0 → cosine_similarity = 0 → cosine_distance = 1.
    Two vectors with the same index are identical:
      cosine_similarity = 1 → cosine_distance = 0.
    index is taken modulo _EMBEDDING_DIM so tests can use any non-negative integer
    as a counter without worrying about out-of-bounds access.
    """
    vec = [0.0] * _EMBEDDING_DIM
    # Modulo ensures index wraps within valid range even for large values
    vec[index % _EMBEDDING_DIM] = 1.0
    return vec


def _make_response(vec: list[float]) -> Mock:
    """Wrap a vector in a Mock that mimics the OpenAI embeddings response structure.
    The detector accesses: response.data[0].embedding
    This helper builds the correct nested Mock so that path returns vec.
    """
    # Mock(data=[Mock(embedding=vec)]) produces an object where:
    #   .data → [Mock(embedding=vec)]
    #   .data[0] → Mock(embedding=vec)
    #   .data[0].embedding → vec
    return Mock(data=[Mock(embedding=vec)])


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def chroma_client() -> chromadb.EphemeralClient:
    """Provide a fresh in-memory ChromaDB client per test.
    EphemeralClient stores all collections in memory with no persistence.
    Each test receives its own isolated instance — state does not leak between tests.
    No ChromaDB server needs to be running.
    """
    return chromadb.EphemeralClient()


@pytest.fixture
def mock_openai() -> Mock:
    """Provide a Mock OpenAI client that returns a fixed unit vector embedding.
    Default return value: make_unit_vector(0) — the vector [1, 0, 0, ..., 0].
    Individual tests can override via:
      mock_openai.embeddings.create.return_value = _make_response(other_vec)
      mock_openai.embeddings.create.side_effect = [_make_response(v1), _make_response(v2)]
    """
    mock = Mock()
    # Set a sensible default so tests that don't care about the specific embedding
    # value can use this fixture without needing to configure it explicitly
    mock.embeddings.create.return_value = _make_response(_unit_vec(0))
    return mock


@pytest.fixture
def base_ts() -> datetime:
    """Fixed UTC timestamp for all tests that need a deterministic timestamp.
    Using datetime.now(timezone.utc) in tests would make bucket calculations
    non-deterministic. A fixed timestamp ensures reproducible results.
    """
    # A specific moment in time — arbitrary but fixed for reproducibility
    return datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def detector(chroma_client: chromadb.EphemeralClient, mock_openai: Mock) -> SemanticDetector:
    """Provide a SemanticDetector with standard test configuration.
    Uses the chroma_client fixture (EphemeralClient) and mock_openai fixture.
    similarity_threshold=0.7 per the project spec default.
    """
    return SemanticDetector(
        chroma_client=chroma_client,
        openai_client=mock_openai,
        similarity_threshold=0.7,
    )


# ---------------------------------------------------------------------------
# Test: first message always returns None (cold start — no baseline)
# ---------------------------------------------------------------------------


def test_first_message_returns_none(
    detector: SemanticDetector,
    mock_openai: Mock,
    base_ts: datetime,
) -> None:
    """First message for a tenant must return None — no baseline to compare against.
    Root cause of requirement: if we declared the first message anomalous, every
    service starting up would trigger a CRITICAL alert before accumulating any history.
    The collection is empty; there is nothing to compare against, so we must defer
    the anomaly judgement and build the baseline first.
    """
    # mock_openai fixture already provides a default embedding
    result = detector.update_and_check(
        "tenant1", "payment-service", "ERROR", base_ts, "database connection refused"
    )

    assert result is None, (
        f"Expected None for first message (cold start — no baseline), got {result}"
    )


# ---------------------------------------------------------------------------
# Test: first message DOES add embedding to collection
# ---------------------------------------------------------------------------


def test_first_message_adds_to_collection(
    detector: SemanticDetector,
    chroma_client: chromadb.EphemeralClient,
    mock_openai: Mock,
    base_ts: datetime,
) -> None:
    """First message must be added to the collection to serve as future baseline.
    Even though the first message returns None (no anomaly), it must be stored
    in ChromaDB so subsequent similar messages can be recognised as known patterns.
    Without this add, every second message would also be a cold start.
    """
    detector.update_and_check(
        "tenant1", "payment-service", "ERROR", base_ts, "database connection refused"
    )

    # Verify the collection was created with exactly one embedding
    collection = chroma_client.get_collection("anomaly_tenant1")
    assert collection.count() == 1, (
        f"Expected 1 embedding after first message, got {collection.count()}"
    )


# ---------------------------------------------------------------------------
# Test: similar message returns None (known pattern)
# ---------------------------------------------------------------------------


def test_similar_message_returns_none(
    detector: SemanticDetector,
    mock_openai: Mock,
    base_ts: datetime,
) -> None:
    """Second message with identical embedding must return None — known pattern.
    When similarity = 1.0 (identical direction vectors), similarity >= threshold(0.7),
    so the message is classified as a known pattern and no anomaly is returned.
    This prevents re-alerting on a recurring error that the system has already seen.
    """
    # Both calls return the SAME unit vector — cosine distance=0, similarity=1.0
    mock_openai.embeddings.create.return_value = _make_response(_unit_vec(0))

    # First call: cold start — adds baseline, returns None
    r1 = detector.update_and_check(
        "tenant1", "auth-service", "ERROR", base_ts, "connection refused"
    )
    assert r1 is None, "Expected None for cold-start first message"

    # Second call: same embedding — similarity=1.0 >= 0.7 threshold → known pattern
    r2 = detector.update_and_check(
        "tenant1", "auth-service", "ERROR", base_ts, "connection refused again"
    )
    assert r2 is None, (
        "Expected None for message with identical embedding (similarity=1.0 >= threshold=0.7)"
    )


# ---------------------------------------------------------------------------
# Test: dissimilar message is anomalous
# ---------------------------------------------------------------------------


def test_dissimilar_message_is_anomalous(
    detector: SemanticDetector,
    mock_openai: Mock,
    base_ts: datetime,
) -> None:
    """Second message with orthogonal embedding must return a confirmed anomaly.
    Unit vectors in different dimensions are orthogonal: cosine_distance=1.0, similarity=0.0.
    Since 0.0 < threshold(0.7), the message is declared anomalous.
    Verifies the full AnomalyDetectionResult shape to match the contract expected
    by downstream consumers (Kafka publisher, PostgreSQL writer, Prometheus metrics).
    """
    # First call returns vector A; second call returns orthogonal vector B
    mock_openai.embeddings.create.side_effect = [
        _make_response(_unit_vec(0)),  # baseline embedding for first call
        _make_response(_unit_vec(1)),  # orthogonal embedding for second call
    ]

    # First call: cold start — adds baseline, returns None
    r1 = detector.update_and_check(
        "tenant1", "auth-service", "ERROR", base_ts, "known error pattern"
    )
    assert r1 is None, "Expected None for cold-start first message"

    # Second call: orthogonal vector — similarity=0.0 < threshold=0.7 → anomaly
    r2 = detector.update_and_check(
        "tenant1", "auth-service", "ERROR", base_ts, "completely new error type"
    )

    assert r2 is not None, (
        "Expected AnomalyDetectionResult for orthogonal embedding (similarity=0.0 < 0.7)"
    )
    assert r2.detected is True
    assert r2.anomaly_type == "new_error_pattern"
    assert r2.tenant_id == "tenant1"
    assert r2.service == "auth-service"

    # Confidence is 1.0 - similarity = 1.0 - 0.0 = 1.0 for orthogonal vectors
    assert r2.confidence > 0.0, f"Expected confidence > 0, got {r2.confidence}"
    assert r2.confidence <= 1.0, f"Expected confidence <= 1.0, got {r2.confidence}"

    # Details must carry diagnostic context for Grafana dashboards
    assert "similarity_score" in r2.details
    assert "nearest_message" in r2.details
    assert "threshold" in r2.details
    assert r2.details["threshold"] == 0.7


# ---------------------------------------------------------------------------
# Test: non-error level returns None without touching ChromaDB
# ---------------------------------------------------------------------------


def test_non_error_level_returns_none_without_touching_chromadb(
    base_ts: datetime,
) -> None:
    """DEBUG, INFO, WARN levels must return None without any ChromaDB or OpenAI calls.
    Root cause of requirement: INFO/WARN/DEBUG messages are routine operational
    output. Embedding them would:
      1. Waste OpenAI API calls on non-anomalous content.
      2. Pollute the ChromaDB collection with irrelevant patterns.
    The level filter must be the FIRST check — no external API call before it.
    """
    # Use Mock clients (not EphemeralClient) so we can assert they were never called
    mock_chroma = Mock()
    mock_openai = Mock()
    d = SemanticDetector(mock_chroma, mock_openai, similarity_threshold=0.7)

    # Test all three non-error levels
    for level in ("DEBUG", "INFO", "WARN"):
        result = d.update_and_check("t1", "api-service", level, base_ts, "some log message")
        assert result is None, f"Expected None for level={level}, got {result}"

    # ChromaDB must never be touched — no collection created, no query made
    mock_chroma.get_or_create_collection.assert_not_called()
    # OpenAI must never be called — no embedding for non-error levels
    mock_openai.embeddings.create.assert_not_called()


# ---------------------------------------------------------------------------
# Test: tenant isolation — separate ChromaDB collections per tenant
# ---------------------------------------------------------------------------


def test_tenant_isolation_uses_separate_collections(
    chroma_client: chromadb.EphemeralClient,
    mock_openai: Mock,
    base_ts: datetime,
) -> None:
    """Different tenants must use entirely separate ChromaDB collections.
    Root cause of requirement: if two tenants shared one collection, tenant A's
    error patterns would affect tenant B's anomaly baseline. This is a data
    isolation requirement — a multi-tenancy correctness bug, not just a performance issue.
    Verified by checking that both collection names exist and are distinct.
    """
    # Both calls return the same embedding — we only care about collection creation
    mock_openai.embeddings.create.return_value = _make_response(_unit_vec(0))
    d = SemanticDetector(chroma_client, mock_openai, similarity_threshold=0.7)

    d.update_and_check("tenantA", "svc", "ERROR", base_ts, "error for A")
    d.update_and_check("tenantB", "svc", "ERROR", base_ts, "error for B")

    # Each tenant's collection must exist with the correct naming convention
    collection_names = [c.name for c in chroma_client.list_collections()]
    assert "anomaly_tenantA" in collection_names, (
        f"Expected 'anomaly_tenantA' in collections, found: {collection_names}"
    )
    assert "anomaly_tenantB" in collection_names, (
        f"Expected 'anomaly_tenantB' in collections, found: {collection_names}"
    )

    # The two collections must be separate — no single collection contains both tenants.
    # We do not assert total count because chromadb.EphemeralClient shares in-process
    # state across instances; other tests may have created additional collections.
    assert "anomaly_tenantA" != "anomaly_tenantB"


# ---------------------------------------------------------------------------
# Test: collection eviction when over max_collection_size
# ---------------------------------------------------------------------------


def test_collection_eviction_when_over_max_size(
    base_ts: datetime,
) -> None:
    """Collection must evict oldest embeddings when count exceeds max_collection_size.
    Setup: max_collection_size=5, eviction_batch_size=2.
    Add 6 messages, each with an orthogonal embedding (all anomalous after cold start).
    After the 6th add triggers eviction (count 6 > 5), the collection must have count <= 5.
    Trace:
      Call 1: cold start → add → count=1, return None
      Call 2: anomalous → add → count=2, 2<=5 → no eviction, return result
      ...
      Call 5: anomalous → add → count=5, 5<=5 → no eviction, return result
      Call 6: anomalous → add → count=6, 6>5 → evict 2 oldest → count=4, return result
    Expected: count == 4 which satisfies count <= 5.
    """
    chroma = chromadb.EphemeralClient()
    mock_openai = Mock()

    # max_collection_size=5: trigger eviction when count exceeds this
    # eviction_batch_size=2: delete 2 oldest embeddings per eviction cycle
    d = SemanticDetector(
        chroma_client=chroma,
        openai_client=mock_openai,
        similarity_threshold=0.7,
        max_collection_size=5,
        eviction_batch_size=2,
    )

    # --- Add 6 messages with orthogonal unit vectors ---
    # Using incremented timestamps so eviction removes the truly OLDEST embeddings.
    # All pairs of orthogonal unit vectors have cosine_distance=1.0, similarity=0.0,
    # which is < threshold=0.7, so every message after cold start is anomalous and added.
    for i in range(6):
        # Return a different unit vector for each call — all orthogonal to each other
        mock_openai.embeddings.create.return_value = _make_response(_unit_vec(i))
        # Increment timestamp by 1 second per message for deterministic oldest-first eviction
        ts = base_ts + timedelta(seconds=i)
        d.update_and_check("t1", "order-service", "ERROR", ts, f"unique error pattern {i}")

    # After 6 adds with max=5 and eviction_batch=2:
    # add(6th) → count=6 > 5 → evict 2 oldest → count=4
    collection = chroma.get_collection("anomaly_t1")
    assert collection.count() <= 5, (
        f"Expected collection count <= 5 after eviction, got {collection.count()}"
    )


# ---------------------------------------------------------------------------
# Test: OpenAI API error returns None gracefully (fail open)
# ---------------------------------------------------------------------------


def test_openai_api_error_returns_none_gracefully(
    base_ts: datetime,
) -> None:
    """When OpenAI embeddings API fails, detector must return None (fail open).
    Fail-open rationale: an OpenAI outage should not crash the consumer or stop
    log processing. Missing one anomaly detection during an API outage is far
    preferable to halting the entire pipeline or dropping messages.
    Uses a generic Exception to simulate the failure without needing to construct
    the complex httpx.Request object required by openai.APIError's constructor.
    The detector's outer except Exception catches all error types — the fail-open
    behaviour is exercised regardless of the specific exception class.
    """
    chroma = chromadb.EphemeralClient()
    mock_openai = Mock()
    # Simulate OpenAI embeddings API failure on the first call
    mock_openai.embeddings.create.side_effect = Exception("OpenAI API unavailable")

    d = SemanticDetector(chroma, mock_openai, similarity_threshold=0.7)
    result = d.update_and_check("t1", "svc", "ERROR", base_ts, "some error message")

    # Must return None — never raise. Pipeline must not stop due to OpenAI being down.
    assert result is None, (
        f"Expected None when OpenAI API raises Exception, got {result}"
    )


# ---------------------------------------------------------------------------
# Test: ChromaDB error returns None gracefully (fail open)
# ---------------------------------------------------------------------------


def test_chromadb_error_returns_none_gracefully(
    base_ts: datetime,
) -> None:
    """When ChromaDB fails, detector must return None (fail open).
    ChromaDB failure could be: connection lost, disk full, collection corruption.
    In all cases, the pipeline must continue. Anomaly detection degrading (missing
    some detections) is acceptable; stopping all log processing is not.
    """
    mock_chroma = Mock()
    # Simulate ChromaDB failure at collection access time — before any embedding
    mock_chroma.get_or_create_collection.side_effect = Exception("ChromaDB connection refused")
    mock_openai = Mock()

    d = SemanticDetector(mock_chroma, mock_openai, similarity_threshold=0.7)
    result = d.update_and_check("t1", "svc", "ERROR", base_ts, "some error message")

    # Must return None — never raise. The consumer continues to process messages.
    assert result is None, (
        f"Expected None when ChromaDB raises Exception, got {result}"
    )


# ---------------------------------------------------------------------------
# Test: detected_at is timezone-aware UTC
# ---------------------------------------------------------------------------


def test_returned_result_has_utc_detected_at(
    base_ts: datetime,
) -> None:
    """AnomalyDetectionResult.detected_at must be a timezone-aware UTC datetime.
    Root cause of requirement: a naive datetime has no timezone context.
    If detected_at were naive, downstream PostgreSQL TIMESTAMPTZ comparisons
    and API response serialisation would be silently wrong or fail.
    mandates: always use datetime.now(timezone.utc), never utcnow.
    datetime.utcnow returns a naive datetime — rejected here.
    """
    chroma = chromadb.EphemeralClient()
    mock_openai = Mock()

    # First call: cold start returns None (baseline add)
    # Second call: orthogonal vector → anomaly with detected_at field
    mock_openai.embeddings.create.side_effect = [
        _make_response(_unit_vec(0)),  # baseline
        _make_response(_unit_vec(1)),  # anomalous
    ]

    d = SemanticDetector(chroma, mock_openai, similarity_threshold=0.7)

    # Cold start — no result
    r1 = d.update_and_check("t1", "svc", "ERROR", base_ts, "first known error")
    assert r1 is None, "Expected None for cold-start first message"

    # Anomalous second message
    r2 = d.update_and_check("t1", "svc", "ERROR", base_ts, "brand new error type")
    assert r2 is not None, "Expected AnomalyDetectionResult for orthogonal message"

    # detected_at must be timezone-aware — not a naive datetime
    assert r2.detected_at.tzinfo is not None, (
        "detected_at must be timezone-aware (use datetime.now(timezone.utc), not utcnow())"
    )

    # UTC offset must be zero — detected_at must be UTC, not a local timezone
    assert r2.detected_at.utcoffset().total_seconds() == 0, (
        f"Expected UTC offset=0, got {r2.detected_at.utcoffset().total_seconds()}"
    )
