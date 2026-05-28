"""Unit tests for StatisticalDetector.
All tests use fakeredis.FakeRedis — an in-memory Redis implementation that
mirrors the real Redis API without requiring a running Redis server. This makes
every test fully self-contained and executable with 'pytest' alone.
Test naming convention: test_<scenario>_<expected_outcome>
Each test asserts specific values, never just "did not raise".
  - Every test mocks all external dependencies (fakeredis replaces real Redis)
  - Each test covers a specific scenario with explicit assertions
  - No time.sleep, no network access, no external services
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import fakeredis
import pytest

from detection.statistical import StatisticalDetector, _map_zscore_to_severity


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def redis_client() -> fakeredis.FakeRedis:
    """Provide a fresh in-memory fakeredis instance per test.
    fakeredis.FakeRedis implements the same API as redis.Redis — INCR, EXPIRE,
    SCAN, GET all work identically. Tests never need a real Redis server.
    Each test gets a separate instance so keyspace state does not leak between tests.
    """
    return fakeredis.FakeRedis()


@pytest.fixture
def detector(redis_client: fakeredis.FakeRedis) -> StatisticalDetector:
    """Provide a StatisticalDetector with standard test configuration.
    Configuration chosen to balance test clarity:
      window_seconds=300: 5-minute window
      z_score_threshold=3.0: standard 3-sigma
      min_data_points=5: need at least 5 historical buckets
      bucket_size_seconds=60: 1-minute buckets
    """
    return StatisticalDetector(
        redis_client=redis_client,
        window_seconds=300,
        z_score_threshold=3.0,
        min_data_points=5,
        bucket_size_seconds=60,
    )


@pytest.fixture
def base_ts() -> datetime:
    """Fixed UTC timestamp used across tests.
    Using a fixed timestamp ensures bucket_epoch calculations are deterministic.
    datetime.now(timezone.utc) in production — never utcnow which is naive.
    """
    # A fixed moment in time — epoch-aligned to a clean minute boundary for clarity
    return datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)


def _feed_baseline(
    detector: StatisticalDetector,
    tenant_id: str,
    service: str,
    level: str,
    ts: datetime,
    n_buckets: int,
    events_per_bucket: int,
) -> None:
    """Helper: fill n_buckets distinct buckets with events_per_bucket events each.
    Each bucket is artificially offset by bucket_size_seconds * bucket_index so
    they land in different Redis keys. This simulates n_buckets of historical data.
    The bucket_size_seconds is read from the detector's configuration.
    """
    from datetime import timedelta
    for i in range(n_buckets):
        # Offset each bucket by one bucket-width to ensure they get separate keys
        bucket_ts = ts - timedelta(seconds=detector._bucket_size_seconds * (n_buckets - i))
        for _ in range(events_per_bucket):
            detector.update_and_check(tenant_id, service, level, bucket_ts)


# ---------------------------------------------------------------------------
# Test: insufficient data points
# ---------------------------------------------------------------------------


def test_returns_none_when_insufficient_data_points(
    detector: StatisticalDetector,
    base_ts: datetime,
) -> None:
    """Detector must return None when fewer than min_data_points buckets exist.
    Root cause: without a meaningful baseline, Z-score would be computed against
    1–4 data points. With N=1, mean=current_value and std=0, so the check returns
    None anyway. But with N=2–4, the result would be statistically meaningless
    and highly likely to fire false positives. min_data_points guards against this.
    """
    # Feed only 3 buckets (min_data_points=5) — not enough baseline
    _feed_baseline(detector, "tenant1", "api-service", "ERROR", base_ts, 3, 2)

    # The 4th call is the 'current' bucket — still not enough historical data
    result = detector.update_and_check("tenant1", "api-service", "ERROR", base_ts)

    assert result is None, (
        f"Expected None with only 3 baseline buckets, got {result}"
    )


# ---------------------------------------------------------------------------
# Test: std == 0 (flat baseline)
# ---------------------------------------------------------------------------


def test_returns_none_when_std_is_zero(
    detector: StatisticalDetector,
    base_ts: datetime,
) -> None:
    """Detector must return None when all baseline buckets have identical counts.
    Root cause: when std=0, Z-score = (x - mean) / 0 = undefined (ZeroDivisionError).
    The correct behaviour is to return None — no variance in the baseline means
    we cannot determine if the current value is anomalous. An all-zero baseline
    (service never errored) is the most common case.
    """
    # Feed 10 baseline buckets, each with exactly 2 errors — perfectly flat baseline
    _feed_baseline(detector, "tenant1", "payment-service", "ERROR", base_ts, 10, 2)

    # Current bucket also has 2 errors — same as every baseline bucket
    result = detector.update_and_check("tenant1", "payment-service", "ERROR", base_ts)

    # std=0 means we cannot compute Z-score — must return None, not raise ZeroDivisionError
    assert result is None, (
        f"Expected None when baseline std=0, got {result}"
    )


# ---------------------------------------------------------------------------
# Test: spike detected above threshold
# ---------------------------------------------------------------------------


def test_detects_error_rate_spike_above_threshold(
    detector: StatisticalDetector,
    base_ts: datetime,
) -> None:
    """Detector must return a result when error rate Z-score exceeds threshold.
    Setup: 8 baseline buckets with mean=2 errors each.
    Spike: 20 errors in the current bucket.
    Expected Z-score ≈ (20 - 2) / std(2,2,...) — all equal so std≈0... wait,
    we need variance. Let's use varied baselines: 1,2,1,3,2,1,2,3 → mean≈1.875.
    Actually with 8 identical values std=0. Let's use varied counts to get real std.
    """
    from datetime import timedelta
    # Build a varied baseline so std > 0: counts of 1,2,3,1,2,3,1,2 per bucket
    baseline_counts = [1, 2, 3, 1, 2, 3, 1, 2]
    for i, count in enumerate(baseline_counts):
        bucket_ts = base_ts - timedelta(seconds=detector._bucket_size_seconds * (len(baseline_counts) - i))
        for _ in range(count):
            detector.update_and_check("tenant1", "payment-service", "ERROR", bucket_ts)

    # Current bucket: 20 errors — should be a massive spike above the baseline mean of ~1.9
    for _ in range(20):
        result = detector.update_and_check("tenant1", "payment-service", "ERROR", base_ts)

    assert result is not None, "Expected anomaly result for 20-error spike, got None"
    assert result.detected is True
    assert result.anomaly_type == "error_rate_spike"
    assert result.tenant_id == "tenant1"
    assert result.service == "payment-service"
    assert result.confidence > 0.0, f"Expected confidence > 0, got {result.confidence}"
    assert result.confidence <= 1.0, f"Expected confidence <= 1.0, got {result.confidence}"
    # detected_at must be timezone-aware UTC
    assert result.detected_at.tzinfo is not None, "detected_at must be timezone-aware"
    # details must carry diagnostic context for dashboards
    assert "z_score" in result.details
    assert result.details["z_score"] > 3.0, f"Expected z_score > 3.0, got {result.details['z_score']}"
    assert "current_value" in result.details
    assert "baseline_mean" in result.details


# ---------------------------------------------------------------------------
# Test: no detection below threshold
# ---------------------------------------------------------------------------


def test_does_not_detect_below_threshold(
    detector: StatisticalDetector,
    base_ts: datetime,
) -> None:
    """Detector must return None when current value is within normal range.
    If this test fails, the detector is firing false positives — engineers
    would investigate non-incidents and lose trust in the alerting system.
    """
    from datetime import timedelta
    # Varied baseline: counts 1,2,3,2,1,3,2,1 → mean≈1.875, std≈0.78
    baseline_counts = [1, 2, 3, 2, 1, 3, 2, 1]
    for i, count in enumerate(baseline_counts):
        bucket_ts = base_ts - timedelta(seconds=detector._bucket_size_seconds * (len(baseline_counts) - i))
        for _ in range(count):
            detector.update_and_check("tenant1", "auth-service", "ERROR", bucket_ts)

    # Current bucket: 3 errors — at the top of normal range, well below z_score=3.0
    # mean≈1.875, std≈0.78 → z = (3 - 1.875) / 0.78 ≈ 1.44 — below threshold
    for _ in range(3):
        result = detector.update_and_check("tenant1", "auth-service", "ERROR", base_ts)

    assert result is None, (
        f"Expected None for value within normal range, got {result}"
    )


# ---------------------------------------------------------------------------
# Test: severity mapping
# ---------------------------------------------------------------------------


def test_severity_mapped_correctly_for_each_zscore_range() -> None:
    """Each Z-score boundary must map to the correct severity label.
    Tests _map_zscore_to_severity directly — this function is pure (no Redis),
    so it is tested in isolation from the full detector pipeline.
    Boundaries per spec: LOW=3-4, MEDIUM=4-5, HIGH=5-7, CRITICAL=>7.
    """
    # LOW: just above the base threshold (3.0), below MEDIUM boundary (4.0)
    assert _map_zscore_to_severity(3.1) == "LOW"
    assert _map_zscore_to_severity(3.9) == "LOW"

    # MEDIUM: between 4.0 and 5.0
    assert _map_zscore_to_severity(4.0) == "MEDIUM", "4.0 should be MEDIUM (above LOW boundary)"
    assert _map_zscore_to_severity(4.5) == "MEDIUM"
    assert _map_zscore_to_severity(4.99) == "MEDIUM"

    # HIGH: between 5.0 and 7.0
    assert _map_zscore_to_severity(5.0) == "HIGH", "5.0 should be HIGH (above MEDIUM boundary)"
    assert _map_zscore_to_severity(6.0) == "HIGH"
    assert _map_zscore_to_severity(6.99) == "HIGH"

    # CRITICAL: above 7.0
    assert _map_zscore_to_severity(7.0) == "CRITICAL", "7.0 should be CRITICAL (above HIGH boundary)"
    assert _map_zscore_to_severity(10.0) == "CRITICAL"
    assert _map_zscore_to_severity(100.0) == "CRITICAL"


# ---------------------------------------------------------------------------
# Test: SCAN not KEYS
# ---------------------------------------------------------------------------


def test_uses_scan_not_keys(detector: StatisticalDetector) -> None:
    """Detector must use SCAN for keyspace iteration, never KEYS.
    KEYS blocks the entire Redis event loop for the duration of the scan.
    On a large keyspace (millions of keys), this can freeze Redis for seconds,
    causing timeout cascades for all other clients. SCAN is O(1) per call.
    This test verifies the implementation contract by mocking Redis and asserting
    scan is called while keys is never called.
    """
    mock_redis = MagicMock()
    # scan returns (cursor, keys_list) — cursor=0 signals end of iteration
    mock_redis.scan.return_value = (0, [])
    mock_redis.incr.return_value = 1
    mock_redis.expire.return_value = True
    mock_redis.get.return_value = None

    test_detector = StatisticalDetector(
        redis_client=mock_redis,
        window_seconds=300,
        z_score_threshold=3.0,
        min_data_points=5,
        bucket_size_seconds=60,
    )

    ts = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    test_detector.update_and_check("tenant1", "svc", "ERROR", ts)

    # SCAN must have been called at least once (once for errors, once for volume)
    assert mock_redis.scan.called, "Expected scan() to be called"
    # KEYS must NEVER be called — it is a blocking operation
    assert not mock_redis.keys.called, (
        "keys() was called — use scan() instead. KEYS blocks the Redis event loop."
    )


# ---------------------------------------------------------------------------
# Test: Redis unavailable returns None
# ---------------------------------------------------------------------------


def test_redis_unavailable_returns_none(detector: StatisticalDetector) -> None:
    """Detector must return None gracefully when Redis raises ConnectionError.
    Fail-open design: a Redis outage should not crash the anomaly detection
    pipeline. Returning None means 'no anomaly detected this cycle', which is
    a safe degraded mode — the pipeline continues and the outage is logged.
    Fail-closed (raising) would halt all log processing during a Redis blip.
    """
    mock_redis = MagicMock()
    # Simulate a Redis connection failure on INCR
    mock_redis.incr.side_effect = ConnectionError("Redis connection refused")

    failing_detector = StatisticalDetector(
        redis_client=mock_redis,
        window_seconds=300,
        z_score_threshold=3.0,
        min_data_points=5,
        bucket_size_seconds=60,
    )

    ts = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    result = failing_detector.update_and_check("tenant1", "svc", "ERROR", ts)

    # Must return None — never raise. The pipeline must not stop.
    assert result is None, (
        f"Expected None when Redis raises ConnectionError, got {result}"
    )


# ---------------------------------------------------------------------------
# Test: tenant isolation
# ---------------------------------------------------------------------------


def test_tenant_isolation(
    detector: StatisticalDetector,
    redis_client: fakeredis.FakeRedis,
    base_ts: datetime,
) -> None:
    """Events for different tenants must use completely separate Redis keys.
    Root cause: if tenant_id is not part of the Redis key, tenant A's error
    spike would inflate tenant B's baseline, causing false positives or missed
    detections across tenant boundaries. This is a data isolation requirement.
    """
    # Insert one event for each tenant — they share the same service name
    detector.update_and_check("tenant_A", "shared-svc", "ERROR", base_ts)
    detector.update_and_check("tenant_B", "shared-svc", "ERROR", base_ts)

    # Inspect the actual Redis keyspace
    all_keys = [k.decode() for k in redis_client.keys("stat:*")]

    # Both tenants must have their own keys
    tenant_a_keys = [k for k in all_keys if "tenant_A" in k]
    tenant_b_keys = [k for k in all_keys if "tenant_B" in k]

    assert len(tenant_a_keys) > 0, "Expected Redis keys for tenant_A, found none"
    assert len(tenant_b_keys) > 0, "Expected Redis keys for tenant_B, found none"

    # No key should contain both tenant IDs — that would be a data corruption bug
    mixed_keys = [k for k in all_keys if "tenant_A" in k and "tenant_B" in k]
    assert len(mixed_keys) == 0, f"Found mixed-tenant keys: {mixed_keys}"

    # The key structures are entirely separate — tenant_A's key never contains tenant_B
    for k in tenant_a_keys:
        assert "tenant_B" not in k, f"tenant_A key contains tenant_B: {k}"
    for k in tenant_b_keys:
        assert "tenant_A" not in k, f"tenant_B key contains tenant_A: {k}"


# ---------------------------------------------------------------------------
# Test: volume spike detected
# ---------------------------------------------------------------------------


def test_volume_spike_detected(
    detector: StatisticalDetector,
    base_ts: datetime,
) -> None:
    """Detector must detect volume_spike when total log volume spikes abnormally.
    Volume spikes (even in INFO/DEBUG logs) can signal floods, DDoS, or
    runaway logging bugs before error rates climb. This test uses INFO level
    to confirm the volume check triggers independently of the error check.
    """
    from datetime import timedelta
    # Varied INFO baseline: 10,12,11,13,10,12,11,13 per bucket — mean≈11.5, std≈1.12
    baseline_counts = [10, 12, 11, 13, 10, 12, 11, 13]
    for i, count in enumerate(baseline_counts):
        bucket_ts = base_ts - timedelta(seconds=detector._bucket_size_seconds * (len(baseline_counts) - i))
        for _ in range(count):
            detector.update_and_check("tenant1", "frontend", "INFO", bucket_ts)

    # Spike: 100 INFO events in the current bucket (mean≈11.5, so z≈(100-11.5)/1.12≈79)
    result = None
    for _ in range(100):
        result = detector.update_and_check("tenant1", "frontend", "INFO", base_ts)

    assert result is not None, "Expected volume_spike result, got None"
    assert result.detected is True
    assert result.anomaly_type == "volume_spike"
    assert result.confidence > 0.0
    assert result.details["metric"] == "volume"


# ---------------------------------------------------------------------------
# Test: non-error level does not increment error bucket
# ---------------------------------------------------------------------------


def test_non_error_level_does_not_trigger_error_bucket(
    detector: StatisticalDetector,
    redis_client: fakeredis.FakeRedis,
    base_ts: datetime,
) -> None:
    """INFO, WARN, DEBUG log events must not increment the error counter.
    Root cause: if all log levels incremented the error bucket, every high-volume
    service would trigger false error rate spikes. Only ERROR and FATAL should
    raise the error baseline.
    """
    # Process 10 INFO events
    for _ in range(10):
        detector.update_and_check("tenant1", "api", "INFO", base_ts)

    # Inspect Redis: error bucket must not exist or be zero, volume bucket must be non-zero
    bucket_epoch = detector._bucket_epoch(base_ts)
    error_key = detector._error_key("tenant1", "api", bucket_epoch)
    volume_key = detector._volume_key("tenant1", "api", bucket_epoch)

    # Error key must not exist — INFO events should not increment it
    error_count = redis_client.get(error_key)
    assert error_count is None or int(error_count) == 0, (
        f"Error bucket was incremented by INFO events: count={error_count}"
    )

    # Volume key must reflect all 10 events
    volume_count = redis_client.get(volume_key)
    assert volume_count is not None, "Volume key missing after INFO events"
    assert int(volume_count) == 10, (
        f"Expected volume_count=10, got {int(volume_count)}"
    )


# ---------------------------------------------------------------------------
# Test: bucket epoch calculation
# ---------------------------------------------------------------------------


def test_bucket_epoch_floors_to_bucket_boundary() -> None:
    """bucket_epoch must floor to the nearest bucket boundary, not round.
    This test verifies the integer division arithmetic directly. If float
    division were used instead, events at different seconds within the same
    bucket would get different keys — breaking the aggregation entirely.
    """
    r = fakeredis.FakeRedis()
    d = StatisticalDetector(
        redis_client=r,
        window_seconds=300,
        z_score_threshold=3.0,
        min_data_points=5,
        bucket_size_seconds=60,
    )

    # Two timestamps 30 seconds apart within the same 60-second bucket
    ts1 = datetime(2024, 1, 15, 10, 0, 15, tzinfo=timezone.utc)  # :15 seconds
    ts2 = datetime(2024, 1, 15, 10, 0, 45, tzinfo=timezone.utc)  # :45 seconds

    epoch1 = d._bucket_epoch(ts1)
    epoch2 = d._bucket_epoch(ts2)

    # Both must produce the same bucket epoch (10:00:00 UTC)
    assert epoch1 == epoch2, (
        f"Two timestamps in the same minute got different bucket epochs: "
        f"{epoch1} != {epoch2}"
    )

    # A timestamp in the next minute must get a different epoch
    ts3 = datetime(2024, 1, 15, 10, 1, 0, tzinfo=timezone.utc)   # 10:01:00
    epoch3 = d._bucket_epoch(ts3)
    assert epoch3 != epoch1, (
        f"Timestamps in different minutes got the same bucket epoch: {epoch3}"
    )
    # epoch3 must be exactly one bucket_size ahead of epoch1
    assert epoch3 == epoch1 + 60, (
        f"Expected epoch3 = epoch1 + 60, got epoch1={epoch1}, epoch3={epoch3}"
    )
