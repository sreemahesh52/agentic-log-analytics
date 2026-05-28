"""Statistical anomaly detector — Strategy implementation of BaseAnomalyDetector.
Uses Z-score analysis over a sliding time window of bucketed error and volume
counts stored in Redis. A Z-score measures how many standard deviations the
current value is from the historical mean. A threshold of 3.0 corresponds to
the 99.7th percentile of a normal distribution (the '3-sigma rule'), meaning
only 0.3% of normal values would trigger a false positive.
Redis is used for the sliding window because:
  1. It survives service restarts (unlike in-process memory).
  2. Multiple replicas of this service share the same baseline.
  3. INCR + EXPIRE are O(1) atomic operations — safe under concurrent writes.
Why bucket-based aggregation instead of per-event storage:
  Storing every log event would exhaust Redis memory quickly. Bucketing into
  60-second intervals collapses N events into 1 integer counter — constant
  memory per time window regardless of log volume.
"""

import math
import structlog

from datetime import datetime, timezone
from typing import Optional

from detection.base import AnomalyDetectionResult, BaseAnomalyDetector

# Module-level structured logger — configured in main.py before this module loads.
# structlog gives us JSON output with consistent field names across all services.
logger = structlog.get_logger(__name__)

# --- Severity mapping constants ---
# Z-score ranges map to severity levels. These thresholds are calibrated so that:
#   LOW (3–4σ): worth watching, not yet urgent
#   MEDIUM (4–5σ): elevated, warrants investigation
#   HIGH (5–7σ): significant spike, likely real incident
#   CRITICAL (>7σ): extreme spike, immediately actionable
_SEVERITY_LOW = "LOW"
_SEVERITY_MEDIUM = "MEDIUM"
_SEVERITY_HIGH = "HIGH"
_SEVERITY_CRITICAL = "CRITICAL"

# Named boundaries for severity thresholds — never use magic numbers in comparisons
_ZSCORE_LOW_BOUNDARY = 3.0
_ZSCORE_MEDIUM_BOUNDARY = 4.0
_ZSCORE_HIGH_BOUNDARY = 5.0
_ZSCORE_CRITICAL_BOUNDARY = 7.0

# Redis SCAN count hint — how many keys to examine per SCAN iteration.
# This is a hint, not a guarantee; Redis may return fewer or more per call.
# A value of 100 balances iteration speed vs. per-call latency.
_SCAN_COUNT_HINT = 100


def _map_zscore_to_severity(z_score: float) -> str:
    """Map a Z-score value to a severity string.
    Uses explicit boundary comparisons instead of if/elif on the threshold
    parameter so the mapping is testable independently of the detector state.
    The boundaries match the project spec: LOW=3-4, MEDIUM=4-5, HIGH=5-7, CRITICAL=>7.
    """
    if z_score >= _ZSCORE_CRITICAL_BOUNDARY:
        return _SEVERITY_CRITICAL
    if z_score >= _ZSCORE_HIGH_BOUNDARY:
        return _SEVERITY_HIGH
    if z_score >= _ZSCORE_MEDIUM_BOUNDARY:
        return _SEVERITY_MEDIUM
    # Caller guarantees z_score > z_score_threshold before calling this function,
    # so the default case always means z_score is in the LOW range (3–4σ).
    return _SEVERITY_LOW


def _compute_zscore(current: float, baseline_values: list[float]) -> Optional[float]:
    """Compute the Z-score of current relative to baseline_values.
    Returns None if the standard deviation is zero — no variation in baseline
    means Z-score is undefined (division by zero). The caller treats None as
    'no anomaly detectable' and returns None to its own caller.
    Args:
        current: the current bucket's count.
        baseline_values: historical counts EXCLUDING the current bucket.
    """
    if not baseline_values:
        return None

    n = len(baseline_values)
    mean = sum(baseline_values) / n

    # Population variance: average of squared deviations from the mean.
    # We use population std (divide by n) not sample std (divide by n-1) because
    # we want to describe the observed distribution, not estimate a population.
    variance = sum((v - mean) ** 2 for v in baseline_values) / n
    std = math.sqrt(variance)

    # std == 0 means every baseline bucket had identical counts — perfectly flat.
    # A spike above a flat baseline produces infinite Z-score; we guard against
    # that by requiring at least some variance before computing.
    if std == 0.0:
        return None

    # Z-score formula: how many standard deviations is current from the mean?
    return (current - mean) / std


class StatisticalDetector(BaseAnomalyDetector):
    """Z-score based anomaly detector using Redis for sliding window baseline storage.
    Detects two anomaly types:
      'error_rate_spike' — the count of ERROR/FATAL logs in the current bucket
                           is unusually high compared to the recent window.
      'volume_spike' — total log volume (all levels) is unusually high.
    Both checks run independently on every update_and_check call. The first
    one that exceeds the threshold is returned; volume_spike is only returned
    if error_rate_spike was not already triggered (to avoid double-alerting).
    Dependency Inversion: redis_client is injected via __init__, never instantiated
    here. The caller owns the connection lifecycle and can inject fakeredis for tests.
    """

    def __init__(
        self,
        redis_client: object,
        window_seconds: int,
        z_score_threshold: float,
        min_data_points: int,
        bucket_size_seconds: int,
    ) -> None:
        """Initialise with injected Redis client and configuration.
        Args:
            redis_client: Redis client instance (real or fakeredis). Never
                                 instantiated here — Dependency Inversion principle.
            window_seconds: How far back to look when computing the baseline.
                                 Must be > bucket_size_seconds.
            z_score_threshold: Z-score above which an anomaly is declared.
                                 Default 3.0 = 99.7th percentile (3-sigma rule).
            min_data_points: Minimum number of buckets required before computing
                                 Z-score. Guards against false positives at startup
                                 when the baseline has too few points to be meaningful.
            bucket_size_seconds: Width of each time bucket in seconds.
                                 Default 60 = one bucket per minute.
        """
        # Store injected Redis client — this class never creates its own connection
        self._redis = redis_client
        self._window_seconds = window_seconds
        self._z_score_threshold = z_score_threshold
        self._min_data_points = min_data_points
        self._bucket_size_seconds = bucket_size_seconds

        # TTL applied to every Redis key: window * 2 ensures keys outlive the window
        # long enough for in-flight reads to complete, then expire automatically.
        # Without TTL, Redis memory grows unboundedly — a production OOM risk.
        self._key_ttl_seconds = window_seconds * 2

    def detector_type(self) -> str:
        """Return the stable identifier for this strategy."""
        return "statistical"

    def _bucket_epoch(self, ts: datetime) -> int:
        """Compute the bucket epoch for a given timestamp.
        Floor-divides the Unix timestamp by bucket_size_seconds to get the
        bucket's start epoch. All events within the same bucket collapse into
        one integer counter in Redis.
        Example: ts=13:42:37, bucket_size=60 → epoch=13:42:00 (Unix seconds).
        Integer division is mandatory — float division would create per-second keys
        and defeat the bucketing purpose entirely.
        """
        # int(ts.timestamp) converts to Unix seconds; integer division floors to bucket boundary
        return (int(ts.timestamp()) // self._bucket_size_seconds) * self._bucket_size_seconds

    def _error_key(self, tenant_id: str, service: str, bucket_epoch: int) -> str:
        """Build the Redis key for the error count bucket.
        Key format: stat:{tenant_id}:{service}:errors:{bucket_epoch}
        tenant_id is always part of the key — this enforces tenant isolation at
        the storage layer. A bug that drops tenant_id would make keys overlap
        between tenants, which is a data leak — prevented structurally here.
        """
        return f"stat:{tenant_id}:{service}:errors:{bucket_epoch}"

    def _volume_key(self, tenant_id: str, service: str, bucket_epoch: int) -> str:
        """Build the Redis key for the total volume count bucket.
        Key format: stat:{tenant_id}:{service}:volume:{bucket_epoch}
        Volume counts every log event (all levels), not just errors.
        """
        return f"stat:{tenant_id}:{service}:volume:{bucket_epoch}"

    def _key_pattern(self, tenant_id: str, service: str, metric: str) -> str:
        """Build the SCAN glob pattern for all buckets of a given metric.
        Returns 'stat:{tenant_id}:{service}:{metric}:*' — the trailing * matches
        any bucket epoch. Used with SCAN, never with KEYS. KEYS blocks the entire
        Redis event loop for the scan duration — on large keyspaces this causes
        timeouts for every other client. SCAN is non-blocking and iterative.
        """
        return f"stat:{tenant_id}:{service}:{metric}:*"

    def _collect_bucket_values(
        self, tenant_id: str, service: str, metric: str, current_epoch: int
    ) -> tuple[float, list[float]]:
        """Collect all bucket counts for the given metric using SCAN.
        Returns (current_value, baseline_values) where:
          current_value: count in the current bucket (the value being tested).
          baseline_values: counts in all other matching buckets (the historical baseline).
        The current bucket is excluded from the baseline because including it
        would dilute the spike — the Z-score measures current vs. historical,
        not current vs. itself-plus-historical.
        SCAN is used (not KEYS) because SCAN is O(1) per iteration and does not
        block the Redis event loop. On a keyspace with millions of keys, KEYS
        would freeze Redis for seconds, causing cascading timeouts downstream.
        """
        pattern = self._key_pattern(tenant_id, service, metric)
        current_key = f"stat:{tenant_id}:{service}:{metric}:{current_epoch}"

        current_value = 0.0
        baseline_values: list[float] = []

        # --- SCAN loop ---
        # SCAN returns a cursor + batch of keys. When cursor returns to 0, the
        # full keyspace matching the pattern has been iterated. Each call is O(1).
        cursor = 0
        while True:
            # scan_iter uses SCAN with COUNT hint — non-blocking, returns generator
            # We use the lower-level scan to control the cursor loop explicitly
            # so we can distinguish current bucket from baseline buckets.
            cursor, keys = self._redis.scan(
                cursor=cursor,
                match=pattern,
                count=_SCAN_COUNT_HINT,
            )
            for key in keys:
                # key may be bytes or str depending on Redis client configuration
                key_str = key.decode() if isinstance(key, bytes) else key
                raw = self._redis.get(key_str)
                if raw is None:
                    # Key expired between SCAN and GET — safe to skip
                    continue
                count = float(raw)

                if key_str == current_key:
                    # This is the current bucket — store separately for Z-score numerator
                    current_value = count
                else:
                    # This is a historical baseline bucket
                    baseline_values.append(count)

            # cursor == 0 signals SCAN has completed a full iteration of the keyspace
            if cursor == 0:
                break

        return current_value, baseline_values

    def _run_zscore_check(
        self,
        tenant_id: str,
        service: str,
        metric: str,
        anomaly_type: str,
        current_epoch: int,
        timestamp: datetime,
    ) -> Optional[AnomalyDetectionResult]:
        """Run Z-score check for one metric (errors or volume).
        Returns AnomalyDetectionResult if an anomaly is detected, None otherwise.
        Extracted to a method so error_rate_spike and volume_spike use identical
        logic — DRY without if/elif branching on metric type.
        """
        current_value, baseline_values = self._collect_bucket_values(
            tenant_id, service, metric, current_epoch
        )

        # Guard: need at least min_data_points baseline buckets for a meaningful baseline.
        # Without this guard, the first few events after service startup would fire
        # alerts against a baseline of 0–4 data points — too noisy to be useful.
        if len(baseline_values) < self._min_data_points:
            return None

        z_score = _compute_zscore(current_value, baseline_values)

        # None means std==0 (flat baseline) — Z-score is undefined, no detection
        if z_score is None:
            return None

        if z_score <= self._z_score_threshold:
            # Current bucket is within normal range — no anomaly
            return None

        # --- Anomaly detected ---
        severity = _map_zscore_to_severity(z_score)

        # Confidence: how far above the threshold is the Z-score, expressed as a
        # fraction of the threshold itself. Clamped to [0.0, 1.0].
        # At exactly threshold: confidence = 0.0 (just barely crossed)
        # At 2× threshold: confidence = 1.0 (twice the threshold)
        confidence = min(1.0, (z_score - self._z_score_threshold) / self._z_score_threshold)

        baseline_mean = sum(baseline_values) / len(baseline_values)

        return AnomalyDetectionResult(
            detected=True,
            tenant_id=tenant_id,
            service=service,
            anomaly_type=anomaly_type,
            severity=severity,
            confidence=confidence,
            details={
                "z_score": round(z_score, 4),
                "current_value": current_value,
                "baseline_mean": round(baseline_mean, 4),
                "baseline_data_points": len(baseline_values),
                "threshold": self._z_score_threshold,
                "metric": metric,
            },
            # detected_at is set to UTC now — not the event timestamp.
            # The event timestamp is when the log was emitted; detected_at is
            # when the anomaly decision was made, which may be minutes later.
            detected_at=datetime.now(timezone.utc),
        )

    def update_and_check(
        self,
        tenant_id: str,
        service: str,
        level: str,
        timestamp: datetime,
    ) -> Optional[AnomalyDetectionResult]:
        """Process one log event: increment Redis counters and check for anomalies.
        Logic:
          1. Compute bucket epoch from the event timestamp.
          2. If level is ERROR or FATAL: increment the error counter for this bucket.
          3. Always: increment the volume counter for this bucket.
          4. Both counters get TTL = window_seconds * 2 to expire automatically.
          5. Run Z-score check on errors → if anomaly, return it immediately.
          6. Run Z-score check on volume → return if triggered and no error spike.
          7. Return None if neither check triggered.
        On any Redis error: log WARN and return None.
          Fail-open rationale: a Redis blip should not crash the pipeline. Missing
          one anomaly detection during a Redis hiccup is safer than stopping
          all log processing. The alternative (fail-closed) would mean any Redis
          instability halts anomaly detection entirely.
        Args:
            tenant_id: tenant namespace — all Redis keys are prefixed with this.
            service: service name, used as part of the Redis key.
            level: log level; only 'ERROR' and 'FATAL' increment the error counter.
            timestamp: event time (must be timezone-aware UTC).
        """
        try:
            # Step 1: compute which time bucket this event belongs to
            bucket_epoch = self._bucket_epoch(timestamp)
            error_key = self._error_key(tenant_id, service, bucket_epoch)
            volume_key = self._volume_key(tenant_id, service, bucket_epoch)

            # Step 2: increment error counter only for ERROR/FATAL levels.
            # INFO, WARN, DEBUG events should not raise the error baseline.
            if level in ("ERROR", "FATAL"):
                # INCR is atomic — safe under concurrent writes from multiple replicas
                self._redis.incr(error_key)
                # EXPIRE must be set on every INCR, not just the first.
                # If TTL was set once at key creation, a INCR on an existing key
                # would reset the count but not the TTL — leading to stale data.
                self._redis.expire(error_key, self._key_ttl_seconds)

            # Step 3: increment total volume counter for every log event (all levels)
            self._redis.incr(volume_key)
            # EXPIRE on every INCR — same reasoning as error_key above
            self._redis.expire(volume_key, self._key_ttl_seconds)

            # Step 4: run error rate Z-score check
            error_result = self._run_zscore_check(
                tenant_id=tenant_id,
                service=service,
                metric="errors",
                anomaly_type="error_rate_spike",
                current_epoch=bucket_epoch,
                timestamp=timestamp,
            )
            if error_result is not None:
                # Return immediately — no need to check volume if error spike found.
                # Returning both would double-alert on the same underlying event.
                return error_result

            # Step 5: run volume spike Z-score check only if error rate was normal
            volume_result = self._run_zscore_check(
                tenant_id=tenant_id,
                service=service,
                metric="volume",
                anomaly_type="volume_spike",
                current_epoch=bucket_epoch,
                timestamp=timestamp,
            )
            return volume_result

        except Exception as exc:
            # Fail-open: log the error but do not propagate it.
            # The pipeline must continue processing even if Redis is temporarily unavailable.
            # This is a deliberate design choice: an undetected anomaly during a brief
            # Redis outage is acceptable; stopping all log processing is not.
            logger.warning(
                "statistical_detector_redis_error",
                tenant_id=tenant_id,
                service=service,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return None
