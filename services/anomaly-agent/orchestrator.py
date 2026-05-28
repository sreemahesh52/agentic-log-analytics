"""Anomaly detection orchestrator — Observer pattern for alert publishing.
This module contains two things:
  1. AlertPublisher (ABC) — the interface that all alert publishers must implement.
  2. AnomalyOrchestrator — the coordinator that runs detectors and notifies publishers.
Why AlertPublisher is defined HERE (not in a shared interfaces.py):
  The orchestrator is the only consumer of this interface. Defining it here keeps
  the coupling explicit: KafkaAlertPublisher and PostgresAlertRepository depend on
  this module, but this module does not depend on them (it receives them as injected
  list[AlertPublisher] at construction time). No circular imports.
Observer Pattern:
  AnomalyOrchestrator is the Subject (event source).
  KafkaAlertPublisher and PostgresAlertRepository are Observers (event handlers).
  The orchestrator fires 'alert confirmed' by calling publish_alert on all
  registered observers. Adding a new observer (e.g., SlackNotifier) requires:
    1. Implement AlertPublisher.
    2. Register it in main.py.
    No changes to AnomalyOrchestrator itself — that is Open/Closed.
Single Responsibility boundaries:
  Orchestrator: coordinates detection + verification + fan-out.
  Detectors: run statistical and semantic analysis (not here).
  LLMVerifier: calls the LLM yes/no API (not here).
  Publishers: write to Kafka and PostgreSQL (not here).
"""

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional

import structlog

from detection.base import AnomalyDetectionResult
from models import AlertPayload, LogEvent

logger = structlog.get_logger(__name__)

# Log levels that trigger the semantic detector.
# INFO/WARN/DEBUG are routine — only ERROR/FATAL carry meaningful anomaly signal.
_ERROR_LEVELS = frozenset({"ERROR", "FATAL"})

# Severity ranking used to pick the higher severity when both detectors fire.
# Higher rank = more urgent. CRITICAL (3) > HIGH (2) > MEDIUM (1) > LOW (0).
_SEVERITY_RANK: dict[str, int] = {
    "LOW": 0,
    "MEDIUM": 1,
    "HIGH": 2,
    "CRITICAL": 3,
}


class AlertPublisher(ABC):
    """Interface for alert publishing strategies — the Observer base class.
    Why an interface here (not just a callable):
      A plain function would work for one publisher, but we have two (Kafka + Postgres)
      and will add more (Slack in step 14, future pager duty). Using an ABC:
        1. Forces all publishers to implement publish_alert — no silent omissions.
        2. Makes publishers substitutable (Liskov Substitution Principle).
        3. Enables mock injection in tests — test code provides a fake AlertPublisher
           that records calls without touching Kafka or PostgreSQL.
    Interface Segregation: this interface has ONE method. Publishers don't need
    to know about each other, about the orchestrator internals, or about detection.
    """

    @abstractmethod
    def publish_alert(self, alert: dict) -> None:
        """Publish a confirmed alert dict.
        Implementations MUST NOT raise — a failing publisher must log at ERROR
        and return. One publisher failing must never block the others in the list.
        Args:
            alert: dict produced by AlertPayload.model_dump(mode='json').
                   Contains: alert_id, tenant_id, service, anomaly_type,
                   severity, confidence, status, created_at, details.
        """
        ...


def _merge_results(
    stat_result: Optional[AnomalyDetectionResult],
    sem_result: Optional[AnomalyDetectionResult],
) -> Optional[AnomalyDetectionResult]:
    """Merge two optional detector results into one canonical result.
    Logic:
      - Both None: return None (no anomaly)
      - One None: return the non-None result unchanged
      - Both fired: return a 'combined' result using the higher severity and
                    max confidence; details dict contains both detectors' output.
    Why max(confidence) instead of average:
      Confidence represents certainty that an anomaly exists. If one detector
      is 90% sure, the combined certainty is at least 90% — not an average of
      90% and 60%. The higher reading is the binding signal.
    """
    if stat_result is None and sem_result is None:
        return None

    if stat_result is not None and sem_result is None:
        return stat_result

    if stat_result is None and sem_result is not None:
        return sem_result

    # Both detectors fired — combine into a single 'combined' result.
    # Pick the base (higher severity) for tenant_id, service, detected_at fields.
    stat_rank = _SEVERITY_RANK.get(stat_result.severity, 0)
    sem_rank = _SEVERITY_RANK.get(sem_result.severity, 0)
    base = stat_result if stat_rank >= sem_rank else sem_result

    return AnomalyDetectionResult(
        detected=True,
        tenant_id=base.tenant_id,
        service=base.service,
        # anomaly_type 'combined' matches the DB CHECK constraint on the alerts table
        anomaly_type="combined",
        severity=base.severity,
        # max confidence: if either detector is highly certain, the combined is too
        confidence=max(stat_result.confidence, sem_result.confidence),
        details={
            # Preserve both detectors' details for downstream consumers and dashboards
            "statistical": stat_result.details,
            "semantic": sem_result.details,
        },
        # detected_at: when this combined decision was made (now), not either
        # individual detector's time — they may have run milliseconds apart.
        detected_at=datetime.now(timezone.utc),
    )


def _build_anomaly_description(result: AnomalyDetectionResult) -> str:
    """Build a human-readable description of the anomaly for the LLM verifier prompt.
    The description is injected into {anomaly_description} in the prompt template.
    It must be concise enough to fit in the LLM's context window but rich enough
    for the model to make a meaningful YES/NO decision.
    """
    if result.anomaly_type == "error_rate_spike":
        z = result.details.get("z_score", "?")
        current = result.details.get("current_value", "?")
        mean = result.details.get("baseline_mean", "?")
        return (
            f"Error rate spike: Z-score={z}, current={current} errors/bucket "
            f"vs baseline mean={mean}"
        )

    if result.anomaly_type == "volume_spike":
        z = result.details.get("z_score", "?")
        current = result.details.get("current_value", "?")
        mean = result.details.get("baseline_mean", "?")
        return (
            f"Log volume spike: Z-score={z}, current={current} logs/bucket "
            f"vs baseline mean={mean}"
        )

    if result.anomaly_type == "new_error_pattern":
        sim = result.details.get("similarity_score", "?")
        threshold = result.details.get("threshold", "?")
        nearest = result.details.get("nearest_message", "")
        desc = (
            f"Novel error pattern detected: similarity={sim} below threshold={threshold}"
        )
        if nearest:
            # Include nearest known pattern so the LLM can judge how novel this is
            desc += f". Nearest known pattern: '{nearest[:60]}'"
        return desc

    if result.anomaly_type == "combined":
        stat = result.details.get("statistical", {})
        sem = result.details.get("semantic", {})
        return (
            f"Multiple anomaly signals: statistical Z-score={stat.get('z_score','?')}, "
            f"semantic similarity={sem.get('similarity_score','?')}"
        )

    # Fallback for any future anomaly types
    return f"Anomaly type '{result.anomaly_type}' at severity {result.severity}"


# Maps detector-internal anomaly types to the wire types enforced by the DB CHECK
# constraint and AlertPayload validator: {'statistical', 'semantic', 'combined'}.
# Detectors use descriptive names (error_rate_spike, new_error_pattern) for
# readability; the alert schema uses coarser category names for query filtering.
_ANOMALY_TYPE_TO_WIRE: dict[str, str] = {
    "error_rate_spike": "statistical",
    "volume_spike": "statistical",
    "new_error_pattern": "semantic",
    "combined": "combined",
}


def _to_wire_anomaly_type(internal_type: str) -> str:
    """Map a detector's internal anomaly_type to the AlertPayload wire type."""
    return _ANOMALY_TYPE_TO_WIRE.get(internal_type, "statistical")


class AnomalyOrchestrator:
    """Coordinates the full anomaly detection pipeline for one log event.
    Pipeline steps (per PROJECT-SPEC.md):
      1. Statistical detection — always runs (fast, no API calls)
      2. Semantic detection — only for ERROR/FATAL (one embedding API call)
      3. Merge results — 'combined' if both fired
      4. Fetch log context — recent errors from DB for LLM prompt
      5. LLM verification — GPT-3.5 YES/NO (only when 1 or 2 fired)
      6. Alert fan-out — Kafka + PostgreSQL via Observer pattern
      7. Metrics — increment confirmed anomalies counter
    Single Responsibility: this class coordinates. It does NOT:
      - Write to Kafka (KafkaAlertPublisher does that)
      - Write to PostgreSQL alerts table (PostgresAlertRepository does that)
      - Call the LLM directly (LLMVerifier does that)
      - Run Z-score math (StatisticalDetector does that)
    Dependency Inversion: all six dependencies are injected. Nothing is
    instantiated here. Tests substitute mocks for any dependency.
    """

    def __init__(
        self,
        statistical_detector: object,
        semantic_detector: object,
        llm_verifier: object,
        alert_publishers: list[AlertPublisher],
        log_repository: object,
        metrics: object,
        alert_cooldown_seconds: int = 60,
    ) -> None:
        """Accept all dependencies — injected by main.py, never created here.
        Args:
            statistical_detector: StatisticalDetector instance.
            semantic_detector: SemanticDetector instance.
            llm_verifier: LLMVerifier instance.
            alert_publishers: list of AlertPublisher implementations (Observer list).
                                   Order matters only for logging; all receive every alert.
            log_repository: LogRepository for fetching recent error context.
            metrics: Metrics dataclass with Prometheus counters.
            alert_cooldown_seconds: Minimum seconds between two confirmed alerts for the
                                   same (tenant, service). Prevents alert flooding when a
                                   Z-score spike persists across many sequential log messages —
                                   without this, every error above the threshold triggers a
                                   separate LLM call, creating a multi-minute backlog that
                                   prevents cross-service CASCADE detection in the correlator.
        """
        self._statistical = statistical_detector
        self._semantic = semantic_detector
        self._llm_verifier = llm_verifier
        # alert_publishers is a list — Observer pattern: all observers notified per event.
        self._publishers = alert_publishers
        self._log_repo = log_repository
        self._metrics = metrics
        self._alert_cooldown_seconds = alert_cooldown_seconds
        # Tracks the last confirmed-alert time per (tenant_id, service).
        # Single-threaded service — no lock needed.
        self._last_alert_time: dict[tuple[str, str], datetime] = {}

    def process_log(self, log: LogEvent) -> None:
        """Run the full detection pipeline for one log event.
        This is the hot path — called for every message from logs.enriched.
        The fast path (no anomaly) exits after step 1/2 with no API calls.
        The slow path (anomaly detected) makes one LLM call and two DB writes.
        Does not raise. All errors are caught and logged; the pipeline continues.
        Args:
            log: validated LogEvent from Kafka logs.enriched.
        """
        # str(log.tenant_id) converts UUID to string for Redis keys and log fields.
        # All downstream state (Redis, ChromaDB, Kafka keys) uses the string form.
        tenant_str = str(log.tenant_id)

        # Bind tenant/service context once — all subsequent log calls inherit it.
        bound_log = logger.bind(tenant_id=tenant_str, service=log.service)

        # --- Step 1: Statistical detection (always runs) ---
        # update_and_check increments Redis counters for this event AND checks
        # Z-score. It returns None if no anomaly — the common case for most logs.
        stat_result = self._statistical.update_and_check(
            tenant_id=tenant_str,
            service=log.service,
            level=log.level,
            timestamp=log.timestamp,
        )

        # --- Step 2: Semantic detection (ERROR/FATAL only) ---
        # Embedding an INFO or WARN message wastes API budget with no signal value.
        # Statistical detection already handles volume spikes on any level.
        sem_result = None
        if log.level in _ERROR_LEVELS:
            sem_result = self._semantic.update_and_check(
                tenant_id=tenant_str,
                service=log.service,
                level=log.level,
                timestamp=log.timestamp,
                # message is the extra parameter on SemanticDetector — not on the base class.
                # Keyword argument used here so the semantic detector can locate it.
                message=log.message,
            )

        # --- Step 3: Merge and early exit ---
        result = _merge_results(stat_result, sem_result)
        if result is None:
            # Common case: no anomaly detected. Return without any API calls.
            return

        # --- Step 3b: Cooldown deduplication ---
        # When a Z-score spike persists across many log messages, every message
        # above the threshold produces a candidate result. Without a cooldown this
        # means O(spike_volume) LLM calls, which:
        #   1. Exhausts the OpenAI API budget for the minute.
        #   2. Creates a processing backlog of 100+ seconds that delays service B's
        #      alert, preventing the alert-correlator from ever seeing two services'
        #      alerts within its 60-second CASCADE window.
        # Cooldown fires at most once per (tenant, service) per cooldown_seconds,
        # regardless of how many subsequent messages exceed the threshold.
        cooldown_key = (tenant_str, log.service)
        last_fired = self._last_alert_time.get(cooldown_key)
        if last_fired is not None:
            elapsed = (datetime.now(timezone.utc) - last_fired).total_seconds()
            if elapsed < self._alert_cooldown_seconds:
                # Still within cooldown — skip LLM call and publishing entirely.
                return

        bound_log.debug(
            "anomaly_candidate",
            anomaly_type=result.anomaly_type,
            severity=result.severity,
            confidence=round(result.confidence, 3),
        )

        # --- Step 4: Fetch recent error logs for LLM context ---
        # We fetch from the database (not from Kafka) because:
        #   1. The Kafka payload carries only the current message.
        #   2. The LLM needs context: "are errors like this normal for this service?"
        #   3. The DB already has a full history indexed by (tenant_id, service, level).
        # Limit 10 to stay within the LLM's context window and keep latency low.
        try:
            sample_logs = self._log_repo.get_recent_errors(
                tenant_id=tenant_str,
                service=log.service,
                limit=10,
            )
        except Exception as exc:
            # DB failure for context fetch: continue with empty sample_logs.
            # The LLM verifier will see no samples but can still make a decision
            # based on the anomaly_description. Fail-open: don't suppress the alert.
            bound_log.warning(
                "log_context_fetch_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
            sample_logs = []

        # --- Step 5: LLM verification ---
        # Build a human-readable description of what the detectors found.
        anomaly_description = _build_anomaly_description(result)

        # Increment the call counter BEFORE calling — tracks all calls, not just successful ones.
        self._metrics.llm_verifier_calls.labels(tenant=tenant_str).inc()

        verified = self._llm_verifier.verify(
            tenant_id=tenant_str,
            service=log.service,
            sample_logs=sample_logs,
            anomaly_description=anomaly_description,
        )

        # --- Step 6: LLM said NO — false positive ---
        if not verified:
            bound_log.info(
                "anomaly_filtered_by_llm",
                anomaly_type=result.anomaly_type,
                anomaly_description=anomaly_description[:80],
            )
            # Increment false positive counter for dashboard visibility.
            self._metrics.false_positives_filtered.labels(tenant=tenant_str).inc()
            return

        # --- Step 7: Build alert dict ---
        # Use AlertPayload model for validation — catches severity/anomaly_type
        # values that don't match the DB CHECK constraints before attempting the insert.
        payload = AlertPayload(
            # tenant_id: pass UUID object directly — AlertPayload accepts UUID
            tenant_id=log.tenant_id,
            service=log.service,
            anomaly_type=_to_wire_anomaly_type(result.anomaly_type),
            severity=result.severity,
            confidence=round(result.confidence, 4),
            details=result.details,
        )

        # mode='json' serialises UUIDs to strings and datetimes to ISO 8601 strings.
        # This is what Kafka publishers and the PostgreSQL INSERT both expect.
        alert_dict = payload.model_dump(mode="json")
        # status is not in AlertPayload — add it explicitly for the DB INSERT.
        alert_dict["status"] = "open"

        bound_log.info(
            "alert_confirmed",
            alert_id=alert_dict["alert_id"],
            anomaly_type=result.anomaly_type,
            severity=result.severity,
            confidence=round(result.confidence, 3),
        )

        # Record confirmed-alert time so the next candidate within cooldown_seconds
        # is skipped (step 3b above). Updated here — after LLM says YES — so the
        # cooldown window starts from confirmed alerts, not from candidates.
        self._last_alert_time[cooldown_key] = datetime.now(timezone.utc)

        # --- Step 8: Notify all publishers (Observer pattern) ---
        # Each publisher is called independently. A failing publisher (e.g., Kafka
        # broker down) must not block PostgreSQL from recording the alert, and vice versa.
        for publisher in self._publishers:
            try:
                publisher.publish_alert(alert_dict)
            except Exception as exc:
                # One publisher failing must not silence others.
                # Log at ERROR — a DLQ would be appropriate here in production.
                bound_log.error(
                    "alert_publisher_failed",
                    publisher=type(publisher).__name__,
                    alert_id=alert_dict["alert_id"],
                    error=str(exc),
                    error_type=type(exc).__name__,
                )

        # --- Step 9: Increment confirmed anomaly counter ---
        # Increment AFTER publishers run — the metric represents persisted alerts,
        # not just detected candidates. A publisher failure does not decrement
        # (it's a Counter), so the metric may slightly over-count on partial failure.
        self._metrics.anomalies_detected.labels(
            type=result.anomaly_type,
            tenant=tenant_str,
        ).inc()
