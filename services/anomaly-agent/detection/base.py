"""Base types for the Strategy pattern used in anomaly detection.
Every detector in this package implements BaseAnomalyDetector. The orchestrator
pipeline depends on this abstraction — never on StatisticalDetector or
SemanticDetector directly. That is Dependency Inversion: high-level modules
(the Kafka consumer) depend on the interface, not on concrete implementations.
Adding a new detector type (e.g., pattern-frequency-based) requires only:
  1. Create a new class implementing BaseAnomalyDetector.
  2. Register it in the orchestrator.
  No existing class changes — that is the Open/Closed principle in action.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime


# --- Shared result type ---
# dataclass (not Pydantic) because AnomalyDetectionResult is an internal domain
# object that never crosses a serialisation boundary at this layer. The Kafka
# publisher converts it to a Pydantic model before sending. Pure dataclasses
# are lighter and have no validation overhead for internal data.
@dataclass
class AnomalyDetectionResult:
    """Immutable result returned by every anomaly detector strategy.
    detected: True if the detector concluded an anomaly is present.
    tenant_id: which tenant's data triggered this — required for all downstream routing.
    service: the service name that produced the anomalous logs.
    anomaly_type: one of 'error_rate_spike', 'volume_spike', 'new_error_pattern'.
    severity: one of 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'.
    confidence: float 0.0–1.0; how certain the detector is.
    details: detector-specific metadata (z_score, threshold, bucket counts, etc.).
    """

    detected: bool
    tenant_id: str
    service: str
    # anomaly_type valid values: 'error_rate_spike' | 'volume_spike' | 'new_error_pattern'
    anomaly_type: str
    # severity valid values: 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL'
    severity: str
    # confidence is clamped to [0.0, 1.0] by the detector before returning
    confidence: float
    # details carries detector-specific context for downstream consumers and dashboards
    details: dict = field(default_factory=dict)
    # detected_at must be timezone-aware UTC — datetime.now(timezone.utc), never utcnow
    detected_at: datetime = field(default_factory=lambda: datetime.now(__import__("datetime").timezone.utc))


# --- Strategy interface ---
# BaseAnomalyDetector is an Abstract Base Class (ABC). Python's ABC machinery
# raises TypeError at instantiation time if any @abstractmethod is not implemented.
# This is the same guarantee as an interface in Java/Go — no runtime surprises.
class BaseAnomalyDetector(ABC):
    """Abstract base for all anomaly detection strategies.
    Interface Segregation: this interface exposes only two methods:
      update_and_check — process one event and return a result or None.
      detector_type — stable string identifier for this strategy.
    A detector that also publishes to Kafka would violate Single Responsibility.
    Publishing is the Kafka handler's concern, not the detector's.
    Liskov Substitution: every concrete subclass must be usable wherever
    BaseAnomalyDetector is expected without the caller knowing the difference.
    That means update_and_check must never raise — callers rely on None
    meaning "no anomaly detected" and a result meaning "anomaly confirmed".
    """

    @abstractmethod
    def update_and_check(
        self,
        tenant_id: str,
        service: str,
        level: str,
        timestamp: datetime,
    ) -> "AnomalyDetectionResult | None":
        """Process one log event and return a result if an anomaly is detected.
        Implementations must:
          - Update internal state (Redis counters, ChromaDB index, etc.)
          - Evaluate current state against a baseline
          - Return AnomalyDetectionResult if anomaly detected, None otherwise
          - Never raise — catch all errors internally, log WARN, return None
            (fail-open: missing an anomaly is safer than crashing the pipeline)
        Args:
            tenant_id: tenant namespace — all state keyed by this.
            service: service name that emitted this log event.
            level: log level string, e.g. 'ERROR', 'FATAL', 'INFO'.
            timestamp: event time — must be timezone-aware UTC.
        """
        ...

    @abstractmethod
    def detector_type(self) -> str:
        """Return a stable lowercase string identifier for this strategy.
        Used in AnomalyDetectionResult.anomaly_type and Prometheus metric labels.
        Changing this value is a breaking change for downstream consumers and dashboards.
        Examples: 'statistical', 'semantic'.
        """
        ...
