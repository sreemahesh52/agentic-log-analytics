"""Detection package for the anomaly-agent service.
Exports the public API so callers import from 'detection' not from submodules.
New detector types are added here without touching existing imports — Open/Closed.
"""

from detection.base import AnomalyDetectionResult, BaseAnomalyDetector
from detection.semantic import SemanticDetector
from detection.statistical import StatisticalDetector

__all__ = [
    "AnomalyDetectionResult",
    "BaseAnomalyDetector",
    "SemanticDetector",
    "StatisticalDetector",
]
