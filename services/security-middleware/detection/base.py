"""Base types for the Strategy pattern used in security detection.
Every detector in this package implements BaseDetector. The consumer pipeline
depends on this abstraction, never on a concrete class — that is Dependency
Inversion: high-level modules (the Kafka consumer) depend on the interface,
not on InjectionDetector or PIIDetector directly.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass


# --- Shared result type ---
# dataclass (not Pydantic) because DetectionResult is internal domain logic
# that never crosses a serialization boundary. Pure Python is faster here.
@dataclass
class DetectionResult:
    """Immutable result returned by every detector strategy.
    detected: True if at least one pattern matched.
    sanitized_message: the message with all detected content replaced by tokens.
    details: detector-specific metadata (matched patterns, field types, counts).
    """

    detected: bool
    sanitized_message: str
    details: dict


# --- Strategy interface ---
# BaseDetector is an Abstract Base Class (ABC). ABCs in Python enforce that
# every subclass implements the @abstractmethod methods — trying to instantiate
# a subclass that skips one raises TypeError at class definition time.
# This is the same role as an interface in Java or Go.
class BaseDetector(ABC):
    """Abstract base for all detection strategies.
    Open/Closed principle: new detector types are added by creating a new
    subclass — existing code (InjectionDetector, PIIDetector, the consumer)
    never needs to change.
    Interface Segregation: this interface is intentionally narrow — only
    detect and detector_type. A detector that also publishes to Kafka
    would violate Single Responsibility.
    """

    @abstractmethod
    def detect(self, message: str) -> DetectionResult:
        """Scan message and return a DetectionResult.
        Implementations must be pure functions with no side effects:
        no I/O, no network, no state mutation. This makes them unit-testable
        with zero external dependencies.
        Implementations must never raise — catch internally and return
        detected=False with the original message on any scanning failure.
        """
        ...

    @abstractmethod
    def detector_type(self) -> str:
        """Return a stable lowercase string identifier, e.g. 'injection' or 'pii'.
        Used as the event_type field in SecurityEvent Kafka messages.
        Changing this value is a breaking change for downstream consumers.
        """
        ...
