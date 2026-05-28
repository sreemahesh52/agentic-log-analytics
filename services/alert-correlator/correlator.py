# --- Alert Correlation Engine — core domain logic ---
# This module is a pure Python class with zero external dependencies.
# It can be imported and tested without Kafka, PostgreSQL, or Redis.
# That is Dependency Inversion in practice: the algorithm does not know
# about the infrastructure that drives it.

import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


# --- Correlation constants ---
# Minimum distinct services in a window to declare a CascadeIncident.
_CASCADE_SERVICE_THRESHOLD = 2

# Severity priority map — higher number = more severe.
# Used to pick the dominant severity when a cascade contains mixed severities.
# A cascade of HIGH + MEDIUM should be routed as HIGH, not averaged or defaulted.
_SEVERITY_PRIORITY: dict[str, int] = {
    "CRITICAL": 4,
    "HIGH": 3,
    "MEDIUM": 2,
    "LOW": 1,
}


def _max_severity(alerts: list[dict[str, Any]]) -> str:
    """Return the highest severity among a list of alert dicts.
    Falls back to 'MEDIUM' if alerts is empty or all severity values are missing.
    The downstream model router depends on this field — a missing severity causes
    it to default to MEDIUM regardless of what the alerts actually said.
    """
    return max(
        (a.get("severity", "LOW") for a in alerts),
        key=lambda s: _SEVERITY_PRIORITY.get(s, 0),
        default="MEDIUM",
    )


@dataclass
class AlertWindow:
    """Holds in-flight alerts for a single tenant within the correlation window.
    Each alert entry is the original alert dict enriched with a 'received_at'
    datetime used for window eviction. Received_at is the wall-clock time the
    alert arrived at the correlator — distinct from the alert's own created_at.
    """

    tenant_id: str
    # List of alert dicts, each with a 'received_at' key added on arrival.
    alerts: list[dict[str, Any]] = field(default_factory=list)
    # Window size in seconds — copied from AlertCorrelator at window creation.
    window_seconds: int = 60


class AlertCorrelator:
    """Groups alerts into incidents using a per-tenant sliding time window.
    Algorithm:
      1. When an alert arrives, add it to the tenant's window with received_at=now.
      2. Evict any alerts older than window_seconds from the window.
      3. If ≥2 distinct services remain in the window → CascadeIncident.
         Clear the window after emitting to prevent re-triggering on the same event.
      4. Otherwise → SingleIncident. Window keeps accumulating (waiting for cascade).
    Thread safety:
      threading.Lock protects _windows so this class is safe to call from multiple
      threads (e.g. a Kafka consumer spawning worker threads). The lock is only held
      for the duration of in-memory computation — never across I/O calls — keeping
      contention minimal.
    Testability:
      The optional `clock` parameter accepts a callable returning datetime. Tests
      inject a fake clock to control time without patching the standard library.
    """

    def __init__(
        self,
        window_seconds: int = 60,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        # Per-tenant alert windows; keyed by tenant_id string.
        self._windows: dict[str, AlertWindow] = {}
        # threading.Lock: protects _windows from concurrent modification.
        # A single-threaded asyncio service still benefits from the lock if it
        # ever runs handlers in executor threads (run_in_executor).
        self._lock = threading.Lock()
        self._window_seconds = window_seconds
        # clock returns current UTC time. Defaults to datetime.now(timezone.utc).
        # Injecting a fake clock lets tests fast-forward time without sleep.
        self._clock: Callable[[], datetime] = clock or (
            lambda: datetime.now(timezone.utc)
        )

    def add_alert(self, alert: dict[str, Any]) -> dict[str, Any]:
        """Process an incoming alert and return an Incident dict.
        Thread-safe via threading.Lock. The lock scope covers only the in-memory
        window mutation — no I/O inside the lock to avoid holding it too long.
        Returns a CascadeIncident dict if ≥2 distinct services are present in the
        window after eviction, or a SingleIncident dict otherwise.
        """
        # Capture time once per call so all operations in this invocation share
        # a single "now" — prevents edge cases from sub-millisecond clock jitter.
        now = self._clock()

        with self._lock:
            tenant_id: str = alert["tenant_id"]

            # --- Get or create window ---
            if tenant_id not in self._windows:
                self._windows[tenant_id] = AlertWindow(
                    tenant_id=tenant_id,
                    window_seconds=self._window_seconds,
                )
            window = self._windows[tenant_id]

            # --- Append new alert ---
            # received_at is the correlator's wall-clock time, not the anomaly
            # agent's created_at. Window eviction is based on arrival time so
            # clock skew between services does not affect window boundaries.
            window.alerts.append({**alert, "received_at": now})

            # --- Evict expired alerts ---
            # cutoff: earliest timestamp still inside the window.
            # Any alert with received_at < cutoff is removed.
            cutoff = now.timestamp() - self._window_seconds
            window.alerts = [
                a
                for a in window.alerts
                if a["received_at"].timestamp() >= cutoff
            ]

            distinct_services = {a["service"] for a in window.alerts}

            if len(distinct_services) >= _CASCADE_SERVICE_THRESHOLD:
                return self._emit_cascade(window, now)

            return self._emit_single(alert, now)

    def _emit_cascade(
        self, window: AlertWindow, now: datetime
    ) -> dict[str, Any]:
        """Build a CascadeIncident from all alerts currently in the window.
        Clears the window after emitting so the same set of alerts does not
        trigger a second cascade on the very next alert for the same tenant.
        Affected services are sorted so the list is deterministic — downstream
        code (deduplication, cache keys) must not depend on insertion order.
        """
        distinct_services = sorted({a["service"] for a in window.alerts})
        incident: dict[str, Any] = {
            "incident_id": str(uuid4()),
            "tenant_id": window.tenant_id,
            "alert_ids": [a["alert_id"] for a in window.alerts],
            "affected_services": distinct_services,
            "is_cascade": True,
            # severity: highest severity among all alerts in the window.
            # A cascade of HIGH + MEDIUM must route as HIGH, not default to MEDIUM.
            # The model router reads this field to select gpt-4-turbo vs gpt-3.5-turbo.
            "severity": _max_severity(window.alerts),
            # correlation_window_ms: tells downstream how wide the window was
            # so they can display "alerts within 60s" in the UI.
            "correlation_window_ms": self._window_seconds * 1000,
            # .isoformat includes the +00:00 offset —
            "created_at": now.isoformat(),
        }
        # Clear window: prevents re-using these alerts in the next incident.
        window.alerts = []
        return incident

    def _emit_single(
        self, alert: dict[str, Any], now: datetime
    ) -> dict[str, Any]:
        """Build a SingleIncident for one alert.
        Does NOT clear the window — single alerts accumulate in case a second
        distinct service fires within the window and upgrades to a cascade.
        """
        return {
            "incident_id": str(uuid4()),
            "tenant_id": alert["tenant_id"],
            "alert_ids": [alert["alert_id"]],
            "affected_services": [alert["service"]],
            "is_cascade": False,
            # severity: forwarded directly from the alert.
            # The model router reads this field — omitting it causes a silent
            # fallback to MEDIUM regardless of what the anomaly agent detected.
            "severity": alert["severity"],
            # correlation_window_ms is 0 for singles: no window was evaluated.
            "correlation_window_ms": 0,
            "created_at": now.isoformat(),
        }
