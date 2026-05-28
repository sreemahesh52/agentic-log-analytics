# --- Unit tests for AlertCorrelator ---
# all tests are pure in-memory; no Kafka, PostgreSQL, or Redis.
# The fake clock fixture eliminates time.sleep and makes time-sensitive tests
# deterministic — prohibits sleep in tests.
# Each test name describes the scenario and expected outcome, making failure
# messages self-explanatory without reading the body.

import threading
from datetime import datetime, timedelta, timezone
from uuid import uuid4

# sys.path is adjusted here so tests can import from the parent package when run
# with `python -m pytest tests/` from the service root directory.
import sys
import os

# Add the services/alert-correlator directory to sys.path so imports resolve.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from correlator import AlertCorrelator  # noqa: E402 (after sys.path manipulation)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_alert(tenant_id: str, service: str) -> dict:
    """Build a minimal alert dict matching the anomaly-agent wire format."""
    return {
        "alert_id": str(uuid4()),
        "tenant_id": tenant_id,
        "service": service,
        "anomaly_type": "statistical",
        "severity": "HIGH",
        "confidence": 0.9,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def _make_clock(start: datetime):
    """Return a mutable fake clock and an advance function.
    The clock is a list-based closure so tests can advance time without
    patching datetime in the standard library — avoids brittle mock.patch targets.
    """
    current = [start]

    def clock() -> datetime:
        # current[0] dereferences the mutable list element.
        return current[0]

    def advance(seconds: float) -> None:
        current[0] = current[0] + timedelta(seconds=seconds)

    return clock, advance


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSingleIncident:
    """Tests that verify single-service alert pass-through behaviour."""

    def test_single_alert_produces_single_incident(self):
        """One alert always produces a SingleIncident with is_cascade=False."""
        correlator = AlertCorrelator(window_seconds=60)
        alert = _make_alert("tenant-1", "payment-service")

        result = correlator.add_alert(alert)

        assert result["is_cascade"] is False
        assert result["affected_services"] == ["payment-service"]
        assert result["alert_ids"] == [alert["alert_id"]]
        assert result["correlation_window_ms"] == 0
        assert result["tenant_id"] == "tenant-1"
        assert "incident_id" in result
        assert "created_at" in result

    def test_two_alerts_same_service_produces_single_not_cascade(self):
        """Two alerts from the same service do NOT form a cascade.
        CascadeIncident requires ≥2 DISTINCT services. Two alerts from
        payment-service are both inside the window but distinct_services has
        cardinality 1 — below the threshold.
        """
        correlator = AlertCorrelator(window_seconds=60)
        alert_a = _make_alert("tenant-1", "payment-service")
        alert_b = _make_alert("tenant-1", "payment-service")

        correlator.add_alert(alert_a)
        result = correlator.add_alert(alert_b)

        # Second alert from the same service → still a single incident.
        assert result["is_cascade"] is False
        assert result["affected_services"] == ["payment-service"]

    def test_single_incident_carries_alert_severity(self):
        """A SingleIncident must include the severity from its source alert.
        Root cause of this test: the model router reads incident['severity'] to
        select gpt-4-turbo vs gpt-3.5-turbo. If severity is absent the router
        defaults to MEDIUM, causing a CRITICAL or HIGH alert to be silently
        downgraded to a cheaper model with no warning.
        """
        correlator = AlertCorrelator(window_seconds=60)
        alert = {**_make_alert("tenant-1", "payment-service"), "severity": "CRITICAL"}

        result = correlator.add_alert(alert)

        assert result["severity"] == "CRITICAL", (
            f"SingleIncident must carry severity from its alert, got {result.get('severity')!r}"
        )


class TestCascadeIncident:
    """Tests that verify multi-service cascade detection."""

    def test_two_alerts_different_services_produces_cascade(self):
        """Two alerts from distinct services within the window → CascadeIncident."""
        correlator = AlertCorrelator(window_seconds=60)
        alert_a = _make_alert("tenant-1", "payment-service")
        alert_b = _make_alert("tenant-1", "auth-service")

        correlator.add_alert(alert_a)
        result = correlator.add_alert(alert_b)

        assert result["is_cascade"] is True
        # sorted means the order is deterministic regardless of insertion order.
        assert result["affected_services"] == ["auth-service", "payment-service"]
        assert len(result["alert_ids"]) == 2
        assert alert_a["alert_id"] in result["alert_ids"]
        assert alert_b["alert_id"] in result["alert_ids"]
        assert result["correlation_window_ms"] == 60_000

    def test_cascade_includes_all_alert_ids(self):
        """All alert IDs in the window are included in the cascade."""
        correlator = AlertCorrelator(window_seconds=60)
        # Three alerts: service-a, service-b, and a second service-a alert.
        # The second service-a alert is already in the window when service-b arrives.
        a1 = _make_alert("tenant-1", "service-a")
        a2 = _make_alert("tenant-1", "service-a")
        b1 = _make_alert("tenant-1", "service-b")

        correlator.add_alert(a1)
        correlator.add_alert(a2)
        result = correlator.add_alert(b1)

        assert result["is_cascade"] is True
        # All three alert IDs must appear in the cascade.
        assert set(result["alert_ids"]) == {a1["alert_id"], a2["alert_id"], b1["alert_id"]}

    def test_cascade_affected_services_sorted(self):
        """affected_services in a CascadeIncident is always alphabetically sorted.
        Sorting is required so downstream consumers (cache keys, deduplication)
        get a deterministic representation regardless of alert arrival order.
        """
        correlator = AlertCorrelator(window_seconds=60)
        # Insert in reverse alphabetical order to verify sorting is applied.
        correlator.add_alert(_make_alert("tenant-1", "zebra-service"))
        result = correlator.add_alert(_make_alert("tenant-1", "alpha-service"))

        assert result["is_cascade"] is True
        assert result["affected_services"] == ["alpha-service", "zebra-service"]

    def test_cascade_uses_highest_severity_from_window(self):
        """A CascadeIncident severity is the highest among all alerts in the window.
        Root cause of this test: a cascade of HIGH + MEDIUM must route as HIGH.
        If the correlator omitted severity (or used the last alert's value), a
        CRITICAL alert combined with a LOW alert could produce a MEDIUM routing
        decision — causing GPT-3.5 to investigate what should be a GPT-4 incident.
        """
        correlator = AlertCorrelator(window_seconds=60)
        low_alert = {**_make_alert("tenant-1", "cache-service"), "severity": "LOW"}
        critical_alert = {**_make_alert("tenant-1", "payment-service"), "severity": "CRITICAL"}

        correlator.add_alert(low_alert)
        result = correlator.add_alert(critical_alert)

        assert result["is_cascade"] is True
        assert result["severity"] == "CRITICAL", (
            f"Cascade with CRITICAL + LOW must use CRITICAL, got {result.get('severity')!r}"
        )

    def test_cascade_mixed_severities_high_wins_over_medium(self):
        """HIGH beats MEDIUM in a cascade — not just CRITICAL beats everything."""
        correlator = AlertCorrelator(window_seconds=60)
        medium_alert = {**_make_alert("tenant-1", "auth-service"), "severity": "MEDIUM"}
        high_alert = {**_make_alert("tenant-1", "order-service"), "severity": "HIGH"}

        correlator.add_alert(medium_alert)
        result = correlator.add_alert(high_alert)

        assert result["is_cascade"] is True
        assert result["severity"] == "HIGH"


class TestWindowEviction:
    """Tests that verify the sliding window eviction logic."""

    def test_expired_alerts_not_counted_in_cascade(self):
        """An alert that entered the window before window_seconds ago is evicted.
        Scenario: Alert-A arrives at T=0 for service-a. Clock advances 61s past
        the 60s window. Alert-B arrives for service-b. Alert-A is evicted because
        its received_at (T=0) is before the cutoff (T=61-60=1s). Only service-b
        remains → SingleIncident, not a cascade.
        """
        start = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        clock, advance = _make_clock(start)
        correlator = AlertCorrelator(window_seconds=60, clock=clock)

        # Alert at T=0
        alert_a = _make_alert("tenant-1", "service-a")
        result_a = correlator.add_alert(alert_a)
        assert result_a["is_cascade"] is False

        # Advance clock past the window boundary (60s + 1s).
        advance(61)

        # Alert at T=61 — alert_a should be evicted.
        alert_b = _make_alert("tenant-1", "service-b")
        result_b = correlator.add_alert(alert_b)

        # Only service-b is in the window → single, not cascade.
        assert result_b["is_cascade"] is False
        assert result_b["affected_services"] == ["service-b"]

    def test_cascade_clears_window(self):
        """After a CascadeIncident is emitted the window is cleared.
        The next alert for the same tenant starts a fresh accumulation cycle
        and produces a SingleIncident, proving the window was reset.
        """
        correlator = AlertCorrelator(window_seconds=60)

        # Trigger cascade.
        correlator.add_alert(_make_alert("tenant-1", "service-a"))
        cascade = correlator.add_alert(_make_alert("tenant-1", "service-b"))
        assert cascade["is_cascade"] is True

        # Next alert after cascade should start fresh → single.
        result = correlator.add_alert(_make_alert("tenant-1", "service-c"))
        assert result["is_cascade"] is False
        assert result["affected_services"] == ["service-c"]
        # Only one alert_id: the new alert, not the previous cascade's alerts.
        assert len(result["alert_ids"]) == 1


class TestThreadSafety:
    """Tests that verify correct behaviour under concurrent access."""

    def test_thread_safety(self):
        """Concurrent alert additions must not corrupt window state.
        10 threads each call add_alert concurrently for the same tenant.
        We assert: no exceptions, all 10 calls returned a well-formed incident,
        and no required fields are missing or have wrong types.
        """
        correlator = AlertCorrelator(window_seconds=60)
        results: list[dict] = []
        errors: list[Exception] = []
        # Protects the results and errors lists themselves from concurrent appends.
        collect_lock = threading.Lock()

        def add_and_collect(idx: int) -> None:
            try:
                # Use 3 different service names so some calls produce cascades.
                service = f"service-{idx % 3}"
                alert = _make_alert("tenant-thread", service)
                incident = correlator.add_alert(alert)
                with collect_lock:
                    results.append(incident)
            except Exception as exc:
                with collect_lock:
                    errors.append(exc)

        threads = [threading.Thread(target=add_and_collect, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Thread errors occurred: {errors}"
        assert len(results) == 10, "All 10 calls must return an incident"

        # Validate structure of every returned incident.
        # severity is included: the model router requires it to select gpt-4 vs gpt-3.5.
        required_keys = {
            "incident_id", "tenant_id", "alert_ids",
            "affected_services", "is_cascade",
            "severity", "correlation_window_ms", "created_at",
        }
        for incident in results:
            missing = required_keys - incident.keys()
            assert not missing, f"Incident missing keys: {missing}"
            assert isinstance(incident["alert_ids"], list)
            assert isinstance(incident["affected_services"], list)
            assert isinstance(incident["is_cascade"], bool)
            assert isinstance(incident["correlation_window_ms"], int)


class TestTenantIsolation:
    """Tests that verify separate tenants maintain independent windows."""

    def test_tenant_isolation(self):
        """Alerts for two different tenants use independent windows.
        Tenant-A and Tenant-B each send one alert to service-x. Neither
        should see a cascade because the windows are separate — there are
        not two distinct services in EITHER tenant's window.
        """
        correlator = AlertCorrelator(window_seconds=60)

        alert_a = _make_alert("tenant-alpha", "service-x")
        alert_b = _make_alert("tenant-beta", "service-x")

        result_a = correlator.add_alert(alert_a)
        result_b = correlator.add_alert(alert_b)

        # Neither tenant has two distinct services — both should be singles.
        assert result_a["is_cascade"] is False
        assert result_a["tenant_id"] == "tenant-alpha"

        assert result_b["is_cascade"] is False
        assert result_b["tenant_id"] == "tenant-beta"

        # Verify window state is not shared: add a second service to tenant-alpha only.
        result_cascade = correlator.add_alert(_make_alert("tenant-alpha", "service-y"))
        assert result_cascade["is_cascade"] is True
        assert result_cascade["tenant_id"] == "tenant-alpha"

        # tenant-beta still only has one service — adding another service-x alert
        # does not cascade because both alerts are service-x.
        result_single = correlator.add_alert(_make_alert("tenant-beta", "service-x"))
        assert result_single["is_cascade"] is False
