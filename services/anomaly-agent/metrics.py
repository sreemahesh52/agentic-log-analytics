"""Prometheus metrics for the anomaly-agent service.
All metrics are defined as a Metrics dataclass so they can be injected into
AnomalyOrchestrator via its constructor. This is Dependency Inversion: the
orchestrator depends on the Metrics abstraction injected at startup, not on
prometheus_client globals scattered through the codebase.
Why a dataclass instead of module-level globals:
  Module-level Counter calls register metrics on the global CollectorRegistry.
  In tests, creating multiple test instances re-registers the same metric name,
  raising ValueError. A dataclass instance created once in main avoids this.
Metric naming convention: {service}_{name}_{unit}
  Prefix: anomaly_agent_ — identifies which service owns the metric.
  Labels: tenant, type — enable per-tenant and per-anomaly-type dashboards.
"""

from dataclasses import dataclass

from prometheus_client import Counter


@dataclass
class Metrics:
    """Holds all Prometheus metric objects for the anomaly-agent.
    Created once in main and injected into AnomalyOrchestrator.
    Calling .inc or .labels(...).inc on these objects is thread-safe
    — prometheus_client uses locks internally for counter mutations.
    """

    # --- Confirmed anomalies counter ---
    # Incremented AFTER LLM verification confirms the anomaly is real.
    # Labels:
    #   type: one of 'statistical', 'semantic', 'combined'
    #   tenant: tenant_id UUID string
    anomalies_detected: Counter

    # --- False positives counter ---
    # Incremented when the LLM verifier returns NO (anomaly was noise).
    # High values relative to anomalies_detected indicate loose detector thresholds.
    # Labels:
    #   tenant: tenant_id UUID string
    false_positives_filtered: Counter

    # --- LLM verifier call counter ---
    # Incremented on every call to LLMVerifier.verify, regardless of outcome.
    # anomalies_detected / llm_verifier_calls = LLM-verified confirmation rate.
    # Labels:
    #   tenant: tenant_id UUID string
    llm_verifier_calls: Counter


def create_metrics() -> Metrics:
    """Instantiate and register all Prometheus metrics.
    Called once in main before start_http_server. Registering metrics here
    (not at import time) means test code can import this module without side effects.
    """
    # anomaly_agent_anomalies_detected_total — prometheus_client appends _total automatically
    # for Counters, matching the OpenMetrics convention.
    anomalies_detected = Counter(
        "anomaly_agent_anomalies_detected_total",
        "Total confirmed anomalies detected by type and tenant",
        # labelnames: positional list of label names. Call .labels(type=..., tenant=...).inc
        ["type", "tenant"],
    )

    false_positives_filtered = Counter(
        "anomaly_agent_false_positives_filtered_total",
        "Total anomaly candidates filtered as false positives by the LLM verifier",
        ["tenant"],
    )

    llm_verifier_calls = Counter(
        "anomaly_agent_llm_verifier_calls_total",
        "Total calls made to the LLM verifier (includes both YES and NO outcomes)",
        ["tenant"],
    )

    return Metrics(
        anomalies_detected=anomalies_detected,
        false_positives_filtered=false_positives_filtered,
        llm_verifier_calls=llm_verifier_calls,
    )
