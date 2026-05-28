"""Prometheus metrics for the rca-agent service.
All metrics are defined as a dataclass so they can be injected into
KafkaIncidentHandler via its constructor. This is Dependency Inversion:
the handler depends on the Metrics abstraction, not on global prometheus_client
state scattered through the handler class.
Why a dataclass rather than module-level globals?
Module-level Counter calls register metrics with the global CollectorRegistry
at import time. In tests, importing the module in multiple test cases raises
ValueError: "Duplicated timeseries in CollectorRegistry". A dataclass instance
created once in main avoids this — tests can skip metrics entirely.
Metric naming convention:
  Prefix: rca_agent_ — identifies which service owns the metric.
  Suffix: _total (Counter), _seconds (Histogram/timing), no suffix (Gauge).
  Labels: tenant (per-tenant breakdown), model (cost attribution per model),
           status (success/failed routing), reason (DLQ failure breakdown).
"""

from dataclasses import dataclass

from prometheus_client import Counter, Gauge, Histogram


@dataclass
class RCAMetrics:
    """Holds all Prometheus metric objects for the rca-agent service.
    Created once in main and injected into KafkaIncidentHandler.
    All prometheus_client objects are thread-safe for .inc / .set / .observe.
    """

    # --- Total investigations by outcome ---
    # Labels:
    #   status: "success" or "failed" — feeds Grafana Panel 5 (success vs failed).
    #   model: the OpenAI model used — feeds Grafana Panel 16 (model distribution).
    #   tenant: tenant_id UUID — feeds Grafana Panels 21 and 22 per-tenant views.
    investigations_total: Counter

    # --- Failure breakdown by reason ---
    # Incremented on every DLQ write with the specific failure_reason.
    # Labels:
    #   reason: one of 'schema_validation_error', 'low_confidence', 'api_error',
    #           'unexpected_error' — feeds Grafana Panel 6 (failure breakdown).
    #   tenant: per-tenant attribution.
    failure_reason_total: Counter

    # --- Current confidence score ---
    # Gauge (not Counter) because confidence is a rolling value that can go up or
    # down with each investigation. Updated after every successful RCA completion.
    # Labels:
    #   tenant: per-tenant confidence tracking — feeds Grafana Panel 7.
    confidence_score: Gauge

    # --- LLM tokens consumed ---
    # Labels:
    #   type: "input" or "output" — input tokens dominate for RAG-heavy prompts;
    #           output tokens dominate for detailed root cause explanations.
    #   model: model name — GPT-4 and GPT-3.5 have different token prices.
    #   tenant: per-tenant cost attribution — feeds Grafana Panel 8 (cost rolling).
    llm_tokens_total: Counter

    # --- LLM call latency ---
    # Histogram: records latency distribution across many investigations.
    # Buckets chosen for LLM API latency: p50 ≈ 2s, p95 ≈ 10s, p99 ≈ 30s.
    # Grafana Panel 9: p50/p95 latency computed via histogram_quantile.
    # Labels:
    #   tenant: per-tenant latency breakdown.
    llm_latency_seconds: Histogram

    # --- Tool call latency ---
    # Histogram of total tool time per investigation.
    # 'tool' label is "all" (aggregated) because the agent tracks total_tool_latency_ms
    # not per-tool breakdown. Per-tool tracking requires instrumenting each tool
    # function individually — a future enhancement if per-tool dashboards are needed.
    # Labels:
    #   tool: tool name or "all" for aggregated view.
    #   tenant: per-tenant tool usage patterns.
    tool_latency_seconds: Histogram

    # --- Per-tool call counts ---
    # Counter incremented once per tool invocation, using the actual tool name
    # from reasoning_steps. Enables per-tool breakdown in Grafana Panel 28.
    # Labels:
    #   tool: individual tool name (e.g. "build_timeline", "search_knowledge_base").
    #   tenant: per-tenant tool usage patterns.
    tool_calls_total: Counter


def create_metrics() -> RCAMetrics:
    """Instantiate and register all Prometheus metrics for the rca-agent.
    Called once in main before start_http_server. Defining metrics here
    (not at module import time) means test code that imports models.py or
    kafka/handler.py does not trigger prometheus_client registration
    as a side effect.
    Returns:
        RCAMetrics: all metrics registered with the global CollectorRegistry.
    """
    # prometheus_client appends _total to Counter names automatically per
    # OpenMetrics convention. rca_agent_investigations becomes rca_agent_investigations_total.
    investigations_total = Counter(
        "rca_agent_investigations_total",
        "Total RCA investigations completed, by outcome status, model, and tenant",
        # labelnames: positional list of label key names.
        # Values are provided at .labels(status=..., model=..., tenant=...).inc time.
        ["status", "model", "tenant"],
    )

    failure_reason_total = Counter(
        "rca_agent_failure_reason_total",
        "Total RCA failures broken down by failure_reason and tenant",
        ["reason", "tenant"],
    )

    # Gauge: can go up or down. Each successful investigation replaces the previous
    # confidence value for that tenant — it is not cumulative.
    confidence_score = Gauge(
        "rca_agent_confidence_score",
        "Most recent RCA confidence score per tenant (0.0 – 1.0)",
        ["tenant"],
    )

    llm_tokens_total = Counter(
        "rca_agent_llm_tokens_total",
        "Total OpenAI tokens consumed by the RCA agent, by token type, model, and tenant",
        ["type", "model", "tenant"],
    )

    # Buckets chosen for typical LLM API response latencies:
    # 0.5s – 60s range covers everything from cache-warm GPT-3.5 to slow GPT-4 ReAct loops.
    llm_latency_seconds = Histogram(
        "rca_agent_llm_latency_seconds",
        "Total LLM API call latency per investigation (seconds)",
        ["tenant"],
        buckets=[0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0],
    )

    # Buckets for tool call latencies: PostgreSQL queries and ChromaDB searches
    # typically complete in 10ms–2s; outliers up to 10s are possible under load.
    tool_latency_seconds = Histogram(
        "rca_agent_tool_latency_seconds",
        "Total tool execution latency per investigation (seconds); tool='all' is aggregated",
        ["tool", "tenant"],
        buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0],
    )

    tool_calls_total = Counter(
        "rca_agent_tool_calls_total",
        "Total individual tool invocations by the RCA agent, by tool name and tenant",
        ["tool", "tenant"],
    )

    return RCAMetrics(
        investigations_total=investigations_total,
        failure_reason_total=failure_reason_total,
        confidence_score=confidence_score,
        llm_tokens_total=llm_tokens_total,
        llm_latency_seconds=llm_latency_seconds,
        tool_latency_seconds=tool_latency_seconds,
        tool_calls_total=tool_calls_total,
    )
