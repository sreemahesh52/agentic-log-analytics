# --- Prometheus metrics for the Model Router service ---
# all metrics registered in one module so they are easy to find,
# audit, and extend. prometheus_client registers metrics in a global registry;
# start_http_server in main.py serves that registry on /metrics.
# Label design rationale:
#   model: tracks model distribution (GPT-4 vs GPT-3.5) — feeds Grafana Panel 16
#   severity: shows which alert severities drive GPT-4 usage and associated cost
#   tenant: enables per-tenant cost attribution — feeds Grafana Panels 21 and 22
# Why a Counter, not a Gauge?
# Counter: monotonically increasing — correct for counting events (routing decisions).
# Gauge: can go up and down — correct for point-in-time values (queue depth, memory).
# Using a Gauge for selections would give misleading results if the process restarts.

from prometheus_client import Counter

# model_router_selections_total: incremented once per successfully routed incident.
# A routing decision that results in DLQ does NOT increment this counter —
# only incidents that reach incidents.ready are counted here.
MODEL_ROUTER_SELECTIONS_TOTAL = Counter(
    "model_router_selections_total",
    "Total number of LLM model routing decisions made, by model, severity, and tenant",
    # Label names match Grafana query variables for panel filtering.
    ["model", "severity", "tenant"],
)
