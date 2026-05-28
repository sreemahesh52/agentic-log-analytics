# --- Prometheus metrics for the eval harness ---
# All metrics are registered here and imported by kafka/handler.py.
# Registering at module level (not per-request) ensures the Prometheus
# registry contains them from startup — scrapes before the first message
# is processed return 0 counts rather than "metric not found" errors.
# Metric naming follows Prometheus conventions:
#   - Counters end in _total
#   - Histograms measure distributions of values (scores, latencies)
#   - Gauges track current-state values (knowledge base size, pass rate)
# Label naming rule: use 'tenant' (not 'tenant_id') to match PROJECT-SPEC.
# The DB column is called tenant_id but the Prometheus label is 'tenant'
# so Grafana queries use {tenant="acme-corp"} consistently across all services.
# Port 8091 is the eval-harness reserved Prometheus scrape port (PROJECT-SPEC).

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# --- eval_faithfulness_score ---
# Histogram: records distribution of faithfulness scores across evaluations.
# Bucket boundaries at 0.0–1.0 in 0.1 increments give enough resolution to
# distinguish scores near the 0.7 and 0.8 pass/fail thresholds.
# Labels:
#   prompt_version: 'v1' or 'v2' — A/B split tracking for Grafana Panel 11.
#   tenant: tenant UUID — per-tenant evaluation quality.
#   eval_mode: 'ground_truth', 'similarity', or 'heuristic' — shows which
#                   tier of faithfulness evaluation was used for this result.
EVAL_FAITHFULNESS_SCORE = Histogram(
    "eval_faithfulness_score",
    "Faithfulness score per RCA evaluation (0.0–1.0)",
    labelnames=["prompt_version", "tenant", "eval_mode"],
    buckets=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)

# --- eval_hallucination_score ---
# Histogram: records hallucination scores (1.0 = no hallucination detected).
# A distribution skewed toward 1.0 means the RCA agent is grounding answers well.
# Labels:
#   prompt_version: 'v1' or 'v2' — A/B tracking matches faithfulness label.
#   tenant: per-tenant hallucination rate.
EVAL_HALLUCINATION_SCORE = Histogram(
    "eval_hallucination_score",
    "Hallucination score per RCA evaluation (0.0–1.0; 1.0 = no hallucination)",
    labelnames=["prompt_version", "tenant"],
    buckets=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)

# --- eval_token_cost_usd_total ---
# Counter (not Gauge) because token cost only accumulates — it never decreases.
# The Grafana rate function over this counter gives cost-per-time-window.
# Labels:
#   tenant: per-tenant cost attribution — feeds Grafana Panel 21 (budget vs actual).
#   model: 'gpt-4-turbo' or 'gpt-3.5-turbo' — cost breakdown by model tier.
#           Kept as an extra dimension beyond the spec minimum for Grafana Panel 16.
EVAL_TOKEN_COST_USD_TOTAL = Counter(
    "eval_token_cost_usd_total",
    "Cumulative USD cost of all LLM calls attributed by the eval harness",
    labelnames=["tenant", "model"],
)

# --- eval_rca_pass_rate ---
# Gauge because pass_rate is a rolling ratio, not a cumulative total.
# Updated after each evaluation: (passed_count / total_count) for the tenant.
# The handler maintains running counts per tenant in memory and calls .set
# after every evaluation with the updated fraction.
# Labels:
#   tenant: per-tenant pass rate — feeds Grafana Panel 10 and Panel 22.
EVAL_RCA_PASS_RATE = Gauge(
    "eval_rca_pass_rate",
    "Fraction of RCA evaluations that passed (faithfulness > 0.7 AND hallucination > 0.7)",
    labelnames=["tenant"],
)

# --- knowledge_base_size ---
# Gauge: current count of rows in past_incidents for this tenant.
# Updated by the Self-Learning Indexer after each successful auto-learn write.
# Labels:
#   tenant: per-tenant knowledge base size — feeds Grafana Panel 18.
KNOWLEDGE_BASE_SIZE = Gauge(
    "knowledge_base_size",
    "Total number of incidents in the knowledge base (past_incidents table)",
    labelnames=["tenant"],
)

# --- knowledge_base_auto_learned_total ---
# Counter: incremented each time the Self-Learning Indexer writes a new entry.
# Monotonically increasing — never decremented even if DB rows are deleted.
# Labels:
#   tenant: per-tenant auto-learning activity.
KNOWLEDGE_BASE_AUTO_LEARNED_TOTAL = Counter(
    "knowledge_base_auto_learned_total",
    "Total RCA results automatically indexed into the knowledge base",
    labelnames=["tenant"],
)

# --- slack_notifications_sent_total ---
# Counter: incremented each time a Slack webhook is POSTed successfully.
# Failures are NOT counted here — they are logged at ERROR separately.
# Labels:
#   severity: 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW' — shows which severities
#             trigger Slack alerts (PROJECT-SPEC: only CRITICAL + faithfulness > 0.7).
#   tenant: per-tenant notification volume.
SLACK_NOTIFICATIONS_SENT_TOTAL = Counter(
    "slack_notifications_sent_total",
    "Total Slack notifications sent for qualifying CRITICAL incidents",
    labelnames=["severity", "tenant"],
)


def start_metrics_server(port: int) -> None:
    """Start the Prometheus HTTP metrics server on the given port.
    Spawns a daemon thread — non-blocking, runs for the lifetime of the process.
    Must be called before any metrics are observed so the /metrics endpoint
    always returns a complete set of metric families from the first scrape.
    Args:
        port: TCP port for the Prometheus scrape endpoint (default 8091).
    """
    # start_http_server from prometheus_client spawns a background daemon thread.
    # The thread serves GET /metrics responses forever — no app code is involved.
    start_http_server(port)
