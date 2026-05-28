# --- Prometheus metrics for the Semantic Cache service ---
# LLMOps: every cache decision is counted so operators can
# monitor hit rate, cost savings, and per-tenant cache efficiency in Grafana.
# Why separate metrics.py module?
# Single Responsibility: the cache logic (cache.py) and Kafka handler
# (kafka/handler.py) import these counters but do not own their registration.
# Centralising registration here prevents double-registration on module reload.
# Counter vs Gauge vs Histogram:
#   Counter — only goes up, survives service restarts (Prometheus rate works).
#   Gauge — can go up or down (e.g. current queue depth) — not needed here.
#   Histogram — samples distributions (latency buckets) — not needed here.
# All metrics here are Counters because they represent cumulative totals.

from prometheus_client import Counter

# cache_hit_total: incremented every time a cache lookup returns a match.
# label tenant: distinguishes per-tenant hit rates in Grafana.
CACHE_HIT_TOTAL = Counter(
    "cache_hit_total",
    "Total number of semantic cache hits (LLM call skipped)",
    ["tenant"],
)

# cache_miss_total: incremented every time no match is found.
# A high miss rate after warm-up indicates the similarity threshold may
# be too high, or incident patterns are genuinely diverse.
CACHE_MISS_TOTAL = Counter(
    "cache_miss_total",
    "Total number of semantic cache misses (incident forwarded to LLM)",
    ["tenant"],
)

# cache_tokens_saved_total: approximate input tokens saved by cache hits.
# Uses the estimated 2000 tokens per RCA investigation as a constant proxy.
# Not exact — real token count varies — but sufficient for cost dashboards.
CACHE_TOKENS_SAVED_TOTAL = Counter(
    "cache_tokens_saved_total",
    "Estimated total tokens saved by cache hits (approx 2000 per hit)",
    ["tenant"],
)

# cache_cost_saved_usd_total: approximate USD saved by cache hits.
# Calculated as tokens_saved * $0.000001 (GPT-4 input token price proxy).
# The actual saving depends on the model that would have been used — this
# is a floor estimate assuming the cheapest model.
CACHE_COST_SAVED_USD_TOTAL = Counter(
    "cache_cost_saved_usd_total",
    "Estimated USD saved by cache hits (approx $0.002 per hit at 2000 tokens)",
    ["tenant"],
)
