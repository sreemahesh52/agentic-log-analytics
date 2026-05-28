# --- Prometheus metrics for context-compressor ---
# LLMOps: every compression operation is observable.
# Metrics are defined at module level so they are registered once with the
# global Prometheus registry when the module is first imported.
# prometheus_client raises ValueError if the same metric name is registered
# twice. Module-level singletons prevent accidental double-registration.

from prometheus_client import Counter, Histogram

# --- context_compression_ratio ---
# Histogram of (compressed_tokens / original_tokens) per compression call.
# A ratio of 1.0 means no compression was applied (below threshold or failed).
# A ratio of 0.3 means the output is 30% of the input size.
# Buckets are linear from 0.1 to 1.0 to give fine-grained view of compression
# quality: ratios near 0.1–0.3 = high-noise logs; near 0.9–1.0 = already dense.
COMPRESSION_RATIO = Histogram(
    "context_compression_ratio",
    "Ratio of compressed tokens to original tokens (lower = more compression)",
    # tenant: distinguish compression behaviour across different tenants
    # whose logs may differ wildly in noise level.
    ["tenant"],
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)

# --- context_compression_requests_total ---
# Counter incremented on every incident processed by the compressor.
# compressed="true" → token count exceeded threshold, GPT was called.
# compressed="false" → token count was below threshold, passed through.
# The split enables cost analysis: how often does the compressor actually fire?
COMPRESSION_REQUESTS = Counter(
    "context_compression_requests_total",
    "Total incidents processed by the context compressor",
    # compressed: "true" or "false" string label (Prometheus labels are strings).
    # tenant: per-tenant breakdown for cost attribution.
    ["compressed", "tenant"],
)
