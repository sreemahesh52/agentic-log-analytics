# --- Prometheus metrics for the alert-correlator service ---
# LLMOps: every service exposes a /metrics endpoint so Prometheus
# can scrape it and Grafana can visualise cascade vs single incident rates
# per tenant in real time.
# promauto registers metrics at import time — no explicit register call needed.
# The HTTP server is started once in main.py; this module only declares counters.

from prometheus_client import Counter

# --- Counters ---
# label: tenant — allows per-tenant breakdown in Grafana panel 22.

# Incremented each time a CascadeIncident is emitted (≥2 distinct services
# fired within the correlation window). High cascade_total / single_total ratio
# means one root cause is triggering alerts across multiple services — classic
# "DB is down and everything fails at once" scenario.
CASCADE_TOTAL = Counter(
    "alert_correlation_cascade_total",
    "Number of CascadeIncidents emitted (multi-service within window)",
    # labelnames: list of label keys. Values provided at .labels(...).inc call.
    ["tenant"],
)

# Incremented each time a SingleIncident is emitted (one service in window).
# A sustained high single_total with zero cascade_total is normal operation.
# Sudden spike in single_total often precedes a cascade as other services fail.
SINGLE_TOTAL = Counter(
    "alert_correlation_single_total",
    "Number of SingleIncidents emitted (one service in window)",
    ["tenant"],
)
