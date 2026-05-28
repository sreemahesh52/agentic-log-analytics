#!/usr/bin/env bash
# verify_metrics.sh — Prometheus metric presence check for all services.
#
# Usage:
#   chmod +x scripts/verify_metrics.sh
#   bash scripts/verify_metrics.sh
#
# What it does:
#   For each service, curl its /metrics endpoint and grep for one key metric name.
#   Prints PASS or FAIL per check. Exits with code 1 if any check fails.
#
# Why curl not Prometheus API?
#   Prometheus may not have scraped yet — this checks the service directly.
#   Simpler: no jq, no Prometheus auth, works before Prometheus is running.
#
# Why one metric per service not all?
#   If the /metrics endpoint responds and one metric is present, the service
#   is correctly exposing its Prometheus registry. Checking every metric name
#   would create a long fragile list duplicating the source code.
#
# Why exit 1 on failure?
#   Enables CI pipeline gating: `bash scripts/verify_metrics.sh || exit 1`
#   fails the build if any service has a broken /metrics endpoint.

# set -e: exit immediately on any command failure.
# Without this, a failed curl would silently continue to the next check.
set -e

# BASE_URL: services are assumed to be on localhost when running this script
# from the host machine. Inside Docker network, use service names instead.
BASE_URL=http://localhost

# Running counters — accumulated across all check_metric calls.
PASS_COUNT=0
FAIL_COUNT=0

# check_metric SERVICE PORT METRIC_NAME
#   Curls /metrics on the given port and greps for metric_name.
#   Updates PASS_COUNT or FAIL_COUNT. Never exits early — always runs all checks.
check_metric() {
  local service=$1
  local port=$2
  local metric_name=$3

  # -s: silent (no progress bar). -f: fail on HTTP 4xx/5xx (exit code 22).
  # 2>/dev/null: suppress curl error messages (connection refused, timeout).
  local response
  response=$(curl -sf "${BASE_URL}:${port}/metrics" 2>/dev/null) || true

  # If curl returned nothing (empty string), the endpoint was unreachable.
  if [ -z "$response" ]; then
    echo "FAIL: ${service} — /metrics endpoint not reachable on port ${port}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    return
  fi

  # grep -q: quiet mode — exits 0 if found, 1 if not found.
  # We grep for the bare metric name; Prometheus output includes the name in
  # both HELP/TYPE comment lines and the actual metric lines, so either match.
  if echo "$response" | grep -q "$metric_name"; then
    echo "PASS: ${service} — ${metric_name}"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "FAIL: ${service} — ${metric_name} not found in /metrics output"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
}

# ---------------------------------------------------------------------------
# Service checks — one key metric per service.
# ---------------------------------------------------------------------------

echo "=== Agentic Log Analytics — Prometheus Metrics Verification ==="
echo ""

# Go service: metrics registered via promauto, served on /metrics by promhttp.
check_metric "log-ingestion"      8082 "log_ingestion_requests_total"

# Python service: metrics registered in SecurityMetrics dataclass via Counter().
check_metric "security-middleware" 8083 "security_injection_attempts_total"

# Go service: metrics registered via promauto.NewCounter().
check_metric "log-consumer"        8084 "log_consumer_logs_inserted_total"

# Python service: metrics created by create_metrics() in anomaly_agent/metrics.py.
check_metric "anomaly-agent"       8085 "anomaly_agent_anomalies_detected_total"

# Python service: module-level Counter() in alert_correlator/metrics.py.
check_metric "alert-correlator"    8086 "alert_correlation_cascade_total"

# Python service: module-level Histogram() in context_compressor/metrics.py.
check_metric "context-compressor"  8087 "context_compression_ratio"

# Python service: module-level Counter() in semantic_cache/metrics.py.
check_metric "semantic-cache"      8088 "cache_hit_total"

# Python service: module-level Counter() in model_router/metrics.py.
check_metric "model-router"        8089 "model_router_selections_total"

# Python service: Counter registered by create_metrics() in rca_agent/metrics.py.
check_metric "rca-agent"           8090 "rca_agent_investigations_total"

# Python service: Histogram registered at module level in eval_harness/metrics.py.
check_metric "eval-harness"        8091 "eval_faithfulness_score"

# FastAPI service: auto-instrumented by prometheus-fastapi-instrumentator.
# The instrumentator registers http_requests_total with handler/method/status labels.
check_metric "api-gateway"         8000 "http_requests_total"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

echo ""
echo "=== Results: ${PASS_COUNT} PASS, ${FAIL_COUNT} FAIL ==="

# Non-zero FAIL_COUNT: exit 1 so CI pipelines catch the regression.
if [ "$FAIL_COUNT" -gt 0 ]; then
  echo "Action required: fix failing services before proceeding to Step 16b."
  exit 1
else
  echo "All metrics verified. Prometheus should show all targets as UP."
  exit 0
fi
