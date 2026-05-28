"""
Seed demo data — fills every UI Dashboard card AND every Grafana panel.
Populates three stores:
  PostgreSQL incidents, rca_results, eval_results
  Redis cache hit/miss counters + 3 entry hashes
  Prometheus all panel metrics pushed via Pushgateway (requires
              docker-compose.demo.yml to be applied so pushgateway
              is running on localhost:9093)
Usage
─────
  docker compose -f infra/docker-compose.yml \\
                 -f infra/docker-compose.demo.yml \\
                 --profile app up -d
  cd scripts && source .venv/bin/activate
  python seed_demo_data.py
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import structlog
from dotenv import load_dotenv

try:
    import redis as redis_lib
except ImportError:
    print("[FAIL] redis package missing — run: pip install redis")
    sys.exit(1)

try:
    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway, delete_from_gateway
    _PROM_OK = True
except ImportError:
    _PROM_OK = False

_REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_REPO_ROOT / "infra" / ".env")
load_dotenv(_REPO_ROOT / ".env")

structlog.configure(processors=[
    structlog.processors.TimeStamper(fmt="iso", utc=True),
    structlog.processors.add_log_level,
    structlog.processors.JSONRenderer(),
])

POSTGRES_URL    = os.getenv("POSTGRES_URL",    "postgresql://admin:admin@localhost:5432/loganalytics")
REDIS_URL       = os.getenv("REDIS_URL",       "redis://localhost:6379")
PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "http://localhost:9093")

# ── Scenarios ──────────────────────────────────────────────────────────────────

SCENARIOS = [
    {"service": "payment-service",  "affected": ["payment-service","order-service"],                   "cascade": True,  "model": "gpt-4-turbo-preview", "pv": "v1", "conf": 0.91, "it": 3420, "ot": 812,  "faith": 0.88, "hall": 0.91, "mode": "ground_truth", "hit": False, "sev": "CRITICAL"},
    {"service": "auth-service",     "affected": ["auth-service"],                                      "cascade": False, "model": "gpt-4-turbo-preview", "pv": "v2", "conf": 0.87, "it": 2890, "ot": 654,  "faith": 0.82, "hall": 0.85, "mode": "ground_truth", "hit": False, "sev": "HIGH"},
    {"service": "order-service",    "affected": ["order-service","inventory-service","payment-service"],"cascade": True,  "model": "gpt-3.5-turbo",       "pv": "v1", "conf": 0.84, "it": 2150, "ot": 521,  "faith": 0.79, "hall": 0.83, "mode": "similarity",   "hit": False, "sev": "CRITICAL"},
    {"service": "payment-service",  "affected": ["payment-service"],                                   "cascade": False, "model": "gpt-4-turbo-preview", "pv": "v2", "conf": 0.91, "it": 0,    "ot": 0,    "faith": 0.90, "hall": 0.92, "mode": "similarity",   "hit": True,  "sev": "HIGH"},
    {"service": "inventory-service","affected": ["inventory-service","order-service"],                  "cascade": True,  "model": "gpt-3.5-turbo",       "pv": "v1", "conf": 0.89, "it": 1980, "ot": 478,  "faith": 0.76, "hall": 0.80, "mode": "heuristic",    "hit": False, "sev": "MEDIUM"},
    {"service": "user-service",     "affected": ["user-service"],                                      "cascade": False, "model": "gpt-3.5-turbo",       "pv": "v2", "conf": 0.82, "it": 2340, "ot": 567,  "faith": 0.77, "hall": 0.81, "mode": "heuristic",    "hit": False, "sev": "LOW"},
]

STEPS = [
    {"type": "Thought",     "content": "Analysing error spike onset at 14:32 UTC."},
    {"type": "Action",      "content": "query_logs(level='ERROR', window_minutes=30)"},
    {"type": "Observation", "content": "87 'connection pool exhausted' errors detected."},
    {"type": "Thought",     "content": "Pool exhaustion is symptom. Checking slow queries."},
    {"type": "Action",      "content": "query_slow_queries(threshold_ms=2000)"},
    {"type": "Observation", "content": "23 queries >5000 ms on orders — all full table scans."},
    {"type": "Thought",     "content": "Missing index likely. Checking knowledge base."},
    {"type": "Action",      "content": "search_knowledge_base(query='connection pool full table scan')"},
    {"type": "Observation", "content": "3 similar past incidents found. Top: missing index on orders.customer_id."},
    {"type": "Thought",     "content": "Strong KB match confirms root cause. Generating recommendations."},
]

# ── Helpers ────────────────────────────────────────────────────────────────────

def _now():
    return datetime.now(timezone.utc)

def _ago(minutes):
    return _now() - timedelta(minutes=minutes)

def _uuid_arr(ids):
    return "{" + ",".join(ids) + "}" if ids else "{}"

# ── PostgreSQL ─────────────────────────────────────────────────────────────────

def seed_postgres(conn, tenant_id, alert_ids):
    ni = nr = ne = 0
    with conn.cursor() as cur:
        for i, sc in enumerate(SCENARIOS):
            off = 90 - i * 12
            inc_id = str(uuid.uuid4())
            linked = alert_ids[(i*2) % max(len(alert_ids),1):(i*2) % max(len(alert_ids),1)+2]
            compressed = (i % 3 == 0)

            cur.execute("""
                INSERT INTO incidents (
                    incident_id, tenant_id, created_at,
                    alert_ids, affected_services, is_cascade,
                    correlation_window_ms, compression_ratio,
                    original_log_count, was_compressed
                ) VALUES (%s,%s::uuid,%s,%s::uuid[],%s,%s,%s,%s,%s,%s)
                ON CONFLICT (incident_id) DO NOTHING
            """, (inc_id, tenant_id, _ago(off),
                  _uuid_arr(linked), sc["affected"], sc["cascade"],
                  30000, 0.62 if compressed else 1.0,
                  150 if compressed else 0, compressed))
            if cur.rowcount: ni += 1
            rca_id = str(uuid.uuid4())
            root_cause = f"Root cause analysis for {sc['service']}: anomaly detected with {sc['conf']*100:.0f}% confidence."
            cost = 0.0 if sc["hit"] else round(sc["it"]*0.00001 + sc["ot"]*0.00003, 6)
            cur.execute("""
                INSERT INTO rca_results (
                    rca_id, tenant_id, incident_id, created_at,
                    root_cause, confidence, recommendations, reasoning_steps,
                    model_used, prompt_version, input_tokens, output_tokens,
                    cache_hit, compression_ratio, status,
                    total_latency_ms, llm_latency_ms, tool_latency_ms
                ) VALUES (%s,%s::uuid,%s::uuid,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (rca_id) DO NOTHING
            """, (rca_id, tenant_id, inc_id, _ago(off-2),
                  root_cause, sc["conf"],
                  [f"Action item {j+1} for {sc['service']}" for j in range(3)],
                  json.dumps(STEPS),
                  sc["model"], sc["pv"], sc["it"], sc["ot"],
                  sc["hit"], 0.62 if compressed else 1.0, "success",
                  200 if sc["hit"] else 4200+i*300,
                  0 if sc["hit"] else 3400+i*200,
                  350+i*40))
            if cur.rowcount: nr += 1
            cur.execute("""
                INSERT INTO eval_results (
                    tenant_id, rca_id, evaluated_at,
                    prompt_version, eval_mode,
                    faithfulness_score, hallucination_score, cost_usd
                ) VALUES (%s::uuid,%s::uuid,%s,%s,%s,%s,%s,%s)
            """, (tenant_id, rca_id, _ago(off-5),
                  sc["pv"], sc["mode"],
                  sc["faith"], sc["hall"], cost))
            if cur.rowcount: ne += 1
    return ni, nr, ne

# ── Redis ──────────────────────────────────────────────────────────────────────

def seed_redis(r, tenant_id):
    r.set(f"cache:{tenant_id}:hits", "4")
    r.set(f"cache:{tenant_id}:misses", "8")
    entry = {"root_cause": "DB connection pool exhausted — missing index.", "confidence": 0.91}
    for _ in range(3):
        k = f"cache:{tenant_id}:{uuid.uuid4()}"
        r.hset(k, mapping={"rca_result": json.dumps(entry), "created_at": _now().isoformat()})
        r.expire(k, 86400)

# ── Prometheus ─────────────────────────────────────────────────────────────────

def _build_registry(tenant: str, m: float) -> "CollectorRegistry":
    """Build a CollectorRegistry with all demo metrics.

    Each metric name is registered ONCE; .labels() is called per label-set.
    m=0.75 for round-1, m=1.0 for round-2 — the delta drives rate().
    """
    reg = CollectorRegistry()
    def G(name, help_, labels):
        return Gauge(name, help_, labels, registry=reg)
    # ── Gauges (same both rounds) ──────────────────────────────────────────
    G("eval_rca_pass_rate",        "RCA pass rate",    ["tenant"]).labels(tenant=tenant).set(0.833)
    G("rca_agent_confidence_score","RCA confidence",   ["tenant"]).labels(tenant=tenant).set(0.87)
    G("knowledge_base_size",       "KB total incidents",["tenant"]).labels(tenant=tenant).set(20)
    fa = G("eval_faithfulness_avg",  "Avg faithfulness", ["tenant", "prompt_version"])
    fa.labels(tenant=tenant, prompt_version="v1").set(0.88)
    fa.labels(tenant=tenant, prompt_version="v2").set(0.82)
    ha = G("eval_hallucination_avg", "Avg hallucination",["tenant", "prompt_version"])
    ha.labels(tenant=tenant, prompt_version="v1").set(0.90)
    ha.labels(tenant=tenant, prompt_version="v2").set(0.86)
    # ── Counter-like gauges (scaled by m — delta between rounds drives rate) ──
    G("cache_hit_total",          "Cache hits",   ["tenant"]).labels(tenant=tenant).set(round(4    * m))
    G("cache_miss_total",         "Cache misses", ["tenant"]).labels(tenant=tenant).set(round(8    * m))
    G("cache_tokens_saved_total", "Tokens saved", ["tenant"]).labels(tenant=tenant).set(round(8000 * m))
    G("cache_cost_saved_usd_total","Cost saved USD",["tenant"]).labels(tenant=tenant).set(round(0.08 * m, 4))
    G("knowledge_base_auto_learned_total","Auto-learned",["tenant"]).labels(tenant=tenant).set(round(2 * m))
    G("alert_correlation_single_total",  "Single", ["tenant"]).labels(tenant=tenant).set(round(4 * m))
    G("alert_correlation_cascade_total", "Cascade",["tenant"]).labels(tenant=tenant).set(round(2 * m))
    aa = G("anomaly_agent_anomalies_detected_total",       "Anomalies", ["tenant", "type"])
    aa.labels(tenant=tenant, type="statistical").set(round(6 * m))
    G("anomaly_agent_false_positives_filtered_total","FP filtered",["tenant"]).labels(tenant=tenant).set(round(2 * m))
    tc = G("eval_token_cost_usd_total", "Token cost USD", ["tenant", "model"])
    tc.labels(tenant=tenant, model="gpt-4-turbo-preview").set(round(0.05 * m, 5))
    tc.labels(tenant=tenant, model="gpt-3.5-turbo"      ).set(round(0.02 * m, 5))
    mr = G("model_router_selections_total", "Model selections", ["tenant", "model", "severity"])
    mr.labels(tenant=tenant, model="gpt-4-turbo-preview", severity="CRITICAL").set(round(3 * m))
    mr.labels(tenant=tenant, model="gpt-3.5-turbo",       severity="MEDIUM"  ).set(round(3 * m))
    ri = G("rca_agent_investigations_total", "RCA investigations", ["tenant", "status", "model"])
    ri.labels(tenant=tenant, status="success", model="gpt-4-turbo-preview").set(round(4 * m))
    ri.labels(tenant=tenant, status="success", model="gpt-3.5-turbo"      ).set(round(2 * m))
    li = G("log_ingestion_requests_total", "Log ingestion requests", ["tenant", "status"])
    li.labels(tenant=tenant, status="accepted").set(round(1240 * m))
    li.labels(tenant=tenant, status="rejected").set(round(18   * m))
    ia = G("security_injection_attempts_total", "Injection attempts", ["tenant", "service"])
    ia.labels(tenant=tenant, service="payment-service" ).set(round(3 * m))
    ia.labels(tenant=tenant, service="auth-service"    ).set(round(2 * m))
    ia.labels(tenant=tenant, service="order-service"   ).set(round(1 * m))
    pr = G("security_pii_redactions_total", "PII redactions", ["tenant", "field_type"])
    pr.labels(tenant=tenant, field_type="email"      ).set(round(47 * m))
    pr.labels(tenant=tenant, field_type="ipv4"       ).set(round(23 * m))
    pr.labels(tenant=tenant, field_type="credit_card").set(round(12 * m))
    pr.labels(tenant=tenant, field_type="phone_us"   ).set(round(8  * m))
    return reg


def push_prometheus(tenant: str) -> bool:
    """Push two metric rounds with a 21 s gap so Prometheus can compute rate.

    Returns True on success, False if pushgateway is unreachable or push fails.
    """
    if not _PROM_OK:
        print(" [SKIP] prometheus_client not installed — pip install prometheus-client")
        return False
    import requests as _req
    try:
        r = _req.get(PUSHGATEWAY_URL + "/-/ready", timeout=4)
        assert r.status_code in (200, 204), f"HTTP {r.status_code}"
    except Exception as exc:
        print(f" [WARN] Pushgateway unreachable ({PUSHGATEWAY_URL}): {exc}")
        print(" Grafana panels will be empty.")
        print(" Start it: docker compose -f infra/docker-compose.yml \\")
        print("           -f infra/docker-compose.demo.yml \\")
        print("           up -d --no-deps pushgateway prometheus")
        return False
    job = "seed_demo"
    grouping = {"tenant": tenant}
    try:
        delete_from_gateway(PUSHGATEWAY_URL, job=job, grouping_key=grouping)
    except Exception:
        pass
    try:
        push_to_gateway(PUSHGATEWAY_URL, job=job,
                        registry=_build_registry(tenant, 0.75),
                        grouping_key=grouping)
        print(" ✓ Prometheus round 1 pushed — waiting 21 s …")
        time.sleep(21)
        push_to_gateway(PUSHGATEWAY_URL, job=job,
                        registry=_build_registry(tenant, 1.0),
                        grouping_key=grouping)
        print(" ✓ Prometheus round 2 pushed — Grafana will update in ~15 s")
        return True
    except Exception as exc:
        print(f" [FAIL] Prometheus push error: {exc}")
        import traceback; traceback.print_exc()
        return False

# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> int:
    bar = "=" * 64
    print(f"\n{bar}")
    print(" seed_demo_data — PostgreSQL + Redis + Prometheus")
    print(f"{bar}")
    try:
        conn = psycopg2.connect(POSTGRES_URL)
        conn.autocommit = False
        print(" ✓ PostgreSQL connected")
    except Exception as exc:
        print(f" [FAIL] PostgreSQL: {exc}")
        return 1
    try:
        rc = redis_lib.from_url(REDIS_URL, decode_responses=True)
        rc.ping()
        print(" ✓ Redis connected")
    except Exception as exc:
        print(f" [FAIL] Redis: {exc}")
        conn.close()
        return 1
    pg_ok = True
    try:
        with conn.cursor() as cur:
            tenants = {}
            for name in ["acme-corp", "startup-co"]:
                cur.execute("SELECT tenant_id FROM tenants WHERE name = %s", (name,))
                row = cur.fetchone()
                if not row:
                    print(f"\n [SKIP] {name} not found — run seed_tenants.py first")
                    continue
                tenants[name] = str(row[0])
        if not tenants:
            print("\n [FAIL] No tenants found. Run seed_tenants.py first.")
            conn.close()
            return 1
        for name, tid in tenants.items():
            print(f"\n ── {name} ({tid[:8]}…) ──")
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT alert_id FROM alerts WHERE tenant_id=%s::uuid "
                    "ORDER BY created_at DESC LIMIT 20", (tid,))
                alert_ids = [str(r[0]) for r in cur.fetchall()]
            ni, nr, ne = seed_postgres(conn, tid, alert_ids)
            conn.commit()
            print(f" ✓ incidents {ni}  rca_results {nr}  eval_results {ne}")
            seed_redis(rc, tid)
            print(" ✓ Redis: hits=4  misses=8  entries=3")
            push_prometheus(name)
    except Exception as exc:
        conn.rollback()
        import traceback
        print(f"\n [FAIL] {exc}")
        traceback.print_exc()
        pg_ok = False
    finally:
        conn.close()
    if not pg_ok:
        return 1
    print(f"\n{bar}")
    print(" DONE")
    print(" UI     → http://localhost:3001/dashboard")
    print(" Grafana → http://localhost:3000  (admin / admin)")
    print()
    print(" UI Dashboard cards:")
    print("   Pass Rate 83%  |  Cache Hit Rate 33%  |  Tokens Saved 8,000")
    print("   Cached Investigations 3  |  KB Size 20")
    print()
    print(" Grafana panels (wait ~15 s then refresh):")
    print("   Cache Hit Rate 33%  |  Tokens Saved 8,000  |  Cost Saved $0.08")
    print("   Model Dist GPT-4+GPT-3.5  |  Pass Rate 83%")
    print("   Faithfulness 0.88/0.82 (v1/v2)  |  Hallucination 0.90/0.86")
    print("   Anomalies 6  |  Cascade 2  |  Single 4  |  KB Size 20")
    print(f"{bar}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
