# Agentic Log Analytics

[![CI](https://github.com/sreemahesh52/argus/actions/workflows/ci.yml/badge.svg)](https://github.com/sreemahesh52/argus/actions/workflows/ci.yml)

A multi-tenant, event-driven platform that ingests structured logs, detects anomalies, and produces AI-generated root cause analyses — with semantic caching, prompt A/B testing, and end-to-end faithfulness evaluation.

---

## Contents

- [Overview](#overview)
- [Screenshots](#screenshots)
- [Architecture](#architecture)
- [Services](#services)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Seeding Data](#seeding-data)
- [Generating Traffic](#generating-traffic)
- [API Reference](#api-reference)
- [Configuration](#configuration)
- [Observability](#observability)
- [Development](#development)
- [Project Structure](#project-structure)

---

## Overview

Logs flow through a Kafka-backed pipeline: ingested via HTTP, security-scanned for PII and injection patterns, persisted to PostgreSQL, then analysed for anomalies. Correlated anomalies trigger an LLM-powered investigation — the RCA agent performs hybrid RAG over a knowledge base of past incidents, compresses the relevant log context, and generates a structured root cause report.

Completed investigations are:

- **Cached** — a semantic similarity check against Redis short-circuits identical investigations without an LLM call.
- **Evaluated** — a separate eval harness scores each report for faithfulness and hallucination resistance, with A/B comparison across prompt versions.
- **Observable** — every service emits Prometheus metrics; a pre-provisioned Grafana dashboard covers ingestion rates, anomaly counts, RCA latency, cache hit rates, and evaluation scores.

The system is multi-tenant from the ground up. Each tenant carries a hashed API key, a model tier (`premium` → GPT-4, `standard` → GPT-3.5), and isolated data throughout the pipeline.

---

## Screenshots

### Initial State

![Initial State — no logs, events, alerts, or investigations yet](docs/screenshots/initial-state.gif)

### Flooding errors

![Flooding errors - simulate error flooding](docs/screenshots/flooding-errors.gif)

### Alert analysis

![Incident logging and RCA investigation](docs/screenshots/alert-analysis.gif)

### Budget tracking

![Token budget and cache tracking](docs/screenshots/budget-tracking.gif)

### Grafana dashboard

![Grafana dashboard with populated metrics](docs/screenshots/grafana-dashboard.gif)

### Infra Setup

![Docker Compose stack with services](docs/screenshots/infra-setup.png)

---

## Architecture

```
 Client / Scripts
       │
       │  POST /api/v1/logs  (X-API-Key auth)
       ▼
 ┌─────────────┐
 │ API Gateway │  FastAPI · multi-tenant auth · REST + SSE
 └──────┬──────┘
        │
        ▼
 ┌──────────────┐     Kafka
 │ Log Ingestion│────────────────► logs.raw
 │    (Go)      │
 └──────────────┘
                              ┌──────────────────────┐
 logs.raw ────────────────────► Security Middleware   │
                              │  PII redaction        │
                              │  Injection detection  │
                              └──────────┬───────────┘
                                         │
                          ┌──────────────┴──────────────┐
                          ▼                             ▼
                   logs.raw.clean               security.events
                          │
                          ▼
                  ┌───────────────┐
                  │ Log Consumer  │──► PostgreSQL (logs table)
                  │    (Go)       │──► logs.enriched
                  └───────┬───────┘
                          │
                   logs.enriched
                          │
                          ▼
                  ┌───────────────┐
                  │ Anomaly Agent │──► PostgreSQL (alerts table)
                  │  z-score +    │
                  │  semantic     │
                  └───────┬───────┘
                          │
                          ▼
                  ┌──────────────────┐
                  │ Alert Correlator │──► PostgreSQL (incidents table)
                  │  sliding window  │
                  └───────┬──────────┘
                          │
              ┌───────────┴────────────┐
              ▼                        ▼
    ┌──────────────────┐     ┌──────────────────┐
    │ Context          │     │ Semantic Cache   │
    │ Compressor       │     │  (Redis + embed) │
    │ PostgreSQL +     │     └────────┬─────────┘
    │ ChromaDB RAG     │              │ cache miss
    └────────┬─────────┘              │
             └──────────┬────────────┘
                        ▼
               ┌────────────────┐
               │   RCA Agent    │──► PostgreSQL (rca_results)
               │  ReAct · RAG   │
               │  Model Router  │──► OpenAI GPT-4 / GPT-3.5
               └────────┬───────┘
                        │
                        ▼
               ┌────────────────┐
               │  Eval Harness  │──► PostgreSQL (eval_results)
               │  faithfulness  │
               │  hallucination │
               │  A/B prompts   │
               └────────────────┘
```

---

## Services

| Service | Language | Port | Responsibility |
|---|---|---|---|
| `api-gateway` | Python (FastAPI) | 8000 | Multi-tenant REST API, auth, SSE stream |
| `log-ingestion` | Go | 8082 | HTTP → Kafka `logs.raw` producer |
| `security-middleware` | Python | 8083 | PII redaction, injection detection |
| `log-consumer` | Go | 8084 | Kafka → PostgreSQL batch writer |
| `anomaly-agent` | Python | 8085 | Z-score + semantic anomaly detection |
| `alert-correlator` | Python | 8086 | Sliding-window alert → incident correlation |
| `context-compressor` | Python | 8087 | Log context window builder for RCA |
| `semantic-cache` | Python | 8088 | Embedding-based RCA deduplication cache |
| `model-router` | Python | 8089 | Per-tenant LLM routing (tier → model) |
| `rca-agent` | Python | 8090 | ReAct RCA loop with hybrid RAG |
| `eval-harness` | Python | 8091 | Faithfulness and hallucination scoring |

### Infrastructure

| Component | Image | Port | Role |
|---|---|---|---|
| PostgreSQL 16 | `postgres:16.3` | 5432 | Primary relational store (8 tables) |
| Kafka (KRaft) | `confluentinc/cp-kafka:7.5.0` | 29092 | Message bus (13 topics) |
| Redis 7 | `redis:7.2.4` | 6379 | Semantic cache + anomaly baselines |
| ChromaDB | `chromadb/chroma:0.4.22` | 8081 | Vector store for incident embeddings |
| Prometheus | `prom/prometheus:v2.48.0` | 9091 | Metrics scraping |
| Grafana | `grafana/grafana:10.2.0` | 3000 | Pre-provisioned dashboard |
| UI | React/Vite + Nginx | 3001 | Frontend |

---

## Tech Stack

- **Go 1.22** — log-ingestion and log-consumer (high-throughput hot path)
- **Python 3.11** — all other services (FastAPI, asyncpg, structlog, pydantic-settings)
- **Apache Kafka** (KRaft mode, no ZooKeeper) — event bus
- **PostgreSQL 16** with `pgxpool` / `asyncpg`
- **Redis** — semantic cache TTL store and anomaly rolling baselines
- **ChromaDB** — tenant-scoped vector collections for RAG
- **OpenAI API** — GPT-4 / GPT-3.5 via the model router
- **Prometheus + Grafana** — structured metrics and dashboards
- **React 18 + Vite** — frontend UI
- **Docker Compose** — local orchestration (21 containers)

---

## Prerequisites

| Requirement | Minimum version | Check |
|---|---|---|
| Docker Engine / Docker Desktop | 24+ | `docker --version` |
| Docker Compose V2 | 2.20+ | `docker compose version` |
| Python | 3.11+ | `python3 --version` |
| OpenAI API key | — | required for RCA, embeddings, anomaly semantic detection |
| Free disk space | ≥ 4 GB | base images + volumes |
| Free RAM | ≥ 6 GB | 21 containers |

---

## Quick Start

### 1. Clone and configure

```bash
git clone https://github.com/sreemahesh52/argus.git
cd argus
cp .env.example infra/.env
```

Open `infra/.env` and set at minimum:

```dotenv
OPENAI_API_KEY=sk-...
```

All other variables have working defaults for local Docker.

### 2. Build service images

First build pulls ~2 GB of base images. Subsequent builds use the Docker layer cache.

```bash
docker compose -f infra/docker-compose.yml --profile app build
```

### 3. Start the full stack

```bash
docker compose -f infra/docker-compose.yml --profile app up -d
```

### 4. Verify all containers are healthy

```bash
docker compose -f infra/docker-compose.yml ps
```

Wait ~60–90 seconds on first start. Kafka elects a leader and the `kafka-init`
container creates all 13 topics before the application services begin consuming.
Expected: all application containers show `running` or `healthy`.

`kafka-init` will show `exited (0)` — that is expected (one-shot setup container).

### 5. Confirm the API gateway is up

```bash
curl -s http://localhost:8000/health | python3 -m json.tool
# {"status": "ok", ...}
```

### 6. Install script dependencies

```bash
cd scripts
python3 -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cd ..
```

---

## Seeding Data

Run these in order from the `scripts/` directory with the virtual environment active.

### Seed tenants

Creates two tenants (`acme-corp` — premium tier, `startup-co` — standard tier) with
hashed API keys. Idempotent.

```bash
python seed_tenants.py
```

### Seed the knowledge base

Inserts 20 historical incidents per tenant into PostgreSQL and embeds each one
into ChromaDB. The RCA agent uses this for hybrid RAG retrieval.

```bash
python seed_incidents.py
```

> If `OPENAI_API_KEY` is not set, DB rows are inserted but embeddings are skipped.
> The RCA agent still runs; RAG retrieval returns no knowledge-base matches until
> embeddings are present.

### Seed demo metrics

Populates PostgreSQL, Redis, and Prometheus with representative data so every UI
card and Grafana panel is non-empty without waiting for a live pipeline run.
The script pushes two rounds of Prometheus metrics 20 s apart to produce non-zero
`rate()` values.

```bash
# Requires the Prometheus Pushgateway — start the stack with docker-compose.demo.yml:
docker compose \
  -f infra/docker-compose.yml \
  -f infra/docker-compose.demo.yml \
  --profile app up -d

# Then seed:
python seed_demo_data.py
```

---

## Generating Traffic

The `generate_logs.py` script sends logs through the full pipeline via the API
gateway. All modes require at least one tenant to be seeded first.

```bash
# Quick smoke test — 10 mixed INFO/WARN/ERROR logs
python generate_logs.py --tenant acme-corp --mode normal

# Trigger anomaly detection — 100 ERROR logs to one service
python generate_logs.py --tenant acme-corp --service payment-service --mode flood

# Continuous traffic loop (Ctrl-C to stop)
python generate_logs.py --tenant acme-corp --mode demo --interval 5

# Security events — PII and injection logs
python generate_logs.py --tenant acme-corp --mode pii
python generate_logs.py --tenant acme-corp --mode injection
```

### Full orchestrated run

The `full-demo` mode runs 10 phases automatically: baseline traffic, security
events, cascade flood, anomaly + alert detection, RCA triggering and polling,
faithfulness labelling, cache warm-up, and a fresh baseline pass. Both tenants
are exercised in a single run.

```bash
# With docker-compose.demo.yml (CORRELATION_WINDOW_SECONDS=30 — faster incident correlation)
python generate_logs.py --mode full-demo --correlation-window 30
```

Expected wall-clock time: **12–15 minutes** on a warm stack.

---

## API Reference

All endpoints require the `X-API-Key` header. The key is the raw value set in
`seed_tenants.py` (check that script's output for the per-tenant keys).

Interactive docs are available at **http://localhost:8000/docs** once the stack
is running.

| Group | Endpoints |
|---|---|
| **Health** | `GET /health` |
| **Logs** | `POST /api/v1/logs`, `GET /api/v1/logs` |
| **Security** | `GET /api/v1/security/events` |
| **Alerts** | `GET /api/v1/alerts`, `PATCH /api/v1/alerts/{id}` |
| **Investigations** | `POST /api/v1/investigations/trigger`, `GET /api/v1/investigations/{id}` |
| **Eval** | `GET /api/v1/eval/results`, `GET /api/v1/eval/summary` |
| **Cache** | `GET /api/v1/cache/stats` |
| **Knowledge Base** | `GET /api/v1/knowledge-base`, `POST /api/v1/knowledge-base` |
| **Simulate** | `POST /api/v1/simulate/flood`, `POST /api/v1/simulate/pii` |
| **Stream** | `GET /api/v1/stream/logs` (SSE) |

---

## Configuration

Copy `.env.example` to `infra/.env` and adjust values as needed. The file is
structured by service; see inline comments for each variable.

Key variables:

| Variable | Description |
|---|---|
| `OPENAI_API_KEY` | Required for RCA, embeddings, semantic anomaly detection |
| `POSTGRES_URL` | PostgreSQL connection string (default: `postgresql://admin:admin@localhost:5432/loganalytics`) |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker list (default: `localhost:9092`) |
| `REDIS_URL` | Redis connection string (default: `redis://localhost:6379`) |
| `CHROMADB_URL` | ChromaDB HTTP URL (default: `http://localhost:8081`) |
| `LOG_LEVEL` | `DEBUG` \| `INFO` \| `WARN` \| `ERROR` (default: `INFO`) |
| `SLACK_WEBHOOK_URL` | Optional — eval harness posts alerts when pass rate drops below 50% |
| `ANOMALY_ZSCORE_THRESHOLD` | Z-score threshold for error-rate spike detection (default: `2.5`) |
| `SEMANTIC_CACHE_TTL_SECONDS` | Redis TTL for cached RCA results (default: `3600`) |
| `CORRELATION_WINDOW_SECONDS` | Alert correlation window (default: `300`; use `30` for faster local testing) |

> `infra/.env` is in `.gitignore`. Never commit it.

---

## Observability

### Grafana dashboard

Open **http://localhost:3000** — default credentials `admin` / `admin`.

Navigate to **Dashboards → Agentic Log Analytics**. The dashboard is
pre-provisioned from `infra/grafana/dashboards/`. Key panels:

- Log ingestion rate and level distribution per service
- Anomaly detection rate and active alerts by severity
- RCA success rate, latency p50/p95, model usage (GPT-4 vs GPT-3.5)
- Prompt version A/B faithfulness comparison
- Semantic cache hit rate and tokens saved
- Eval pass rate, faithfulness score, and hallucination resistance
- Per-tenant token budget and cost tracking

### Prometheus

Raw metrics at **http://localhost:9091**.

### Structured logs

All services emit JSON-structured logs to stdout. Aggregate with:

```bash
docker compose -f infra/docker-compose.yml logs -f --tail=100
# Or per-service:
docker compose -f infra/docker-compose.yml logs -f api-gateway rca-agent
```

---

## Development

### Running tests

```bash
# Go services
cd services/log-ingestion && go test ./... -v
cd services/log-consumer  && go test ./... -v

# Python services (example — repeat per service)
cd services/anomaly-agent
pip install -r requirements.txt
pytest -v
```

### CI

GitHub Actions runs on every push and pull request:

- **Go**: `go vet` + `go test` for both Go services (parallel matrix)
- **Python lint**: `ruff` across all Python services (parallel matrix)
- **Python tests**: `pytest` per service (parallel matrix)
- **UI build**: `npm ci && npm run build`

See `.github/workflows/ci.yml` for the full pipeline.

### Integration tests

`integration.yml` runs a subset of tests that require Docker services. Triggered
manually or on tagged releases.

### Adding a new service

1. Create `services/<name>/` with a `Dockerfile`, `requirements.txt` (or `go.mod`), and a `Dockerfile`.
2. Add the service block to `infra/docker-compose.yml` following the existing pattern (health check, resource limits, `depends_on: condition: service_healthy`).
3. Add a `python-lint` and `python-test` (or `go-services`) matrix entry in `.github/workflows/ci.yml`.
4. Expose Prometheus metrics on `/metrics` and register a scrape job in `infra/prometheus/prometheus.yml`.

---

## Project Structure

```
.
├── infra/
│   ├── docker-compose.yml          # Main stack (21 containers)
│   ├── docker-compose.demo.yml     # Override: shorter correlation window + Pushgateway
│   ├── postgres/
│   │   └── init.sql                # Schema: 8 tables, indexes, CHECK constraints
│   ├── prometheus/
│   │   └── prometheus.yml          # Scrape config for all 11 services
│   └── grafana/
│       ├── dashboards/             # Pre-provisioned JSON dashboard
│       └── provisioning/           # Datasource + dashboard provisioning config
├── services/
│   ├── api-gateway/                # FastAPI · Python
│   ├── log-ingestion/              # HTTP → Kafka · Go
│   ├── security-middleware/        # PII + injection · Python
│   ├── log-consumer/               # Kafka → PostgreSQL · Go
│   ├── anomaly-agent/              # Z-score + semantic · Python
│   ├── alert-correlator/           # Incident correlation · Python
│   ├── context-compressor/         # Log context builder · Python
│   ├── semantic-cache/             # Embedding dedup cache · Python
│   ├── model-router/               # Tenant tier → LLM · Python
│   ├── rca-agent/                  # ReAct RCA + RAG · Python
│   └── eval-harness/               # Faithfulness evaluation · Python
├── ui/                             # React 18 + Vite frontend
├── prompts/                        # Versioned LLM prompt templates
├── scripts/
│   ├── generate_logs.py            # Traffic generator and full-demo orchestrator
│   ├── seed_tenants.py             # Create demo tenants
│   ├── seed_incidents.py           # Seed knowledge base (PostgreSQL + ChromaDB)
│   ├── seed_demo_data.py           # Pre-populate all metrics for instant dashboard fill
│   ├── smoke_test.sh               # End-to-end health check
│   └── verify_metrics.sh           # Prometheus metric assertion script
└── .env.example                    # Variable reference (copy to infra/.env)
```
