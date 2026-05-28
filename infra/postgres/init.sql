-- =============================================================================
-- Agentic Log Analytics — PostgreSQL Schema
--
-- Root-cause timestamp rule: every column that stores a point in time uses
-- TIMESTAMPTZ (timestamp with time zone). TIMESTAMP (without time zone) stores
-- the wall-clock value of the inserting session with no offset attached, so two
-- rows written by sessions in different timezones cannot be correctly compared
-- or ordered. TIMESTAMPTZ stores UTC internally and projects correctly into any
-- session timezone on read. Combined with PGTZ=UTC in the container environment,
-- every session is guaranteed to read and write in UTC regardless of host OS.
--
-- =============================================================================

SET timezone = 'UTC';

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =============================================================================
-- Trigger function: automatically stamp updated_at on every UPDATE.
-- Applied to the tenants table — the only table with mutable metadata.
-- =============================================================================
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

-- =============================================================================
-- Table 0: tenants
--
-- One row per customer organisation. The API key is stored as a SHA-256 hex
-- digest — never as plaintext — so a database breach does not expose raw keys.
-- model_tier controls LLM routing (standard → GPT-3.5, premium → GPT-4).
-- token_budget_usd_daily enforces the per-tenant daily spend ceiling checked
-- by the Model Router before every investigation.
-- =============================================================================
CREATE TABLE tenants (
    tenant_id               UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    name                    TEXT        NOT NULL UNIQUE,
    api_key_hash            TEXT        NOT NULL UNIQUE,
    model_tier              TEXT        NOT NULL DEFAULT 'standard'
                                CONSTRAINT tenants_model_tier_check
                                CHECK (model_tier IN ('standard', 'premium')),
    token_budget_usd_daily  FLOAT       NOT NULL DEFAULT 5.0
                                CONSTRAINT tenants_budget_positive
                                CHECK (token_budget_usd_daily > 0),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER tenants_set_updated_at
    BEFORE UPDATE ON tenants
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();

-- =============================================================================
-- Table 1: logs (partitioned by week)
--
-- Raw log lines from all tenant services after passing through the Security
-- Middleware. Partitioned by timestamp using RANGE so time-bounded queries
-- (e.g. "last 500 lines for payment-service") touch only the relevant
-- partition instead of scanning the entire table.
--
-- No PRIMARY KEY is declared here: the partition key (timestamp) must be
-- included in any PK on a partitioned table, and a composite
-- (id, timestamp) PK is unnecessarily wide for this schema. The BIGSERIAL id
-- column provides a unique row identity without the PK constraint.
--
-- Four weekly partitions are created dynamically (DO block below) so the
-- partition boundaries are always current at database initialisation time.
-- =============================================================================
CREATE TABLE logs (
    id                  BIGSERIAL   NOT NULL,
    tenant_id           UUID        NOT NULL REFERENCES tenants (tenant_id) ON DELETE RESTRICT,
    timestamp           TIMESTAMPTZ NOT NULL,
    service             TEXT        NOT NULL,
    level               TEXT        NOT NULL
                            CONSTRAINT logs_level_check
                            CHECK (level IN ('DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL')),
    message             TEXT        NOT NULL,
    trace_id            UUID,
    metadata            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    injection_attempted BOOLEAN     NOT NULL DEFAULT FALSE
) PARTITION BY RANGE (timestamp);

-- create_log_partitions(weeks_ahead): creates weekly partitions from the
-- current ISO week through (current + weeks_ahead - 1) weeks.
-- IF NOT EXISTS makes it safe to call repeatedly (e.g. from a weekly cron).
-- Usage: SELECT create_log_partitions(12);
CREATE OR REPLACE FUNCTION create_log_partitions(weeks_ahead INTEGER DEFAULT 12)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    week_start  TIMESTAMPTZ;
    week_end    TIMESTAMPTZ;
    part_name   TEXT;
    i           INTEGER;
BEGIN
    FOR i IN 0..(weeks_ahead - 1) LOOP
        week_start := date_trunc('week', NOW() AT TIME ZONE 'UTC') + (i * INTERVAL '7 days');
        week_end   := week_start + INTERVAL '7 days';
        part_name  := 'logs_y'
                      || to_char(week_start AT TIME ZONE 'UTC', 'YYYY')
                      || '_w'
                      || to_char(week_start AT TIME ZONE 'UTC', 'IW');

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF logs
             FOR VALUES FROM (%L) TO (%L)',
            part_name,
            week_start,
            week_end
        );
    END LOOP;
END;
$$;

-- Create 52 weekly partitions at init (covers 1 full year from first boot).
SELECT create_log_partitions(52);

-- =============================================================================
-- Table 2: alerts
--
-- Confirmed anomalies raised by the Anomaly Detection Agent after statistical
-- Z-score detection, semantic novelty detection, and LLM false-positive
-- filtering. ground_truth is NULL until a human labels the alert via the API;
-- once set, the Evaluation Harness can use eval_mode='ground_truth'.
-- =============================================================================
CREATE TABLE alerts (
    alert_id        UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID        NOT NULL REFERENCES tenants (tenant_id) ON DELETE RESTRICT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service         TEXT        NOT NULL,
    anomaly_type    TEXT        NOT NULL
                        CONSTRAINT alerts_anomaly_type_check
                        CHECK (anomaly_type IN ('statistical', 'semantic', 'combined')),
    severity        TEXT        NOT NULL
                        CONSTRAINT alerts_severity_check
                        CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    confidence      FLOAT
                        CONSTRAINT alerts_confidence_range
                        CHECK (confidence >= 0 AND confidence <= 1),
    status          TEXT        NOT NULL DEFAULT 'open'
                        CONSTRAINT alerts_status_check
                        CHECK (status IN ('open', 'investigating', 'resolved')),
    ground_truth    TEXT
);

-- =============================================================================
-- Table 3: incidents
--
-- Correlated alert groups produced by the Alert Correlation Engine. A single
-- alert passes through as a single Incident (is_cascade = FALSE). Multiple
-- alerts from different services within the 60-second correlation window are
-- merged into one CascadeIncident (is_cascade = TRUE), triggering a single
-- RCA investigation rather than N redundant ones.
-- =============================================================================
CREATE TABLE incidents (
    incident_id             UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id               UUID        NOT NULL REFERENCES tenants (tenant_id) ON DELETE RESTRICT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    alert_ids               UUID[]      NOT NULL DEFAULT '{}',
    affected_services       TEXT[]      NOT NULL DEFAULT '{}',
    is_cascade              BOOLEAN     NOT NULL DEFAULT FALSE,
    correlation_window_ms   INTEGER     NOT NULL DEFAULT 0
                                CONSTRAINT incidents_window_nonneg
                                CHECK (correlation_window_ms >= 0),
    compression_ratio       FLOAT       NOT NULL DEFAULT 1.0,
    original_log_count      INTEGER     NOT NULL DEFAULT 0,
    was_compressed          BOOLEAN     NOT NULL DEFAULT FALSE
);

-- =============================================================================
-- Table 4: rca_results
--
-- One row per RCA investigation. Stores the agent's structured conclusion,
-- the full reasoning chain (JSONB array of Thought/Action/Observation steps),
-- the model and prompt version used for A/B tracking, token counts, latency
-- breakdown across LLM/tools/total, and whether the result was served from
-- the semantic cache (cache_hit = TRUE skips token spend entirely).
-- Failed investigations set status = 'failed' with a populated failure_reason.
-- =============================================================================
CREATE TABLE rca_results (
    rca_id              UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID        NOT NULL REFERENCES tenants (tenant_id) ON DELETE RESTRICT,
    incident_id         UUID        NOT NULL REFERENCES incidents (incident_id) ON DELETE RESTRICT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    root_cause          TEXT,
    confidence          FLOAT
                            CONSTRAINT rca_confidence_range
                            CHECK (confidence >= 0 AND confidence <= 1),
    recommendations     TEXT[]      NOT NULL DEFAULT '{}',
    reasoning_steps     JSONB       NOT NULL DEFAULT '[]'::jsonb,
    model_used          TEXT,
    prompt_version      TEXT,
    input_tokens        INTEGER     NOT NULL DEFAULT 0,
    output_tokens       INTEGER     NOT NULL DEFAULT 0,
    cache_hit           BOOLEAN     NOT NULL DEFAULT FALSE,
    compression_ratio   FLOAT       NOT NULL DEFAULT 1.0
                            CONSTRAINT rca_compression_positive
                            CHECK (compression_ratio > 0),
    status              TEXT        NOT NULL DEFAULT 'success'
                            CONSTRAINT rca_status_check
                            CHECK (status IN ('success', 'failed', 'retried')),
    failure_reason      TEXT,
    total_latency_ms    INTEGER     NOT NULL DEFAULT 0,
    llm_latency_ms      INTEGER     NOT NULL DEFAULT 0,
    tool_latency_ms     INTEGER     NOT NULL DEFAULT 0
);

-- =============================================================================
-- Table 5: eval_results
--
-- One evaluation record per RCA result. eval_mode records which faithfulness
-- strategy was active: 'ground_truth' when a human label exists on the alert,
-- 'similarity' when a comparable past incident is found, 'heuristic' as the
-- fallback. The passed column is a database-computed boolean: TRUE only when
-- both faithfulness_score > 0.7 AND hallucination_score > 0.7. Storing this
-- as a GENERATED column prevents application code from writing an inconsistent
-- value and makes it queryable as a plain column in Prometheus queries.
-- NULL scores produce a NULL passed value — correct, not a false negative.
-- =============================================================================
CREATE TABLE eval_results (
    eval_id                 UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id               UUID        NOT NULL REFERENCES tenants (tenant_id) ON DELETE RESTRICT,
    rca_id                  UUID        NOT NULL REFERENCES rca_results (rca_id) ON DELETE RESTRICT,
    evaluated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    prompt_version          TEXT,
    eval_mode               TEXT        NOT NULL
                                CONSTRAINT eval_mode_check
                                CHECK (eval_mode IN ('ground_truth', 'heuristic', 'similarity')),
    faithfulness_score      FLOAT
                                CONSTRAINT eval_faithfulness_range
                                CHECK (faithfulness_score >= 0 AND faithfulness_score <= 1),
    hallucination_score     FLOAT
                                CONSTRAINT eval_hallucination_range
                                CHECK (hallucination_score >= 0 AND hallucination_score <= 1),
    cost_usd                FLOAT       NOT NULL DEFAULT 0.0
                                CONSTRAINT eval_cost_nonneg
                                CHECK (cost_usd >= 0),
    total_latency_ms        INTEGER     NOT NULL DEFAULT 0,
    llm_latency_ms          INTEGER     NOT NULL DEFAULT 0,
    tool_latency_ms         INTEGER     NOT NULL DEFAULT 0,
    cache_latency_ms        INTEGER     NOT NULL DEFAULT 0,
    compression_latency_ms  INTEGER     NOT NULL DEFAULT 0,
    passed                  BOOLEAN     GENERATED ALWAYS AS (
                                faithfulness_score > 0.7 AND hallucination_score > 0.7
                            ) STORED
);

-- =============================================================================
-- Table 6: past_incidents
--
-- Knowledge base powering the RCA Agent's hybrid RAG tool. source = 'seed'
-- for the 20 incidents seeded per tenant at startup; source = 'auto_learned'
-- for entries the Self-Learning Indexer adds when an RCA achieves
-- faithfulness > 0.8 and hallucination > 0.7 under eval_mode = 'ground_truth'.
-- Each row's embedding lives in ChromaDB collection past_incidents_{tenant_id},
-- keyed by incident_id, keeping the relational and vector stores in sync.
-- =============================================================================
CREATE TABLE past_incidents (
    incident_id     UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID        NOT NULL REFERENCES tenants (tenant_id) ON DELETE RESTRICT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source          TEXT        NOT NULL DEFAULT 'seed'
                        CONSTRAINT past_incidents_source_check
                        CHECK (source IN ('seed', 'auto_learned')),
    service         TEXT        NOT NULL,
    description     TEXT        NOT NULL,
    root_cause      TEXT        NOT NULL,
    resolution      TEXT        NOT NULL,
    tags            TEXT[]      NOT NULL DEFAULT '{}'
);

-- =============================================================================
-- Table 7: llm_audit_log
--
-- Immutable append-only audit trail for every outbound LLM call across all
-- services. input_hash and output_hash are SHA-256 hex digests of the prompt
-- and completion text — never the raw content. Hashing satisfies audit
-- requirements (tamper evidence via a known digest) without duplicating
-- potentially sensitive prompt content in a second table. The pii_fields_redacted
-- array records which PII categories were stripped before the call (e.g.
-- ['email', 'ip_address']), enabling the security events dashboard panel.
-- =============================================================================
CREATE TABLE llm_audit_log (
    id                      BIGSERIAL   NOT NULL PRIMARY KEY,
    tenant_id               UUID        NOT NULL REFERENCES tenants (tenant_id) ON DELETE RESTRICT,
    logged_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service                 TEXT        NOT NULL,
    prompt_version          TEXT,
    model                   TEXT        NOT NULL,
    input_hash              TEXT        NOT NULL,
    output_hash             TEXT        NOT NULL,
    pii_fields_redacted     TEXT[]      NOT NULL DEFAULT '{}',
    call_latency_ms         INTEGER     NOT NULL DEFAULT 0
);

-- =============================================================================
-- INDEXES
--
-- PostgreSQL does NOT automatically create indexes on foreign key columns,
-- unlike MySQL. Every FK column that participates in a JOIN or a WHERE clause
-- must have an explicit index or PostgreSQL will perform a sequential scan of
-- the referenced table. This is the single most common PostgreSQL performance
-- mistake made by developers migrating from MySQL.
--
-- Partial indexes on boolean and nullable columns (injection_attempted,
-- trace_id) are smaller and faster than full indexes because they index only
-- the rows that qualify — typically a small fraction of the total.
--
-- Indexes on the partitioned logs parent table are automatically propagated
-- to all existing partitions and to any partitions created in the future.
-- =============================================================================

-- logs (partitioned parent — indexes propagate to all partitions)
CREATE INDEX idx_logs_tenant_timestamp
    ON logs (tenant_id, timestamp DESC);

CREATE INDEX idx_logs_service_timestamp
    ON logs (service, timestamp DESC);

CREATE INDEX idx_logs_level_timestamp
    ON logs (level, timestamp DESC);

CREATE INDEX idx_logs_trace_id
    ON logs (trace_id)
    WHERE trace_id IS NOT NULL;

CREATE INDEX idx_logs_injection_attempted
    ON logs (tenant_id)
    WHERE injection_attempted = TRUE;

-- alerts
CREATE INDEX idx_alerts_tenant_id
    ON alerts (tenant_id);

CREATE INDEX idx_alerts_tenant_created
    ON alerts (tenant_id, created_at DESC);

CREATE INDEX idx_alerts_severity
    ON alerts (severity);

CREATE INDEX idx_alerts_status
    ON alerts (status);

-- incidents
CREATE INDEX idx_incidents_tenant_id
    ON incidents (tenant_id);

CREATE INDEX idx_incidents_tenant_created
    ON incidents (tenant_id, created_at DESC);

-- rca_results
CREATE INDEX idx_rca_tenant_id
    ON rca_results (tenant_id);

CREATE INDEX idx_rca_incident_id
    ON rca_results (incident_id);

CREATE INDEX idx_rca_tenant_created
    ON rca_results (tenant_id, created_at DESC);

CREATE INDEX idx_rca_status
    ON rca_results (status);

CREATE INDEX idx_rca_prompt_version
    ON rca_results (prompt_version);

-- eval_results
CREATE INDEX idx_eval_rca_id
    ON eval_results (rca_id);

CREATE INDEX idx_eval_tenant_id
    ON eval_results (tenant_id);

CREATE INDEX idx_eval_tenant_evaluated
    ON eval_results (tenant_id, evaluated_at DESC);

CREATE INDEX idx_eval_prompt_version
    ON eval_results (prompt_version);

CREATE INDEX idx_eval_mode
    ON eval_results (eval_mode);

-- past_incidents
CREATE INDEX idx_past_incidents_tenant_id
    ON past_incidents (tenant_id);

CREATE INDEX idx_past_incidents_service
    ON past_incidents (service);

CREATE INDEX idx_past_incidents_source
    ON past_incidents (source);

-- llm_audit_log
CREATE INDEX idx_llm_audit_tenant_id
    ON llm_audit_log (tenant_id);

CREATE INDEX idx_llm_audit_logged_at
    ON llm_audit_log (logged_at DESC);

-- =============================================================================
-- Table 8: security_events
--
-- Audit log for injection and PII detections from the security middleware.
-- tenant_id is nullable: events may be recorded before tenant resolution.
-- event_type is constrained to 'injection' or 'pii' — no other values accepted.
-- original_message_hash is SHA-256 of the raw message before sanitisation;
-- never the raw content (which may contain PII before redaction).
-- =============================================================================
CREATE TABLE IF NOT EXISTS security_events (
    event_id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id              UUID        REFERENCES tenants (tenant_id) ON DELETE RESTRICT,
    logged_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service                TEXT        NOT NULL,
    event_type             TEXT        NOT NULL
                               CONSTRAINT security_events_type_check
                               CHECK (event_type IN ('injection', 'pii')),
    details                JSONB       NOT NULL DEFAULT '{}'::jsonb,
    original_message_hash  TEXT        NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_security_events_tenant
    ON security_events (tenant_id);

CREATE INDEX IF NOT EXISTS idx_security_events_logged
    ON security_events (tenant_id, logged_at DESC);

-- =============================================================================
-- Seed Data: Development Tenants
--
-- Two tenants are created at database initialization for development and testing.
-- API keys are stored as SHA-256 hex digests — never plaintext.
-- Idempotent: ON CONFLICT DO NOTHING is safe to re-run.
--
-- Tenant: acme-corp    API key: acme-api-key-2024
-- Tenant: startup-co   API key: startup-api-key-2024
-- =============================================================================
INSERT INTO tenants (name, api_key_hash, model_tier, token_budget_usd_daily)
VALUES
    (
        'acme-corp',
        'db81da4a9a4b232c2348513d4e23705e11120e488e3e61be1477649e6a37226d',
        'premium',
        10.0
    ),
    (
        'startup-co',
        'fa4ea9a95c1613d104d183d53f75490684df3952d158c5df5f1b5d88c4bace9b',
        'standard',
        3.0
    )
ON CONFLICT (name) DO NOTHING;
