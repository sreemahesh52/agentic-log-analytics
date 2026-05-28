-- migrate_001_security_events.sql
-- Creates the security_events table used by the security middleware audit log.
--
-- Run order: after init.sql (requires the tenants table to exist for the FK).
-- Idempotent: all statements use IF NOT EXISTS — safe to re-run.
--
-- Why a separate migration file rather than adding to init.sql:
--   init.sql runs once when the PostgreSQL container first boots. Any table
--   needed after initial setup must be added as a numbered migration so the
--   change is tracked, auditable, and can be applied to an already-running DB.
--   "Schema changes after initial creation → numbered migration files."

CREATE TABLE IF NOT EXISTS security_events (
    event_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Nullable: security events may be recorded before tenant resolution.
    -- The security middleware processes logs without a mandatory tenant_id
    -- in the raw Kafka payload. Events are still useful for audit purposes.
    tenant_id              UUID REFERENCES tenants(tenant_id) ON DELETE RESTRICT,

    -- TIMESTAMPTZ — never TIMESTAMP. stores UTC offset internally.
    -- DEFAULT NOW() uses the session timezone which is forced to UTC by PGTZ=UTC.
    logged_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    service                TEXT NOT NULL,

    -- CHECK constraint matches the two event types the detectors can produce.
    -- Any other event_type is rejected at the DB level — defence-in-depth.
    event_type             TEXT NOT NULL
        CONSTRAINT security_events_type_check
        CHECK (event_type IN ('injection', 'pii')),

    -- JSONB (not JSON): binary storage enables index scans on detail fields
    -- in future. Default '{}' avoids NULL propagation in JSONB operators.
    details                JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- SHA-256 hex of the original raw message before sanitisation.
    -- We store the hash, not the original content, for two reasons:
    --   1. Privacy: the original may contain PII we are trying to protect.
    --   2. Storage: long log messages would balloon the audit table.
    -- To verify a specific event, re-hash the original and compare.
    original_message_hash  TEXT NOT NULL
);

-- Covering index for the most common query: "events for tenant X".
CREATE INDEX IF NOT EXISTS idx_security_events_tenant
    ON security_events (tenant_id);

-- Composite index for the API endpoint query:
--   SELECT ... FROM security_events WHERE tenant_id=$1 ORDER BY logged_at DESC
-- PostgreSQL can satisfy the WHERE and ORDER BY from this single index scan.
CREATE INDEX IF NOT EXISTS idx_security_events_logged
    ON security_events (tenant_id, logged_at DESC);
