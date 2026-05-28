"""
RCARepository — Repository Pattern for rca_results persistence.
this module owns ALL SQL for the rca_results
table. No SQL lives in the Kafka handler, the agent, or anywhere else. If the
rca_results schema changes (new column, renamed field), this is the only file
that needs updating.
RCARepository depends on asyncpg.Pool which
is injected via __init__. It does not call asyncpg.create_pool itself. Tests
inject a mock pool without touching the real database.
every SQL statement uses $N positional
parameters. String concatenation in SQL is never acceptable — even internal
services can be attacked via malformed Kafka messages that contain SQL injection
payloads in service names or root_cause strings.
Why UPSERT (INSERT ... ON CONFLICT DO UPDATE) instead of a plain INSERT?
The trigger endpoint (POST /investigations/trigger) pre-creates a placeholder row
with status='retried' and failure_reason='pending' so the UI can navigate to
/investigations/{rca_id} immediately. When the agent completes, it calls save
with the real RCAResult. UPSERT overwrites the placeholder row in-place, preserving
the rca_id that the UI is already polling. A plain INSERT would fail with a unique
constraint violation on the pre-created row.
"""

from __future__ import annotations

import structlog
import asyncpg

from models import RCAResult

log = structlog.get_logger(__name__)


class RCARepository:
    """Provides save/load operations on the rca_results PostgreSQL table.
    All methods are async coroutines to avoid blocking the asyncio event loop
    during database I/O. asyncpg is a fully async PostgreSQL driver — its
    connection methods return awaitables that yield control to the event loop
    while waiting for the database response.
    """

    def __init__(self, db_pool: asyncpg.Pool) -> None:
        """Inject the shared asyncpg connection pool.
        Args:
            db_pool: asyncpg.Pool created once at startup in main.py.
                     Shared across all concurrent investigations.
        """
        # Store the pool — never call asyncpg.create_pool here.
        self._pool = db_pool

    async def save(self, result: RCAResult) -> None:
        """UPSERT an RCAResult into rca_results.
        INSERT ... ON CONFLICT (rca_id) DO UPDATE ensures that:
          - Normal path: a new row is created for first-time saves.
          - Trigger path: the pre-created placeholder row is overwritten with
            the completed investigation data.
        The EXCLUDED pseudo-table refers to the values from the attempted INSERT
        (the new data) — UPDATE SET col = EXCLUDED.col copies the new value over
        the existing row. This is standard PostgreSQL UPSERT syntax.
        Args:
            result: Validated RCAResult from the agent or from the DLQ handler.
        Raises:
            RuntimeError: wraps asyncpg errors with context for structured logging.
        """
        d = result.to_db_dict()

        # $N parameters map to: rca_id, tenant_id, incident_id, root_cause,
        # confidence, recommendations (TEXT[] — asyncpg maps Python list natively),
        # reasoning_steps (JSONB string from to_db_dict), model_used, prompt_version,
        # input_tokens, output_tokens, cache_hit, compression_ratio, status,
        # failure_reason, total_latency_ms, llm_latency_ms, tool_latency_ms, created_at.
        # Why no ::jsonb cast for recommendations?
        # The DB column is TEXT[] not JSONB. asyncpg maps Python list[str] → TEXT[]
        # natively — no explicit cast needed. Adding ::jsonb would cause a type error.
        query = """
            INSERT INTO rca_results (
                rca_id, tenant_id, incident_id, root_cause, confidence,
                recommendations, reasoning_steps, model_used, prompt_version,
                input_tokens, output_tokens, cache_hit, compression_ratio,
                status, failure_reason, total_latency_ms, llm_latency_ms,
                tool_latency_ms, created_at
            ) VALUES (
                $1::uuid, $2::uuid, $3::uuid, $4, $5,
                $6, $7::jsonb, $8, $9,
                $10, $11, $12, $13,
                $14, $15, $16, $17,
                $18, $19::timestamptz
            )
            ON CONFLICT (rca_id) DO UPDATE SET
                root_cause        = EXCLUDED.root_cause,
                confidence        = EXCLUDED.confidence,
                recommendations   = EXCLUDED.recommendations,
                reasoning_steps   = EXCLUDED.reasoning_steps,
                model_used        = EXCLUDED.model_used,
                prompt_version    = EXCLUDED.prompt_version,
                input_tokens      = EXCLUDED.input_tokens,
                output_tokens     = EXCLUDED.output_tokens,
                cache_hit         = EXCLUDED.cache_hit,
                compression_ratio = EXCLUDED.compression_ratio,
                status            = EXCLUDED.status,
                failure_reason    = EXCLUDED.failure_reason,
                total_latency_ms  = EXCLUDED.total_latency_ms,
                llm_latency_ms    = EXCLUDED.llm_latency_ms,
                tool_latency_ms   = EXCLUDED.tool_latency_ms
        """
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    query,
                    d["rca_id"],
                    d["tenant_id"],
                    d["incident_id"],
                    d["root_cause"],
                    d["confidence"],
                    # recommendations: list[str] passed directly — asyncpg maps to TEXT[].
                    d["recommendations"],
                    # reasoning_steps: JSON string from to_db_dict for JSONB column.
                    d["reasoning_steps"],
                    d["model_used"],
                    d["prompt_version"],
                    d["input_tokens"],
                    d["output_tokens"],
                    d["cache_hit"],
                    d["compression_ratio"],
                    d["status"],
                    d["failure_reason"],
                    d["total_latency_ms"],
                    d["llm_latency_ms"],
                    d["tool_latency_ms"],
                    d["created_at"],
                )
            log.info(
                "rca_result_saved",
                rca_id=d["rca_id"],
                status=d["status"],
                tenant_id=d["tenant_id"],
                incident_id=d["incident_id"],
            )
        except Exception as exc:
            log.error(
                "rca_result_save_failed",
                rca_id=d["rca_id"],
                tenant_id=d["tenant_id"],
                error=str(exc),
            )
            raise RuntimeError(
                f"Failed to save RCAResult {d['rca_id']}: {exc}"
            ) from exc
    async def get_by_id(self, rca_id: str, tenant_id: str) -> dict | None:
        """Fetch a single rca_results row by rca_id, scoped to tenant_id.

        The tenant_id guard prevents cross-tenant data leakage even if a caller
        somehow obtains a valid rca_id from another tenant's investigation.

        Args:
            rca_id:    UUID string of the RCA result to retrieve.
            tenant_id: UUID string of the requesting tenant.

        Returns:
            dict of row fields, or None if not found / wrong tenant.

        Raises:
            RuntimeError: wraps asyncpg errors for structured logging.
        """
        query = """
            SELECT
                rca_id::text,
                tenant_id::text,
                incident_id::text,
                root_cause,
                confidence,
                recommendations,
                reasoning_steps,
                model_used,
                prompt_version,
                input_tokens,
                output_tokens,
                cache_hit,
                compression_ratio,
                status,
                failure_reason,
                total_latency_ms,
                llm_latency_ms,
                tool_latency_ms,
                created_at AT TIME ZONE 'UTC' AS created_at
            FROM rca_results
            WHERE rca_id   = $1::uuid
              AND tenant_id = $2::uuid
        """
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(query, rca_id, tenant_id)
        except Exception as exc:
            log.error(
                "rca_result_get_failed",
                rca_id=rca_id,
                tenant_id=tenant_id,
                error=str(exc),
            )
            raise RuntimeError(
                f"Failed to fetch RCAResult {rca_id}: {exc}"
            ) from exc
        if row is None:
            return None
        return dict(row)
    async def list_by_tenant(
        self,
        tenant_id: str,
        limit: int = 50,
        status: str | None = None,
    ) -> list[dict]:
        """Fetch recent RCA results for a tenant, newest first.

        Optional status filter allows the UI to show only failed investigations
        or only successful ones without client-side filtering.

        Args:
            tenant_id: UUID string for the requesting tenant.
            limit:     Maximum rows to return (capped in the gateway router).
            status:    Optional filter: 'success', 'failed', or 'retried'.

        Returns:
            list of row dicts, ordered by created_at DESC.

        Raises:
            RuntimeError: wraps asyncpg errors for structured logging.
        """
        query = """
            SELECT
                rca_id::text,
                incident_id::text,
                root_cause,
                confidence,
                model_used,
                status,
                failure_reason,
                total_latency_ms,
                created_at AT TIME ZONE 'UTC' AS created_at
            FROM rca_results
            WHERE tenant_id = $1::uuid
              AND ($2::text IS NULL OR status = $2)
            ORDER BY created_at DESC
            LIMIT $3
        """
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(query, tenant_id, status, limit)
        except Exception as exc:
            log.error(
                "rca_result_list_failed",
                tenant_id=tenant_id,
                error=str(exc),
            )
            raise RuntimeError(
                f"Failed to list RCAResults for tenant {tenant_id}: {exc}"
            ) from exc
        return [dict(row) for row in rows]
    async def create_placeholder(
        self,
        rca_id: str,
        tenant_id: str,
        incident_id: str,
    ) -> None:
        """Insert a pending placeholder row for the trigger endpoint.

        Called by POST /investigations/trigger before publishing to Kafka so
        the UI can navigate immediately to /investigations/{rca_id}. The RCADetail
        page polls this row every 5 seconds; when status changes from 'retried'
        to 'success' or 'failed', the real investigation data is shown.

        status='retried' is used for the placeholder because it is a valid
        Literal["success", "failed", "retried"] value in RCAResult, and it
        communicates "pending retry" semantics to the UI while the agent runs.

        Args:
            rca_id:      Pre-generated UUID string (from the trigger endpoint).
            tenant_id:   UUID string of the requesting tenant.
            incident_id: UUID string of the incident being investigated.

        Raises:
            RuntimeError: wraps asyncpg errors for structured logging.
        """
        query = """
            INSERT INTO rca_results (
                rca_id, tenant_id, incident_id, root_cause, confidence,
                recommendations, reasoning_steps, model_used, prompt_version,
                input_tokens, output_tokens, cache_hit, compression_ratio,
                status, failure_reason, total_latency_ms, llm_latency_ms,
                tool_latency_ms, created_at
            ) VALUES (
                $1::uuid, $2::uuid, $3::uuid,
                'Investigation in progress...', 0.0,
                '{}'::text[], '[]'::jsonb,
                'pending', 'pending',
                0, 0, false, 1.0,
                'retried', 'pending',
                0, 0, 0,
                NOW() AT TIME ZONE 'UTC'
            )
            ON CONFLICT (rca_id) DO NOTHING
        """
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(query, rca_id, tenant_id, incident_id)
            log.info(
                "rca_placeholder_created",
                rca_id=rca_id,
                tenant_id=tenant_id,
                incident_id=incident_id,
            )
        except Exception as exc:
            log.error(
                "rca_placeholder_create_failed",
                rca_id=rca_id,
                error=str(exc),
            )
            raise RuntimeError(
                f"Failed to create placeholder for rca_id {rca_id}: {exc}"
            ) from exc
