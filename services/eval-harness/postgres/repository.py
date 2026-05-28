"""EvalRepository — Repository pattern for all eval_results SQL.
Why a Repository?
No SQL appears in kafka/handler.py, self_learner.py, or main.py. All
eval_results database operations are centralised here. This makes it easy
to test the handler with a mock repository and to change the schema
without hunting for SQL fragments scattered across multiple files.
Why not use an ORM?
asyncpg parameterised queries give us direct SQL control without the
abstraction overhead of SQLAlchemy async. For a service with two SQL
operations (INSERT + SELECT aggregate), an ORM would add complexity
without benefit.
Note on the `passed` column:
eval_results.passed is a GENERATED ALWAYS AS column in PostgreSQL:
  passed BOOLEAN GENERATED ALWAYS AS (faithfulness_score > 0.7 AND hallucination_score > 0.7) STORED
It must NOT be included in INSERT statements — PostgreSQL computes it automatically.
Including it would raise "column passed is a generated column" error.
"""

from __future__ import annotations

import structlog

from models import EvalResult

log = structlog.get_logger(__name__)


class EvalRepository:
    """Owns all SQL for the eval_results table.
    Single Responsibility: this class only reads/writes eval_results.
    It has no knowledge of Kafka, Prometheus, or the evaluation pipeline.
    Dependency Inversion: the asyncpg Pool is injected, not created here.
    Tests replace the pool with a mock to verify exact SQL without a real DB.
    """

    def __init__(self, db_pool: object) -> None:
        """Inject the shared asyncpg connection pool.
        Args:
            db_pool: asyncpg Pool created at service startup with timezone=UTC.
        """
        self._pool = db_pool

    async def save(self, eval_result: EvalResult) -> None:
        """Persist one EvalResult row to eval_results.
        The `passed` column is excluded from the INSERT — PostgreSQL computes it
        as a GENERATED ALWAYS column from faithfulness_score and hallucination_score.
        Raises:
            Any asyncpg exception on database error — the caller handles DLQ routing.
        """
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO eval_results (
                    eval_id,
                    tenant_id,
                    rca_id,
                    evaluated_at,
                    prompt_version,
                    eval_mode,
                    faithfulness_score,
                    hallucination_score,
                    cost_usd,
                    total_latency_ms,
                    llm_latency_ms,
                    tool_latency_ms,
                    cache_latency_ms,
                    compression_latency_ms
                ) VALUES (
                    $1::uuid,
                    $2::uuid,
                    $3::uuid,
                    $4::timestamptz,
                    $5,
                    $6,
                    $7,
                    $8,
                    $9,
                    $10,
                    $11,
                    $12,
                    $13,
                    $14
                )
                ON CONFLICT (eval_id) DO NOTHING
        """,
                eval_result.eval_id,
                eval_result.tenant_id,
                eval_result.rca_id,
                # evaluated_at is an ISO 8601 string from EvalResult — parse to datetime
                # because asyncpg requires datetime.datetime for TIMESTAMPTZ columns.
                _parse_iso(eval_result.evaluated_at),
                eval_result.prompt_version,
                eval_result.eval_mode,
                eval_result.faithfulness_score,
                eval_result.hallucination_score,
                eval_result.cost_usd,
                eval_result.total_latency_ms,
                eval_result.llm_latency_ms,
                eval_result.tool_latency_ms,
                eval_result.cache_latency_ms,
                eval_result.compression_latency_ms,
            )

        log.info(
            "eval_result_saved",
            eval_id=eval_result.eval_id,
            rca_id=eval_result.rca_id,
            tenant_id=eval_result.tenant_id,
            eval_mode=eval_result.eval_mode,
        )

    async def get_summary(self, tenant_id: str) -> dict:
        """Return aggregate eval statistics for the authenticated tenant.
        Returns a summary of the most recent 1000 evaluations — enough to
        compute accurate averages without a full-table scan.
        Returns:
            dict with keys: total_evaluations, avg_faithfulness, avg_hallucination,
            avg_cost_usd, total_cost_usd, passed_count, ground_truth_count,
            similarity_count, heuristic_count.
            All numeric fields default to 0 / 0.0 when no evaluations exist.
        """
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT
                        COUNT(*)::int AS total_evaluations,
                        ROUND(COALESCE(AVG(faithfulness_score), 0)::numeric, 4)::float
                                                                               AS avg_faithfulness,
                        ROUND(COALESCE(AVG(hallucination_score), 0)::numeric, 4)::float
                                                                               AS avg_hallucination,
                        ROUND(COALESCE(AVG(cost_usd), 0)::numeric, 6)::float AS avg_cost_usd,
                        ROUND(COALESCE(SUM(cost_usd), 0)::numeric, 6)::float AS total_cost_usd,
                        COUNT(CASE WHEN passed THEN 1 END)::int AS passed_count,
                        COUNT(CASE WHEN eval_mode = 'ground_truth' THEN 1 END)::int
                                                                               AS ground_truth_count,
                        COUNT(CASE WHEN eval_mode = 'similarity' THEN 1 END)::int
                                                                               AS similarity_count,
                        COUNT(CASE WHEN eval_mode = 'heuristic' THEN 1 END)::int
                                                                               AS heuristic_count
                    FROM (
                        SELECT faithfulness_score, hallucination_score, cost_usd,
                               passed, eval_mode
                        FROM eval_results
                        WHERE tenant_id = $1::uuid
                        ORDER BY evaluated_at DESC
                        LIMIT 1000
                    ) recent
        """,
                    tenant_id,
                )
        except Exception as exc:
            log.error(
                "eval_summary_query_failed",
                tenant_id=tenant_id,
                error=str(exc),
            )
            return _empty_summary()

        if row is None:
            return _empty_summary()

        total = row["total_evaluations"] or 0
        passed = row["passed_count"] or 0
        pass_rate = round(passed / total, 4) if total > 0 else 0.0

        return {
            "total_evaluations": total,
            "avg_faithfulness_score": row["avg_faithfulness"],
            "avg_hallucination_score": row["avg_hallucination"],
            "avg_cost_usd": row["avg_cost_usd"],
            "total_cost_usd": row["total_cost_usd"],
            "passed_count": passed,
            "pass_rate": pass_rate,
            "eval_mode_breakdown": {
                "ground_truth": row["ground_truth_count"] or 0,
                "similarity": row["similarity_count"] or 0,
                "heuristic": row["heuristic_count"] or 0,
            },
        }


def _parse_iso(ts_str: str):
    """Parse an ISO 8601 datetime string to a timezone-aware datetime object.
    asyncpg requires datetime.datetime for TIMESTAMPTZ columns.
    EvalResult.evaluated_at is stored as an ISO 8601 string (e.g.
    "2024-01-15T10:30:00+00:00") — this converts it for asyncpg.
    fromisoformat handles the +00:00 offset produced by
    datetime.now(timezone.utc).isoformat.
    """
    from datetime import datetime
    return datetime.fromisoformat(ts_str)


def _empty_summary() -> dict:
    """Return a zeroed summary dict when no eval_results exist yet.
    Ensures the API always returns a valid JSON body even on cold start.
    """
    return {
        "total_evaluations": 0,
        "avg_faithfulness_score": 0.0,
        "avg_hallucination_score": 0.0,
        "avg_cost_usd": 0.0,
        "total_cost_usd": 0.0,
        "passed_count": 0,
        "pass_rate": 0.0,
        "eval_mode_breakdown": {
            "ground_truth": 0,
            "similarity": 0,
            "heuristic": 0,
        },
    }
