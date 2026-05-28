# --- Eval router ---
# Provides the authenticated tenant with a summary of their evaluation results.
# One endpoint:
#   GET /api/v1/eval/summary — aggregate faithfulness, hallucination, cost, pass rate
# Why a summary endpoint rather than raw eval_results rows?
# The UI EvalScoresPanel shows aggregate metrics across all investigations for the
# current tenant. Returning raw rows would require client-side aggregation over
# potentially thousands of rows. A single aggregate query is more efficient and
# transfers less data.

from typing import Any

import asyncpg
import structlog
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from auth import verify_api_key
from dependencies import get_db_pool

logger = structlog.get_logger()
router = APIRouter()

# Cap at 1000 most recent evaluations to keep the aggregate query fast.
# This is large enough to produce accurate averages across any reasonable
# evaluation window without needing a full table scan.
_SUMMARY_LIMIT = 1000


@router.get("/api/v1/eval/summary")
async def get_eval_summary(
    tenant: dict[str, Any] = Depends(verify_api_key),
    db_pool: asyncpg.Pool = Depends(get_db_pool),
) -> JSONResponse:
    """Return aggregate evaluation statistics for the authenticated tenant.
    Queries the most recent 1000 eval_results rows. Returns zeroed stats
    when no evaluations have been recorded yet (cold start returns 200, not 404).
    Response fields:
      total_evaluations: Count of evaluations in the summary window.
      avg_faithfulness_score: Mean faithfulness across all evaluations.
      avg_hallucination_score: Mean hallucination score across all evaluations.
      avg_cost_usd: Mean USD cost per evaluation.
      total_cost_usd: Cumulative cost across all evaluations in window.
      passed_count: Count where faithfulness > 0.7 AND hallucination > 0.7.
      pass_rate: Ratio of passed_count to total_evaluations.
      eval_mode_breakdown: Count per eval_mode (ground_truth / similarity / heuristic).
    """
    log = logger.bind(tenant_id=tenant["tenant_id"])

    try:
        async with db_pool.acquire() as conn:
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
                    LIMIT $2
                ) recent
    """,
                tenant["tenant_id"],
                _SUMMARY_LIMIT,
            )

            # --- Per-prompt-version faithfulness for A/B comparison ---
            # Groups the same window of evaluations by prompt_version so the
            # Dashboard ABChart can compare Prompt v1 vs v2 faithfulness scores.
            # prompt_version IS NOT NULL excludes cache hits (which have no version).
            version_rows = await conn.fetch(
                """
                SELECT
                    prompt_version,
                    ROUND(COALESCE(AVG(faithfulness_score), 0)::numeric, 4)::float
                        AS avg_faithfulness
                FROM (
                    SELECT prompt_version, faithfulness_score
                    FROM eval_results
                    WHERE tenant_id = $1::uuid
                      AND prompt_version IS NOT NULL
                    ORDER BY evaluated_at DESC
                    LIMIT $2
                ) recent
                GROUP BY prompt_version
    """,
                tenant["tenant_id"],
                _SUMMARY_LIMIT,
            )
    except Exception as exc:
        log.error("eval_summary_query_failed", error=str(exc))
        raise

    if row is None or row["total_evaluations"] == 0:
        log.info("eval_summary_returned_empty")
        return JSONResponse(content=_empty_summary())

    total = row["total_evaluations"]
    passed = row["passed_count"] or 0
    pass_rate = round(passed / total, 4) if total > 0 else 0.0

    # Build per-prompt-version faithfulness map for the Dashboard A/B chart.
    # version_rows contains (prompt_version, avg_faithfulness) pairs.
    # Keys like "v1", "v2" are passed directly through — the UI uses them as-is.
    faithfulness_by_version = {
        vr["prompt_version"]: vr["avg_faithfulness"]
        for vr in version_rows
    }

    log.info("eval_summary_returned", total=total)
    return JSONResponse(
        content={
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
            "faithfulness_by_prompt_version": faithfulness_by_version,
        }
    )


def _empty_summary() -> dict:
    """Return a zeroed summary when no evaluations exist yet."""
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
        "faithfulness_by_prompt_version": {},
    }
