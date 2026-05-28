# --- Knowledge base router ---
# Provides the authenticated tenant with statistics about their past_incidents
# knowledge base — the RAG store used by the RCA Agent's SearchKnowledgeBase tool.
# One endpoint:
#   GET /api/v1/knowledge-base/stats — incident counts by source (seed / auto_learned)
# Why expose this as a separate endpoint?
# The knowledge base size directly affects RCA quality — more incidents means better
# similarity matching in both BM25 and ChromaDB. The UI EvalScoresPanel shows this
# metric alongside faithfulness/hallucination scores so operators can correlate
# knowledge base growth with evaluation quality improvements.

from typing import Any

import asyncpg
import structlog
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from auth import verify_api_key
from dependencies import get_db_pool

logger = structlog.get_logger()
router = APIRouter()


@router.get("/api/v1/knowledge-base/stats")
async def get_knowledge_base_stats(
    tenant: dict[str, Any] = Depends(verify_api_key),
    db_pool: asyncpg.Pool = Depends(get_db_pool),
) -> JSONResponse:
    """Return past_incidents knowledge base statistics for the authenticated tenant.
    Queries past_incidents to return total size and breakdown by source (seed vs
    auto_learned). Returns zeroed stats on cold start — 200 with zeros, never 404.
    Response fields:
      total_incidents: Total rows in past_incidents for this tenant.
      seed_count: Incidents seeded at startup (source = 'seed').
      auto_learned_count: Incidents auto-indexed by Self-Learning Indexer.
      services: Distinct service names present in the knowledge base.
    """
    log = logger.bind(tenant_id=tenant["tenant_id"])

    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*)::int AS total_incidents,
                    COUNT(CASE WHEN source = 'seed' THEN 1 END)::int AS seed_count,
                    COUNT(CASE WHEN source = 'auto_learned' THEN 1 END)::int AS auto_learned_count,
                    ARRAY(
                        SELECT DISTINCT service
                        FROM past_incidents
                        WHERE tenant_id = $1::uuid
                        ORDER BY service
                    ) AS services
                FROM past_incidents
                WHERE tenant_id = $1::uuid
    """,
                tenant["tenant_id"],
            )
    except Exception as exc:
        log.error("knowledge_base_stats_query_failed", error=str(exc))
        raise

    if row is None or row["total_incidents"] == 0:
        log.info("knowledge_base_stats_returned_empty")
        return JSONResponse(
            content={
                "total_incidents": 0,
                "seed_count": 0,
                "auto_learned_count": 0,
                "services": [],
            }
        )

    log.info(
        "knowledge_base_stats_returned",
        total=row["total_incidents"],
        auto_learned=row["auto_learned_count"],
    )
    return JSONResponse(
        content={
            "total_incidents": row["total_incidents"],
            "seed_count": row["seed_count"] or 0,
            "auto_learned_count": row["auto_learned_count"] or 0,
            "services": list(row["services"]) if row["services"] else [],
        }
    )
