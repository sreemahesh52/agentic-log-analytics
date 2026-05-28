# --- Investigations router ---
# Provides read access to rca_results and a trigger endpoint to manually
# start an RCA investigation for an existing incident.
# Four endpoints:
#   GET /api/v1/investigations — paginated list for the tenant
#   GET /api/v1/investigations/failed — MUST be registered before /{rca_id}
#   GET /api/v1/investigations/{rca_id} — single investigation detail
#   POST /api/v1/investigations/trigger — trigger RCA for an incident
# IMPORTANT — route ordering:
#   FastAPI matches routes top-to-bottom. The literal path /investigations/failed
#   MUST be registered before /investigations/{rca_id}. Without this ordering,
#   FastAPI would treat the literal "failed" as a rca_id UUID string and route
#   GET /investigations/failed to get_investigation — which would then return
#   404 (invalid UUID) instead of the failed list.

import json
import uuid
from typing import Any

import asyncpg
import structlog
from aiokafka import AIOKafkaProducer
from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from auth import verify_api_key
from config import settings
from dependencies import get_db_pool, get_kafka_producer

logger = structlog.get_logger()
router = APIRouter()

# Maximum rows per list request — prevents full-table scans via the API.
_MAX_LIMIT = 100
_DEFAULT_LIMIT = 50


def _format_ts(ts: Any) -> str | None:
    """Convert a datetime (or None) to ISO 8601 with Z suffix.
    API responses always use the Z suffix, not +00:00.
    asyncpg returns timezone-aware datetimes for TIMESTAMPTZ columns.
    .isoformat on a UTC-aware datetime gives "...+00:00" — replace with Z.
    """
    if ts is None:
        return None
    if hasattr(ts, "isoformat"):
        return ts.isoformat().replace("+00:00", "Z")
    return str(ts)


def _error_body(code: str, message: str, request_id: str) -> dict[str, Any]:
    """Build the standard error envelope."""
    return {"error": {"code": code, "message": message, "request_id": request_id}}


# ---------------------------------------------------------------------------
# GET /api/v1/investigations
# ---------------------------------------------------------------------------


@router.get("/api/v1/investigations")
async def list_investigations(
    # Optional status filter: 'success', 'failed', 'retried'
    status: str | None = Query(default=None),
    limit: int = Query(default=_DEFAULT_LIMIT, ge=1, le=_MAX_LIMIT),
    tenant: dict[str, Any] = Depends(verify_api_key),
    db_pool: asyncpg.Pool = Depends(get_db_pool),
) -> JSONResponse:
    """Return the most recent RCA investigations for the authenticated tenant.
    Returns a summary list suitable for the investigations index page.
    Full investigation details (reasoning_steps, recommendations) are in
    GET /api/v1/investigations/{rca_id}.
    Args:
        status: Optional filter ('success', 'failed', 'retried').
        limit: Maximum rows to return, capped at _MAX_LIMIT.
    """
    log = logger.bind(tenant_id=tenant["tenant_id"])

    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    r.rca_id::text,
                    r.incident_id::text,
                    r.root_cause,
                    r.confidence,
                    r.model_used,
                    r.status,
                    r.failure_reason,
                    r.total_latency_ms,
                    r.created_at AT TIME ZONE 'UTC' AS created_at
                FROM rca_results r
                WHERE r.tenant_id = $1::uuid
                  AND ($2::text IS NULL OR r.status = $2)
                ORDER BY r.created_at DESC
                LIMIT $3
    """,
                tenant["tenant_id"],
                status,
                limit,
            )
    except Exception as exc:
        log.error("investigations_list_query_failed", error=str(exc))
        raise

    investigations = [
        {
            "rca_id": row["rca_id"],
            "incident_id": row["incident_id"],
            # Truncate root_cause for the list view — full text in detail endpoint.
            "root_cause": (row["root_cause"] or "")[:200],
            "confidence": row["confidence"],
            "model_used": row["model_used"],
            "status": row["status"],
            "failure_reason": row["failure_reason"],
            "total_latency_ms": row["total_latency_ms"],
            "created_at": _format_ts(row["created_at"]),
        }
        for row in rows
    ]

    log.info("investigations_listed", count=len(investigations))
    return JSONResponse(
        content={"investigations": investigations, "total": len(investigations)}
    )


# ---------------------------------------------------------------------------
# GET /api/v1/investigations/failed
# MUST be registered BEFORE /{rca_id} — see module docstring.
# ---------------------------------------------------------------------------


@router.get("/api/v1/investigations/failed")
async def list_failed_investigations(
    limit: int = Query(default=_DEFAULT_LIMIT, ge=1, le=_MAX_LIMIT),
    tenant: dict[str, Any] = Depends(verify_api_key),
    db_pool: asyncpg.Pool = Depends(get_db_pool),
) -> JSONResponse:
    """Return failed RCA investigations for the authenticated tenant.
    Convenience endpoint for the operations dashboard that shows investigations
    needing attention. Equivalent to GET /investigations?status=failed.
    The status='retried' rows with failure_reason='pending' are excluded —
    they represent in-progress investigations, not completed failures.
    """
    log = logger.bind(tenant_id=tenant["tenant_id"])

    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    r.rca_id::text,
                    r.incident_id::text,
                    r.failure_reason,
                    r.total_latency_ms,
                    r.created_at AT TIME ZONE 'UTC' AS created_at
                FROM rca_results r
                WHERE r.tenant_id = $1::uuid
                  AND r.status = 'failed'
                ORDER BY r.created_at DESC
                LIMIT $2
    """,
                tenant["tenant_id"],
                limit,
            )
    except Exception as exc:
        log.error("failed_investigations_query_failed", error=str(exc))
        raise

    failures = [
        {
            "rca_id": row["rca_id"],
            "incident_id": row["incident_id"],
            "failure_reason": row["failure_reason"],
            "total_latency_ms": row["total_latency_ms"],
            "created_at": _format_ts(row["created_at"]),
        }
        for row in rows
    ]

    log.info("failed_investigations_returned", count=len(failures))
    return JSONResponse(content={"failed": failures, "total": len(failures)})


# ---------------------------------------------------------------------------
# GET /api/v1/investigations/{rca_id}
# ---------------------------------------------------------------------------


@router.get("/api/v1/investigations/{rca_id}")
async def get_investigation(
    rca_id: str,
    tenant: dict[str, Any] = Depends(verify_api_key),
    db_pool: asyncpg.Pool = Depends(get_db_pool),
) -> JSONResponse:
    """Return full investigation details including reasoning_steps and recommendations.
    Returns 404 if the rca_id does not exist or belongs to a different tenant.
    The tenant_id guard prevents cross-tenant data leakage.
    When status='retried' and failure_reason='pending', the investigation is
    still in progress. The UI polls this endpoint every 5 seconds until status
    changes to 'success' or 'failed'.
    reasoning_steps is a JSONB array — each element has:
      step_number, thought, action, action_input, observation, timestamp.
    recommendations is a TEXT[] array of remediation steps.
    """
    log = logger.bind(tenant_id=tenant["tenant_id"], rca_id=rca_id)

    # Validate rca_id is a real UUID — return 404 instead of a DB error.
    try:
        uuid.UUID(rca_id)
    except ValueError:
        return JSONResponse(
            status_code=404,
            content=_error_body(
                "INVESTIGATION_NOT_FOUND",
                "Investigation not found",
                str(uuid.uuid4()),
            ),
        )

    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    r.rca_id::text,
                    r.tenant_id::text,
                    r.incident_id::text,
                    r.root_cause,
                    r.confidence,
                    r.recommendations,
                    r.reasoning_steps,
                    r.model_used,
                    r.prompt_version,
                    r.input_tokens,
                    r.output_tokens,
                    r.cache_hit,
                    r.compression_ratio,
                    r.status,
                    r.failure_reason,
                    r.total_latency_ms,
                    r.llm_latency_ms,
                    r.tool_latency_ms,
                    r.created_at AT TIME ZONE 'UTC' AS created_at,
                    -- alert_ids from the linked incident for ground truth labelling.
                    -- NULL when no incident is linked (should never happen in practice).
                    ARRAY(
                        SELECT elem::text
                        FROM unnest(i.alert_ids) AS elem
                    ) AS alert_ids,
                    -- Eval fields — NULL until the eval harness processes this investigation.
                    -- The UI polls this endpoint while status='retried'; eval data appears
                    -- here once the eval harness consumes the agent.results Kafka message.
                    e.eval_id::text AS eval_id,
                    e.faithfulness_score,
                    e.hallucination_score,
                    e.eval_mode,
                    e.cost_usd AS eval_cost_usd,
                    e.passed AS eval_passed
                FROM rca_results r
                LEFT JOIN incidents i
                    ON i.incident_id = r.incident_id
                   AND i.tenant_id = r.tenant_id
                LEFT JOIN eval_results e
                    ON e.rca_id = r.rca_id
                   AND e.tenant_id = r.tenant_id
                WHERE r.rca_id = $1::uuid
                  AND r.tenant_id = $2::uuid
    """,
                rca_id,
                tenant["tenant_id"],
            )
    except Exception as exc:
        log.error("investigation_detail_query_failed", error=str(exc))
        raise

    if row is None:
        return JSONResponse(
            status_code=404,
            content=_error_body(
                "INVESTIGATION_NOT_FOUND",
                "Investigation not found",
                str(uuid.uuid4()),
            ),
        )

    # reasoning_steps: asyncpg returns the JSONB column as a Python string
    # (raw JSON). Parse it so the API response is a proper JSON array.
    reasoning_steps_raw = row["reasoning_steps"]
    if isinstance(reasoning_steps_raw, str):
        reasoning_steps = json.loads(reasoning_steps_raw)
    elif reasoning_steps_raw is None:
        reasoning_steps = []
    else:
        # asyncpg may return JSONB as a dict/list directly in some versions.
        reasoning_steps = reasoning_steps_raw

    # recommendations: asyncpg returns TEXT[] as Python list[str].
    recommendations = list(row["recommendations"]) if row["recommendations"] else []

    log.info("investigation_detail_returned", status=row["status"])
    return JSONResponse(
        content={
            "rca_id": row["rca_id"],
            "incident_id": row["incident_id"],
            "root_cause": row["root_cause"],
            "confidence": row["confidence"],
            "recommendations": recommendations,
            "reasoning_steps": reasoning_steps,
            "model_used": row["model_used"],
            "prompt_version": row["prompt_version"],
            "input_tokens": row["input_tokens"],
            "output_tokens": row["output_tokens"],
            "cache_hit": row["cache_hit"],
            "compression_ratio": row["compression_ratio"],
            "status": row["status"],
            # failure_reason is null for successful investigations.
            "failure_reason": row["failure_reason"],
            "total_latency_ms": row["total_latency_ms"],
            "llm_latency_ms": row["llm_latency_ms"],
            "tool_latency_ms": row["tool_latency_ms"],
            "created_at": _format_ts(row["created_at"]),
            # alert_ids from the linked incident — used by the UI to enable ground
            # truth labelling via PATCH /api/v1/alerts/{alert_id}/label.
            # Null if no incident is linked; empty list if incident has no alerts.
            "alert_ids": list(row["alert_ids"]) if row["alert_ids"] else [],
            # Eval fields — null until the eval harness processes this investigation.
            # All four fields are null together (LEFT JOIN produces all-null row).
            "eval_id": row["eval_id"],
            "faithfulness_score": row["faithfulness_score"],
            "hallucination_score": row["hallucination_score"],
            "eval_mode": row["eval_mode"],
            "eval_cost_usd": row["eval_cost_usd"],
            "eval_passed": row["eval_passed"],
        }
    )


# ---------------------------------------------------------------------------
# POST /api/v1/investigations/trigger
# ---------------------------------------------------------------------------


@router.post("/api/v1/investigations/trigger")
async def trigger_investigation(
    body: dict[str, Any],
    tenant: dict[str, Any] = Depends(verify_api_key),
    db_pool: asyncpg.Pool = Depends(get_db_pool),
    kafka_producer: AIOKafkaProducer = Depends(get_kafka_producer),
) -> JSONResponse:
    """Manually trigger an RCA investigation for an existing incident.
    Flow:
      1. Validate incident_id — 422 if missing/invalid UUID.
      2. Fetch the incident from the incidents table — 404 if not found.
      3. Pre-generate rca_id so the UI can navigate to /investigations/{rca_id}
         immediately after this call returns.
      4. Write a placeholder rca_results row (status='retried',
         failure_reason='pending') so GET /investigations/{rca_id} returns 200
         (with "in progress" data) rather than 404 while the agent runs.
      5. Publish to incidents.ready with rca_id_hint so the RCA Agent picks it
         up and UPSERTs the real result over the placeholder row.
      6. Return 202 Accepted with {"incident_id": ..., "rca_id": ...}.
    Why write the placeholder from the gateway?
    The UI navigates to /investigations/{rca_id} immediately. Without a DB row,
    the detail page returns 404. The placeholder ensures the page loads (showing
    "in progress") while the RCA Agent processes the Kafka message asynchronously.
    Why publish to Kafka from the gateway?
    The gateway already has aiokafka (same library as alert-correlator and
    model-router). This keeps the trigger flow self-contained in one request
    handler without adding a new HTTP server to the RCA Agent service.
    Returns:
        202 Accepted: {"incident_id": str, "rca_id": str}
        404 Not Found: incident_id does not exist or belongs to different tenant.
        422 Unprocessable: missing or invalid incident_id in request body.
    """
    log = logger.bind(tenant_id=tenant["tenant_id"])

    # --- Validate request body ---
    incident_id = body.get("incident_id")
    if not incident_id:
        return JSONResponse(
            status_code=422,
            content=_error_body(
                "VALIDATION_ERROR",
                "incident_id is required",
                str(uuid.uuid4()),
            ),
        )

    try:
        uuid.UUID(str(incident_id))
    except ValueError:
        return JSONResponse(
            status_code=422,
            content=_error_body(
                "VALIDATION_ERROR",
                "incident_id must be a valid UUID",
                str(uuid.uuid4()),
            ),
        )

    # --- Fetch incident + severity from DB ---
    # The incidents table stores structural fields (alert_ids, affected_services,
    # is_cascade, compression_ratio). Fields consumed from the Kafka pipeline
    # (model_id, prompt_variant, compressed_context) are NOT stored in the DB —
    # they live only in Kafka messages. For manual triggers we supply sensible
    # defaults: gpt-4-turbo (highest capability model), prompt variant v1,
    # and severity from the most recent associated alert.
    try:
        async with db_pool.acquire() as conn:
            incident_row = await conn.fetchrow(
                """
                SELECT
                    i.incident_id::text,
                    i.tenant_id::text,
                    ARRAY(SELECT elem::text FROM unnest(i.alert_ids) AS elem) AS alert_ids,
                    i.affected_services::text[] AS affected_services,
                    i.is_cascade,
                    i.compression_ratio,
                    i.created_at AT TIME ZONE 'UTC' AS created_at,
                    -- Derive severity from the most severe alert in this incident.
                    -- CASE gives CRITICAL > HIGH > MEDIUM > LOW ordering.
                    COALESCE(
                        (SELECT a.severity
                         FROM alerts a
                         WHERE a.alert_id = ANY(i.alert_ids)
                           AND a.tenant_id = i.tenant_id
                         ORDER BY CASE a.severity
                             WHEN 'CRITICAL' THEN 4
                             WHEN 'HIGH' THEN 3
                             WHEN 'MEDIUM' THEN 2
                             WHEN 'LOW' THEN 1
                             ELSE 0
                         END DESC
                         LIMIT 1),
                        'HIGH'
                    ) AS severity
                FROM incidents i
                WHERE i.incident_id = $1::uuid
                  AND i.tenant_id = $2::uuid
    """,
                str(incident_id),
                tenant["tenant_id"],
            )
    except Exception as exc:
        log.error("trigger_incident_fetch_failed", error=str(exc))
        raise

    if incident_row is None:
        return JSONResponse(
            status_code=404,
            content=_error_body(
                "INCIDENT_NOT_FOUND",
                "Incident not found",
                str(uuid.uuid4()),
            ),
        )

    # --- Pre-generate rca_id and write placeholder row ---
    rca_id = str(uuid.uuid4())

    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
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
                    NOW AT TIME ZONE 'UTC'
                )
                ON CONFLICT (rca_id) DO NOTHING
    """,
                rca_id,
                tenant["tenant_id"],
                incident_row["incident_id"],
            )
    except Exception as exc:
        log.error("trigger_placeholder_insert_failed", rca_id=rca_id, error=str(exc))
        raise

    # --- Publish incident payload to incidents.ready with rca_id_hint ---
    # The RCA Agent's Kafka consumer picks this up and runs the investigation.
    # rca_id_hint tells the agent to use our pre-assigned rca_id when writing
    # the final result, so the UPSERT lands on the placeholder row above.
    # Build the IncidentPayload for the Kafka message.
    # model_id, prompt_variant, compressed_context, incident_description are
    # NOT stored in the DB — supply defaults for manual triggers.
    affected_services = list(incident_row["affected_services"] or [])
    incident_payload = {
        "incident_id": incident_row["incident_id"],
        "tenant_id": incident_row["tenant_id"],
        "alert_ids": list(incident_row["alert_ids"] or []),
        "affected_services": affected_services,
        "is_cascade": incident_row["is_cascade"],
        "severity": incident_row["severity"],
        # Manual triggers always use gpt-4-turbo — highest capability model.
        # The model-router is bypassed for manual triggers.
        "model_id": body.get("model_id", "gpt-4-turbo"),
        "prompt_variant": body.get("prompt_variant", "v1"),
        # No pre-compressed context for manual triggers — the agent's
        # QueryLogs tool fetches fresh logs during the investigation.
        "compressed_context": body.get(
            "compressed_context",
            f"Manual RCA trigger for incident {incident_row['incident_id']}. "
            f"Affected services: {', '.join(affected_services)}. "
            f"Severity: {incident_row['severity']}.",
        ),
        "compression_ratio": float(incident_row["compression_ratio"] or 1.0),
        "incident_description": (
            f"Manual investigation trigger. "
            f"Services: {', '.join(affected_services)}. "
            f"Cascade: {incident_row['is_cascade']}."
        ),
        "created_at": _format_ts(incident_row["created_at"]) or "",
        "rca_id_hint": rca_id,
    }

    try:
        await kafka_producer.send_and_wait(
            topic=settings.kafka_incidents_topic,
            # Encode as UTF-8 bytes — Kafka messages are binary.
            value=json.dumps(incident_payload).encode("utf-8"),
            # Key on tenant_id so all messages for a tenant land on one partition.
            key=tenant["tenant_id"].encode("utf-8"),
        )
    except Exception as exc:
        log.error(
            "trigger_kafka_publish_failed",
            rca_id=rca_id,
            topic=settings.kafka_incidents_topic,
            error=str(exc),
        )
        # Do not return 503 — the placeholder row exists and will show "in progress".
        # The investigation can be retried manually. Log the failure for alerting.

    log.info(
        "investigation_triggered",
        incident_id=incident_id,
        rca_id=rca_id,
        topic=settings.kafka_incidents_topic,
    )

    # 202 Accepted: queued, not yet completed.
    return JSONResponse(
        status_code=202,
        content={
            "incident_id": str(incident_id),
            "rca_id": rca_id,
        },
    )
