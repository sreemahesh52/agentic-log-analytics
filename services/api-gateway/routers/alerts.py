# --- Alerts router ---
# Provides read access to the alerts table for the authenticated tenant.
# Two endpoints:
#   GET /api/v1/alerts — paginated alert list with cascade info
#   GET /api/v1/alerts/{alert_id} — single alert with linked incident and RCA

import uuid
from typing import Any

import asyncpg
import structlog
from fastapi import APIRouter, Body, Depends, Query
from fastapi.responses import JSONResponse

from auth import verify_api_key
from dependencies import get_db_pool

logger = structlog.get_logger()
router = APIRouter()

# Maximum rows per alert list request — prevents full-table scans via the API.
_MAX_ALERT_LIMIT = 200
_DEFAULT_ALERT_LIMIT = 50


def _format_ts(ts: Any) -> str | None:
    """Convert a datetime (or None) to ISO 8601 with Z suffix.
    API responses use Z suffix, not +00:00.
    asyncpg returns timezone-aware datetimes for TIMESTAMPTZ columns.
    .isoformat on a UTC-aware datetime gives "...+00:00" — replace with "Z".
    Returns None unchanged (for nullable columns like ground_truth).
    """
    if ts is None:
        return None
    if hasattr(ts, "isoformat"):
        return ts.isoformat().replace("+00:00", "Z")
    return str(ts)


@router.get("/api/v1/alerts")
async def list_alerts(
    # Optional severity filter — e.g. ?severity=CRITICAL
    severity: str | None = Query(default=None),
    # Optional service filter — e.g. ?service=payment-service
    service: str | None = Query(default=None),
    # Pagination limit — capped at _MAX_ALERT_LIMIT
    limit: int = Query(default=_DEFAULT_ALERT_LIMIT, ge=1, le=_MAX_ALERT_LIMIT),
    tenant: dict[str, Any] = Depends(verify_api_key),
    db_pool: asyncpg.Pool = Depends(get_db_pool),
) -> JSONResponse:
    """Return the most recent alerts for the authenticated tenant.
    LEFT JOIN with incidents: adds is_cascade and affected_services to each alert
    row where the alert has been correlated into an incident. NULL values from the
    LEFT JOIN (alert not yet correlated) are returned as False/[] in the response.
    LATERAL JOIN is used to avoid duplicate rows when the same alert appears in
    multiple incidents (unlikely but possible during cascade re-correlation).
    """
    log = logger.bind(tenant_id=tenant["tenant_id"])

    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    a.alert_id::text,
                    a.service,
                    a.anomaly_type,
                    a.severity,
                    a.confidence,
                    a.status,
                    a.ground_truth,
                    a.created_at AT TIME ZONE 'UTC' AS created_at,
                    COALESCE(inc.is_cascade, FALSE) AS is_cascade,
                    COALESCE(inc.affected_services, ARRAY[]::text[]) AS affected_services
                FROM alerts a
                LEFT JOIN LATERAL (
                    SELECT is_cascade, affected_services
                    FROM incidents
                    WHERE a.alert_id = ANY(alert_ids)
                      AND tenant_id = $1::uuid
                    ORDER BY created_at DESC
                    LIMIT 1
                ) inc ON TRUE
                WHERE a.tenant_id = $1::uuid
                  AND ($2::text IS NULL OR a.severity = $2)
                  AND ($3::text IS NULL OR a.service = $3)
                ORDER BY a.created_at DESC
                LIMIT $4
    """,
                tenant["tenant_id"],
                severity.upper() if severity else None,
                service if service else None,
                limit,
            )
    except Exception as exc:
        log.error("alerts_list_query_failed", error=str(exc))
        raise

    # --- Serialise rows ---
    # asyncpg Record objects are dict-like; we build plain dicts for JSONResponse.
    alerts_list = [
        {
            "alert_id": row["alert_id"],
            "service": row["service"],
            "anomaly_type": row["anomaly_type"],
            "severity": row["severity"],
            "confidence": row["confidence"],
            "status": row["status"],
            "ground_truth": row["ground_truth"],
            "created_at": _format_ts(row["created_at"]),
            # is_cascade: True if this alert is part of a multi-service incident.
            # False if single-service or not yet correlated (COALESCE above handles NULL).
            "is_cascade": row["is_cascade"],
            # affected_services: list of service names in the linked incident.
            # asyncpg returns PostgreSQL text[] as Python list[str].
            "affected_services": list(row["affected_services"]) if row["affected_services"] else [],
        }
        for row in rows
    ]

    log.info("alerts_returned", count=len(alerts_list))
    return JSONResponse(content={"alerts": alerts_list, "total": len(alerts_list)})


@router.get("/api/v1/alerts/{alert_id}")
async def get_alert(
    alert_id: str,
    tenant: dict[str, Any] = Depends(verify_api_key),
    db_pool: asyncpg.Pool = Depends(get_db_pool),
) -> JSONResponse:
    """Return a single alert by ID with linked incident, compression stats, and RCA.
    Returns 404 if the alert does not exist or belongs to a different tenant.
    The tenant check prevents information leakage across tenants even if the
    caller guesses a valid alert_id that belongs to another tenant.
    Field availability by pipeline stage:
      - correlation_window_ms, affected_services: populated after Alert Correlator (step 8).
      - compression_ratio, original_log_count, was_compressed: populated after Context
        Compressor (step 9) via migrate_002_incidents_compression.sql columns.
      - model_used, rca_status: populated after RCA Agent (step 13).
    Fields not yet populated are returned as null — never as 404.
    """
    log = logger.bind(tenant_id=tenant["tenant_id"], alert_id=alert_id)

    # Validate alert_id is a valid UUID before querying to return a clear 404.
    # uuid.UUID raises ValueError on an invalid UUID string.
    try:
        uuid.UUID(alert_id)
    except ValueError:
        return JSONResponse(
            status_code=404,
            content={
                "error": {
                    "code": "ALERT_NOT_FOUND",
                    "message": "Alert not found",
                    "request_id": str(uuid.uuid4()),
                }
            },
        )

    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    a.alert_id::text,
                    a.service,
                    a.anomaly_type,
                    a.severity,
                    a.confidence,
                    a.status,
                    a.ground_truth,
                    a.created_at AT TIME ZONE 'UTC' AS created_at,
                    COALESCE(inc.is_cascade, FALSE) AS is_cascade,
                    COALESCE(inc.affected_services, ARRAY[]::text[]) AS affected_services,
                    inc.incident_id::text AS incident_id,
                    -- correlation_window_ms: how wide the cascade window was (0 for singles).
                    inc.correlation_window_ms AS correlation_window_ms,
                    -- Compression columns added by migrate_002_incidents_compression.sql.
                    -- NULL when there is no linked incident yet.
                    inc.compression_ratio AS compression_ratio,
                    inc.original_log_count AS original_log_count,
                    inc.was_compressed AS was_compressed,
                    -- RCA fields: use LATERAL to get the most recent RCA regardless of status.
                    -- NULL when no RCA has been run yet.
                    rca.rca_id::text AS rca_id,
                    rca.model_used AS model_used,
                    rca.status AS rca_status
                FROM alerts a
                LEFT JOIN LATERAL (
                    -- Fetch the most recent incident that contains this alert_id.
                    -- LATERAL + ORDER BY + LIMIT 1 avoids duplicates if the alert
                    -- appears in multiple incidents (rare but possible during re-correlation).
                    SELECT
                        incident_id,
                        is_cascade,
                        affected_services,
                        correlation_window_ms,
                        compression_ratio,
                        original_log_count,
                        was_compressed
                    FROM incidents
                    WHERE a.alert_id = ANY(alert_ids)
                      AND tenant_id = $2::uuid
                    ORDER BY created_at DESC
                    LIMIT 1
                ) inc ON TRUE
                LEFT JOIN LATERAL (
                    -- Fetch the most recent RCA for this incident, any status.
                    -- We return rca_status so the UI can show "Investigation Failed"
                    -- when status='failed', not just when status='success'.
                    SELECT rca_id, model_used, status
                    FROM rca_results
                    WHERE incident_id = inc.incident_id
                      AND tenant_id = $2::uuid
                    ORDER BY created_at DESC
                    LIMIT 1
                ) rca ON TRUE
                WHERE a.alert_id = $1::uuid
                  AND a.tenant_id = $2::uuid
    """,
                alert_id,
                tenant["tenant_id"],
            )
    except Exception as exc:
        log.error("alert_detail_query_failed", error=str(exc))
        raise

    if row is None:
        return JSONResponse(
            status_code=404,
            content={
                "error": {
                    "code": "ALERT_NOT_FOUND",
                    "message": "Alert not found",
                    "request_id": str(uuid.uuid4()),
                }
            },
        )

    log.info("alert_detail_returned")
    return JSONResponse(
        content={
            "alert_id": row["alert_id"],
            "service": row["service"],
            "anomaly_type": row["anomaly_type"],
            "severity": row["severity"],
            "confidence": row["confidence"],
            "status": row["status"],
            "ground_truth": row["ground_truth"],
            "created_at": _format_ts(row["created_at"]),
            "is_cascade": row["is_cascade"],
            "affected_services": list(row["affected_services"]) if row["affected_services"] else [],
            # Incident fields — null until Alert Correlator processes this alert.
            "incident_id": row["incident_id"],
            "correlation_window_ms": row["correlation_window_ms"],
            # Compression fields — null until Context Compressor runs (step 9).
            "compression_ratio": row["compression_ratio"],
            "original_log_count": row["original_log_count"],
            "was_compressed": row["was_compressed"],
            # RCA fields — null until RCA Agent runs (step 13).
            "rca_id": row["rca_id"],
            "model_used": row["model_used"],
            "rca_status": row["rca_status"],
        }
    )


@router.patch("/api/v1/alerts/{alert_id}/label")
async def label_alert(
    alert_id: str,
    body: dict[str, Any] = Body(...),
    tenant: dict[str, Any] = Depends(verify_api_key),
    db_pool: asyncpg.Pool = Depends(get_db_pool),
) -> JSONResponse:
    """Set the ground_truth label on an alert for faithfulness evaluation.
    The GroundTruthStrategy in the eval harness reads ground_truth from the
    alerts table. Providing a human-verified root cause here upgrades future
    evaluations from heuristic/similarity tier to the more reliable ground_truth tier.
    Returns 404 if the alert does not exist or belongs to a different tenant.
    The tenant_id guard prevents cross-tenant label writes.
    Body: {"ground_truth": str} (min 10 chars, max 1000 chars)
    Returns: {"updated": true, "alert_id": str}
    """
    log = logger.bind(tenant_id=tenant["tenant_id"], alert_id=alert_id)

    # Validate alert_id is a real UUID — return 404 instead of a DB error.
    try:
        uuid.UUID(alert_id)
    except ValueError:
        return JSONResponse(
            status_code=404,
            content={
                "error": {
                    "code": "ALERT_NOT_FOUND",
                    "message": "Alert not found",
                    "request_id": str(uuid.uuid4()),
                }
            },
        )

    # --- Validate ground_truth field ---
    ground_truth = body.get("ground_truth")
    if not ground_truth or not isinstance(ground_truth, str):
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "VALIDATION_ERROR",
                    "message": "ground_truth is required and must be a string",
                    "request_id": str(uuid.uuid4()),
                }
            },
        )

    ground_truth = ground_truth.strip()

    # Minimum length: 10 characters — prevents trivially short labels.
    if len(ground_truth) < 10:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "VALIDATION_ERROR",
                    "message": "ground_truth must be at least 10 characters",
                    "request_id": str(uuid.uuid4()),
                }
            },
        )

    # Maximum length: 1000 characters — prevents oversized labels.
    if len(ground_truth) > 1000:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "VALIDATION_ERROR",
                    "message": "ground_truth must be at most 1000 characters",
                    "request_id": str(uuid.uuid4()),
                }
            },
        )

    # --- Update the alert's ground_truth field ---
    # Parameterised query — never f-strings in SQL (Standard: no f-strings in SQL).
    # The WHERE clause includes tenant_id to prevent cross-tenant writes.
    # rowcount == 0 means either the alert doesn't exist or belongs to another tenant —
    # we return 404 either way to avoid information leakage.
    try:
        async with db_pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE alerts
                SET ground_truth = $1
                WHERE alert_id = $2::uuid
                  AND tenant_id = $3::uuid
    """,
                ground_truth,
                alert_id,
                tenant["tenant_id"],
            )
    except Exception as exc:
        log.error("alert_label_update_failed", error=str(exc))
        raise

    # asyncpg returns "UPDATE N" where N is the number of rows updated.
    # N == 0 means the alert was not found or belongs to another tenant.
    rows_updated = int(result.split()[-1]) if result else 0

    if rows_updated == 0:
        return JSONResponse(
            status_code=404,
            content={
                "error": {
                    "code": "ALERT_NOT_FOUND",
                    "message": "Alert not found",
                    "request_id": str(uuid.uuid4()),
                }
            },
        )

    log.info("alert_ground_truth_set", alert_id=alert_id)
    return JSONResponse(content={"updated": True, "alert_id": alert_id})
