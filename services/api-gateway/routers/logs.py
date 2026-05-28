# --- Log ingest proxy router and recent logs endpoint ---
# This router handles two concerns:
#   1. POST /api/v1/logs/ingest — authenticate, validate, proxy to Go log-ingestion service
#   2. GET /api/v1/logs/recent — authenticate, query PostgreSQL, return recent logs

from typing import Any

import httpx
import structlog
from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import asyncpg

from auth import verify_api_key
from config import settings
from dependencies import get_db_pool, get_http_client
from exceptions import UpstreamServiceError

logger = structlog.get_logger()
router = APIRouter()


class LogIngestRequest(BaseModel):
    """Request body for POST /api/v1/logs/ingest.
    Matches the Go log-ingestion service's LogEntry struct exactly.
 Pydantic validates required fields and types before the handler runs."""

    service: str
    level: str
    message: str
    # Optional fields — None is excluded from the forwarded JSON payload
    # via model_dump(exclude_none=True) to avoid sending null to Go.
    trace_id: str | None = None
    metadata: dict[str, Any] | None = None


@router.post("/api/v1/logs/ingest", status_code=202)
async def ingest_log(
    body: LogIngestRequest,
    # Depends(verify_api_key) runs the full auth chain before this function
    # is called. If auth fails, FastAPI returns 401 and never reaches here.
    tenant: dict[str, Any] = Depends(verify_api_key),
    # Depends(get_http_client) returns the shared AsyncClient from app.state.
    # Injecting it here (not instantiating it) means the same TCP connections
    # are reused across requests — no new connection per request.
    http_client: httpx.AsyncClient = Depends(get_http_client),
) -> JSONResponse:
    """Proxy a validated log entry to the Go log-ingestion service.
    Returns 202 on success, mirrors upstream 4xx, raises UpstreamServiceError
 on timeout or 5xx so the global handler maps it to 503."""
    # --- Bind tenant context to all log lines in this handler ---
    # logger.bind returns a new logger with these fields attached to every
    # subsequent log call. This avoids repeating tenant_id= on each log line.
    log = logger.bind(tenant_id=tenant["tenant_id"], service=body.service)

    # f-string used for URL construction only — not for SQL.
    target_url = f"{settings.log_ingestion_url}/api/v1/logs"

    # --- Proxy the request ---
    try:
        # Build the forwarded payload and inject tenant_id into metadata.
        # The Go service passes metadata through unchanged into the Kafka message.
        # Downstream consumers (security middleware, log consumer, anomaly agent)
        # read tenant_id from metadata — without this injection, every downstream
        # service stores NULL for tenant_id, making records invisible to all tenants.
        payload = body.model_dump(exclude_none=True)
        # setdefault creates the key if absent; this preserves any metadata the
        # caller already provided (e.g., request_id, version) while adding tenant_id.
        payload.setdefault("metadata", {})["tenant_id"] = tenant["tenant_id"]

        response = await http_client.post(target_url, json=payload)
    except httpx.TimeoutException as exc:
        log.error("upstream_timeout", url=target_url, error=str(exc))
        raise UpstreamServiceError("Log ingestion service timed out")
    except httpx.RequestError as exc:
        # RequestError covers DNS failure, connection refused, etc.
        log.error("upstream_unreachable", url=target_url, error=str(exc))
        raise UpstreamServiceError("Log ingestion service unreachable")

    # --- Mirror upstream error codes ---
    # 4xx from the Go service means the payload was invalid — pass it through
    # unchanged so the caller sees the precise validation error from upstream.
    if 400 <= response.status_code < 500:
        log.warning("upstream_client_error", status=response.status_code)
        return JSONResponse(status_code=response.status_code, content=response.json())

    # 5xx means the Go service is broken — raise so the global handler returns 503.
    if response.status_code >= 500:
        log.error("upstream_server_error", status=response.status_code)
        raise UpstreamServiceError("Log ingestion service returned server error")

    log.info("log_ingested", trace_id=response.json().get("trace_id"))
    return JSONResponse(status_code=202, content=response.json())


# Maximum number of rows the caller may request in a single query.
# Prevents accidental or malicious full-table scans through the API.
MAX_RECENT_LOGS_LIMIT = 100


@router.get("/api/v1/logs/recent")
async def get_recent_logs(
    # Optional filter: only return logs for this service name.
    service: str | None = Query(default=None),
    # Optional filter: only return logs at this level (case-insensitive).
    level: str | None = Query(default=None),
    # How many rows to return. Capped at MAX_RECENT_LOGS_LIMIT.
    limit: int = Query(default=20, ge=1, le=MAX_RECENT_LOGS_LIMIT),
    tenant: dict[str, Any] = Depends(verify_api_key),
    db_pool: asyncpg.Pool = Depends(get_db_pool),
) -> JSONResponse:
    """Return the most recent logs for the authenticated tenant.
    Supports optional filtering by service name and log level.
 Results are ordered newest-first. Maximum 100 rows per call."""
    log = logger.bind(tenant_id=tenant["tenant_id"])

    # --- Execute query with fixed parameterised SQL ---
    # no f-strings or string concatenation in SQL — no exceptions.
    # All four parameters are always present. Optional filters use a NULL-coalescing
    # pattern: "$2::text IS NULL OR service = $2" short-circuits to TRUE when the
    # caller passes None, matching all rows for that column. This avoids dynamic
    # SQL construction entirely.
    # Passing None for an asyncpg parameter is equivalent to SQL NULL.
    # $3 (level) is uppercased before the query so callers may send "error" or "ERROR".
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    timestamp AT TIME ZONE 'UTC' AS timestamp,
                    service,
                    level,
                    message,
                    trace_id::text,
                    injection_attempted
                FROM logs
                WHERE tenant_id = $1
                  AND ($2::text IS NULL OR service = $2)
                  AND ($3::text IS NULL OR level = $3)
                ORDER BY timestamp DESC
                LIMIT $4
    """,
                tenant["tenant_id"],
                service if service else None,
                level.upper() if level else None,
                limit,
            )
    except Exception as exc:
        log.error("recent_logs_query_failed", error=str(exc))
        raise

    # --- Serialise timestamps as ISO 8601 with Z suffix ---
    # API responses use "2024-01-15T10:23:45.123Z" format.
    # asyncpg returns timezone-aware datetime objects; .isoformat gives "+00:00".
    # We replace "+00:00" with "Z" to match the API contract spec.
    logs_list = []
    for row in rows:
        ts = row["timestamp"]
        # .isoformat on a UTC-aware datetime gives "...+00:00" — replace with "Z".
        timestamp_str = ts.isoformat().replace("+00:00", "Z")
        logs_list.append({
            "timestamp": timestamp_str,
            "service": row["service"],
            "level": row["level"],
            "message": row["message"],
            "trace_id": row["trace_id"],
            "injection_attempted": row["injection_attempted"],
        })

    log.info("recent_logs_returned", count=len(logs_list))
    # total reflects the count returned in this response (not the full table count).
    return JSONResponse(content={"logs": logs_list, "total": len(logs_list)})
