"""Security events router — GET /api/v1/security/events.
Repository Pattern: SecurityEventsRepository owns all SQL for the
security_events table. Zero SQL lives in the route handler.
This endpoint is polled by the UI every 3 seconds to display injection
and PII events in the Security Events panel.
"""

import json
import uuid
from typing import Any

import asyncpg
import structlog
from fastapi import APIRouter, Depends, Query

from auth import verify_api_key
from dependencies import get_db_pool

logger = structlog.get_logger()
router = APIRouter()


class SecurityEventsRepository:
    """Repository for all queries against the security_events table.
    SecurityEventsRepository is an interface seam: in tests, inject a mock
    that returns fixture rows without touching a real database.
    All parameterised queries live here — never in the route handler.
    """

    def __init__(self, db_pool: asyncpg.Pool) -> None:
        """Accept an asyncpg pool — never open a connection directly."""
        self._pool = db_pool

    async def find_recent(
        self,
        tenant_id: uuid.UUID,
        limit: int,
        event_type: str | None,
    ) -> tuple[list[asyncpg.Record], int]:
        """Fetch recent security events for a tenant, newest first.
        Uses a window function (COUNT(*) OVER) to return both the page
        of events AND the total matching count in a single query — no
        separate COUNT(*) round-trip needed.
        Returns (rows, total_count). total_count is 0 for empty results.
        """
        async with self._pool.acquire() as conn:
            if event_type is not None:
                # $1=tenant_id, $2=event_type, $3=limit
                rows = await conn.fetch(
                    """
                    SELECT event_id, logged_at, service, event_type, details,
                           COUNT(*) OVER () AS total_count
                    FROM security_events
                    WHERE tenant_id = $1 AND event_type = $2
                    ORDER BY logged_at DESC
                    LIMIT $3
        """,
                    tenant_id,
                    event_type,
                    limit,
                )
            else:
                # $1=tenant_id, $2=limit
                rows = await conn.fetch(
                    """
                    SELECT event_id, logged_at, service, event_type, details,
                           COUNT(*) OVER () AS total_count
                    FROM security_events
                    WHERE tenant_id = $1
                    ORDER BY logged_at DESC
                    LIMIT $2
        """,
                    tenant_id,
                    limit,
                )

        # If no rows, total is 0 — rows[0] would raise IndexError.
        total = int(rows[0]["total_count"]) if rows else 0
        return list(rows), total


def _format_event(row: asyncpg.Record) -> dict[str, Any]:
    """Convert one asyncpg Record to the API response shape.
    logged_at: TIMESTAMPTZ from PostgreSQL comes back as a timezone-aware
    Python datetime. .isoformat returns "2024-01-15T10:23:45.123456+00:00".
    .replace("+00:00", "Z") converts to the ISO 8601 UTC shorthand "...Z".
    """
    # asyncpg may return JSONB as a Python dict (if a codec is registered)
    # or as a JSON string (default). Handle both cases defensively.
    raw_details = row["details"]
    if isinstance(raw_details, str):
        # Default asyncpg behaviour: JSONB returned as JSON string.
        details = json.loads(raw_details) if raw_details else {}
    else:
        # Codec registered: asyncpg returned a dict directly.
        details = raw_details or {}

    return {
        "event_id": str(row["event_id"]),
        "logged_at": row["logged_at"].isoformat().replace("+00:00", "Z"),
        "service": row["service"],
        "event_type": row["event_type"],
        "details": details,
    }


@router.get("/api/v1/security/events")
async def get_security_events(
    # Depends(verify_api_key) validates X-API-Key and returns the tenant dict.
    # If auth fails, FastAPI returns 401 before this handler runs.
    tenant: dict[str, Any] = Depends(verify_api_key),
    # limit: max rows to return. ge=1 prevents zero-row requests. le=200 caps
    # response size to prevent accidental large payloads.
    limit: int = Query(default=50, ge=1, le=200),
    # event_type: optional filter. pattern enforces only "injection" or "pii".
    # Any other value returns a 422 Unprocessable Entity before the DB query runs.
    event_type: str | None = Query(default=None, pattern="^(injection|pii)$"),
    db_pool: asyncpg.Pool = Depends(get_db_pool),
) -> dict[str, Any]:
    """Return recent security events for the authenticated tenant.
    Never returns 404 for an empty list — an empty tenant has no events yet,
    not a missing resource. Returns {"events": [], "total": 0} in that case.
    """
    # uuid.UUID validates the tenant_id string from the auth layer.
    # If it is not a valid UUID (should not happen after auth), this raises
    # ValueError which the global exception handler maps to 500.
    tenant_uuid = uuid.UUID(tenant["tenant_id"])
    log = logger.bind(tenant_id=str(tenant_uuid))

    repo = SecurityEventsRepository(db_pool)
    rows, total = await repo.find_recent(tenant_uuid, limit, event_type)

    log.info(
        "security_events_fetched",
        count=len(rows),
        total=total,
        event_type=event_type,
    )

    return {
        "events": [_format_event(row) for row in rows],
        "total": total,
    }
