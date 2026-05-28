# --- Incident Repository ---
# Repository Pattern: all SQL for the incidents table lives here.
# No SQL string appears anywhere else in this service.
# The asyncpg pool is injected via __init__ — never created inside this class
# (Dependency Inversion, ).

from datetime import datetime
from typing import Any

import asyncpg
import structlog

from exceptions import DatabaseWriteError

logger = structlog.get_logger()


class IncidentRepository:
    """Data-access layer for the incidents table.
    Single Responsibility: this class only persists incident rows.
    It does not build incidents, publish to Kafka, or update alerts.
    All queries are parameterised — never f-strings or string concatenation
    in SQL. UUID arrays require explicit ::uuid[] casting because
    PostgreSQL's type inference cannot deduce the element type from a Python list.
    """

    def __init__(self, db_pool: asyncpg.Pool) -> None:
        # Pool injected from main.py — never created here (Dependency Inversion).
        self._pool = db_pool

    async def save(self, incident: dict[str, Any]) -> None:
        """Insert a new incident row into the incidents table.
        Raises DatabaseWriteError on any failure so the caller can decide
        whether to continue (Kafka publish still proceeds — Kafka is the
        source of truth) or escalate.
        created_at is parsed from ISO 8601 string to a timezone-aware datetime
        before binding so asyncpg sends the correct wire type to PostgreSQL.
        """
        log = logger.bind(
            incident_id=incident["incident_id"],
            tenant_id=incident["tenant_id"],
            is_cascade=incident["is_cascade"],
        )

        # Parse ISO string to timezone-aware datetime.
        # .fromisoformat handles "+00:00" suffix (Python ≥3.7).
        # We never pass a naive datetime to a TIMESTAMPTZ column.
        created_at: datetime = datetime.fromisoformat(incident["created_at"])

        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO incidents (
                        incident_id,
                        tenant_id,
                        created_at,
                        alert_ids,
                        affected_services,
                        is_cascade,
                        correlation_window_ms
                    ) VALUES (
                        $1::uuid,
                        $2::uuid,
                        $3::timestamptz,
                        $4::uuid[],
                        $5::text[],
                        $6::boolean,
                        $7::integer
                    )
        """,
                    incident["incident_id"],
                    incident["tenant_id"],
                    created_at,
                    # alert_ids: list of UUID strings; cast to uuid[] in SQL.
                    incident["alert_ids"],
                    incident["affected_services"],
                    incident["is_cascade"],
                    incident["correlation_window_ms"],
                )
            log.info("incident_saved")
        except Exception as exc:
            log.error(
                "incident_save_failed",
                error=str(exc),
                incident_id=incident["incident_id"],
            )
            raise DatabaseWriteError(
                f"Failed to save incident {incident['incident_id']}: {exc}"
            ) from exc
