# --- PostgreSQL repositories for context-compressor ---
# Repository Pattern: ALL SQL for this service lives here.
# No SQL string appears anywhere else — not in the Kafka handler, not in main.py.
# Two repositories follow Single Responsibility:
#   LogRepository — owns log-fetching queries.
#   IncidentRepository — owns compression-stats update queries.
# Splitting them means each class has exactly one reason to change.

import asyncpg
import structlog

from exceptions import DatabaseWriteError

logger = structlog.get_logger()

# Maximum log lines fetched per service if no override is provided.
# This default matches the service config default — both can be overridden
# independently, but the config value is the authoritative runtime setting.
_DEFAULT_LIMIT_PER_SERVICE = 500


class LogRepository:
    """Fetches recent logs for one or more services from the logs table.
    Single Responsibility: the only query this class executes is a
    SELECT on the logs table. It does not write, delete, or touch other tables.
    Dependency Inversion: asyncpg pool is injected — never created here.
    Tests inject a mock pool without needing a real PostgreSQL instance.
    """

    def __init__(self, db_pool: asyncpg.Pool) -> None:
        # Pool injected from main.py — never instantiated inside this class.
        self._pool = db_pool

    async def fetch_recent_logs(
        self,
        tenant_id: str,
        services: list[str],
        limit_per_service: int = _DEFAULT_LIMIT_PER_SERVICE,
    ) -> list[dict]:
        """Fetch the most recent logs for each affected service.
        Queries each service separately to allow independent LIMIT per service
        (a JOIN + LIMIT would not apply the limit per-service). Results are
        combined and sorted chronologically (ASC) so the caller receives
        logs in causal order ready for formatting.
        Args:
            tenant_id: Tenant scope — never query across tenants.
            services: List of service names to fetch logs for.
            limit_per_service: Max rows per service (default 500).
        Returns:
            List of dicts with keys: timestamp (ISO 8601 str), service, level, message.
            Sorted by timestamp ASC. May be empty if no logs exist.
        """
        all_logs: list[dict] = []

        async with self._pool.acquire() as conn:
            for service in services:
                # --- Parameterised query ---
                # ORDER BY timestamp DESC + LIMIT fetches the MOST RECENT logs.
                # We reverse to ASC later when combining all services.
                rows = await conn.fetch(
                    """
                    SELECT
                        (timestamp AT TIME ZONE 'UTC') AS timestamp,
                        service,
                        level,
                        message
                    FROM logs
                    WHERE tenant_id = $1::uuid
                      AND service = $2
                    ORDER BY timestamp DESC
                    LIMIT $3
        """,
                    tenant_id,
                    service,
                    limit_per_service,
                )

                for row in rows:
                    all_logs.append({
                        # .isoformat on a UTC-aware datetime gives "+00:00" suffix.
                        # always use timezone-aware datetimes from DB.
                        "timestamp": row["timestamp"].isoformat(),
                        "service": row["service"],
                        "level": row["level"],
                        "message": row["message"],
                    })

        # Sort combined list chronologically ASC.
        # Each service was fetched DESC (most recent first), so after combining
        # we need a full sort to interleave events from different services correctly.
        all_logs.sort(key=lambda entry: entry["timestamp"])
        return all_logs


class IncidentRepository:
    """Updates incident rows with compression statistics after processing.
    Single Responsibility: this class only writes compression metadata back
    to the incidents table. It does not read incidents or touch other tables.
    Why update the incidents table?
      The API gateway's GET /api/v1/alerts/{id} endpoint needs compression_ratio
      and related fields for the AlertDrawer UI component. Storing them on the
      incident row (rather than a separate table) avoids an extra JOIN and keeps
      the read path simple. The context-compressor is the only writer of these
      three columns — no concurrent update conflict risk.
    """

    def __init__(self, db_pool: asyncpg.Pool) -> None:
        self._pool = db_pool

    async def update_compression_stats(
        self,
        incident_id: str,
        compression_ratio: float,
        original_log_count: int,
        was_compressed: bool,
    ) -> None:
        """Write compression result back to the incidents table.
        This is a best-effort operation: if it fails, the Kafka message is
        still published to incidents.compressed and the pipeline continues.
        The DB update is for UI display only — not for pipeline correctness.
        Raises:
            DatabaseWriteError: if the UPDATE fails, so the caller can log
                at ERROR and continue (best-effort, not fatal).
        """
        try:
            async with self._pool.acquire() as conn:
                # parameterised UPDATE — no f-strings.
                # $1::uuid casts the string to UUID before comparing.
                await conn.execute(
                    """
                    UPDATE incidents
                    SET compression_ratio = $2,
                        original_log_count = $3,
                        was_compressed = $4
                    WHERE incident_id = $1::uuid
        """,
                    incident_id,
                    compression_ratio,
                    original_log_count,
                    was_compressed,
                )
            logger.debug(
                "incident_compression_stats_updated",
                incident_id=incident_id,
                compression_ratio=compression_ratio,
                was_compressed=was_compressed,
            )
        except Exception as exc:
            raise DatabaseWriteError(
                f"Failed to update compression stats for incident {incident_id}: {exc}"
            ) from exc
