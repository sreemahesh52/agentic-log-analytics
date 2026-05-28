"""PostgreSQL repository classes for the anomaly-agent service.
Repository Pattern: ALL SQL lives here, never in the orchestrator.
Two repositories serve two distinct domain concerns:
  LogRepository: fetches recent error logs (read-only).
  PostgresAlertRepository: inserts confirmed alerts (write).
PostgresAlertRepository also implements AlertPublisher so it can be registered
as an Observer in AnomalyOrchestrator. The orchestrator calls publish_alert
on each publisher without knowing which ones are DB writers vs. Kafka writers.
Why psycopg2 (not asyncpg) here:
  The anomaly-agent is a synchronous Kafka consumer loop.
  asyncpg requires an asyncio event loop to be running, which would require
  wrapping the consumer loop in asyncio.run and awaiting all DB calls.
  psycopg2 with a single connection is simpler, testable, and correct for
  a single-threaded consumer. Step 8 onward (API gateway extensions) uses asyncpg.
Connection ownership:
  The connection is created in main.py and injected here.
  Repositories NEVER create their own connections. This is Dependency Inversion:
  the repository depends on the connection abstraction, not on psycopg2.connect.
"""

import structlog
import psycopg2
import psycopg2.extras

from orchestrator import AlertPublisher

logger = structlog.get_logger(__name__)

# Maximum number of recent error log messages returned for LLM context.
# 10 is enough for GPT-3.5 to judge whether the error pattern is real.
# More would increase token cost without improving decision quality.
_DEFAULT_CONTEXT_LIMIT = 10


class LogRepository:
    """Read-only repository for fetching recent error logs by service and tenant.
    Single Responsibility: this class only reads from the logs table.
    It has no knowledge of anomaly detection, Kafka, or alert publishing.
    Used by AnomalyOrchestrator to populate the LLM verifier's sample_logs
    parameter — giving GPT-3.5 real-world context about what is normal for
    this service before asking it to judge the current anomaly.
    """

    def __init__(self, connection: psycopg2.extensions.connection) -> None:
        """Accept an injected psycopg2 connection.
        The connection is owned by main.py and shared across repositories.
        This class never creates or closes connections.
        """
        # Store the connection — never created here (Dependency Inversion)
        self._conn = connection

    def get_recent_errors(
        self,
        tenant_id: str,
        service: str,
        limit: int = _DEFAULT_CONTEXT_LIMIT,
    ) -> list[str]:
        """Return the most recent ERROR/FATAL log messages for the given service.
        Results are ordered newest-first. Only the message text is returned —
        the LLM verifier only needs the content, not the full row.
        Args:
            tenant_id: tenant UUID string — all queries are scoped to this tenant.
            service: service name to filter logs.
            limit: maximum number of messages to return (default 10).
        Returns:
            list[str] of message strings, newest first.
            Returns empty list on any DB error — caller must handle this case.
        """
        try:
            with self._conn.cursor() as cur:
                # --- Parameterised query — no f-strings in SQL ---
                # $1/$2/$3 are psycopg2 %s placeholders.
                # tenant_id::uuid cast handles both UUID objects and UUID strings.
                # level IN ('ERROR','FATAL') mirrors the anomaly levels in semantic.py.
                cur.execute(
                    """
                    SELECT message
                    FROM logs
                    WHERE tenant_id = %s::uuid
                      AND service = %s
                      AND level IN ('ERROR', 'FATAL')
                    ORDER BY timestamp DESC
                    LIMIT %s
        """,
                    (tenant_id, service, limit),
                )
                # fetchall returns list of tuples — extract the first column (message)
                rows = cur.fetchall()
                return [row[0] for row in rows]
        except Exception as exc:
            logger.warning(
                "log_repository_query_failed",
                tenant_id=tenant_id,
                service=service,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            # Return empty list — orchestrator handles empty sample_logs gracefully
            return []


class PostgresAlertRepository(AlertPublisher):
    """Inserts confirmed alert rows into the PostgreSQL alerts table.
    Implements AlertPublisher so it can be registered as an Observer in
    AnomalyOrchestrator. The orchestrator calls publish_alert without
    knowing this implementation writes to PostgreSQL — it only knows it
    calls something that satisfies the AlertPublisher interface.
    Why INSERT here and not in the orchestrator:
      Single Responsibility: the orchestrator coordinates, repositories persist.
      All SQL for the alerts table lives here — the orchestrator has zero SQL.
      If the alerts schema changes, only this file needs updating.
    """

    def __init__(self, connection: psycopg2.extensions.connection) -> None:
        """Accept an injected psycopg2 connection.
        The same connection shared with LogRepository in main.py.
        Single connection is safe for the single-threaded consumer loop.
        """
        # Store connection — never created or closed here
        self._conn = connection

    def publish_alert(self, alert: dict) -> None:
        """Insert one alert row into the alerts table.
        Per AlertPublisher contract: must not raise. All errors are caught,
        logged at ERROR, and swallowed so the Kafka publisher can still run.
        The 'details' column does not exist on the alerts table — it lives in
        the alert payload for Kafka consumers but is not stored in PostgreSQL.
        PostgreSQL stores only the normalised fields defined in init.sql.
        Args:
            alert: dict with keys: alert_id, tenant_id, service, anomaly_type,
                   severity, confidence, status, created_at, details.
        """
        try:
            with self._conn.cursor() as cur:
                # --- Parameterised INSERT — no f-strings ---
                # All eight columns are from the alert dict produced by AlertPayload.model_dump.
                # The alert dict's created_at is an ISO 8601 string with UTC offset;
                # PostgreSQL parses this correctly into TIMESTAMPTZ.
                cur.execute(
                    """
                    INSERT INTO alerts
                        (alert_id, tenant_id, service, anomaly_type,
                         severity, confidence, status, created_at)
                    VALUES
                        (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s::timestamptz)
                    ON CONFLICT (alert_id) DO NOTHING
        """,
                    (
                        alert["alert_id"],
                        alert["tenant_id"],
                        alert["service"],
                        alert["anomaly_type"],
                        alert["severity"],
                        alert["confidence"],
                        alert["status"],
                        # created_at: ISO 8601 string from AlertPayload — PostgreSQL
                        # parses "2024-01-15T10:23:45.123456+00:00" into TIMESTAMPTZ correctly.
                        alert["created_at"],
                    ),
                )
                # commit persists the row. Without autocommit, psycopg2 uses an
                # implicit transaction that must be committed explicitly.
                self._conn.commit()

            logger.info(
                "alert_inserted_postgres",
                alert_id=alert["alert_id"],
                tenant_id=alert["tenant_id"],
                service=alert["service"],
                anomaly_type=alert["anomaly_type"],
                severity=alert["severity"],
            )

        except Exception as exc:
            # Roll back the failed transaction to avoid "transaction aborted" errors
            # on subsequent queries to the same connection.
            try:
                self._conn.rollback()
            except Exception:
                pass  # rollback failure is secondary — log the original error below

            # Log at ERROR — a failed DB insert means the alert is lost from PostgreSQL.
            # The Kafka publisher may still have succeeded, so the alert is not fully lost.
            # A production system would write to a DLQ here.
            logger.error(
                "alert_insert_postgres_failed",
                alert_id=alert.get("alert_id", "unknown"),
                tenant_id=alert.get("tenant_id", "unknown"),
                error=str(exc),
                error_type=type(exc).__name__,
            )
            # Per AlertPublisher contract: do not re-raise. The orchestrator continues.
