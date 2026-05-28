"""
BuildTimeline tool — reconstruct a chronological sequence of errors across services.
This tool is the critical differentiator between root cause and cascading effect.
When multiple services fail, the one that logged its first ERROR earliest is most
likely the root cause — the others are downstream victims of the cascade.
Example: auth-service first ERROR at 10:05:12, payment-service first ERROR at
10:05:47, gateway-service first ERROR at 10:06:03. The timeline proves that
auth-service failed 35 seconds before payment-service even registered an error.
Without a timeline, the agent might incorrectly attribute the root cause to
gateway-service (the most visible failure to users).
this module only queries and formats a
chronological event sequence. It does not correlate, score, or make decisions —
that is the LLM's job after reading the observation.
tenant_id and db_pool are bound at registration
via functools.partial. The LLM provides services (the list of services to include)
and minutes_back.
the ANY($2::text[]) pattern passes the
service list as a PostgreSQL text array parameter — no dynamic SQL generation
required. This avoids both SQL injection and the brittle IN-clause placeholders
needed by psycopg2 for variable-length lists.
"""

from __future__ import annotations

from datetime import timezone

import asyncpg
import structlog

from tools.base import ToolSchema

# structlog for structured JSON log output.
log = structlog.get_logger(__name__)

# Default lookback window. 60 minutes is wider than QueryLogs (30 min) because
# cascade failures can unfold slowly — the root cause may have started failing
# long before the incident was detected and the RCA triggered.
_DEFAULT_MINUTES_BACK = 60

# Hard cap on timeline events. 100 events covers a complex cascade incident
# (e.g., 10 services × 10 errors each) without overwhelming the LLM context.
_MAX_EVENTS = 100


async def build_timeline(
    tenant_id: str,
    db_pool: asyncpg.Pool,
    services: list[str],
    minutes_back: int = _DEFAULT_MINUTES_BACK,
) -> str:
    """Build a chronological timeline of ERROR and FATAL events across services.
    Queries ERROR and FATAL logs (the two most actionable severity levels) for
    all services in the given list, sorted by timestamp ascending. Identifies
    the service with the earliest error entry — this is the likely root cause.
    tenant_id and db_pool are bound at registration time via functools.partial.
    The LLM calls this tool with: services (list of service names), minutes_back.
    Why only ERROR and FATAL?
    INFO and WARN logs are too numerous to form a readable timeline and rarely
    identify root causes directly. The agent can use QueryLogs to fetch lower-
    level logs for a specific service if it needs more granularity.
    Why ANY($2::text[]) instead of IN ($2, $3, $4, ...)?
    ANY with a text array parameter works for zero, one, or many services
    with a single parameterised query. An IN clause requires one placeholder
    per element (asyncpg does not support dynamic IN-list expansion). An empty
    array returns no rows cleanly (ANY('{}'::text[]) is always false), which
    is the desired graceful behaviour for the empty-services edge case.
    Args:
        tenant_id: UUID string of the requesting tenant (pre-bound).
        db_pool: asyncpg connection pool (pre-bound).
        services: List of service names to include in the timeline.
        minutes_back: How far back to look for error events.
    Returns:
        str: Formatted chronological timeline with the first-failing service
             identified, or a descriptive empty message.
    """
    log.debug(
        "build_timeline_start",
        tenant_id=tenant_id,
        service_count=len(services),
        minutes_back=minutes_back,
    )

    # --- Edge case: empty services list ---
    # ANY(ARRAY[]::text[]) is valid PostgreSQL and returns no rows, so the query
    # would succeed but return nothing. Return early with a clear message so the
    # LLM understands why there are no results, rather than seeing a generic
    # "no events found" message that gives no diagnostic information.
    if not services:
        return (
            "No services provided to build_timeline. "
            "Call GetDependencies first to identify affected services, "
            "then pass them to BuildTimeline."
        )

    # --- SQL query ---
    # Only ERROR and FATAL events are fetched — these represent actionable failures.
    # $2 is the services array: asyncpg accepts a Python list[str] for text[] columns.
    # The ::text[] cast ensures PostgreSQL infers the correct array element type.
    query = """
        SELECT timestamp, service, level, message
        FROM logs
        WHERE tenant_id   = $1
          AND service     = ANY($2::text[])
          AND level       IN ('ERROR', 'FATAL')
          AND timestamp   > NOW() - ($3 * INTERVAL '1 minute')
        ORDER BY timestamp ASC
        LIMIT $4
    """
    try:
        # acquire borrows a connection from the pool and returns it on exit.
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                query, tenant_id, services, minutes_back, _MAX_EVENTS
            )
    except Exception as exc:
        log.warning(
            "build_timeline_db_error",
            tenant_id=tenant_id,
            services=services,
            error=str(exc),
        )
        raise RuntimeError(
            f"Database query failed building timeline for {services}: {exc}"
        ) from exc
    # --- Empty result ---
    if not rows:
        return (
            f"No ERROR or FATAL events found for services "
            f"{services} in the last {minutes_back} minutes."
        )
    # --- Identify the first failing service ---
    # rows are already sorted by timestamp ASC, so rows[0] is the earliest event.
    # This is the most important single piece of information in the timeline:
    # the service that logged an error first is most likely the root cause,
    # not a downstream victim of the cascade.
    first_failing_service = rows[0]["service"]
    first_failure_time = rows[0]["timestamp"].astimezone(timezone.utc).isoformat()
    # --- Format chronological timeline ---
    lines: list[str] = [
        f"=== Error timeline for services {services} (last {minutes_back} minutes) ===",
        f"First failing service: {first_failing_service} (first error at {first_failure_time})",
        "--- Chronological events ---",
    ]
    for row in rows:
        # astimezone(timezone.utc).isoformat → "2024-01-15T10:05:12+00:00"
        # Explicit UTC conversion guards against rare cases where asyncpg returns
        # a datetime with a non-UTC offset on misconfigured connections.
        ts = row["timestamp"].astimezone(timezone.utc).isoformat()
        lines.append(
            f"[{ts}] {row['service']} {row['level']} {row['message']}"
        )
    lines.append(f"=== Total: {len(rows)} event(s) ===")
    return "\n".join(lines)
# ---------------------------------------------------------------------------
# BUILD_TIMELINE_SCHEMA — OpenAI function calling schema.
# ---------------------------------------------------------------------------
BUILD_TIMELINE_SCHEMA: ToolSchema = {
    "name": "BuildTimeline",
    # description: the phrase "which service failed first" is critical — it
    # tells the LLM exactly what insight this tool provides, so it knows to call
    # it when it needs to distinguish root cause from cascading effects.
    "description": (
        "Build a chronological timeline of ERROR and FATAL events across multiple services. "
        "Use to identify which service failed first and understand the cascade sequence. "
        "Returns events sorted by time and labels the first failing service."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "services": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "List of service names to include in the timeline. "
                    "Include the primary service and all its dependencies."
                ),
                # minItems: 1 item minimum — an empty list produces no results.
                # The LLM should use GetDependencies first, then pass results here.
                "minItems": 1,
            },
            "minutes_back": {
                "type": "integer",
                "description": "How many minutes back to look for events. Default: 60.",
                "minimum": 1,
            },
        },
        # services is required — the tool has no default list of services.
        "required": ["services"],
    },
}
