"""
GetDependencies tool — discover services co-occurring in the same request traces.
This tool answers: "which services handled the same user requests as the
failing service?" — enabling the agent to understand blast radius (how many
other services are affected) and identify upstream dependencies (which service
called the failing one, making it the likely root cause).
Why trace-based discovery instead of a static service map?
Static topology maps become stale the moment deployment changes occur. Trace-based
discovery is dynamic: it reflects the actual traffic patterns right now, during
the incident. If payment-service and auth-service share 47 traces in the last
30 minutes, that is empirical evidence of a real dependency at the time of failure.
this module only performs the trace-join
query and formats the result. It has no knowledge of the LLM, Kafka, or Redis.
tenant_id and db_pool are bound at tool
registration via functools.partial. The LLM only provides service and minutes_back.
the self-join on logs uses $N placeholders.
Interpolating tenant_id or service into the SQL string is not acceptable.
"""

from __future__ import annotations

import asyncpg
import structlog

from tools.base import ToolSchema

# structlog for structured JSON log output.
log = structlog.get_logger(__name__)

# Default lookback window. 30 minutes captures the incident window without
# including older, unrelated traffic that would add noise to the dependency map.
_DEFAULT_MINUTES_BACK = 30

# Maximum number of dependent services to return.
# Top 10 covers all realistic microservice fan-out patterns. Returning all
# services (possibly hundreds in a large system) would overwhelm the LLM context.
_MAX_DEPENDENCIES = 10


async def get_dependencies(
    tenant_id: str,
    db_pool: asyncpg.Pool,
    service: str,
    minutes_back: int = _DEFAULT_MINUTES_BACK,
) -> str:
    """Find services that share distributed trace IDs with the given service.
    Two services sharing a trace_id means they both handled legs of the same
    user request. High shared-trace counts indicate tight coupling; low counts
    suggest occasional cross-service calls (sidechannels, async callbacks).
    The SQL self-join is the most reliable way to extract the dependency graph
    from log data without requiring a separate topology service. It requires
    that both services log with consistent trace propagation (OpenTelemetry or
    equivalent).
    tenant_id and db_pool are bound at registration time via functools.partial.
    The LLM calls this tool with: service, (optionally) minutes_back.
    Args:
        tenant_id: UUID string of the requesting tenant (pre-bound).
        db_pool: asyncpg connection pool (pre-bound).
        service: Service whose dependencies we are discovering.
        minutes_back: Lookback window in minutes for the trace join.
    Returns:
        str: Formatted dependency list or a descriptive empty message.
    """
    log.debug(
        "get_dependencies_start",
        tenant_id=tenant_id,
        service=service,
        minutes_back=minutes_back,
    )

    # --- SQL self-join on trace_id ---
    # l1: logs FROM the service we are investigating (the anchor).
    # l2: logs FROM any OTHER service that handled the same trace.
    # The join conditions are:
    #   l1.trace_id = l2.trace_id → same request
    #   l2.service != l1.service → exclude l1's own service
    #   l2.tenant_id = l1.tenant_id → stay within tenant (multi-tenancy safety)
    # Why GROUP BY l2.service with COUNT(DISTINCT l1.trace_id)?
    # A single service pair might share many log lines per trace (e.g. 10 log
    # lines per service per request). DISTINCT trace_id counts requests, not
    # individual log lines, giving a more meaningful coupling metric.
    # AND l1.trace_id IS NOT NULL: logs without a trace_id were not part of
    # a distributed trace. Joining on NULL = NULL would produce false positives
    # (all NULL-trace services would appear connected to each other).
    query = """
        SELECT l2.service, COUNT(DISTINCT l1.trace_id) AS shared_traces
        FROM logs l1
        INNER JOIN logs l2
            ON  l1.trace_id    = l2.trace_id
            AND l2.service    != l1.service
            AND l2.tenant_id   = l1.tenant_id
        WHERE l1.tenant_id = $1
          AND l1.service   = $2
          AND l1.timestamp > NOW() - ($3 * INTERVAL '1 minute')
          AND l1.trace_id IS NOT NULL
        GROUP BY l2.service
        ORDER BY shared_traces DESC
        LIMIT $4
    """
    try:
        # acquire borrows one connection; `async with` returns it on exit
        # even if the query raises an exception (connection hygiene).
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                query, tenant_id, service, minutes_back, _MAX_DEPENDENCIES
            )
    except Exception as exc:
        log.warning(
            "get_dependencies_db_error",
            tenant_id=tenant_id,
            service=service,
            error=str(exc),
        )
        raise RuntimeError(
            f"Database query failed for dependencies of '{service}': {exc}"
        ) from exc
    # --- Empty result ---
    if not rows:
        return (
            f"No trace-sharing services found for '{service}' "
            f"in the last {minutes_back} minutes. "
            "Either this service operates independently or trace propagation "
            "is not enabled for its upstream callers."
        )
    # --- Format output ---
    lines: list[str] = [
        f"=== Services sharing traces with '{service}' (last {minutes_back} minutes) ===",
    ]
    for row in rows:
        # shared_traces is the number of distinct request traces that involved
        # both services. Higher = tighter coupling = stronger evidence of a
        # dependency relationship.
        lines.append(
            f" {row['service']}: {row['shared_traces']} shared trace(s)"
        )
    lines.append(
        f"=== Total: {len(rows)} dependent service(s) found ==="
    )
    return "\n".join(lines)
# ---------------------------------------------------------------------------
# GET_DEPENDENCIES_SCHEMA — OpenAI function calling schema.
# ---------------------------------------------------------------------------
GET_DEPENDENCIES_SCHEMA: ToolSchema = {
    "name": "GetDependencies",
    # description: guides the LLM on when to use this tool. Mentioning
    # "cascade failures" and "upstream" connects this tool to the specific
    # investigation patterns described in the RCA system prompt.
    "description": (
        "Find services that share distributed trace IDs with a given service. "
        "Use to understand cascade failures and identify upstream dependencies. "
        "Returns services ranked by number of shared request traces."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "service": {
                "type": "string",
                "description": "The service to find dependencies for.",
            },
            "minutes_back": {
                "type": "integer",
                "description": "How many minutes back to search traces. Default: 30.",
                "minimum": 1,
            },
        },
        # service is required — without it the query has no anchor point.
        "required": ["service"],
    },
}
