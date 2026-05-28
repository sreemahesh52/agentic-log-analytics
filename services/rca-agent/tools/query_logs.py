"""
QueryLogs tool — fetch recent log entries for a specific service from PostgreSQL.
This is the primary evidence-gathering tool for the RCA Agent. An investigation
typically starts here: the agent reads error messages and stack traces for the
affected service before deciding which other tools to call.
this module queries the logs table and
formats results. It does not publish to Kafka, call the LLM, or modify data.
tenant_id and db_pool are NOT created here.
They are bound to the function via functools.partial at tool registration time
in the Kafka consumer (Step 13d). The LLM only ever provides the business
parameters: service name, log level, and time range. This separation means the
LLM prompt never contains infrastructure details (connection strings, tenant IDs).
all SQL uses $N positional parameters with
asyncpg. String concatenation or f-strings in SQL are never acceptable — they
open SQL injection vulnerabilities even for internal services.
Why asyncpg and not psycopg2?
asyncpg is a fully async PostgreSQL driver designed for asyncio event loops.
The RCA Agent is async throughout (ReAct loop, tool dispatch, Kafka consumer).
psycopg2 uses sync I/O which would block the event loop, stalling all other
concurrent coroutines while a database query runs.
"""

from __future__ import annotations

from datetime import timezone

import asyncpg
import structlog

from tools.base import ToolSchema

# structlog provides structured JSON logging for every tool call.
log = structlog.get_logger(__name__)

# Valid PostgreSQL log level values — must match the CHECK constraint in init.sql.
# Used for input validation before the query runs, returning an error string
# instead of letting the DB raise a CHECK constraint violation exception.
_VALID_LEVELS = frozenset({"DEBUG", "INFO", "WARN", "ERROR", "FATAL"})

# Maximum number of log lines returned in a single tool call.
# Why 50? The LLM's context window is finite. Returning 500 lines would use
# most of the available tokens before the agent can write its conclusion.
# 50 lines is enough to identify error patterns while leaving room for tool
# results from GetDependencies and BuildTimeline.
_DEFAULT_LIMIT = 50

# Lookback window in minutes. 30 minutes captures most transient incidents
# without overwhelming the LLM with historical noise from unrelated events.
_DEFAULT_MINUTES_BACK = 30


async def query_logs(
    tenant_id: str,
    db_pool: asyncpg.Pool,
    service: str,
    level: str = "ERROR",
    minutes_back: int = _DEFAULT_MINUTES_BACK,
    limit: int = _DEFAULT_LIMIT,
) -> str:
    """Query PostgreSQL for recent log entries matching service and level.
    tenant_id and db_pool are bound at registration time via functools.partial.
    The LLM calls this tool with: service, (optionally) level, minutes_back, limit.
    Why validate level here instead of relying on the DB CHECK constraint?
    If an invalid level reached the DB, PostgreSQL would raise a check violation
    exception. The exception would propagate through the agent's tool dispatch,
    which catches it and returns an error string anyway. Validating here gives a
    cleaner error message that the LLM can understand without DB internals.
    Args:
        tenant_id: UUID string of the requesting tenant (pre-bound).
        db_pool: asyncpg connection pool (pre-bound).
        service: Service name to filter logs by (LLM provides this).
        level: Log level filter. Must be one of DEBUG/INFO/WARN/ERROR/FATAL.
        minutes_back: How far back in time to look, in minutes.
        limit: Maximum number of log rows to return.
    Returns:
        str: Formatted log output or an error/empty message.
    """
    # --- Input validation: level must be in the known set ---
    # Return an error string (not raise) so Tool.execute passes this to the
    # LLM as an observation. The LLM can then retry with a valid level.
    level_upper = level.upper()
    if level_upper not in _VALID_LEVELS:
        return (
            f"Invalid log level '{level}'. "
            f"Valid levels: {', '.join(sorted(_VALID_LEVELS))}"
        )

    log.debug(
        "query_logs_start",
        tenant_id=tenant_id,
        service=service,
        level=level_upper,
        minutes_back=minutes_back,
    )

    # --- SQL query ---
    # Why $4 * INTERVAL '1 minute' instead of ($4 || ' minutes')::INTERVAL?
    # asyncpg sends Python int parameters as PostgreSQL INTEGER — the ||
    # text concatenation operator does not accept INTEGER as left operand.
    # Multiplying INTERVAL '1 minute' by an integer is the correct SQL pattern
    # for a parameterised interval. It stays entirely in TIMESTAMPTZ space:
    #   NOW returns TIMESTAMPTZ
    #   TIMESTAMPTZ - INTERVAL returns TIMESTAMPTZ
    # No implicit TIMESTAMP / TIMESTAMPTZ coercion needed.
    query = """
        SELECT timestamp, level, message, trace_id
        FROM logs
        WHERE tenant_id = $1
          AND service   = $2
          AND level     = $3
          AND timestamp > NOW() - ($4 * INTERVAL '1 minute')
        ORDER BY timestamp ASC
        LIMIT $5
    """
    try:
        # acquire borrows one connection from the pool for this query.
        # The `async with` block ensures the connection is returned to the
        # pool even if the query raises an exception.
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(query, tenant_id, service, level_upper, minutes_back, limit)
    except Exception as exc:
        # Wrap the asyncpg error with context. The caller (Tool.execute) will
        # catch this and return a "Tool error" observation to the LLM.
        log.warning(
            "query_logs_db_error",
            tenant_id=tenant_id,
            service=service,
            error=str(exc),
        )
        raise RuntimeError(f"Database query failed for service '{service}': {exc}") from exc
    # --- Empty result ---
    if not rows:
        return (
            f"No {level_upper} logs found for service '{service}' "
            f"in the last {minutes_back} minutes."
        )
    # --- Format output ---
    # Header identifies what was queried so the LLM has context when reading
    # multiple tool results in the same investigation.
    lines: list[str] = [
        f"=== {level_upper} logs for '{service}' (last {minutes_back} minutes) ===",
    ]
    for row in rows:
        # asyncpg returns TIMESTAMPTZ columns as timezone-aware datetime objects
        # when the connection has server_settings={"timezone": "UTC"}.
        # .isoformat produces "2024-01-15T10:23:45+00:00" — the +00:00 offset
        # makes the UTC basis explicit. Never use str on datetime objects —
        # the output format is implementation-defined and lacks timezone info.
        ts = row["timestamp"].astimezone(timezone.utc).isoformat()
        # trace_id is NULL for logs that were not part of a distributed trace.
        # Show "no-trace" as a visible signal so the agent knows it cannot
        # use GetDependencies to follow this log entry across services.
        trace_str = str(row["trace_id"]) if row["trace_id"] is not None else "no-trace"
        lines.append(f"[{ts}] {row['level']} trace={trace_str} {row['message']}")
    # Footer helps the LLM understand the completeness of the evidence.
    lines.append(f"=== Total: {len(rows)} log(s) found ===")
    return "\n".join(lines)
# ---------------------------------------------------------------------------
# QUERY_LOGS_SCHEMA — OpenAI function calling schema for query_logs.
# ---------------------------------------------------------------------------
# This schema is passed to the OpenAI tools parameter as:
# {"type": "function", "function": QUERY_LOGS_SCHEMA}
# The LLM reads the description and parameters to decide when and how to call
# this tool. Vague descriptions lead to incorrect tool selection — every field
# in the description is intentional.
QUERY_LOGS_SCHEMA: ToolSchema = {
    "name": "QueryLogs",
    # description: tells the LLM what this tool does and when to use it.
    # "Start with ERROR level" guides the agent toward the most useful first call
    # rather than starting with DEBUG and flooding the context with noise.
    "description": (
        "Query recent logs for a specific service and level. "
        "Use to find error patterns and stack traces. "
        "Start with ERROR level. "
        "Returns formatted log lines with timestamps, levels, and trace IDs."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "service": {
                "type": "string",
                "description": "The service name to query logs for (e.g. 'payment-service').",
            },
            "level": {
                "type": "string",
                # enum restricts the LLM to valid values — prevents hallucinated
                # levels like 'CRITICAL' or 'warning' that would return no results.
                "enum": list(_VALID_LEVELS),
                "description": "Log level to filter by. Default: ERROR.",
            },
            "minutes_back": {
                "type": "integer",
                "description": "How many minutes back to search. Default: 30.",
                # minimum prevents negative lookback which would always return empty.
                "minimum": 1,
            },
            "limit": {
                "type": "integer",
                "description": "Maximum number of log lines to return. Default: 50.",
                "minimum": 1,
                "maximum": 200,
            },
        },
        # required: the LLM must always provide service. Other args have sensible defaults.
        "required": ["service"],
    },
}
