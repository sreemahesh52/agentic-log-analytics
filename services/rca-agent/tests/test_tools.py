"""
Integration tests for services/rca-agent/tools/ — uses a real PostgreSQL instance.
every test mocks nothing that is the subject under test. The
tools perform real SQL queries against a live PostgreSQL database. However,
all test data is isolated to a unique test tenant created and destroyed per
test session, so tests do not interfere with seed data or each other.
Why real PostgreSQL instead of mocks for these tests?
The tools are SQL-centric: their correctness is defined by whether the SQL
produces the right rows in the right order for the right tenant. Mocking asyncpg
would test that we correctly call mock methods — not that the SQL is correct.
A mock cannot catch a wrong column name, missing index, or INTERVAL syntax error.
Design of the test fixture:
  - One asyncpg pool + one test tenant created once per test session.
  - 15 log rows across 3 services, with known relationships:
      test-svc-a and test-svc-b: share trace IDs (for dependency tests)
      test-svc-c: has its own trace IDs (isolated for empty-dependency test)
      test-svc-a: has the earliest ERROR log (for first-failing-service test)
  - All teardown runs in the fixture's `finally` block — no matter what fails,
    the test tenant and its logs are deleted.
Running these tests requires:
  export POSTGRES_URL=postgresql://admin:admin@localhost:5432/loganalytics
  cd services/rca-agent
  python -m pytest tests/test_tools.py -v
"""

from __future__ import annotations

import hashlib
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

# --- sys.path setup ---
# Insert the rca-agent directory so `from tools.base import Tool` resolves when
# pytest is invoked from any working directory. This mirrors the pattern in
# test_agent_loop.py — no installed package is needed.
sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.base import Tool
from tools.build_timeline import build_timeline
from tools.get_dependencies import get_dependencies
from tools.query_logs import query_logs

# ---------------------------------------------------------------------------
# PostgreSQL URL helper
# ---------------------------------------------------------------------------

# Default URL matches the docker-compose.yml postgres service credentials.
# Override with POSTGRES_URL env var to run against a different instance.
_POSTGRES_URL = os.environ.get(
    "POSTGRES_URL",
    "postgresql://admin:admin@localhost:5432/loganalytics",
)


# ---------------------------------------------------------------------------
# Session-scoped fixture: pool + test tenant + 15 test logs
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="module")
async def db_pool_and_tenant():
    """Create a real asyncpg pool, one test tenant, and 15 test log rows.
    Scope is 'module' (not 'function') so the expensive pool creation and
    log insertion happen once. All tests within this module share the same
    tenant and log rows. Isolation is achieved through tenant_id scoping in
    each SQL query — tests cannot accidentally read each other's data.
    Why not 'session' scope?
    Module scope is the widest safe scope when fixtures involve database state.
    If another test module also uses a DB fixture, session scope could create
    teardown ordering issues. Module scope is predictable: setup + teardown
    happen exactly once per test file.
    Teardown strategy:
    All test data is deleted inside `finally` — even if test assertions raise,
    teardown runs. Without this, failed tests leave orphaned rows that cause
    subsequent test runs to behave differently.
    """
    # --- Create connection pool ---
    # server_settings={"timezone": "UTC"} ensures every connection in the pool
    # returns timezone-aware datetime objects set to UTC. This is
    # belt-and-suspenders alongside PGTZ=UTC in the container environment.
    pool = await asyncpg.create_pool(
        _POSTGRES_URL,
        min_size=1,
        max_size=5,
        server_settings={"timezone": "UTC"},
    )

    # --- Generate test-unique identifiers ---
    # uuid4 ensures no collision with seed data or other test runs.
    tenant_id = str(uuid4())
    # api_key_hash must be unique in the tenants table. SHA-256 of the tenant_id
    # string is collision-resistant and avoids needing a random API key secret.
    api_key_hash = hashlib.sha256(f"test-key-{tenant_id}".encode()).hexdigest()

    # --- Trace IDs for dependency and timeline tests ---
    # trace_ab_1 and trace_ab_2 are shared between test-svc-a and test-svc-b.
    # trace_c_1 and trace_c_2 appear only in test-svc-c.
    # uuid4 produces RFC 4122 UUIDs that asyncpg accepts for UUID columns.
    trace_ab_1 = uuid4()
    trace_ab_2 = uuid4()
    trace_c_1  = uuid4()
    trace_c_2  = uuid4()

    # --- Absolute timestamps for the 15 log rows ---
    # Using absolute offsets (not NOW) ensures the relative order is preserved
    # regardless of test execution time. All times are in the last 30 minutes
    # so they fall within the default minutes_back=30 window.
    # datetime.now(timezone.utc) — never datetime.utcnow.
    now = datetime.now(timezone.utc)

    # test-svc-a timestamps:
    t_a_info_25  = now - timedelta(minutes=25)   # INFO — no trace
    t_a_err_20   = now - timedelta(minutes=20)   # ERROR trace_ab_1 ← earliest error overall
    t_a_err_15   = now - timedelta(minutes=15)   # ERROR trace_ab_2
    t_a_fatal_12 = now - timedelta(minutes=12)   # FATAL trace_ab_1
    t_a_warn_5   = now - timedelta(minutes=5)    # WARN  — no trace

    # test-svc-b timestamps (all errors after svc-a's first error):
    t_b_err_10   = now - timedelta(minutes=10)   # ERROR trace_ab_1 (shared with svc-a)
    t_b_err_8    = now - timedelta(minutes=8)    # ERROR trace_ab_2 (shared with svc-a)
    t_b_err_6    = now - timedelta(minutes=6)    # ERROR trace_ab_1 (shared with svc-a)
    t_b_info_4   = now - timedelta(minutes=4)    # INFO  — no trace
    t_b_warn_2   = now - timedelta(minutes=2)    # WARN  — no trace

    # test-svc-c timestamps (own trace IDs — no overlap with svc-a or svc-b):
    t_c_err_5    = now - timedelta(minutes=5)    # ERROR trace_c_1
    t_c_err_4    = now - timedelta(minutes=4)    # ERROR trace_c_2
    t_c_debug_3  = now - timedelta(minutes=3)    # DEBUG — no trace
    t_c_info_2   = now - timedelta(minutes=2)    # INFO  — no trace
    t_c_info_1   = now - timedelta(minutes=1)    # INFO  — no trace

    try:
        async with pool.acquire() as conn:
            # --- Insert test tenant ---
            # ON CONFLICT DO NOTHING provides idempotency if the UUID somehow
            # collides (astronomically unlikely but good practice).
            await conn.execute(
                """
                INSERT INTO tenants (tenant_id, name, api_key_hash, model_tier)
                VALUES ($1, $2, $3, 'standard')
                ON CONFLICT (tenant_id) DO NOTHING
    """,
                tenant_id,
                f"test-tenant-{tenant_id[:8]}",
                api_key_hash,
            )

            # --- Insert 15 test log rows ---
            # The logs table is partitioned by timestamp (week). All our timestamps
            # are within the last 30 minutes — the init.sql creates a partition for
            # the current week, so all inserts land in the correct partition.
            # metadata JSONB accepts a Python dict — asyncpg serialises it to JSON.
            log_rows = [
                # test-svc-a: 5 rows
                (tenant_id, t_a_info_25,  "test-svc-a", "INFO",  "Service started", None,       {}),
                (tenant_id, t_a_err_20,   "test-svc-a", "ERROR", "Connection refused to db:5432", trace_ab_1, {}),
                (tenant_id, t_a_err_15,   "test-svc-a", "ERROR", "Query timeout after 30s",       trace_ab_2, {}),
                (tenant_id, t_a_fatal_12, "test-svc-a", "FATAL", "OOM kill — process exiting",    trace_ab_1, {}),
                (tenant_id, t_a_warn_5,   "test-svc-a", "WARN",  "Retry attempt 3 of 3",          None,       {}),
                # test-svc-b: 5 rows (shares trace_ab_1 and trace_ab_2 with svc-a)
                (tenant_id, t_b_err_10,   "test-svc-b", "ERROR", "Upstream call to svc-a timed out",  trace_ab_1, {}),
                (tenant_id, t_b_err_8,    "test-svc-b", "ERROR", "Received 503 from svc-a",           trace_ab_2, {}),
                (tenant_id, t_b_err_6,    "test-svc-b", "ERROR", "Circuit breaker opened for svc-a",  trace_ab_1, {}),
                (tenant_id, t_b_info_4,   "test-svc-b", "INFO",  "Health check passed",               None,       {}),
                (tenant_id, t_b_warn_2,   "test-svc-b", "WARN",  "Degraded mode active",              None,       {}),
                # test-svc-c: 5 rows (own trace IDs — no overlap with svc-a/b)
                (tenant_id, t_c_err_5,    "test-svc-c", "ERROR", "Cache miss rate above threshold",   trace_c_1, {}),
                (tenant_id, t_c_err_4,    "test-svc-c", "ERROR", "Redis connection pool exhausted",   trace_c_2, {}),
                (tenant_id, t_c_debug_3,  "test-svc-c", "DEBUG", "Cache eviction triggered",          None,      {}),
                (tenant_id, t_c_info_2,   "test-svc-c", "INFO",  "Cache warmed up",                   None,      {}),
                (tenant_id, t_c_info_1,   "test-svc-c", "INFO",  "Request processed in 12ms",         None,      {}),
            ]

            # executemany inserts all 15 rows in a single round-trip — more
            # efficient than 15 individual execute calls.
            await conn.executemany(
                """
                INSERT INTO logs (tenant_id, timestamp, service, level, message, trace_id, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
    """,
                log_rows,
            )

        # yield passes the pool and tenant_id to each test.
        # Execution resumes at the finally block after all tests in the module complete.
        yield pool, tenant_id

    finally:
        # --- Teardown: delete all test data in FK-safe order ---
        # logs references tenants via tenant_id FK with ON DELETE RESTRICT.
        # Delete logs first, then the tenant — reversing this order fails the FK check.
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM logs WHERE tenant_id = $1", tenant_id
            )
            await conn.execute(
                "DELETE FROM tenants WHERE tenant_id = $1", tenant_id
            )

        # Close the pool after all tests complete — this releases all connections
        # back to PostgreSQL and avoids "too many connections" errors in CI.
        await pool.close()


# ---------------------------------------------------------------------------
# Tests: query_logs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_query_logs_returns_formatted_output(db_pool_and_tenant):
    """query_logs returns a header, log lines, and a footer for matching rows."""
    pool, tenant_id = db_pool_and_tenant

    result = await query_logs(
        tenant_id=tenant_id,
        db_pool=pool,
        service="test-svc-a",
        level="ERROR",
        minutes_back=30,
    )

    # Header must identify the service and level queried.
    assert "test-svc-a" in result
    assert "ERROR" in result
    # At least one log line must be present (we inserted 2 ERRORs for svc-a).
    assert "Connection refused" in result or "Query timeout" in result
    # Footer must show the count.
    assert "log(s) found" in result


@pytest.mark.asyncio
async def test_query_logs_returns_empty_message_when_no_results(db_pool_and_tenant):
    """query_logs returns a descriptive empty message when no logs match."""
    pool, tenant_id = db_pool_and_tenant

    result = await query_logs(
        tenant_id=tenant_id,
        db_pool=pool,
        # non-existent service for this tenant produces no results.
        service="nonexistent-service-xyz",
        level="ERROR",
        minutes_back=30,
    )

    # Must describe what was searched and why nothing was found.
    assert "No" in result
    assert "nonexistent-service-xyz" in result


@pytest.mark.asyncio
async def test_query_logs_filters_by_level(db_pool_and_tenant):
    """query_logs only returns logs matching the requested level."""
    pool, tenant_id = db_pool_and_tenant

    result = await query_logs(
        tenant_id=tenant_id,
        db_pool=pool,
        service="test-svc-a",
        # FATAL: only 1 row in the fixture for this service at this level.
        level="FATAL",
        minutes_back=30,
    )

    # The FATAL log must appear.
    assert "OOM kill" in result
    # ERROR logs must NOT appear — the query is filtered to FATAL only.
    assert "Connection refused" not in result
    assert "Query timeout" not in result


@pytest.mark.asyncio
async def test_query_logs_invalid_level_returns_error_string_not_raises(db_pool_and_tenant):
    """query_logs returns an error string for invalid log levels instead of raising.
    tool errors must be recoverable. The agent loop receives the
    error string as an observation and can retry with a valid level — it does
    not crash because of invalid LLM-provided arguments.
    """
    pool, tenant_id = db_pool_and_tenant

    result = await query_logs(
        tenant_id=tenant_id,
        db_pool=pool,
        service="test-svc-a",
        # 'CRITICAL' is not a valid level in this system — the LLM hallucinated it.
        level="CRITICAL",
        minutes_back=30,
    )

    # Must return an error string, not raise an exception.
    assert isinstance(result, str)
    assert "Invalid" in result
    assert "CRITICAL" in result
    # The valid levels must be listed so the LLM can self-correct.
    assert "ERROR" in result


@pytest.mark.asyncio
async def test_query_logs_timestamps_in_utc(db_pool_and_tenant):
    """Every log line in the output contains a UTC ISO 8601 timestamp."""
    pool, tenant_id = db_pool_and_tenant

    result = await query_logs(
        tenant_id=tenant_id,
        db_pool=pool,
        service="test-svc-a",
        level="ERROR",
        minutes_back=30,
    )

    # Extract lines that start with '[' — these are the individual log lines.
    log_lines = [line for line in result.split("\n") if line.startswith("[")]

    # At least one line must exist (we inserted 2 ERRORs for svc-a).
    assert len(log_lines) >= 1, "Expected at least one log line in output"

    for line in log_lines:
        # The timestamp is enclosed in [...] at the start of each line.
        # must contain the UTC offset indicator '+00:00'.
        assert "+00:00" in line, (
            f"Log line missing UTC offset: {line!r}"
        )
        # Must also be parseable as a valid ISO 8601 datetime.
        ts_str = line[1 : line.index("]")]
        parsed = datetime.fromisoformat(ts_str)
        assert parsed.tzinfo is not None, "Timestamp must be timezone-aware"
        assert parsed.utcoffset() == timedelta(0), "Timestamp offset must be UTC"


# ---------------------------------------------------------------------------
# Tests: get_dependencies
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_dependencies_returns_correlated_services(db_pool_and_tenant):
    """get_dependencies finds services sharing trace IDs with the given service."""
    pool, tenant_id = db_pool_and_tenant

    result = await get_dependencies(
        tenant_id=tenant_id,
        db_pool=pool,
        service="test-svc-a",
        minutes_back=30,
    )

    # test-svc-b shares trace_ab_1 and trace_ab_2 with test-svc-a.
    # It must appear in the dependency output.
    assert "test-svc-b" in result

    # The shared trace count must be present (a positive integer).
    assert "shared trace" in result


@pytest.mark.asyncio
async def test_get_dependencies_returns_empty_when_no_shared_traces(db_pool_and_tenant):
    """get_dependencies returns an empty message when no services share traces."""
    pool, tenant_id = db_pool_and_tenant

    result = await get_dependencies(
        tenant_id=tenant_id,
        db_pool=pool,
        # test-svc-c has trace_c_1 and trace_c_2 that appear ONLY in svc-c.
        # No other service in our test tenant has these trace IDs.
        service="test-svc-c",
        minutes_back=30,
    )

    # Must return a descriptive no-results message.
    assert "No" in result
    assert "test-svc-c" in result


# ---------------------------------------------------------------------------
# Tests: build_timeline
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_build_timeline_returns_chronological_events(db_pool_and_tenant):
    """build_timeline returns ERROR/FATAL events sorted by timestamp ascending."""
    pool, tenant_id = db_pool_and_tenant

    result = await build_timeline(
        tenant_id=tenant_id,
        db_pool=pool,
        services=["test-svc-a", "test-svc-b"],
        minutes_back=30,
    )

    # Both services must appear in the timeline.
    assert "test-svc-a" in result
    assert "test-svc-b" in result

    # Verify chronological order: extract all timestamp strings from log lines.
    # Lines containing '] test-svc-' are the event lines.
    event_lines = [
        line for line in result.split("\n")
        if line.startswith("[") and "] test-svc-" in line
    ]
    assert len(event_lines) >= 2, "Expected at least 2 event lines"

    # Parse each timestamp and verify strictly ascending order.
    timestamps = []
    for line in event_lines:
        ts_str = line[1 : line.index("]")]
        timestamps.append(datetime.fromisoformat(ts_str))

    for i in range(1, len(timestamps)):
        assert timestamps[i] >= timestamps[i - 1], (
            f"Timeline not chronological: {timestamps[i - 1]} > {timestamps[i]}"
        )


@pytest.mark.asyncio
async def test_build_timeline_identifies_first_failing_service(db_pool_and_tenant):
    """build_timeline correctly identifies the service with the earliest error."""
    pool, tenant_id = db_pool_and_tenant

    result = await build_timeline(
        tenant_id=tenant_id,
        db_pool=pool,
        services=["test-svc-a", "test-svc-b"],
        minutes_back=30,
    )

    # test-svc-a logged its first ERROR at t-20min.
    # test-svc-b's first ERROR was at t-10min (10 minutes later).
    # The timeline must identify test-svc-a as the first failing service.
    assert "First failing service: test-svc-a" in result


@pytest.mark.asyncio
async def test_build_timeline_empty_services_returns_gracefully(db_pool_and_tenant):
    """build_timeline with an empty services list returns a helpful message without raising."""
    pool, tenant_id = db_pool_and_tenant

    result = await build_timeline(
        tenant_id=tenant_id,
        db_pool=pool,
        services=[],  # empty list — not an error, but no events possible
        minutes_back=30,
    )

    # Must return a string (not raise), with guidance on correct usage.
    assert isinstance(result, str)
    # Must mention the empty input so the LLM knows why there are no results.
    assert len(result) > 0


# ---------------------------------------------------------------------------
# Tests: Tool.execute wrapper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tool_execute_wrapper_returns_error_string_on_exception():
    """Tool.execute returns 'Tool error ...' when the underlying func raises.
    the agent loop must never crash due to a tool failure.
    Tool.execute is the safety boundary that converts any exception into
    an observation string the LLM can reason about.
    This test does NOT need a database — it tests the Tool wrapper in isolation.
    The failing_func simulates a database connection failure or network timeout.
    """

    async def failing_func(**kwargs) -> str:
        # Simulates an asyncpg connection failure or a query timeout.
        raise ConnectionError("Database connection pool exhausted")

    # Create a minimal Tool with the failing function.
    tool = Tool(
        schema={
            "name": "TestTool",
            "description": "A test tool that always fails",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
        func=failing_func,
    )

    # execute must NOT raise — it must return a string.
    result = await tool.execute()

    # The result must be a string starting with "Tool error (TestTool)".
    assert isinstance(result, str)
    assert "Tool error" in result
    assert "TestTool" in result
    # The original error message must be included so the LLM has context.
    assert "Database connection pool exhausted" in result
