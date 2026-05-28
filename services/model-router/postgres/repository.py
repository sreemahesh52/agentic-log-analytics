# --- Tenant Repository for the Model Router service ---
# Repository Pattern: ALL SQL lives in this module.
# ModelRouter calls find_by_id and get_daily_spend by intent — it never
# sees a single SQL keyword. This makes business logic testable without a DB:
# swap TenantRepository for an AsyncMock in any test.
# Why NOT cache daily_spend?
# Tenant metadata (model_tier, budget) changes rarely — a 60s stale read is
# acceptable. Daily spend changes with every eval_result row insert, and a
# stale spend figure could allow a tenant to exceed their budget. Real-time
# PostgreSQL query is the safe choice for spend enforcement.

import time
from typing import Any

import asyncpg
import structlog

from exceptions import DatabaseQueryError

logger = structlog.get_logger()

# --- Cache constants ---
# Tenant rows are cached for 60 seconds to avoid a DB round-trip per incident.
# Why 60s? A model_tier change (e.g. standard → premium) propagates within
# one minute — acceptable drift for a non-critical routing metadata field.
_DEFAULT_CACHE_TTL_SECONDS = 60


class TenantRepository:
    """Manages all SQL queries related to tenant data.
    Repository Pattern: if we ever change how tenants are stored
    (e.g., primary source moves to Redis), we change only this class. All callers
    (ModelRouter) remain unchanged — they depend on the repository's interface,
    not on asyncpg internals.
    The db_pool is injected at construction — never created inside this class.
    the class depends on asyncpg.Pool as an
    abstraction, not on connection string parsing or pool initialisation code.
    """

    def __init__(
        self,
        db_pool: asyncpg.Pool,
        cache_ttl_seconds: int = _DEFAULT_CACHE_TTL_SECONDS,
    ) -> None:
        self._pool = db_pool
        self._cache_ttl_seconds = cache_ttl_seconds
        # In-memory cache: {tenant_id: {"data": dict, "cached_at": float}}
        # dict access is safe here because all cache operations happen on the
        # asyncio event loop (single-threaded) — no concurrent mutation risk.
        self._cache: dict[str, dict[str, Any]] = {}

    async def find_by_id(self, tenant_id: str) -> dict[str, Any] | None:
        """Fetch tenant metadata by tenant_id, using an in-memory TTL cache.
        Returns None if the tenant does not exist in the tenants table.
        Callers must handle None explicitly — a missing tenant is a data
        integrity issue that warrants a DLQ write, not silent default routing.
        Raises DatabaseQueryError if the PostgreSQL query itself fails.
        """
        # --- Cache lookup ---
        cached = self._cache.get(tenant_id)
        if cached is not None:
            # time.monotonic is preferred over time.time for elapsed-time
            # calculations because it is not affected by system clock changes.
            age_seconds = time.monotonic() - cached["cached_at"]
            if age_seconds < self._cache_ttl_seconds:
                # Cache hit: return the stored dict without hitting PostgreSQL.
                return cached["data"]  # type: ignore[return-value]

        # --- Cache miss: query PostgreSQL ---
        # $1 is the parameterised placeholder for asyncpg.
        # Never interpolate tenant_id into the query string — SQL injection risk.
        query = """
            SELECT tenant_id, name, model_tier, token_budget_usd_daily
            FROM tenants
            WHERE tenant_id = $1
        """
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(query, tenant_id)
        except Exception as exc:
            raise DatabaseQueryError(
                f"Failed to query tenant {tenant_id!r}: {exc}"
            ) from exc
        if row is None:
            return None
        # asyncpg Record objects behave like dicts but are not serialisable.
        # Convert to a plain dict so callers can safely json.dumps the value.
        data: dict[str, Any] = dict(row)
        # Store in cache with current monotonic timestamp for TTL enforcement.
        self._cache[tenant_id] = {
            "data": data,
            "cached_at": time.monotonic(),
        }
        return data
    async def get_daily_spend(self, tenant_id: str) -> float:
        """Return the total cost_usd spent by this tenant in the current UTC day.

        Why UTC day boundary?
        The daily budget resets at UTC midnight. Using explicit AT TIME ZONE 'UTC'
        in date_trunc prevents the boundary from shifting if the PostgreSQL server
        is configured with a non-UTC timezone. Belt-and-suspenders alongside
        PGTZ=UTC in the container env — belt can fail; suspenders still hold.

        Why PostgreSQL for this, not Prometheus?
        Prometheus is an observation tool, not an authoritative data store.
        Querying Prometheus from application code creates a dependency on the
        monitoring system for business-critical decisions (budget enforcement).
        If Prometheus is down or lagging, routing decisions would be incorrect.
        PostgreSQL is the source of truth for eval_results cost_usd records.

        Raises DatabaseQueryError if the query fails.
        """
        # date_trunc('day', NOW AT TIME ZONE 'UTC') returns the start of the
        # current UTC day: e.g. 2024-01-15 00:00:00+00. Adding INTERVAL '1 day'
        # gives midnight tomorrow UTC. This range is a closed-open interval
        # [today_start, tomorrow_start) that captures exactly today's spend.
        # COALESCE ensures 0.0 is returned when no eval_results rows exist yet.
        query = """
            SELECT COALESCE(SUM(cost_usd), 0.0)
            FROM eval_results
            WHERE tenant_id = $1
              AND evaluated_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC')
              AND evaluated_at <  date_trunc('day', NOW() AT TIME ZONE 'UTC') + INTERVAL '1 day'
        """
        try:
            async with self._pool.acquire() as conn:
                result = await conn.fetchval(query, tenant_id)
        except Exception as exc:
            raise DatabaseQueryError(
                f"Failed to query daily spend for tenant {tenant_id!r}: {exc}"
            ) from exc
        # fetchval returns Decimal from PostgreSQL COALESCE(SUM(...)) — convert
        # to float for comparison against token_budget_usd_daily (also float).
        return float(result) if result is not None else 0.0
