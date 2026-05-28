# --- Authentication: cache, repository, and FastAPI dependency ---
# The flow for every authenticated request:
#   1. Hash the incoming X-API-Key with SHA-256
#   2. Check TenantCache — if hit and not expired, return immediately (no DB)
#   3. On miss: TenantRepository queries PostgreSQL
#   4. If found, populate cache; if not found, raise AuthenticationError

import hashlib
import threading
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

import asyncpg
import structlog
from fastapi import Depends, Header

from config import settings
from dependencies import get_db_pool
from exceptions import AuthenticationError

logger = structlog.get_logger()


class TenantCache:
    """Thread-safe in-memory store mapping api_key_hash → tenant dict with TTL eviction.
    Purpose: avoid a PostgreSQL round-trip on every authenticated request.
 Trade-off: a revoked key remains valid for up to api_key_cache_ttl_seconds."""

    def __init__(self, ttl_seconds: int) -> None:
        """Initialise the cache with a TTL in seconds."""
        # Plain dict is the backing store — fast O(1) lookups.
        self._store: dict[str, dict[str, Any]] = {}
        # threading.Lock guards all reads and writes. Although FastAPI is async,
        # this dict is accessed from sync context inside the dependency function,
        # and Python dict mutations are not atomic under all GIL schedules.
        self._lock = threading.Lock()
        self._ttl_seconds = ttl_seconds

    def get(self, key_hash: str) -> dict[str, Any] | None:
        """Return the cached tenant dict if present and not expired, else None."""
        # --- Cache lookup with TTL check ---
        # Acquire the lock for the full read-then-maybe-delete operation
        # so no other thread can modify _store between the two steps.
        with self._lock:
            entry = self._store.get(key_hash)
            if entry is None:
                return None
            # datetime.now(timezone.utc) — always timezone-aware UTC.
            # Never datetime.utcnow which returns a naive datetime.
            age = (datetime.now(timezone.utc) - entry["cached_at"]).total_seconds()
            if age > self._ttl_seconds:
                # Expired — evict now rather than waiting for the next set.
                del self._store[key_hash]
                return None
            return entry["tenant"]

    def set(self, key_hash: str, tenant: dict[str, Any]) -> None:
        """Store a tenant dict under key_hash, stamped with the current UTC time."""
        with self._lock:
            self._store[key_hash] = {
                "tenant": tenant,
                # Stamp with UTC so the TTL comparison in get works correctly
                # regardless of the host's local timezone setting.
                "cached_at": datetime.now(timezone.utc),
            }


# --- Repository interface ---
# AbstractTenantRepository is an ABC so that verify_api_key depends on the
# abstraction, not the concrete PostgreSQL implementation. In tests, inject
# a MockTenantRepository that returns fixture data without touching a database.
# This is Dependency Inversion: high-level auth logic is decoupled from storage.
class AbstractTenantRepository(ABC):
    """Interface for all tenant lookup implementations."""

    @abstractmethod
    async def find_by_api_key_hash(self, key_hash: str) -> dict[str, Any] | None:
        """Return the tenant row for the given hashed key, or None if not found."""
        ...


class TenantRepository(AbstractTenantRepository):
    """Concrete PostgreSQL implementation of AbstractTenantRepository.
    All SQL for the tenants table lives here — never scattered in route handlers.
    The pool is injected so this class has no knowledge of how connections are
 created or managed (Single Responsibility)."""

    def __init__(self, db_pool: asyncpg.Pool) -> None:
        """Accept an asyncpg pool — never open a connection directly."""
        # Storing the pool, not a connection. Each query borrows a connection
        # from the pool for its duration, then returns it automatically.
        self._pool = db_pool

    async def find_by_api_key_hash(self, key_hash: str) -> dict[str, Any] | None:
        """Query tenants by pre-hashed API key. Returns None on no match — never raises."""
        # --- Parameterised query ---
        # $1 is the asyncpg placeholder. Never use f-strings or string
        # concatenation in SQL — parameterised queries only.
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT tenant_id::text, name, model_tier, token_budget_usd_daily
                FROM tenants
                WHERE api_key_hash = $1
        """,
                key_hash,
            )
            if row is None:
                return None
            # dict(row) converts asyncpg Record to a plain dict that is
            # JSON-serialisable and usable as a Pydantic field value.
            return dict(row)


# --- Module-level cache singleton ---
# Created once when this module is imported. Shared across all requests for
# the lifetime of the process. The TTL comes from settings which has already
# been validated at import time by pydantic-settings.
_tenant_cache = TenantCache(ttl_seconds=settings.api_key_cache_ttl_seconds)


def get_tenant_repository(
    db_pool: asyncpg.Pool = Depends(get_db_pool),
) -> TenantRepository:
    """FastAPI dependency factory: create a TenantRepository for each request.
    The pool is injected by FastAPI via Depends(get_db_pool). Creating a new
    TenantRepository per request is cheap — it holds no state beyond the pool
 reference. This pattern makes the repository mockable in tests."""
    return TenantRepository(db_pool)


async def verify_api_key(
    # Header(...) tells FastAPI this is a required HTTP header.
    # alias="X-API-Key" maps the hyphenated header name to a Python identifier.
    # Without alias, FastAPI would look for a parameter named x_api_key.
    x_api_key: str = Header(..., alias="X-API-Key"),
    repo: TenantRepository = Depends(get_tenant_repository),
) -> dict[str, Any]:
    """FastAPI dependency: validate X-API-Key and return the resolved tenant dict.
    Raises AuthenticationError (→ 401) if the key is not found.
 Used as Depends(verify_api_key) in any route that requires authentication."""
    # --- Hash before any lookup ---
    # sha256(key).hexdigest returns a 64-character lowercase hex string.
    # We never compare the raw key — only its hash. This means a DB dump
    # reveals only hashes, which are computationally infeasible to reverse.
    key_hash = hashlib.sha256(x_api_key.encode()).hexdigest()

    # --- Cache-first lookup ---
    # Log only the first 8 chars of the hash — enough to correlate log lines
    # without exposing the full hash (which could aid precomputed attacks).
    cached = _tenant_cache.get(key_hash)
    if cached is not None:
        logger.debug("api_key_cache_hit", key_prefix=key_hash[:8])
        return cached

    # --- Database fallback ---
    tenant = await repo.find_by_api_key_hash(key_hash)
    if tenant is None:
        # Log at WARNING — an invalid key could be a misconfigured client
        # or a probing attempt. Do not log the raw key or full hash.
        logger.warning("api_key_not_found", key_prefix=key_hash[:8])
        raise AuthenticationError("Invalid or missing API key")

    # --- Populate cache on first DB hit ---
    _tenant_cache.set(key_hash, tenant)
    logger.info("api_key_verified", tenant_id=tenant["tenant_id"], name=tenant["name"])
    return tenant
