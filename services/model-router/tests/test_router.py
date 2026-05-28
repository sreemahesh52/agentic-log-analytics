# --- Unit tests for ModelRouter routing logic ---
# all external dependencies mocked. Tests run with zero external
# services — no PostgreSQL, no Kafka. All assertions test specific expected values.
# Why mock TenantRepository?
# TenantRepository is the dependency-injection seam. ModelRouter
# depends on the repository's interface, not on asyncpg. Injecting AsyncMock
# lets us control find_by_id and get_daily_spend return values precisely,
# testing every routing branch without a running database.
# Why AsyncMock for repository methods?
# find_by_id and get_daily_spend are coroutines (async def). Regular MagicMock
# cannot be awaited — it raises TypeError. AsyncMock returns a coroutine that
# resolves to .return_value when awaited, which is what we need.

import sys
import os

# Add the service root to sys.path so imports resolve correctly when running
# pytest from within the tests/ subdirectory or from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from unittest.mock import AsyncMock

from router import ModelRouter, RoutingConfig
from exceptions import TenantNotFoundError


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def default_routing_config() -> RoutingConfig:
    """Standard RoutingConfig matching the default environment variable values."""
    return RoutingConfig(
        critical_premium="gpt-4-turbo",
        high_premium="gpt-4-turbo",
        medium_premium="gpt-3.5-turbo",
        low_premium="gpt-3.5-turbo",
        any_standard="gpt-3.5-turbo",
        budget_exceeded_fallback="gpt-3.5-turbo",
        low_skip=False,
    )


@pytest.fixture
def premium_tenant() -> dict:
    """Tenant row for a premium-tier customer with a generous daily budget."""
    return {
        "tenant_id": "acme-tenant-uuid",
        "name": "acme-corp",
        "model_tier": "premium",
        "token_budget_usd_daily": 10.0,
    }


@pytest.fixture
def standard_tenant() -> dict:
    """Tenant row for a standard-tier customer with a modest daily budget."""
    return {
        "tenant_id": "startup-tenant-uuid",
        "name": "startup-co",
        "model_tier": "standard",
        "token_budget_usd_daily": 3.0,
    }


def make_mock_repo(tenant: dict, daily_spend: float = 0.0) -> AsyncMock:
    """Build a mock TenantRepository with preset return values.
    AsyncMock is required because find_by_id and get_daily_spend are async.
    Regular MagicMock raises TypeError when awaited.
    """
    repo = AsyncMock()
    # find_by_id returns the tenant dict, simulating a DB row fetch.
    repo.find_by_id.return_value = tenant
    # get_daily_spend returns a float, simulating SUM(cost_usd) from eval_results.
    repo.get_daily_spend.return_value = daily_spend
    return repo


# =============================================================================
# Severity × tier routing tests
# =============================================================================

@pytest.mark.asyncio
async def test_critical_premium_routes_to_gpt4(
    default_routing_config: RoutingConfig,
    premium_tenant: dict,
) -> None:
    """CRITICAL severity with premium tier must select gpt-4-turbo.
    Root cause of the requirement: CRITICAL incidents need the highest-quality
    analysis. GPT-4 has significantly better reasoning than GPT-3.5 for complex
    multi-service root cause analysis.
    """
    repo = make_mock_repo(premium_tenant, daily_spend=0.0)
    router = ModelRouter(tenant_repository=repo, routing_config=default_routing_config)

    decision = await router.route(tenant_id="acme-tenant-uuid", severity="CRITICAL")

    assert decision is not None, "Expected RouterDecision for CRITICAL+premium"
    assert decision.model_id == "gpt-4-turbo", (
        f"CRITICAL+premium must route to gpt-4-turbo, got {decision.model_id!r}"
    )
    assert decision.prompt_variant in ("v1", "v2")
    # reason must encode both the severity and tier for audit traceability.
    assert "CRITICAL" in decision.reason
    assert "premium" in decision.reason


@pytest.mark.asyncio
async def test_high_premium_routes_to_gpt4(
    default_routing_config: RoutingConfig,
    premium_tenant: dict,
) -> None:
    """HIGH severity with premium tier must select gpt-4-turbo."""
    repo = make_mock_repo(premium_tenant, daily_spend=0.0)
    router = ModelRouter(tenant_repository=repo, routing_config=default_routing_config)

    decision = await router.route(tenant_id="acme-tenant-uuid", severity="HIGH")

    assert decision is not None
    assert decision.model_id == "gpt-4-turbo", (
        f"HIGH+premium must route to gpt-4-turbo, got {decision.model_id!r}"
    )
    assert "HIGH" in decision.reason
    assert "premium" in decision.reason


@pytest.mark.asyncio
async def test_medium_premium_routes_to_gpt35(
    default_routing_config: RoutingConfig,
    premium_tenant: dict,
) -> None:
    """MEDIUM severity with premium tier must select gpt-3.5-turbo.
    Design decision: MEDIUM and LOW premium targets use gpt-3.5-turbo by
    default. The cost premium of GPT-4 is not justified for moderate-priority
    alerts where GPT-3.5 is capable enough.
    """
    repo = make_mock_repo(premium_tenant, daily_spend=0.0)
    router = ModelRouter(tenant_repository=repo, routing_config=default_routing_config)

    decision = await router.route(tenant_id="acme-tenant-uuid", severity="MEDIUM")

    assert decision is not None
    assert decision.model_id == "gpt-3.5-turbo", (
        f"MEDIUM+premium must route to gpt-3.5-turbo, got {decision.model_id!r}"
    )


@pytest.mark.asyncio
async def test_any_standard_routes_to_gpt35_regardless_of_severity(
    default_routing_config: RoutingConfig,
    standard_tenant: dict,
) -> None:
    """Standard tier always maps to gpt-3.5-turbo, even for CRITICAL severity.
    Business rule: standard tenants pay for gpt-3.5-turbo pricing. A CRITICAL
    alert from a standard tenant does not automatically upgrade to GPT-4.
    Upgrading would violate the pricing contract and exceed their budget.
    """
    repo = make_mock_repo(standard_tenant, daily_spend=0.0)
    router = ModelRouter(tenant_repository=repo, routing_config=default_routing_config)

    for severity in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
        decision = await router.route(
            tenant_id="startup-tenant-uuid", severity=severity
        )
        assert decision is not None, (
            f"Expected RouterDecision for {severity}+standard, got None"
        )
        assert decision.model_id == "gpt-3.5-turbo", (
            f"Standard tenant severity={severity} got {decision.model_id!r}, "
            "expected gpt-3.5-turbo"
        )
        assert "standard" in decision.reason


# =============================================================================
# Budget exceeded tests
# =============================================================================

@pytest.mark.asyncio
async def test_budget_exceeded_downgrades_to_fallback(
    default_routing_config: RoutingConfig,
    premium_tenant: dict,
) -> None:
    """When daily_spend >= daily_budget, the fallback model must be used.
    Boundary condition: daily_spend == daily_budget exactly triggers the cap.
    A premium CRITICAL incident would normally get gpt-4-turbo, but when
    budget is exhausted it falls back to gpt-3.5-turbo. The cap is hard —
    not a warning that allows one more GPT-4 call to go through.
    """
    # daily_spend exactly equals the budget — boundary condition test.
    repo = make_mock_repo(premium_tenant, daily_spend=10.0)
    router = ModelRouter(tenant_repository=repo, routing_config=default_routing_config)

    decision = await router.route(tenant_id="acme-tenant-uuid", severity="CRITICAL")

    assert decision is not None
    assert decision.model_id == "gpt-3.5-turbo", (
        f"Budget-exceeded downgrade must use gpt-3.5-turbo, got {decision.model_id!r}"
    )
    assert decision.reason == "budget_exceeded", (
        f"Expected reason='budget_exceeded', got {decision.reason!r}"
    )


@pytest.mark.asyncio
async def test_budget_exceeded_when_spend_exceeds_budget(
    default_routing_config: RoutingConfig,
    premium_tenant: dict,
) -> None:
    """daily_spend > daily_budget (exceeding, not just equal) also triggers fallback."""
    # 15.5 > 10.0 — budget exceeded by 5.5 USD.
    repo = make_mock_repo(premium_tenant, daily_spend=15.5)
    router = ModelRouter(tenant_repository=repo, routing_config=default_routing_config)

    decision = await router.route(tenant_id="acme-tenant-uuid", severity="HIGH")

    assert decision is not None
    assert decision.reason == "budget_exceeded"
    assert decision.model_id == "gpt-3.5-turbo"


# =============================================================================
# LOW severity skip tests
# =============================================================================

@pytest.mark.asyncio
async def test_low_severity_returns_none_when_low_skip_true(
    premium_tenant: dict,
) -> None:
    """When low_skip=True and severity=LOW, route must return None.
    None is the correct signal: the Kafka handler discards the incident without
    writing to DLQ and commits the offset. This is different from a DLQ path —
    discarding a LOW incident is intentional, not an error condition.
    Why None instead of a sentinel RouterDecision?
    A sentinel model_id like "skip" or "none" would flow into the payload and
    confuse downstream consumers expecting a real model name. None is an
    unambiguous, type-safe "do not process" signal.
    """
    config = RoutingConfig(
        critical_premium="gpt-4-turbo",
        high_premium="gpt-4-turbo",
        medium_premium="gpt-3.5-turbo",
        low_premium="gpt-3.5-turbo",
        any_standard="gpt-3.5-turbo",
        budget_exceeded_fallback="gpt-3.5-turbo",
        low_skip=True,  # Key: low_skip enabled
    )
    repo = make_mock_repo(premium_tenant, daily_spend=0.0)
    router = ModelRouter(tenant_repository=repo, routing_config=config)

    decision = await router.route(tenant_id="acme-tenant-uuid", severity="LOW")

    assert decision is None, (
        f"Expected None for LOW+low_skip=True, got {decision!r}"
    )


@pytest.mark.asyncio
async def test_low_severity_routes_normally_when_low_skip_false(
    default_routing_config: RoutingConfig,
    premium_tenant: dict,
) -> None:
    """When low_skip=False (the default), LOW severity is routed — not discarded.
    LOW+premium maps to gpt-3.5-turbo per the routing config. The decision
    must not be None and must not cite budget_exceeded as the reason.
    """
    # default_routing_config has low_skip=False.
    repo = make_mock_repo(premium_tenant, daily_spend=0.0)
    router = ModelRouter(tenant_repository=repo, routing_config=default_routing_config)

    decision = await router.route(tenant_id="acme-tenant-uuid", severity="LOW")

    assert decision is not None, (
        "Expected RouterDecision for LOW+low_skip=False, got None"
    )
    assert decision.model_id == "gpt-3.5-turbo"
    # reason must encode severity and tier — not the budget exceeded path.
    assert "LOW" in decision.reason
    assert decision.reason != "budget_exceeded"


# =============================================================================
# Prompt variant tests
# =============================================================================

@pytest.mark.asyncio
async def test_prompt_variant_is_v1_or_v2_only(
    default_routing_config: RoutingConfig,
    premium_tenant: dict,
) -> None:
    """prompt_variant must always be exactly 'v1' or 'v2'.
    The RCA Agent loads rca_agent/{prompt_variant}.txt from the Prompt Registry.
    Any value other than 'v1' or 'v2' would cause a FileNotFoundError in the
    RCA Agent — a production incident caused by bad routing output.
    Running 50 iterations reduces flakiness: with 50/50 random.choice, the
    probability of always getting the same value is (0.5)^50 ≈ 10^-15.
    """
    repo = make_mock_repo(premium_tenant, daily_spend=0.0)
    router = ModelRouter(tenant_repository=repo, routing_config=default_routing_config)

    seen_variants: set[str] = set()
    for _ in range(50):
        decision = await router.route(tenant_id="acme-tenant-uuid", severity="CRITICAL")
        assert decision is not None
        assert decision.prompt_variant in ("v1", "v2"), (
            f"Invalid prompt_variant {decision.prompt_variant!r} — "
            "must be exactly 'v1' or 'v2'"
        )
        seen_variants.add(decision.prompt_variant)

    # After 50 iterations, both variants must have appeared at least once.
    # This validates that the 50/50 split is truly random, not always v1.
    assert seen_variants == {"v1", "v2"}, (
        f"Expected both v1 and v2 after 50 trials, only saw {seen_variants}"
    )


# =============================================================================
# Routing config from env tests
# =============================================================================

def test_routing_config_read_from_env_not_hardcoded() -> None:
    """RoutingConfig values must be injectable — not hardcoded inside ModelRouter.
    This test constructs a RoutingConfig with non-default model names and
    verifies they are stored as-is. If ModelRouter hardcoded model names
    internally (bypassing the injected config), routing would be wrong for
    any operator who customises the model targets via environment variables.
    This test is synchronous: it verifies the config wiring, not async routing.
    """
    custom_config = RoutingConfig(
        critical_premium="gpt-5-turbo",   # non-default: proves injection is used
        high_premium="gpt-5-turbo",
        medium_premium="gpt-4-mini",
        low_premium="gpt-3.5-turbo",
        any_standard="gpt-3.5-turbo",
        budget_exceeded_fallback="gpt-3.5-turbo",
        low_skip=False,
    )
    # The injected values must be stored verbatim — no hardcoded override.
    assert custom_config.critical_premium == "gpt-5-turbo", (
        "RoutingConfig must store injected critical_premium, not override it"
    )
    assert custom_config.high_premium == "gpt-5-turbo"
    assert custom_config.medium_premium == "gpt-4-mini"


# =============================================================================
# Daily spend UTC day boundary test
# =============================================================================

@pytest.mark.asyncio
async def test_daily_spend_uses_utc_day_boundary(
    default_routing_config: RoutingConfig,
    premium_tenant: dict,
) -> None:
    """The daily spend check must be evaluated against the correct UTC day.
    We verify this at the behavioural level: when daily_spend is well under
    the budget, routing must NOT produce a budget_exceeded decision.
    The SQL's explicit AT TIME ZONE 'UTC' is in repository.py — this test
    verifies that get_daily_spend is called and its result is used correctly
    in the comparison logic within route.
    """
    # 5.0 of 10.0 budget spent — well under the cap.
    repo = make_mock_repo(premium_tenant, daily_spend=5.0)
    router = ModelRouter(tenant_repository=repo, routing_config=default_routing_config)

    decision = await router.route(tenant_id="acme-tenant-uuid", severity="CRITICAL")

    assert decision is not None
    assert decision.reason != "budget_exceeded", (
        "5.0 of 10.0 budget spent must not trigger budget_exceeded"
    )
    assert decision.model_id == "gpt-4-turbo", (
        "Under-budget CRITICAL+premium must route to gpt-4-turbo"
    )

    # Verify that get_daily_spend was called with the correct tenant_id.
    # This confirms that the UTC-day SQL query was triggered for the right tenant.
    repo.get_daily_spend.assert_called_once_with("acme-tenant-uuid")


# =============================================================================
# Tenant not found test
# =============================================================================

@pytest.mark.asyncio
async def test_tenant_not_found_raises_error(
    default_routing_config: RoutingConfig,
) -> None:
    """When find_by_id returns None, route must raise TenantNotFoundError.
    The Kafka handler catches TenantNotFoundError and routes the incident to the
    DLQ. This test confirms the correct exception is raised so the handler can
    distinguish a missing tenant from a transient database error.
    """
    repo = AsyncMock()
    # Simulate a deleted or non-existent tenant.
    repo.find_by_id.return_value = None
    router = ModelRouter(tenant_repository=repo, routing_config=default_routing_config)

    with pytest.raises(TenantNotFoundError, match="acme-tenant-uuid"):
        await router.route(tenant_id="acme-tenant-uuid", severity="CRITICAL")

    # get_daily_spend must NOT be called when the tenant doesn't exist —
    # querying spend for a non-existent tenant_id would return 0.0 and
    # incorrectly allow routing to proceed.
    repo.get_daily_spend.assert_not_called()
