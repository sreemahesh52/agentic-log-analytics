# --- Model Router: core routing logic ---
# Single Responsibility: this module decides which LLM model and
# prompt variant to use for an incident. It does NOT touch Kafka or PostgreSQL
# directly — those dependencies are injected via constructor arguments.
# Why config-driven routing instead of if/elif chains?
# An if/elif chain for model selection grows with every new severity tier or
# model option. It must be re-tested and re-deployed on every change.
# A RoutingConfig object reads targets from the environment: operators swap
# models by updating an env var and restarting the container — zero code change.
# Strategy-like pattern: the routing table (RoutingConfig) is the swappable
# strategy. ModelRouter.route is the context that applies it. New routing
# rules = new RoutingConfig construction in main.py, not edits here.

import random
from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    # Type-checking import only: avoids a circular dependency at runtime.
    # TenantRepository is used only in type hints, not in any runtime call here.
    from postgres.repository import TenantRepository

logger = structlog.get_logger()

# --- Valid prompt variants for A/B testing ---
# v1: systematic evidence-first RCA strategy.
# v2: hypothesis-driven RCA strategy.
# 50/50 random split provides equal traffic for statistically valid comparison.
_PROMPT_VARIANTS: list[str] = ["v1", "v2"]


@dataclass(frozen=True)
class RouterDecision:
    """Immutable result of a single routing decision.
    frozen=True: RouterDecision is a value object — it must never be mutated
    after creation. The fields map directly to what gets attached to the
    incident payload before publishing to incidents.ready.
    Why a dataclass instead of a dict?
    Type safety: callers cannot access decision.nonexistent_field at runtime
    without an AttributeError. A dict silently returns None for missing keys.
    """

    # model_id: the OpenAI model name selected for this incident's RCA.
    model_id: str
    # prompt_variant: 'v1' or 'v2' — selects which prompt file the RCA Agent loads.
    prompt_variant: str
    # reason: why this decision was made — forwarded to the RCA Agent for audit.
    reason: str


class RoutingConfig:
    """Routing model targets. All values must be injected from Settings.
    Why a separate class instead of reading settings directly inside ModelRouter?
    Dependency Inversion: ModelRouter depends on an injected config
    object, not on the Settings singleton. Tests can construct any RoutingConfig
    they need without patching environment variables.
    """

    def __init__(
        self,
        critical_premium: str,
        high_premium: str,
        medium_premium: str,
        low_premium: str,
        any_standard: str,
        budget_exceeded_fallback: str,
        low_skip: bool,
    ) -> None:
        # Each attribute maps to one severity × tier combination.
        # Attribute names follow the pattern {severity_lower}_premium so that
        # ModelRouter.route can use getattr without an if/elif chain.
        self.critical_premium = critical_premium
        self.high_premium = high_premium
        self.medium_premium = medium_premium
        self.low_premium = low_premium
        # any_standard: applied regardless of severity for standard-tier tenants.
        self.any_standard = any_standard
        # budget_exceeded_fallback: used when daily spend >= daily budget.
        self.budget_exceeded_fallback = budget_exceeded_fallback
        # low_skip: when True, LOW severity incidents are discarded (return None).
        self.low_skip = low_skip


class ModelRouter:
    """Routes an incident to the appropriate LLM model based on business rules.
    Dependencies are injected at construction (Dependency Inversion).
    ModelRouter depends on TenantRepository as an abstraction — tests inject
    an AsyncMock without needing a real PostgreSQL connection.
    Why not read from the database inside this class?
    Single Responsibility: ModelRouter's job is to apply routing logic, not to
    manage database connections. If the query changes, only TenantRepository
    changes — not this class.
    """

    def __init__(
        self,
        tenant_repository: "TenantRepository",
        routing_config: RoutingConfig,
    ) -> None:
        # Both injected — never instantiated here.
        self._repo = tenant_repository
        self._config = routing_config

    async def route(
        self, tenant_id: str, severity: str
    ) -> RouterDecision | None:
        """Select the LLM model and prompt variant for this incident.
        Returns None when severity is LOW and low_skip=True, signalling the
        Kafka handler to discard the incident without writing to DLQ.
        Returns RouterDecision in all other cases.
        Why None instead of a sentinel RouterDecision?
        A sentinel model_id like "skip" would flow into the payload and
        potentially confuse downstream consumers. None is an unambiguous
        signal to the handler: do not publish, just commit and move on.
        Raises TenantNotFoundError if the tenant_id does not exist.
        The Kafka handler catches this and routes to DLQ.
        """
        log = logger.bind(tenant_id=tenant_id, severity=severity)

        # --- Step 1: fetch tenant row ---
        tenant = await self._repo.find_by_id(tenant_id)
        if tenant is None:
            # Tenant deleted mid-pipeline: cannot make a routing decision.
            # Import inside the method to avoid circular import at module level.
            from exceptions import TenantNotFoundError
            log.error("tenant_not_found_cannot_route")
            raise TenantNotFoundError(
                f"Tenant {tenant_id!r} not found in tenants table"
            )

        # --- Step 2: check daily spend against the tenant's configured budget ---
        daily_spend = await self._repo.get_daily_spend(tenant_id)
        daily_budget: float = tenant["token_budget_usd_daily"]

        if daily_spend >= daily_budget:
            # Budget exhausted: hard downgrade to the cheapest fallback.
            # This is a hard cap, not a soft warning — if we allowed GPT-4 here
            # the tenant could run up unbounded costs beyond their daily limit.
            log.warn(
                "daily_budget_exceeded_downgrading_model",
                daily_spend=daily_spend,
                daily_budget=daily_budget,
                fallback_model=self._config.budget_exceeded_fallback,
            )
            return RouterDecision(
                model_id=self._config.budget_exceeded_fallback,
                # Still assign a random prompt variant so A/B data is collected
                # even for budget-exceeded incidents.
                prompt_variant=random.choice(_PROMPT_VARIANTS),
                reason="budget_exceeded",
            )

        # --- Step 3: discard LOW severity when low_skip is enabled ---
        # Return None — the handler commits the offset and moves on.
        # LOW alerts are typically noise in high-volume environments; low_skip
        # lets operators tune out the LLM cost for low-value alerts.
        if severity == "LOW" and self._config.low_skip:
            log.info("low_severity_discarded_low_skip_enabled")
            return None

        # --- Step 4: route by severity × model tier ---
        model_tier: str = tenant["model_tier"]

        if model_tier == "premium":
            # Dynamic attribute lookup avoids a hardcoded if/elif for each severity.
            # Attribute pattern: "{severity_lower}_premium" — e.g. "critical_premium".
            # getattr fallback to any_standard handles unexpected severity values safely.
            attr_name = f"{severity.lower()}_premium"
            model_id = getattr(self._config, attr_name, self._config.any_standard)
        else:
            # Standard tier: always routes to the cheapest model regardless of severity.
            # Business rule: standard tenants purchase gpt-3.5-turbo pricing.
            model_id = self._config.any_standard

        decision = RouterDecision(
            model_id=model_id,
            # random.choice is intentionally unseeded — true randomness is required
            # for a statistically valid 50/50 A/B split across requests.
            prompt_variant=random.choice(_PROMPT_VARIANTS),
            reason=f"severity_{severity}_tier_{model_tier}",
        )
        log.info(
            "routing_decision_made",
            model_id=decision.model_id,
            prompt_variant=decision.prompt_variant,
            reason=decision.reason,
        )
        return decision
