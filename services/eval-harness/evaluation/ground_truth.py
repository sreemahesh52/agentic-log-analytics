"""Ground truth faithfulness evaluation strategy.
This is Tier 1 of the faithfulness pipeline — the most reliable tier.
It compares the RCA conclusion against a human-labelled ground_truth value
set via the PATCH /api/v1/alerts/{id}/label gateway endpoint.
Why is this the most reliable tier?
A human operator who investigated the incident and knows the real root cause
provides the ground_truth. Comparing against a human label is more reliable
than using a similar past incident (Tier 2) or the agent's own observations
(Tier 3). The downside: most incidents are never labelled, so this tier
returns None most of the time and the pipeline falls through to Tier 2.
Returns None when:
  - context has no alert_ids
  - No alert in the DB has a non-null ground_truth for those alert_ids
  - The LLM returns an unparseable response
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import structlog

from .base import FaithfulnessResult, FaithfulnessStrategy

if TYPE_CHECKING:
    import asyncpg

# Module-level structlog logger. Every log entry in this module gets the
# module name bound automatically, matching the service-wide JSON format.
log = structlog.get_logger(__name__)

# Maximum number of characters sent as rca_conclusion to the LLM.
# Keeps the faithfulness prompt within the gpt-3.5-turbo context window.
MAX_CONCLUSION_CHARS = 2000

# Maximum characters from ground_truth text sent to the evaluator LLM.
MAX_GROUND_TRUTH_CHARS = 1000


class GroundTruthStrategy(FaithfulnessStrategy):
    """Faithfulness evaluation against a human-labelled ground truth.
    Dependency Inversion: openai_client, prompt_registry, and db_pool are
    injected via __init__, not instantiated here. This class never knows
    which OpenAI model is configured or how the DB pool was created — it only
    calls the injected interfaces. Tests inject mocks without touching the real
    OpenAI API or PostgreSQL.
    """

    def __init__(
        self,
        openai_client: object,
        prompt_registry: object,
        db_pool: "asyncpg.Pool",
    ) -> None:
        """Inject all dependencies — never instantiate them internally.
        Args:
            openai_client: AsyncOpenAI client (or compatible mock).
            prompt_registry: PromptRegistry instance for template loading.
            db_pool: asyncpg connection pool with timezone=UTC.
        """
        self._openai_client = openai_client
        self._prompt_registry = prompt_registry
        self._db_pool = db_pool

    async def evaluate(
        self, rca_conclusion: str, context: dict
    ) -> FaithfulnessResult | None:
        """Compare RCA conclusion against human-labelled ground truth.
        Returns None (triggering the next strategy) when:
          - alert_ids is missing or empty in context
          - No matching alert has a ground_truth value set
          - The LLM evaluator returns unparseable JSON
        Returns FaithfulnessResult with eval_mode='ground_truth' on success.
        """
        # --- Guard: need alert_ids to look up ground truth ---
        alert_ids = context.get("alert_ids", [])
        if not alert_ids:
            # No alert IDs in context — cannot look up ground truth.
            # Return None to let the pipeline try the next strategy.
            return None

        ground_truth = await self._fetch_ground_truth(alert_ids)
        if ground_truth is None:
            # No ground truth has been manually labelled for these alerts yet.
            # This is the common case in production — falls through to Tier 2.
            return None

        # --- Build and call the faithfulness evaluator prompt ---
        # Truncate both inputs to avoid exceeding the gpt-3.5-turbo context limit.
        prompt = self._prompt_registry.load(
            "evaluator",
            "faithfulness_v1",
            variables={
                "ground_truth": ground_truth[:MAX_GROUND_TRUTH_CHARS],
                "rca_conclusion": rca_conclusion[:MAX_CONCLUSION_CHARS],
            },
        )

        # gpt-3.5-turbo: cheap and fast for yes/no scoring tasks.
        # temperature=0.0: eliminates randomness — same input always produces
        # the same score, making A/B comparisons statistically valid.
        # max_tokens=200: the faithfulness_v1 prompt returns compact JSON.
        response = await self._openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=200,
        )
        raw = response.choices[0].message.content

        # --- Parse the LLM JSON response ---
        try:
            parsed = json.loads(raw)
            return FaithfulnessResult(
                score=float(parsed["score"]),
                reasoning=parsed.get("reasoning", ""),
                eval_mode="ground_truth",
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            # LLM output was not valid JSON or missing required fields.
            # Log the raw output for debugging but do not raise — return None
            # to let the pipeline fall through to the similarity strategy.
            log.warning(
                "Failed to parse ground_truth faithfulness response",
                raw_response=raw[:300],
            )
            return None

    async def _fetch_ground_truth(self, alert_ids: list[str]) -> str | None:
        """Query alerts table for a human-labelled ground_truth value.
        Fetches the first non-null ground_truth from any of the given alert IDs.
        Returns None if none of the alerts have been labelled yet.
        Args:
            alert_ids: list of alert UUIDs as strings.
        Returns:
            ground_truth text string, or None if not found.
        """
        # --- Parameterised query — never f-strings in SQL ---
        # ANY($1::uuid[]) casts the Python list to a PostgreSQL UUID array.
        # This is safer and faster than a dynamic IN clause with N parameters.
        async with self._db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT ground_truth FROM alerts "
                "WHERE alert_id = ANY($1::uuid[]) "
                "AND ground_truth IS NOT NULL "
                "LIMIT 1",
                alert_ids,
            )
        # row is None if no matching row found; dict-like if found.
        return row["ground_truth"] if row else None

    def strategy_name(self) -> str:
        """Return the stable identifier for this evaluation strategy."""
        return "ground_truth"
