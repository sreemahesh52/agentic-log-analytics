"""Factory and pipeline orchestration for the faithfulness evaluation pipeline.
Why a Factory pattern here?
Creating the three-strategy pipeline requires injecting several dependencies
(openai_client, prompt_registry, db_pool, chroma_client) into three different
strategy objects in the correct order. The factory encapsulates this wiring.
Benefits:
  - Test code creates the pipeline with one call and injects all mocks at once.
  - Service startup code creates the pipeline with one call and injects real clients.
  - The pipeline order (ground_truth → similarity → heuristic) is defined in one place.
    If a new tier is added, only the factory changes — the orchestration function
    evaluate_faithfulness needs no modification.
Why a free function (evaluate_faithfulness) rather than a class?
The pipeline logic is trivially simple: iterate, call evaluate, stop on first
non-None result. There is no state and no config to encapsulate. A free function
is the right choice when there is nothing to put in __init__. The function is
tested directly in test_evaluator.py without instantiating anything.
Why raise RuntimeError if all strategies return None?
This is a programming contract violation: HeuristicStrategy must never return
None (it is documented, tested, and enforced). If it does return None, something
is fundamentally broken in the evaluation code. RuntimeError at this point is
correct because the system is in an unexpected state — not a user error, not an
external service failure.
"""

from __future__ import annotations

import structlog

from .base import FaithfulnessResult, FaithfulnessStrategy
from .ground_truth import GroundTruthStrategy
from .heuristic import HeuristicStrategy
from .similarity import SimilarityStrategy

log = structlog.get_logger(__name__)


class EvaluatorFactory:
    """Creates the configured faithfulness evaluation pipeline.
    Static factory — no state, no instance needed. The create method returns
    a list of strategies in the order they must be tried. Returning a plain
    list (not a pipeline class) keeps the API transparent: callers can inspect,
    extend, or swap the list for tests without subclassing or monkeypatching.
    """

    @staticmethod
    def create_faithfulness_pipeline(
        openai_client: object,
        prompt_registry: object,
        db_pool: object,
        chroma_client: object,
    ) -> list[FaithfulnessStrategy]:
        """Create and return the ordered list of faithfulness strategies.
        Pipeline order (must not change without updating documentation):
          1. GroundTruthStrategy — requires human-labelled ground_truth in DB.
          2. SimilarityStrategy — requires similar past incidents in ChromaDB.
          3. HeuristicStrategy — always produces a result; final fallback.
        Args:
            openai_client: AsyncOpenAI-compatible client.
            prompt_registry: PromptRegistry instance.
            db_pool: asyncpg Pool with timezone=UTC.
            chroma_client: ChromaDB client for past_incidents collections.
        Returns:
            Ordered list of FaithfulnessStrategy instances.
        """
        # --- Build strategies in pipeline order ---
        # Each strategy depends only on what it actually needs.
        # GroundTruthStrategy: needs db_pool for alerts table lookup.
        # SimilarityStrategy: needs chroma_client for past_incidents search.
        # HeuristicStrategy: needs neither DB nor ChromaDB — only reasoning steps.
        return [
            GroundTruthStrategy(openai_client, prompt_registry, db_pool),
            SimilarityStrategy(openai_client, prompt_registry, chroma_client),
            HeuristicStrategy(openai_client, prompt_registry),
        ]


async def evaluate_faithfulness(
    strategies: list[FaithfulnessStrategy],
    rca_conclusion: str,
    context: dict,
) -> FaithfulnessResult:
    """Run the faithfulness pipeline and return the first successful result.
    Tries strategies in order. Returns immediately when one returns a non-None
    FaithfulnessResult. HeuristicStrategy is always last and must never return
    None, so this function always returns a result without looping to exhaustion.
    Why log which strategy succeeded?
    Grafana panel 11 (faithfulness score A/B) must show eval_mode breakdown.
    Operators need to know if a score came from ground_truth (most reliable)
    or heuristic (least reliable) to interpret the metric correctly.
    Args:
        strategies: ordered list from EvaluatorFactory.create_faithfulness_pipeline.
        rca_conclusion: the root_cause text produced by the RCA Agent.
        context: dict with 'alert_ids', 'tenant_id', 'reasoning_steps'.
    Returns:
        FaithfulnessResult from the first strategy that succeeds.
    Raises:
        RuntimeError: if all strategies return None (programming contract violation).
    """
    for strategy in strategies:
        # --- Try this strategy ---
        result = await strategy.evaluate(rca_conclusion, context)

        if result is not None:
            # This strategy produced a result — stop the pipeline here.
            log.info(
                "Faithfulness evaluation complete",
                strategy=strategy.strategy_name(),
                eval_mode=result.eval_mode,
                score=result.score,
            )
            return result

        # Log that this strategy could not evaluate — useful for debugging
        # low ground_truth labelling rates in Grafana.
        log.debug(
            "Strategy returned None — trying next",
            strategy=strategy.strategy_name(),
        )

    # --- This point must never be reached ---
    # HeuristicStrategy is always last and always returns a FaithfulnessResult.
    # If we get here, something is fundamentally broken in the code.
    raise RuntimeError(
        "Evaluation pipeline exhausted without a result. "
        "HeuristicStrategy must never return None — this is a programming error."
    )
