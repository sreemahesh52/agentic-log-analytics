"""Base types for the Strategy pattern used in faithfulness evaluation.
Why a Strategy pattern here?
Three evaluation tiers exist (ground_truth, similarity, heuristic) and the
correct one to use is determined at runtime by what context is available.
The pipeline tries each strategy in order and stops at the first non-None result.
Adding a new evaluation tier requires creating a new FaithfulnessStrategy
subclass — no changes to the pipeline orchestrator, factory, or any existing
strategy. That is the Open/Closed principle applied.
Why is FaithfulnessStrategy an ABC?
ABCs in Python use the same mechanism as interfaces in Java/Go. If a subclass
does not implement evaluate or strategy_name, Python raises TypeError at
class definition time — not at the first call to evaluate, which might happen
hours into a production run. Catching missing implementations at import time is
cheaper than catching them during an investigation.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass


# --- Shared result type ---
# dataclass (not Pydantic) because FaithfulnessResult is an internal domain
# object that does not cross a JSON serialisation boundary at this layer.
# The Kafka handler converts it to an EvalResult (models.py) before persisting.
@dataclass
class FaithfulnessResult:
    """Immutable result returned by any faithfulness evaluation strategy.
    score: float 0.0–1.0. Higher is better — 1.0 means RCA perfectly
               matches the known or inferred ground truth.
    reasoning: one-sentence explanation from the LLM evaluator. Stored in
               Prometheus labels for Grafana drill-down.
    eval_mode: which strategy produced this result. MUST be stored on every
               eval_results row — Grafana must never average across modes
               because ground_truth scores are not comparable to heuristic scores.
    eval_mode valid values: 'ground_truth' | 'similarity' | 'heuristic'
    """

    score: float
    reasoning: str
    # eval_mode valid values: 'ground_truth' | 'similarity' | 'heuristic'
    eval_mode: str


# --- Strategy interface ---
class FaithfulnessStrategy(ABC):
    """Abstract base for all faithfulness evaluation strategies.
    Interface Segregation: each strategy exposes only two methods.
    A strategy that also writes to PostgreSQL would violate Single Responsibility.
    The None return contract:
    evaluate returning None means "I cannot evaluate this case."
    The pipeline interprets None as "try the next strategy."
    HeuristicStrategy is the final fallback and NEVER returns None —
    that contract is documented on the class and enforced by a RuntimeError
    in evaluate_faithfulness if all strategies somehow return None.
    Liskov Substitution: every concrete subclass must be usable wherever
    FaithfulnessStrategy is expected. That means evaluate either returns
    a FaithfulnessResult or None — never raises, never returns other types.
    """

    @abstractmethod
    async def evaluate(
        self, rca_conclusion: str, context: dict
    ) -> "FaithfulnessResult | None":
        """Evaluate faithfulness of an RCA conclusion against available context.
        Implementations must:
          - Return FaithfulnessResult if evaluation is possible.
          - Return None if evaluation is not possible (triggers next strategy).
          - Never raise — catch all errors internally and return None.
            Exception: HeuristicStrategy must never return None.
        Args:
            rca_conclusion: the root_cause text produced by the RCA Agent.
            context: dict containing any of:
              - 'alert_ids': list[str] — for ground_truth DB lookup
              - 'tenant_id': str — for similarity ChromaDB search
              - 'reasoning_steps': list[dict] — for heuristic observation scan
        """
        ...

    @abstractmethod
    def strategy_name(self) -> str:
        """Return a stable lowercase string identifier for this strategy.
        Used in structlog entries and Prometheus metric labels.
        Changing this value is a breaking change for dashboards.
        Examples: 'ground_truth', 'similarity', 'heuristic'
        """
        ...
