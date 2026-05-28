"""Data models and pricing logic for the evaluation harness.
This module defines:
  - MODEL_PRICING: per-1K-token costs for each supported OpenAI model.
  - calculate_cost: converts token counts to USD based on model pricing.
  - EvalResult: the complete evaluation record persisted to eval_results table.
Why keep pricing in code rather than a database table?
Model pricing changes infrequently (a few times per year). Storing it in code
keeps cost calculations deterministic and version-controlled alongside the
service that uses them. A database table would require a migration for every
pricing update — overkill for data that changes rarely and has no user input.
Why separate EvalResult from RCAResult (services/rca-agent/models.py)?
RCAResult records the investigation itself. EvalResult records the quality
assessment of that investigation. Keeping them separate enforces Single
Responsibility: the RCA Agent owns investigation models, the Eval Harness
owns evaluation models. Neither module needs to import from the other.
Why dataclass instead of Pydantic for EvalResult?
EvalResult is populated internally by the eval harness — it is not parsed
from JSON or user input. Pydantic's validation overhead is justified at system
boundaries (Kafka messages, HTTP bodies) but not for internal data flow.
The compute_passed method requires mutation, which dataclasses support
cleanly via a method with no return value.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import uuid4


# ---------------------------------------------------------------------------
# Model pricing constants
# ---------------------------------------------------------------------------

# Per-1K-token pricing in USD for each supported OpenAI model.
# input_per_1k: cost per 1000 prompt (input) tokens
# output_per_1k: cost per 1000 completion (output) tokens
# Source: OpenAI pricing page (updated for gpt-4-turbo and gpt-3.5-turbo).
# When new models are added, add an entry here — no code changes elsewhere.
MODEL_PRICING: dict[str, dict[str, float]] = {
    "gpt-4-turbo": {
        "input": 0.01,   # $0.01 per 1K input tokens
        "output": 0.03,  # $0.03 per 1K output tokens
    },
    "gpt-4": {
        "input": 0.01,
        "output": 0.03,
    },
    "gpt-3.5-turbo": {
        "input": 0.001,  # $0.001 per 1K input tokens
        "output": 0.002, # $0.002 per 1K output tokens
    },
}

# Fallback pricing used when a model name is not in MODEL_PRICING.
# Defaults to gpt-3.5-turbo rates — conservative underestimate rather than
# an overestimate, preventing false budget-exceeded alerts.
_DEFAULT_PRICING_KEY = "gpt-3.5-turbo"


def calculate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Calculate USD cost of one LLM call based on model and token counts.
    Uses per-1K-token pricing from MODEL_PRICING. Unknown models fall back
    to gpt-3.5-turbo pricing — intentionally conservative.
    Args:
        model: OpenAI model name, e.g. 'gpt-4-turbo' or 'gpt-3.5-turbo'.
        input_tokens: number of prompt tokens billed by OpenAI.
        output_tokens: number of completion tokens billed by OpenAI.
    Returns:
        Total cost in USD as a float.
    Examples:
        >>> calculate_cost('gpt-4-turbo', 1000, 500)
        0.025 # (1000 * 0.01 + 500 * 0.03) / 1000
        >>> calculate_cost('gpt-3.5-turbo', 1000, 500)
        0.002 # (1000 * 0.001 + 500 * 0.002) / 1000
    """
    # dict.get with a fallback key ensures unknown models never raise KeyError.
    pricing = MODEL_PRICING.get(model, MODEL_PRICING[_DEFAULT_PRICING_KEY])

    # Divide by 1000: pricing dict stores cost per 1K tokens, tokens are raw counts.
    return (
        input_tokens * pricing["input"] + output_tokens * pricing["output"]
    ) / 1000.0


# ---------------------------------------------------------------------------
# EvalResult — the complete evaluation record for one RCA investigation.
# ---------------------------------------------------------------------------


@dataclass
class EvalResult:
    """One row in the eval_results PostgreSQL table.
    Populated by the eval harness after running faithfulness and hallucination
    evaluations on an RCA result from agent.results Kafka topic.
    Field mapping to eval_results columns (see infra/postgres/init.sql):
      eval_id — uuid PRIMARY KEY
      tenant_id — uuid FK tenants
      rca_id — uuid FK rca_results
      evaluated_at — TIMESTAMPTZ NOT NULL DEFAULT NOW
      prompt_version — TEXT
      eval_mode — TEXT ('ground_truth' | 'heuristic' | 'similarity')
      faithfulness_score — FLOAT
      hallucination_score — FLOAT
      cost_usd — FLOAT
      total_latency_ms — INTEGER
      llm_latency_ms — INTEGER
      tool_latency_ms — INTEGER
      cache_latency_ms — INTEGER
      compression_latency_ms — INTEGER
      passed — BOOLEAN
    """

    # uuid4 generates a unique UUID for this evaluation row.
    # Generating in Python ensures the Kafka message and DB row share the same ID.
    eval_id: str = field(default_factory=lambda: str(uuid4()))

    tenant_id: str = ""
    rca_id: str = ""

    evaluated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    prompt_version: str = ""

    # eval_mode: which faithfulness strategy produced the faithfulness_score.
    # Valid values: 'ground_truth' | 'similarity' | 'heuristic'
    # MUST be stored on every row — Grafana averages must never mix modes.
    eval_mode: str = ""

    # faithfulness_score: 0.0–1.0. Higher is better (matches ground truth).
    faithfulness_score: float = 0.0

    # hallucination_score: 0.0–1.0. Higher is better (1.0 = no hallucination).
    # Note: higher hallucination_score means LESS hallucination — this naming
    # convention matches the evaluation prompt's scoring description.
    hallucination_score: float = 0.0

    # cost_usd: USD cost of this RCA investigation, computed by calculate_cost.
    cost_usd: float = 0.0

    # Latency fields mirror rca_results for cross-table joins in Grafana.
    total_latency_ms: int = 0
    llm_latency_ms: int = 0
    tool_latency_ms: int = 0
    cache_latency_ms: int = 0
    compression_latency_ms: int = 0

    # passed: True only if BOTH faithfulness_score > 0.7 AND hallucination_score > 0.7.
    # Set by calling compute_passed after scores are populated.
    # A result that passes faithfulness but fails hallucination is not reliable
    # enough to auto-add to the knowledge base via the self-learning indexer.
    passed: bool = False

    def compute_passed(self) -> None:
        """Set passed=True if both faithfulness and hallucination exceed 0.7.
        The 0.7 threshold comes from PROJECT-SPEC.md:
          "Self-Learning Indexer: only on eval_mode=ground_truth, faithfulness>0.8,
          hallucination>0.7, AUTO_LEARN=true"
          "On CRITICAL + faithfulness>0.7: triggers Slack Notifier"
        Using > 0.7 (not >= 0.7) follows the spec's notation precisely.
        """
        # Both conditions must be true: an investigation that correctly identified
        # the root cause but hallucinated supporting evidence is not trustworthy.
        self.passed = (
            self.faithfulness_score > 0.7
            and self.hallucination_score > 0.7
        )
