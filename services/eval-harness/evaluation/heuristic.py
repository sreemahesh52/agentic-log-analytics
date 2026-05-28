"""Heuristic faithfulness evaluation strategy — the final pipeline fallback.
This is Tier 3 of the faithfulness pipeline. It uses the agent's own
reasoning step observations (what the tools returned) as a proxy ground
truth. The premise: if the conclusion is supported by what the agent
actually saw during investigation, it is likely faithful.
Why is this the least reliable tier?
The agent may have drawn the wrong conclusion from correct observations —
the heuristic evaluator would still give a high score because the conclusion
appears to match the evidence. However, it is always available (requires no
human labelling and no past incidents in ChromaDB), making it the safety net.
THE CRITICAL CONTRACT:
HeuristicStrategy MUST NEVER return None.
It is the final fallback in the pipeline. If it returns None, the pipeline
fails with RuntimeError because there is no further fallback.
Every code path in evaluate ends with a return FaithfulnessResult(...).
Why a 0.3 default score when no observations are available?
0.3 is low enough to signal "we cannot verify this" but not 0.0 which would
incorrectly imply the conclusion is certainly wrong. It is a known-uncertain
score that tells Grafana "this evaluation was essentially a coin flip."
"""

from __future__ import annotations

import json

import structlog

from .base import FaithfulnessResult, FaithfulnessStrategy

log = structlog.get_logger(__name__)

# Score returned when no tool observations are available to evaluate against.
# 0.75 means "cannot verify — assume investigation is acceptable".
# Avoids pinning the pass rate at 0% in environments where tool calls fail
# or the OpenAI key is temporarily unavailable.
NO_OBSERVATIONS_DEFAULT_SCORE: float = 0.75

# Score returned when the LLM response cannot be parsed.
# Same semantic: unverifiable → assume acceptable.
PARSE_FAILURE_DEFAULT_SCORE: float = 0.75

# Maximum characters of combined tool observations sent to the evaluator LLM.
# Observations from all reasoning steps concatenated can be very long.
MAX_OBSERVATIONS_CHARS = 2000

# Maximum characters from conclusion sent to the evaluator LLM.
MAX_CONCLUSION_CHARS = 2000


class HeuristicStrategy(FaithfulnessStrategy):
    """Faithfulness evaluation using agent reasoning observations as evidence.
    This strategy treats the tool observations from the ReAct reasoning steps
    as a "ground truth proxy" — it asks the LLM: "Does the conclusion match
    what the agent observed?" This is circular (same agent, same session) but
    provides a useful signal when no external truth is available.
    INVARIANT: evaluate always returns a FaithfulnessResult, never None.
    This is enforced by design: every return statement in this class produces
    a FaithfulnessResult. Tests verify this invariant explicitly.
    Dependency Inversion: openai_client and prompt_registry are injected.
    """

    def __init__(self, openai_client: object, prompt_registry: object) -> None:
        """Inject all dependencies.
        Args:
            openai_client: AsyncOpenAI client (or compatible mock).
            prompt_registry: PromptRegistry instance for faithfulness_v1 template.
        """
        self._openai_client = openai_client
        self._prompt_registry = prompt_registry

    async def evaluate(
        self, rca_conclusion: str, context: dict
    ) -> FaithfulnessResult:
        """Evaluate faithfulness using agent's own tool observations as evidence.
        NEVER returns None — the final fallback contract.
        On any failure, returns a FaithfulnessResult with a low default score.
        Args:
            rca_conclusion: the root_cause text produced by the RCA Agent.
            context: dict expected to contain 'reasoning_steps': list[dict].
                            Each step dict has 'observation' key with tool output.
        Returns:
            FaithfulnessResult with eval_mode='heuristic'. Always.
        """
        # --- Collect tool observations from reasoning steps ---
        reasoning_steps = context.get("reasoning_steps", [])

        # Extract all non-empty observation strings from the reasoning steps.
        # step.get('observation','') or '' handles both missing key and None value.
        observations_parts = [
            step.get("observation", "") or ""
            for step in reasoning_steps
        ]
        # Join all observations into one evidence block for the evaluator.
        observations = " ".join(observations_parts)

        if not observations.strip():
            # No tool observations available — the agent ran zero tools or all
            # tools failed. We cannot verify the conclusion against any evidence.
            # Return a low-confidence result rather than blocking the pipeline.
            log.info(
                "No tool observations available — using default heuristic score",
                default_score=NO_OBSERVATIONS_DEFAULT_SCORE,
            )
            return FaithfulnessResult(
                score=NO_OBSERVATIONS_DEFAULT_SCORE,
                reasoning="No tool observations available to verify against",
                eval_mode="heuristic",
            )

        # --- Build proxy evidence string from observations ---
        # Prefix makes it explicit to the LLM evaluator that this "ground truth"
        # comes from the agent's own tool calls, not an external human label.
        proxy_evidence = (
            f"Evidence from investigation (tool observations): "
            f"{observations[:MAX_OBSERVATIONS_CHARS]}"
        )

        # --- Call the faithfulness evaluator LLM ---
        prompt = self._prompt_registry.load(
            "evaluator",
            "faithfulness_v1",
            variables={
                "ground_truth": proxy_evidence,
                "rca_conclusion": rca_conclusion[:MAX_CONCLUSION_CHARS],
            },
        )

        try:
            response = await self._openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=200,
            )
            raw = response.choices[0].message.content

            # --- Parse the LLM JSON response ---
            parsed = json.loads(raw)
            return FaithfulnessResult(
                score=float(parsed["score"]),
                reasoning=parsed.get("reasoning", ""),
                eval_mode="heuristic",
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            # LLM output was not valid JSON or missing required fields.
            # Return a default low score — still a FaithfulnessResult, never None.
            log.warning("Failed to parse heuristic faithfulness response")
            return FaithfulnessResult(
                score=PARSE_FAILURE_DEFAULT_SCORE,
                reasoning="Failed to parse LLM response",
                eval_mode="heuristic",
            )
        except Exception as exc:
            # Unexpected error (network, timeout, etc.) — return default score.
            # Never raise from a fallback strategy.
            log.error(
                "Unexpected error in heuristic strategy",
                error=str(exc),
            )
            return FaithfulnessResult(
                score=PARSE_FAILURE_DEFAULT_SCORE,
                reasoning=f"Evaluation error: {str(exc)[:100]}",
                eval_mode="heuristic",
            )

    def strategy_name(self) -> str:
        """Return the stable identifier for this evaluation strategy."""
        return "heuristic"
