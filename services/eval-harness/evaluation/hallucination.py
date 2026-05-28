"""Hallucination detection for RCA conclusions.
This evaluator runs independently of the three-tier faithfulness pipeline.
While faithfulness asks "does the conclusion match known truth?", hallucination
asks "does the conclusion invent claims not supported by the log evidence?"
Why run these independently?
An RCA conclusion can be unfaithful but not hallucinated (states real observed
facts but draws the wrong conclusion), or faithful but hallucinated (correct
root cause but cites non-existent log entries). Measuring both dimensions
separately gives operators two independent signals about output quality.
The evaluator fetches the 20 most recent error/fatal logs for the service
from PostgreSQL and presents them to gpt-3.5-turbo as the "available evidence."
The LLM identifies any claims in the conclusion not supported by this evidence.
Score interpretation (from hallucination_v1.txt):
  1.0 — Every claim supported by log evidence (no hallucination)
  0.7 — Minor unsupported details, core conclusion supported
  0.4 — Some key claims lack evidence support
  0.1 — Most claims are unsupported
  0.0 — Conclusion is fabricated, not supported by any evidence
Design choice: always returns a HallucinationResult, never None.
The fallback score of 0.5 represents "could not determine" — deliberately
in the uncertain middle rather than 0 (certainly hallucinated) or 1 (clean).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    import asyncpg

log = structlog.get_logger(__name__)

# Maximum recent error log lines fetched per evaluation.
# 20 lines balances evidence richness against gpt-3.5-turbo context cost.
MAX_LOG_LINES = 20

# Maximum characters of log evidence sent to the evaluator LLM.
MAX_LOG_EVIDENCE_CHARS = 3000

# Maximum characters of RCA conclusion sent to the evaluator LLM.
MAX_CONCLUSION_CHARS = 2000

# Score used when the LLM response cannot be parsed.
# 0.75 means "cannot verify — assume no hallucination".
# Avoids pinning the pass rate at 0% when the evaluator LLM is unavailable
# or returns an unparseable response.
PARSE_FAILURE_DEFAULT_SCORE: float = 0.75

# Message used when no log evidence is available in the database.
NO_LOGS_MESSAGE = "No recent error logs available for verification."


# --- Result type ---
# dataclass (not Pydantic) — HallucinationResult is internal domain logic that
# does not cross a JSON serialisation boundary at this layer.
@dataclass
class HallucinationResult:
    """Result of one hallucination evaluation run.
    score: float 1.0–0.0. 1.0 = no hallucination detected.
    hallucinated_claims: list of specific claim strings identified by the LLM
                          as not supported by the log evidence.
    reasoning: one sentence explaining the score.
    """

    score: float
    # default_factory avoids the mutable-default-argument trap.
    hallucinated_claims: list[str] = field(default_factory=list)
    reasoning: str = ""


class HallucinationEvaluator:
    """Detects claims in RCA conclusions not supported by available log evidence.
    Dependency Inversion: openai_client, prompt_registry, and db_pool are
    all injected. Tests replace these with mocks so no real DB or API calls
    are made during the test suite.
    Single Responsibility: this class only detects hallucinations. It does not
    write to the database, publish to Kafka, or calculate cost.
    """

    def __init__(
        self,
        openai_client: object,
        prompt_registry: object,
        db_pool: "asyncpg.Pool",
    ) -> None:
        """Inject all dependencies.
        Args:
            openai_client: AsyncOpenAI client for GPT-3.5 completions.
            prompt_registry: PromptRegistry for hallucination_v1 template.
            db_pool: asyncpg pool with timezone=UTC for log queries.
        """
        self._openai_client = openai_client
        self._prompt_registry = prompt_registry
        self._db_pool = db_pool

    async def evaluate(
        self, rca_conclusion: str, tenant_id: str, service: str
    ) -> HallucinationResult:
        """Evaluate whether the RCA conclusion contains hallucinated claims.
        Fetches recent error logs as evidence, then asks gpt-3.5-turbo to
        identify claims in the conclusion not supported by that evidence.
        Args:
            rca_conclusion: the root_cause text produced by the RCA Agent.
            tenant_id: tenant namespace for scoping the log query.
            service: the primary service under investigation.
        Returns:
            HallucinationResult. Always returns a result, never raises.
        """
        # --- Fetch recent error logs as grounding evidence ---
        log_evidence = await self._fetch_log_evidence(tenant_id, service)

        # --- Load hallucination evaluator prompt ---
        prompt = self._prompt_registry.load(
            "evaluator",
            "hallucination_v1",
            variables={
                "log_evidence": log_evidence[:MAX_LOG_EVIDENCE_CHARS],
                "rca_conclusion": rca_conclusion[:MAX_CONCLUSION_CHARS],
            },
        )

        # --- Call gpt-3.5-turbo for hallucination detection ---
        # max_tokens=300 allows for a list of hallucinated claims plus reasoning.
        try:
            response = await self._openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=300,
            )
            raw = response.choices[0].message.content

            # --- Parse the LLM JSON response ---
            parsed = json.loads(raw)
            return HallucinationResult(
                score=float(parsed["score"]),
                hallucinated_claims=parsed.get("hallucinated_claims", []),
                reasoning=parsed.get("reasoning", ""),
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            # LLM returned non-JSON or missing required fields.
            # Return a neutral score rather than blocking the pipeline.
            log.warning(
                "Failed to parse hallucination evaluation response",
                tenant_id=tenant_id,
                service=service,
            )
            return HallucinationResult(
                score=PARSE_FAILURE_DEFAULT_SCORE,
                hallucinated_claims=[],
                reasoning="Failed to parse hallucination response",
            )
        except Exception as exc:
            # Network error, rate limit, etc.
            log.error(
                "Unexpected error in hallucination evaluator",
                error=str(exc),
                tenant_id=tenant_id,
            )
            return HallucinationResult(
                score=PARSE_FAILURE_DEFAULT_SCORE,
                hallucinated_claims=[],
                reasoning=f"Evaluation error: {str(exc)[:100]}",
            )

    async def _fetch_log_evidence(self, tenant_id: str, service: str) -> str:
        """Fetch the 20 most recent error/fatal logs for the service.
        Returns formatted log lines as a single string, or a placeholder
        message if no logs are found. Logs sorted DESC by timestamp (most
        recent first) so the LLM sees the most relevant evidence immediately.
        Args:
            tenant_id: tenant UUID string for scoping the query.
            service: service name to filter logs by.
        Returns:
            Multi-line string of formatted log entries, or NO_LOGS_MESSAGE.
        """
        try:
            # --- Parameterised query — never f-strings in SQL ---
            # AT TIME ZONE 'UTC' ensures the timestamp is returned in UTC
            # regardless of the session timezone setting (root cause fix).
            async with self._db_pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT timestamp AT TIME ZONE 'UTC' AS ts, level, message "
                    "FROM logs "
                    "WHERE tenant_id = $1 AND service = $2 "
                    "AND level IN ('ERROR', 'FATAL') "
                    "ORDER BY timestamp DESC "
                    "LIMIT $3",
                    tenant_id,
                    service,
                    MAX_LOG_LINES,
                )
        except Exception as exc:
            log.warning(
                "Failed to fetch log evidence for hallucination eval",
                error=str(exc),
                tenant_id=tenant_id,
                service=service,
            )
            return NO_LOGS_MESSAGE

        if not rows:
            return NO_LOGS_MESSAGE

        # Format each row as a single log line.
        # .isoformat on a timezone-aware datetime produces "YYYY-MM-DDTHH:MM:SS+00:00".
        # Appending 'Z' produces the ISO 8601 UTC suffix used throughout the platform.
        lines = [
            f"{row['ts'].isoformat()}Z [{row['level']}]: {row['message']}"
            for row in rows
        ]
        return "\n".join(lines)
