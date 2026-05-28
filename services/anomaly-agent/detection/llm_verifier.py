"""LLM-based false-positive filter for the anomaly detection pipeline.
Position in the three-stage pipeline:
  Stage 1 (Statistical): Z-score on error rate / volume — fast, no API calls.
  Stage 2 (Semantic): Embedding similarity — one OpenAI call per ERROR/FATAL.
  Stage 3 (LLM Verifier): GPT-3.5 YES/NO classification — only fires when
                          Stage 1 or 2 detected something.
Why the LLM verifier runs LAST (not first):
  Statistical and semantic detection are cheap. Running them first means the
  expensive LLM call only happens when an actual anomaly candidate exists —
  which is rare compared to the total volume of logs. If we ran the LLM on
  every log, token costs would be ~1000x higher with no quality benefit.
Why GPT-3.5 instead of GPT-4 for this step:
  YES/NO binary classification requires minimal reasoning. GPT-3.5 achieves
  near-identical accuracy to GPT-4 on classification tasks while costing 10x
  less and responding ~3x faster. GPT-4 is reserved for the RCA agent where
  complex multi-step reasoning matters.
Fail-open design (return True on API error):
  If OpenAI is down, we cannot verify anomalies — but we also cannot miss
  real incidents. Returning True (assume real) means alerts keep flowing.
  The cost: a higher false-positive rate during an OpenAI outage.
  The alternative (return False = assume noise): would silently suppress real
  alerts during an outage, causing missed incidents. That is worse.
"""

import structlog
from openai import APIError, RateLimitError

logger = structlog.get_logger(__name__)

# GPT-3.5-turbo: cheapest model capable of binary classification with high accuracy.
# Changing this to gpt-4 is a cost multiplier of ~10x for no quality gain on YES/NO.
_VERIFIER_MODEL = "gpt-3.5-turbo"

# temperature=0.0: deterministic output. Binary classification should never be
# random — the same input must always produce the same YES or NO answer.
_TEMPERATURE = 0.0

# max_tokens=10: YES or NO plus optional punctuation. Limiting tokens:
#   1. Prevents the model from generating explanations (we only want the decision).
#   2. Reduces latency by stopping generation immediately after the answer.
#   3. Reduces token cost — output tokens cost 2x input tokens for GPT-3.5.
_MAX_TOKENS = 10

# System prompt anchors the model's role — sent once per API call.
# "Answer only YES or NO" is critical: without it, GPT-3.5 tends to add
# explanations ("YES, this is a real anomaly because...") which break our
# startswith("YES") parsing logic.
_SYSTEM_PROMPT = "You are an anomaly detection expert. Answer only YES or NO."


class LLMVerifier:
    """Uses GPT-3.5-turbo to classify detected anomaly candidates as real or noise.
    Single Responsibility: this class only calls the LLM and interprets YES/NO.
    It does not detect anomalies (that is StatisticalDetector and SemanticDetector),
    does not publish alerts (that is AlertPublisher), and does not fetch logs
    (that is LogRepository). Each concern has its own class.
    Dependency Inversion: openai_client and prompt_registry are injected via
    __init__, never instantiated here. Tests can inject a Mock OpenAI client
    that returns a fixture response without any network call.
    """

    def __init__(self, openai_client: object, prompt_registry: object) -> None:
        """Accept an OpenAI client and a PromptRegistry — both injected, never created here.
        Args:
            openai_client: OpenAI client instance (real or mock).
                             The caller owns the client lifecycle.
            prompt_registry: PromptRegistry instance for loading the verifier prompt.
                             Injected so the prompt path is configurable, not hardcoded.
        """
        # Store injected dependencies — this class creates no external connections
        self._openai = openai_client
        self._registry = prompt_registry

    def verify(
        self,
        tenant_id: str,
        service: str,
        sample_logs: list[str],
        anomaly_description: str,
    ) -> bool:
        """Call GPT-3.5-turbo to determine if the anomaly candidate is real.
        Fail-open: returns True (assume real anomaly) on any OpenAI error.
        See module docstring for the reasoning behind this design choice.
        Args:
            tenant_id: tenant namespace — used only for logging context.
            service: service name injected into the prompt.
            sample_logs: recent ERROR/FATAL log messages from this service.
                                 The last 10 are used; older entries are trimmed.
            anomaly_description: human-readable summary of what the detectors found
                                 (e.g., "Error rate spike: Z-score=4.2").
        Returns:
            True — anomaly is real; continue to alert publishing.
            False — anomaly is noise; suppress the alert (increment false_positive counter).
        """
        log = logger.bind(tenant_id=tenant_id, service=service)

        # --- Load and render the versioned prompt ---
        # prompt_registry.load substitutes {service}, {sample_logs}, {anomaly_description}
        # into the prompt template from prompts/anomaly_verifier/v1.txt.
        # Using the registry (not a raw string) means the prompt is versionable and
        # auditable — every LLM call can be traced back to the prompt version that drove it.
        try:
            # sample_logs[-10:] takes the 10 most recent logs — older context is less relevant.
            # '\n'.join formats them as one log per line for readability in the LLM prompt.
            prompt = self._registry.load(
                "anomaly_verifier",
                "v1",
                {
                    "service": service,
                    "sample_logs": "\n".join(sample_logs[-10:]),
                    "anomaly_description": anomaly_description,
                },
            )
        except (FileNotFoundError, KeyError) as exc:
            # Prompt loading failure is a configuration error, not a transient one.
            # Log at ERROR and fail open — the anomaly is real until proved otherwise.
            log.error(
                "llm_verifier_prompt_load_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return True

        # --- Call the LLM ---
        try:
            response = self._openai.chat.completions.create(
                model=_VERIFIER_MODEL,
                # temperature=0.0: same input must always produce same output.
                # Non-zero temperature introduces randomness — unacceptable for binary classification.
                temperature=_TEMPERATURE,
                # max_tokens=10: cuts off any explanation after YES or NO.
                # Without this limit, GPT-3.5 appends reasons which break parsing.
                max_tokens=_MAX_TOKENS,
                messages=[
                    {
                        "role": "system",
                        # System prompt anchors the model's role before seeing the user message.
                        "content": _SYSTEM_PROMPT,
                    },
                    {
                        "role": "user",
                        # User message contains the rendered prompt with context injected.
                        "content": prompt,
                    },
                ],
            )
        except RateLimitError as exc:
            # Rate limit: transient, expected under high API load. Log WARN, fail open.
            # "fail open" = return True = treat as real anomaly = keep alerting.
            log.warning(
                "llm_verifier_rate_limited",
                error=str(exc),
            )
            return True
        except APIError as exc:
            # Non-rate-limit API error (auth, quota, server error). Log WARN, fail open.
            # Same reasoning: we cannot silence alerts during an API outage.
            log.warning(
                "llm_verifier_api_error",
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return True
        except Exception as exc:
            # Unexpected error (network, serialisation, etc.). Log WARN, fail open.
            log.warning(
                "llm_verifier_unexpected_error",
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return True

        # --- Parse the YES/NO decision ---
        # .strip removes leading/trailing whitespace from the model's response.
        # .upper normalises case — GPT-3.5 sometimes returns "yes" instead of "YES".
        answer = response.choices[0].message.content.strip().upper()
        is_real = answer.startswith("YES")

        log.info(
            "llm_verifier_result",
            answer=answer[:10],  # first 10 chars — avoids logging long unexpected responses
            is_real=is_real,
            anomaly_description=anomaly_description[:80],
        )
        return is_real
