# --- Context Compressor — core domain logic ---
# Single Responsibility: this module only decides whether to compress and
# executes the compression. It does not fetch logs (repository) or publish
# to Kafka (handler). Those responsibilities live in their own modules.
# Dependency Inversion: ContextCompressor accepts injected openai_client and
# prompt_registry — it never creates them. This makes the class testable
# without a real OpenAI API key (mock client injected in tests).
# Fail-open principle:
#   On OpenAI error, we return the original uncompressed text rather than
#   raising an exception. An oversized context is better than an empty one:
#   the RCA agent can still reason over too much text, but cannot reason
#   over nothing. This is a deliberate trade-off favouring correctness over cost.

from dataclasses import dataclass
from datetime import datetime

import structlog
import tiktoken

logger = structlog.get_logger()

# Model name for tiktoken tokeniser and OpenAI API calls.
# Using the same name for both ensures token counts match reality — if you
# count with cl100k_base but call gpt-4, the counts may drift.
_COMPRESSION_MODEL = "gpt-3.5-turbo"

# Maximum tokens GPT-3.5-turbo will emit in its response.
# 4000 gives room for the compressed output while staying within the model's
# output limit. The compressed output is an excerpt, not a full copy, so
# 4000 tokens is generous.
_MAX_RESPONSE_TOKENS = 4000


@dataclass
class CompressionResult:
    """Structured result from a single compress call.
    Passed from ContextCompressor → KafkaHandler → Kafka payload.
    All fields are included in the Kafka message so downstream services
    (and the Grafana dashboard) can observe compression behaviour.
    """

    # How many log lines were in the input before formatting.
    original_log_count: int
    # How many tiktoken tokens the formatted log text contained.
    original_token_count: int
    # The text to pass downstream (compressed or original, depending on was_compressed).
    compressed_text: str
    # compressed_tokens / original_tokens. 1.0 = no compression.
    # Values < 1.0 indicate compression occurred. Used for cost tracking.
    compression_ratio: float
    # True if GPT was called for compression; False if below threshold or if
    # GPT call failed and we fell back to the original text.
    was_compressed: bool


class ContextCompressor:
    """Compresses log context before it reaches the expensive RCA agent.
    Algorithm:
      1. Sort logs chronologically (oldest first) so the LLM sees causal order.
      2. Format each log into a single line: timestamp + level + service + message.
      3. Count tokens using tiktoken (same tokeniser the model uses internally).
      4. If tokens <= threshold: return original text without an API call.
      5. If tokens > threshold: call GPT-3.5-turbo with the compression prompt.
      6. On any OpenAI error: log WARN and return original text (fail-open).
    Why tiktoken (not word count)?
      tiktoken uses the exact same tokenisation as the OpenAI model. Word count
      is inaccurate: a single stack trace token like 'ConnectionRefusedError'
      counts as one word but encodes as multiple tokens. Using tiktoken prevents
      under-counting and accidentally sending oversized payloads to the RCA agent.
    """

    def __init__(
        self,
        openai_client: object,
        prompt_registry: object,
        token_threshold: int = 6000,
    ) -> None:
        self._client = openai_client
        self._registry = prompt_registry
        self._threshold = token_threshold
        # encoding_for_model returns the tiktoken BPE encoding for the model.
        # Calling it here (not lazily) catches bad model names at startup,
        # not mid-request where the error would be harder to diagnose.
        self._encoding = tiktoken.encoding_for_model(_COMPRESSION_MODEL)

    async def compress(
        self,
        tenant_id: str,
        affected_services: list[str],
        logs: list[dict],
    ) -> CompressionResult:
        """Compress a list of log dicts to fit within the LLM context window.
        Args:
            tenant_id: For structured log context (not used in query here).
            affected_services: Names of services whose logs were fetched.
            logs: List of dicts with keys: timestamp, level, service, message.
        Returns:
            CompressionResult with compressed_text set to either the GPT output
            (was_compressed=True) or the original formatted text (was_compressed=False).
        """
        log = logger.bind(tenant_id=tenant_id, services=affected_services)

        # --- Step 1: sort logs chronologically ---
        # Older events explain newer ones. The LLM performs best when it can
        # trace the causal chain from first error to cascading failure.
        # Sorting by timestamp string is correct here because all timestamps are
        # UTC ISO 8601 with +00:00 suffix — lexicographic == chronological.
        sorted_logs = sorted(
            logs,
            key=lambda entry: _parse_ts_for_sort(entry["timestamp"]),
        )

        # --- Step 2: format each log as a single readable line ---
        log_text = _format_logs(sorted_logs)

        # --- Step 3: count tokens ---
        # tiktoken.encode returns a list of integer token IDs. len gives
        # the token count. This is exactly what the model will charge for.
        token_count = len(self._encoding.encode(log_text))
        original_log_count = len(logs)

        log.debug(
            "token_count_computed",
            token_count=token_count,
            threshold=self._threshold,
            log_lines=original_log_count,
        )

        # --- Step 4: below threshold — no compression needed ---
        if token_count <= self._threshold:
            log.info(
                "compression_not_needed",
                token_count=token_count,
                threshold=self._threshold,
            )
            return CompressionResult(
                original_log_count=original_log_count,
                original_token_count=token_count,
                compressed_text=log_text,
                compression_ratio=1.0,
                was_compressed=False,
            )

        # --- Step 5: above threshold — call GPT for compression ---
        log.info(
            "compression_triggered",
            token_count=token_count,
            threshold=self._threshold,
        )
        try:
            return await self._call_llm_compress(
                log_text=log_text,
                token_count=token_count,
                original_log_count=original_log_count,
                log=log,
            )
        except Exception as exc:
            # --- Step 6: fail-open on any OpenAI error ---
            # Log at WARN (not ERROR) because we are recovering gracefully:
            # the original text is passed through, pipeline does not stall.
            # a recoverable error handled at WARN level.
            log.warning(
                "compression_failed_using_original_text",
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return CompressionResult(
                original_log_count=original_log_count,
                original_token_count=token_count,
                compressed_text=log_text,
                compression_ratio=1.0,
                was_compressed=False,
            )

    async def _call_llm_compress(
        self,
        log_text: str,
        token_count: int,
        original_log_count: int,
        log: object,
    ) -> CompressionResult:
        """Call GPT-3.5-turbo to compress the log text.
        Separated from compress to keep the main method under 40 lines
         and to isolate the OpenAI-specific code for easy mocking.
        """
        # Load the prompt template and inject the log text as a variable.
        prompt = self._registry.load(
            "context_compressor",
            "v1",
            {"log_text": log_text},
        )

        # chat.completions.create is the OpenAI v1.x async API call.
        # temperature=0.0: deterministic output — compression should not vary
        # between runs for the same input logs.
        response = await self._client.chat.completions.create(
            model=_COMPRESSION_MODEL,
            temperature=0.0,
            max_tokens=_MAX_RESPONSE_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
        compressed = response.choices[0].message.content

        # Count tokens in the compressed output to compute the ratio.
        compressed_tokens = len(self._encoding.encode(compressed))
        ratio = compressed_tokens / token_count

        log.info(
            "compression_complete",
            original_tokens=token_count,
            compressed_tokens=compressed_tokens,
            compression_ratio=ratio,
        )
        return CompressionResult(
            original_log_count=original_log_count,
            original_token_count=token_count,
            compressed_text=compressed,
            compression_ratio=ratio,
            was_compressed=True,
        )


def _parse_ts_for_sort(ts: str) -> datetime:
    """Parse an ISO 8601 timestamp string to a timezone-aware datetime for sorting.
    Handles both '+00:00' and 'Z' suffix formats that can appear in log dicts.
    Python 3.10 fromisoformat does not accept 'Z' — we normalise to '+00:00'.
    Python 3.11+ accepts both, but we normalise for maximum compatibility.
    """
    # Replace 'Z' with '+00:00' so fromisoformat works on Python ≤3.10.
    normalised = ts.replace("Z", "+00:00")
    return datetime.fromisoformat(normalised)


def _format_logs(sorted_logs: list[dict]) -> str:
    """Format sorted log dicts into a single multi-line string for the LLM.
    Each line: {timestamp}Z [{level}] {service}: {message}
    The 'Z' suffix makes UTC unambiguous to the LLM — avoids the model
    trying to infer timezone from context.
    """
    lines = []
    for log in sorted_logs:
        # Normalise timestamp: replace +00:00 with Z for clean LLM display.
        ts = log["timestamp"].replace("+00:00", "Z")
        lines.append(f"{ts} [{log['level']}] {log['service']}: {log['message']}")
    return "\n".join(lines)
