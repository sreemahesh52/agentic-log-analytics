# --- Unit tests for ContextCompressor ---
# all tests are pure in-memory — no real OpenAI API calls,
# no Kafka, no PostgreSQL. Every external dependency is mocked.
# Test names describe the scenario and expected outcome so failure messages
# are self-explanatory without reading the body (requirement).
# Why patch _encoding rather than tiktoken.encoding_for_model?
#   We patch the instance attribute directly after construction. This avoids
#   patching at the module level (which can be brittle across import paths)
#   and precisely targets the token-counting behaviour being tested.

import sys
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

# Add the services/context-compressor directory to sys.path so imports resolve
# when the test is run from the service root with `python -m pytest tests/`.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from compressor import ContextCompressor  # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

class _FakePromptRegistry:
    """Minimal prompt registry stub: returns a predictable string for any load call.
    Interface: same load(name, version, variables) signature as PromptRegistry.
    Dependency Inversion: ContextCompressor depends on the interface, not the file-based
    implementation, so this stub substitutes transparently in tests.
    """

    def load(self, name: str, version: str, variables: dict) -> str:
        # Return the log_text variable so we can inspect what was passed to GPT.
        return variables.get("log_text", "")


def _make_logs(
    n: int,
    service: str = "test-service",
    level: str = "INFO",
    base: datetime | None = None,
) -> list[dict]:
    """Create n synthetic log dicts with sequential UTC timestamps.
    Using a fixed base datetime makes tests deterministic — they do not depend
    on wall-clock time.
    """
    if base is None:
        base = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    return [
        {
            "timestamp": (base + timedelta(seconds=i)).isoformat(),
            "service": service,
            "level": level,
            "message": f"log message {i}",
        }
        for i in range(n)
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_openai_client() -> AsyncMock:
    """AsyncMock OpenAI client so tests never make real network calls."""
    client = AsyncMock()
    return client


@pytest.fixture
def compressor(mock_openai_client: AsyncMock) -> ContextCompressor:
    """ContextCompressor with injected mock client and stub prompt registry.
    token_threshold=6000 matches the production default so tests cover the
    real threshold boundary.
    """
    return ContextCompressor(
        openai_client=mock_openai_client,
        prompt_registry=_FakePromptRegistry(),
        token_threshold=6000,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_returns_uncompressed_when_below_threshold(
    compressor: ContextCompressor,
    mock_openai_client: AsyncMock,
) -> None:
    """50 short log lines produce far fewer than 6000 tokens: no GPT call."""
    logs = _make_logs(50)

    result = await compressor.compress(
        tenant_id="tenant-abc",
        affected_services=["test-service"],
        logs=logs,
    )

    # was_compressed must be False — GPT should never have been called.
    assert result.was_compressed is False
    # compression_ratio of 1.0 means no reduction occurred.
    assert result.compression_ratio == 1.0
    # original_log_count must match what was passed in.
    assert result.original_log_count == 50
    # original_token_count > 0: tiktoken counted something.
    assert result.original_token_count > 0
    # compressed_text is the formatted original (not empty).
    assert len(result.compressed_text) > 0

    # Confirm the OpenAI client was never called.
    mock_openai_client.chat.completions.create.assert_not_called()


@pytest.mark.asyncio
async def test_returns_compressed_when_above_threshold(
    compressor: ContextCompressor,
    mock_openai_client: AsyncMock,
) -> None:
    """When tiktoken reports 7000 tokens (above 6000), GPT is called and was_compressed=True."""
    # Patch the encoding's encode method to return a fixed-length token list
    # without requiring us to generate 24,000+ characters of real log text.
    # First call: original text → 7000 tokens. Second call: compressed text → 3500 tokens.
    compressor._encoding = MagicMock()
    compressor._encoding.encode.side_effect = [
        list(range(7000)),   # token count for original log text
        list(range(3500)),   # token count for compressed output
    ]

    # Mock the GPT response: return a short "compressed" string.
    mock_openai_client.chat.completions.create.return_value = MagicMock(
        choices=[MagicMock(message=MagicMock(content="compressed output text"))]
    )

    logs = _make_logs(10)
    result = await compressor.compress("tenant-abc", ["test-service"], logs)

    assert result.was_compressed is True
    # ratio = compressed_tokens / original_tokens = 3500 / 7000 = 0.5
    assert abs(result.compression_ratio - 0.5) < 0.001
    assert result.compressed_text == "compressed output text"
    assert result.original_log_count == 10
    assert result.original_token_count == 7000

    # GPT must have been called exactly once.
    mock_openai_client.chat.completions.create.assert_called_once()


@pytest.mark.asyncio
async def test_logs_sorted_chronologically_in_output(
    compressor: ContextCompressor,
) -> None:
    """Logs provided in reverse order must appear oldest-first in compressed_text."""
    # Provide three logs in reverse chronological order.
    base = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    logs = [
        # Intentionally out of order: third, first, second.
        {"timestamp": (base + timedelta(seconds=2)).isoformat(), "service": "svc", "level": "INFO", "message": "third"},
        {"timestamp": (base + timedelta(seconds=0)).isoformat(), "service": "svc", "level": "INFO", "message": "first"},
        {"timestamp": (base + timedelta(seconds=1)).isoformat(), "service": "svc", "level": "INFO", "message": "second"},
    ]

    # Three short logs are well below 6000 tokens — no GPT call, output is the
    # formatted original text in chronological order.
    result = await compressor.compress("tenant-abc", ["svc"], logs)

    assert result.was_compressed is False

    # Split output into lines and verify chronological order by message content.
    output_lines = result.compressed_text.split("\n")
    assert len(output_lines) == 3
    # Oldest event appears first.
    assert "first" in output_lines[0]
    assert "second" in output_lines[1]
    assert "third" in output_lines[2]


@pytest.mark.asyncio
async def test_openai_error_returns_original_uncompressed(
    compressor: ContextCompressor,
    mock_openai_client: AsyncMock,
) -> None:
    """On OpenAI error, fail-open: return original text with was_compressed=False."""
    # Force the encoding to report 7000 tokens so compression is attempted.
    compressor._encoding = MagicMock()
    # Both calls (original text + any fallback) return 7000 tokens.
    compressor._encoding.encode.return_value = list(range(7000))

    # Make GPT raise an unexpected exception.
    mock_openai_client.chat.completions.create.side_effect = Exception(
        "OpenAI API connection failed"
    )

    logs = _make_logs(10)
    result = await compressor.compress("tenant-abc", ["test-service"], logs)

    # Fail-open: was_compressed=False, original text preserved.
    assert result.was_compressed is False
    assert result.compression_ratio == 1.0
    # The original formatted text must still be present (not empty).
    assert len(result.compressed_text) > 0
    # original_log_count preserved so caller can still log/display it.
    assert result.original_log_count == 10


@pytest.mark.asyncio
async def test_compression_ratio_calculated_correctly(
    compressor: ContextCompressor,
    mock_openai_client: AsyncMock,
) -> None:
    """compression_ratio = compressed_tokens / original_tokens with known values."""
    # Original: 8000 tokens. Compressed: 2000 tokens. Expected ratio: 0.25.
    compressor._encoding = MagicMock()
    compressor._encoding.encode.side_effect = [
        list(range(8000)),   # original text → 8000 tokens
        list(range(2000)),   # compressed text → 2000 tokens
    ]

    mock_openai_client.chat.completions.create.return_value = MagicMock(
        choices=[MagicMock(message=MagicMock(content="very short compressed"))]
    )

    logs = _make_logs(5)
    result = await compressor.compress("tenant-abc", ["test-service"], logs)

    # 2000 / 8000 = 0.25 — floating-point comparison with tolerance.
    assert abs(result.compression_ratio - 0.25) < 0.001
    assert result.original_token_count == 8000
    assert result.was_compressed is True
