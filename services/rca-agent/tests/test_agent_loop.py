"""
Unit tests for services/rca-agent/agent.py — the ReAct loop.
all external dependencies (OpenAI client, PromptRegistry) are
replaced with mocks. Tests are fully in-memory with zero infrastructure. No
test requires a real OpenAI API key, a real Kafka topic, or a real database.
Why AsyncMock for the OpenAI client?
RCAAgent.run uses `await self._client.chat.completions.create(...)`.
AsyncMock makes the mock awaitable so the test can call run inside a real
asyncio event loop without TypeError. Regular MagicMock raises
"TypeError: object is not a coroutine" when awaited.
Test structure:
  - Helper functions build mock OpenAI response objects for the two most common
    finish_reason values: 'tool_calls' and 'stop'.
  - Fixtures construct a minimal IncidentPayload and a wired-up RCAAgent.
  - Each test exercises exactly one behaviour path.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

# --- sys.path setup ---
# Insert the rca-agent directory so that 'from agent import ...' and
# 'from models import ...' resolve when pytest is invoked from any cwd.
sys.path.insert(0, str(Path(__file__).parent.parent))

from agent import RCAAgent
from exceptions import LowConfidenceError, SchemaValidationError
from models import IncidentPayload, RCAResult

# ---------------------------------------------------------------------------
# Mock response builders
# ---------------------------------------------------------------------------


def _make_tool_call_response(
    tool_name: str,
    arguments: dict,
    content: str | None = None,
    prompt_tokens: int = 150,
    completion_tokens: int = 80,
) -> MagicMock:
    """Build a mock OpenAI response with finish_reason='tool_calls'.
    The mock mirrors the structure that RCAAgent.run accesses:
      response.usage.prompt_tokens
      response.usage.completion_tokens
      response.choices[0].finish_reason
      response.choices[0].message.content
      response.choices[0].message.tool_calls[0].id
      response.choices[0].message.tool_calls[0].function.name
      response.choices[0].message.tool_calls[0].function.arguments
    """
    # Build the tool_call mock bottom-up — each sub-attribute must be a named
    # MagicMock so its value can be set explicitly without auto-spec interference.
    tool_call = MagicMock()
    # tool_call_id links the tool result message back to this specific call.
    tool_call.id = "call_test_001"
    tool_call.function.name = tool_name
    # json.dumps ensures the agent can safely json.loads these arguments.
    tool_call.function.arguments = json.dumps(arguments)

    choice = MagicMock()
    choice.finish_reason = "tool_calls"
    # content is None when the LLM jumps straight to a tool call with no preamble.
    # The agent falls back to f"Using tool: {tool_name}" in this case.
    choice.message.content = content
    choice.message.tool_calls = [tool_call]

    usage = MagicMock()
    usage.prompt_tokens = prompt_tokens
    usage.completion_tokens = completion_tokens

    response = MagicMock()
    response.choices = [choice]
    response.usage = usage

    return response


def _make_stop_response(
    root_cause: str,
    confidence: float,
    recommendations: list[str],
    prompt_tokens: int = 200,
    completion_tokens: int = 120,
) -> MagicMock:
    """Build a mock OpenAI response with finish_reason='stop'.
    The content is a JSON string matching the RCAOutput schema so the agent's
    json.loads + RCAOutput.model_validate pipeline succeeds.
    """
    # Build the JSON content the agent will parse. Using json.dumps ensures
    # the string is valid JSON — not hand-crafted with possible escaping errors.
    content = json.dumps(
        {
            "root_cause": root_cause,
            "confidence": confidence,
            "recommendations": recommendations,
        }
    )

    choice = MagicMock()
    choice.finish_reason = "stop"
    choice.message.content = content
    # tool_calls is None on stop responses — the agent accesses it only in the
    # tool_calls branch, but MagicMock would auto-create it otherwise.
    choice.message.tool_calls = None

    usage = MagicMock()
    usage.prompt_tokens = prompt_tokens
    usage.completion_tokens = completion_tokens

    response = MagicMock()
    response.choices = [choice]
    response.usage = usage

    return response


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def incident() -> IncidentPayload:
    """Minimal valid IncidentPayload for use across all agent tests."""
    return IncidentPayload(
        incident_id="inc-001",
        tenant_id="tenant-abc",
        alert_ids=["alert-001"],
        affected_services=["payment-service"],
        is_cascade=False,
        severity="HIGH",
        model_id="gpt-4-turbo",
        prompt_variant="v1",
        compressed_context="ERROR: connection refused at 10:01:32 UTC\n" * 5,
        compression_ratio=1.5,
        incident_description="Payment service returning 500 errors",
        created_at="2024-01-15T10:00:00+00:00",
    )


@pytest.fixture()
def mock_prompt_registry() -> MagicMock:
    """PromptRegistry mock that returns a fixed system prompt string."""
    registry = MagicMock()
    # load returns the system prompt — a non-empty string is all the agent
    # needs to build the messages list. Content does not matter for loop tests.
    registry.load.return_value = (
        "You are an SRE investigating a production incident. "
        "Use the available tools to find the root cause."
    )
    return registry


@pytest.fixture()
def mock_openai_client() -> MagicMock:
    """OpenAI client mock with an async create method (AsyncMock).
    The default side_effect is not set here — each test configures it
    via mock_openai_client.chat.completions.create.side_effect = [...]
    """
    client = MagicMock()
    # chat.completions.create must be AsyncMock so `await create(...)` works.
    # Without AsyncMock, Python raises "TypeError: object is not a coroutine".
    client.chat.completions.create = AsyncMock()
    return client


@pytest.fixture()
def agent(mock_openai_client: MagicMock, mock_prompt_registry: MagicMock) -> RCAAgent:
    """RCAAgent wired with mock dependencies and default thresholds."""
    return RCAAgent(
        openai_client=mock_openai_client,
        prompt_registry=mock_prompt_registry,
        max_iterations=5,
        confidence_threshold=0.8,
    )


# ---------------------------------------------------------------------------
# Helper: async no-op tool for tests that need a registered tool.
# ---------------------------------------------------------------------------


async def _dummy_query_tool(service: str, level: str = "ERROR") -> str:
    """Minimal async tool that returns a fixed observation string.
    async because RCAAgent.run uses `await self._tools[name](...)`.
    Returning a plain string mimics what QueryLogs returns in Step 13b.
    """
    return f"Found 10 {level} logs for service '{service}'"


_DUMMY_TOOL_SCHEMA = {
    "name": "QueryLogs",
    "description": "Query recent log entries for a service.",
    "parameters": {
        "type": "object",
        "properties": {
            "service": {"type": "string"},
            "level": {"type": "string"},
        },
        "required": ["service"],
    },
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tool_call_executes_registered_tool(
    agent: RCAAgent,
    mock_openai_client: MagicMock,
    incident: IncidentPayload,
) -> None:
    """When the LLM returns tool_calls, the registered tool function is invoked."""
    # Register a spy-wrapped tool so we can assert it was called with the
    # correct arguments without needing a separate mock object.
    spy_calls: list[dict] = []

    async def spy_tool(service: str, level: str = "ERROR") -> str:
        # Record call args so the test can assert them after run completes.
        spy_calls.append({"service": service, "level": level})
        return f"10 {level} logs for {service}"

    agent.register_tool("QueryLogs", spy_tool, _DUMMY_TOOL_SCHEMA)

    # First call: LLM requests QueryLogs with specific arguments.
    # Second call: LLM returns a stop response with high confidence.
    mock_openai_client.chat.completions.create.side_effect = [
        _make_tool_call_response("QueryLogs", {"service": "payment-service", "level": "ERROR"}),
        _make_stop_response(
            root_cause="Database connection pool exhausted due to a slow query backlog",
            confidence=0.92,
            recommendations=["Increase pool size", "Add query timeout"],
        ),
    ]

    result = await agent.run(incident)

    # The tool was invoked once with the LLM-provided arguments.
    assert len(spy_calls) == 1
    assert spy_calls[0]["service"] == "payment-service"
    assert spy_calls[0]["level"] == "ERROR"

    # run should return a valid RCAResult.
    assert isinstance(result, RCAResult)
    assert result.confidence == 0.92


@pytest.mark.asyncio
async def test_final_answer_above_threshold_returns_result(
    agent: RCAAgent,
    mock_openai_client: MagicMock,
    incident: IncidentPayload,
) -> None:
    """When the LLM stops with confidence above threshold, run returns RCAResult."""
    root_cause_text = "Redis cache eviction caused a stampede on the auth service"

    mock_openai_client.chat.completions.create.side_effect = [
        _make_stop_response(
            root_cause=root_cause_text,
            confidence=0.9,
            recommendations=["Increase Redis maxmemory", "Add request coalescing"],
        )
    ]

    result = await agent.run(incident)

    assert isinstance(result, RCAResult)
    assert result.root_cause == root_cause_text
    assert result.confidence == 0.9
    assert result.tenant_id == incident.tenant_id
    assert result.incident_id == incident.incident_id
    assert result.model_used == incident.model_id
    assert result.prompt_version == incident.prompt_variant
    # Token counts must be positive — both usage fields from the mock were set.
    assert result.input_tokens > 0
    assert result.output_tokens > 0


@pytest.mark.asyncio
async def test_confidence_below_threshold_continues_loop(
    agent: RCAAgent,
    mock_openai_client: MagicMock,
    incident: IncidentPayload,
) -> None:
    """When confidence is below threshold, the agent requests more investigation."""
    # First stop response: confidence 0.4 — below 0.8 threshold.
    # Second stop response: confidence 0.91 — above threshold, loop exits.
    mock_openai_client.chat.completions.create.side_effect = [
        _make_stop_response(
            root_cause="Preliminary finding: connection pool issues in payment service",
            confidence=0.4,
            recommendations=["Check pool config"],
        ),
        _make_stop_response(
            root_cause="Database connection pool exhausted due to a slow query accumulation",
            confidence=0.91,
            recommendations=["Increase pool size to 20", "Add 30s query timeout"],
        ),
    ]

    result = await agent.run(incident)

    # Two calls were made: first for the low-confidence answer, second after nudge.
    assert mock_openai_client.chat.completions.create.call_count == 2

    # The final result uses the second (high-confidence) answer.
    assert result.confidence == 0.91


@pytest.mark.asyncio
async def test_max_iterations_raises_low_confidence_error(
    mock_openai_client: MagicMock,
    mock_prompt_registry: MagicMock,
    incident: IncidentPayload,
) -> None:
    """When max_iterations is exhausted via tool_calls only, LowConfidenceError is raised."""
    # Use max_iterations=2 to keep the test fast while still exercising the limit.
    small_agent = RCAAgent(
        openai_client=mock_openai_client,
        prompt_registry=mock_prompt_registry,
        max_iterations=2,
        confidence_threshold=0.8,
    )
    small_agent.register_tool("QueryLogs", _dummy_query_tool, _DUMMY_TOOL_SCHEMA)

    # Both iterations return tool_calls — the LLM never produces a stop response.
    # After 2 iterations the while loop exits and LowConfidenceError is raised.
    mock_openai_client.chat.completions.create.side_effect = [
        _make_tool_call_response("QueryLogs", {"service": "payment-service"}),
        _make_tool_call_response("QueryLogs", {"service": "payment-service"}),
    ]

    with pytest.raises(LowConfidenceError) as exc_info:
        await small_agent.run(incident)

    error = exc_info.value
    # iterations should equal max_iterations — the loop ran to completion.
    assert error.iterations == 2
    # confidence is 0.0 because no stop response was ever received.
    assert error.final_confidence == 0.0
    # context dict must be populated for structured DLQ message building.
    assert "iterations" in error.context
    assert "final_confidence" in error.context


@pytest.mark.asyncio
async def test_invalid_json_raises_schema_validation_error(
    agent: RCAAgent,
    mock_openai_client: MagicMock,
    incident: IncidentPayload,
) -> None:
    """When the LLM returns unparseable JSON, SchemaValidationError is raised."""
    bad_response = MagicMock()
    bad_response.finish_reason = "stop"
    # Deliberately malformed: missing closing brace and wrong confidence type.
    bad_response.message.content = '{"root_cause": "something broke", "confidence": "high"'
    bad_response.message.tool_calls = None

    usage = MagicMock()
    usage.prompt_tokens = 100
    usage.completion_tokens = 50

    mock_response = MagicMock()
    mock_response.choices = [bad_response]
    mock_response.usage = usage

    mock_openai_client.chat.completions.create.side_effect = [mock_response]

    with pytest.raises(SchemaValidationError) as exc_info:
        await agent.run(incident)

    error = exc_info.value
    # raw_response must be captured (truncated) so engineers can debug the prompt.
    assert "raw_response" in error.context
    assert len(error.context["raw_response"]) <= 500


@pytest.mark.asyncio
async def test_unknown_tool_returns_error_observation_not_raises(
    agent: RCAAgent,
    mock_openai_client: MagicMock,
    incident: IncidentPayload,
) -> None:
    """When the LLM calls a tool not in the registry, an error observation is returned.
    The agent must NOT raise an exception — it returns the error as an observation
    string so the LLM can self-correct in the next iteration.
    """
    # No tools registered — agent._tools is empty.
    # LLM requests a non-existent tool, then stops with a valid answer.
    mock_openai_client.chat.completions.create.side_effect = [
        _make_tool_call_response("NonExistentTool", {"arg": "value"}),
        _make_stop_response(
            root_cause="Connection pool exhausted due to a misconfigured timeout setting",
            confidence=0.85,
            recommendations=["Set pool_timeout=30s"],
        ),
    ]

    # Should complete successfully despite the unknown tool — no exception raised.
    result = await agent.run(incident)

    assert isinstance(result, RCAResult)

    # The reasoning step for the unknown tool must record the error observation.
    assert len(result.reasoning_steps) == 1
    step = result.reasoning_steps[0]
    assert "NonExistentTool" in step.observation
    assert "Unknown tool" in step.observation or "Available" in step.observation


@pytest.mark.asyncio
async def test_stream_callback_called_for_each_reasoning_step(
    agent: RCAAgent,
    mock_openai_client: MagicMock,
    incident: IncidentPayload,
) -> None:
    """The stream callback is invoked once per ReasoningStep produced."""
    agent.register_tool("QueryLogs", _dummy_query_tool, _DUMMY_TOOL_SCHEMA)

    captured_steps: list[Any] = []

    def capture_step(step: Any) -> None:
        # Capture the step so we can assert on its content after run completes.
        captured_steps.append(step)

    agent.set_stream_callback(capture_step)

    # One tool_call iteration (produces one step) then stop.
    mock_openai_client.chat.completions.create.side_effect = [
        _make_tool_call_response("QueryLogs", {"service": "payment-service"}),
        _make_stop_response(
            root_cause="Database connection pool exhausted due to query accumulation",
            confidence=0.88,
            recommendations=["Increase pool size"],
        ),
    ]

    result = await agent.run(incident)

    # Exactly one step was emitted (one tool_call iteration).
    assert len(captured_steps) == 1
    assert captured_steps[0].step_number == 1
    assert captured_steps[0].action == "QueryLogs"

    # The step emitted to the callback is the same step in the result.
    assert len(result.reasoning_steps) == 1


@pytest.mark.asyncio
async def test_stream_callback_error_does_not_crash_agent(
    agent: RCAAgent,
    mock_openai_client: MagicMock,
    incident: IncidentPayload,
) -> None:
    """When the stream callback raises, run continues and returns a valid result."""
    agent.register_tool("QueryLogs", _dummy_query_tool, _DUMMY_TOOL_SCHEMA)

    def failing_callback(step: Any) -> None:
        # Simulates a Kafka publish failure or a serialisation error.
        raise RuntimeError("Kafka broker unreachable")

    agent.set_stream_callback(failing_callback)

    mock_openai_client.chat.completions.create.side_effect = [
        _make_tool_call_response("QueryLogs", {"service": "payment-service"}),
        _make_stop_response(
            root_cause="Database connection pool exhausted due to slow query accumulation",
            confidence=0.90,
            recommendations=["Increase pool size to 20"],
        ),
    ]

    # run must succeed even though the callback raised on every step.
    # Streaming is best-effort; the RCA result is the primary deliverable.
    result = await agent.run(incident)

    assert isinstance(result, RCAResult)
    assert result.confidence == 0.90


@pytest.mark.asyncio
async def test_reasoning_steps_have_utc_timestamps(
    agent: RCAAgent,
    mock_openai_client: MagicMock,
    incident: IncidentPayload,
) -> None:
    """Every ReasoningStep recorded in the result carries a UTC ISO 8601 timestamp."""
    agent.register_tool("QueryLogs", _dummy_query_tool, _DUMMY_TOOL_SCHEMA)

    mock_openai_client.chat.completions.create.side_effect = [
        _make_tool_call_response("QueryLogs", {"service": "payment-service"}),
        _make_stop_response(
            root_cause="Database connection pool exhausted due to query accumulation",
            confidence=0.95,
            recommendations=["Increase pool size"],
        ),
    ]

    result = await agent.run(incident)

    assert len(result.reasoning_steps) >= 1

    for step in result.reasoning_steps:
        # Timestamp must contain the UTC offset indicator produced by
        # datetime.now(timezone.utc).isoformat.
        assert "+00:00" in step.timestamp, (
            f"step {step.step_number} timestamp '{step.timestamp}' "
            "is missing UTC offset — Standard 2 violation"
        )

        # Must be parseable as a real datetime with a zero (UTC) offset.
        parsed = datetime.fromisoformat(step.timestamp)
        assert parsed.tzinfo is not None
        assert parsed.utcoffset() == timedelta(0)


@pytest.mark.asyncio
async def test_token_counts_accumulate_across_iterations(
    agent: RCAAgent,
    mock_openai_client: MagicMock,
    incident: IncidentPayload,
) -> None:
    """input_tokens and output_tokens in RCAResult sum across all loop iterations."""
    agent.register_tool("QueryLogs", _dummy_query_tool, _DUMMY_TOOL_SCHEMA)

    # First call: 100 + 50 tokens. Second call: 200 + 120 tokens.
    mock_openai_client.chat.completions.create.side_effect = [
        _make_tool_call_response(
            "QueryLogs",
            {"service": "payment-service"},
            prompt_tokens=100,
            completion_tokens=50,
        ),
        _make_stop_response(
            root_cause="Database connection pool exhausted due to slow query accumulation",
            confidence=0.88,
            recommendations=["Increase pool size"],
            prompt_tokens=200,
            completion_tokens=120,
        ),
    ]

    result = await agent.run(incident)

    # Total tokens must equal the sum across both iterations.
    assert result.input_tokens == 300   # 100 + 200
    assert result.output_tokens == 170  # 50 + 120
