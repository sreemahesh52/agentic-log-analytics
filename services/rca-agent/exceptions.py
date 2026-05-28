"""
Typed exceptions for the RCA Agent service.
every error path raises a specific typed exception so callers
can distinguish failure modes — DLQ routing, Slack alert, or human review —
without inspecting exception messages as strings.
Why a base class RCAAgentError?
All agent errors carry a context dict so the Step 13d consumer can attach
structured metadata to every DLQ message without string parsing. The base
class enforces this contract across all subclasses.
Why separate exception types for each failure mode?
The Step 13d DLQ consumer checks the exception type (not the message string)
to select the correct failure_reason in rca_results:
  SchemaValidationError → failure_reason='schema_validation_error'
  LowConfidenceError → failure_reason='low_confidence'
  ToolExecutionError → failure_reason='tool_error'
Using a single generic Exception would force string parsing and introduce
fragility whenever error messages change.
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------


class RCAAgentError(Exception):
    """Base class for all RCA Agent typed exceptions.
    Every subclass carries a context dict that the DLQ consumer writes into
    the rca_results.failure_reason field and the structured log at ERROR level.
    Why context dict instead of individual keyword arguments on each subclass?
    Different failure modes carry different metadata. A common dict keeps the
    base class signature stable while each subclass freely populates its own
    keys. Callers that only need to log the failure can call str(exc) for the
    message and exc.context for the structured payload — no subclass casting
    required.
    """

    def __init__(self, message: str, context: dict | None = None) -> None:
        super().__init__(message)
        # Always initialise to an empty dict — callers must never need to
        # guard against None before accessing context keys.
        self.context: dict = context or {}


# ---------------------------------------------------------------------------
# Schema validation failure
# ---------------------------------------------------------------------------


class SchemaValidationError(RCAAgentError):
    """Raised when the LLM returns output that fails Pydantic model validation.
    This failure is NOT retryable on the same payload. A malformed LLM
    response cannot be repaired by re-sending the same message. The Step 13d
    consumer routes immediately to rca.dlq without any retry loop.
    Why store raw_response (truncated to 500 chars)?
    The raw LLM output is the primary debugging artifact for prompt regression.
    Without it, engineers cannot determine whether the LLM ignored the JSON
    instruction, returned markdown fences, or hallucinated field names.
    Truncating to 500 chars keeps the DLQ message size bounded.
    Why store validation_errors as a list?
    Pydantic v2 can raise multiple field errors in a single ValidationError.
    Storing them all — not just the first — avoids iterative debugging cycles
    where fixing one error reveals another.
    """

    def __init__(
        self,
        message: str,
        raw_response: str,
        validation_errors: list,
    ) -> None:
        super().__init__(
            message,
            {
                # Truncate raw LLM output — it can be thousands of tokens long.
                "raw_response": raw_response[:500],
                "errors": validation_errors,
            },
        )
        # Also store as direct attributes for callers that want typed access
        # without going through the context dict.
        self.raw_response = raw_response
        self.validation_errors = validation_errors


# ---------------------------------------------------------------------------
# Max iterations / low confidence
# ---------------------------------------------------------------------------


class LowConfidenceError(RCAAgentError):
    """Raised when the agent exhausts max_iterations without meeting the confidence threshold.
    The Step 13d DLQ consumer sends a Slack notification for this failure mode
    because it requires human review — the agent genuinely could not diagnose
    the incident rather than failing due to a code or infrastructure error.
    Why store both final_confidence and iterations?
    The pair tells engineers two different things:
      - confidence=0.4, iterations=15: agent ran fully but remained uncertain.
        Action: add relevant past incidents to the knowledge base.
      - confidence=0.0, iterations=15: agent never produced a stop response
        (always returned tool_calls). Action: check tool reliability or raise
        max_iterations.
    The two failure modes require different remediation even though both route
    to rca.dlq with failure_reason='low_confidence'.
    """

    def __init__(self, final_confidence: float, iterations: int) -> None:
        super().__init__(
            # f-string with :.2f keeps the message readable in logs and Slack.
            f"Agent reached {iterations} iterations with confidence {final_confidence:.2f}",
            {
                "final_confidence": final_confidence,
                "iterations": iterations,
            },
        )
        # Direct attributes for type-safe access by Step 13d consumer.
        self.final_confidence = final_confidence
        self.iterations = iterations


# ---------------------------------------------------------------------------
# Tool execution failure
# ---------------------------------------------------------------------------


class ToolExecutionError(RCAAgentError):
    """Raised when a registered agent tool encounters an unrecoverable failure.
    Note: most tool errors inside the ReAct loop are RECOVERED, not raised.
    The agent catches individual tool exceptions and returns the error text as
    an observation string so the LLM can reason about it and try a different
    approach. This exception is only raised when the error is unrecoverable at
    the agent level — e.g., the database connection pool is gone entirely, not
    just one query that failed.
    See agent.py: the try/except around tool calls converts recoverable errors
    to observation strings. ToolExecutionError is for the outer failure layer.
    """

    def __init__(self, tool_name: str, error: str) -> None:
        super().__init__(
            f"Tool '{tool_name}' failed: {error}",
            {"tool_name": tool_name},
        )
        self.tool_name = tool_name
        self.error = error
