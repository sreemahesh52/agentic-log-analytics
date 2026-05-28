"""
RCA Agent — autonomous root cause analysis via the ReAct reasoning loop.
Architecture overview:
  RCAAgent is a pure reasoning engine. It knows nothing about Kafka, PostgreSQL,
  or Redis. All I/O dependencies (OpenAI client, PromptRegistry) are injected
  via the constructor, making the class fully testable with mocks.
Why raw OpenAI API instead of LangChain?
  LangChain abstracts the ReAct loop but makes it opaque: token counting,
  finish_reason handling, and streaming callbacks all happen inside framework
  code that is hard to inspect or override. Using the raw OpenAI SDK gives
  complete control over:
    - How tool calls are dispatched (single vs. parallel)
    - How tokens are counted (input + output separately for cost tracking)
    - How the stop condition is evaluated (confidence threshold OR max iterations)
    - How partial results are streamed per step (custom callback, not framework hook)
  The trade-off is more boilerplate, which is offset by the educational value of
  seeing the full ReAct mechanics explicitly.
  RCAAgent depends on:
    - An openai_client duck-typed as any object with chat.completions.create
    - A PromptRegistry for prompt loading
  Neither is instantiated here. The caller (Step 13d Kafka consumer) creates
  them and injects via __init__. Tests inject mocks.
  Registered tools are Strategy implementations. register_tool adds new
  capabilities without modifying RCAAgent. New tool = new async function
  registered in the consumer, not an edit to this class.
"""

from __future__ import annotations

import json
import time
from typing import Any, Callable, TypedDict

import structlog

from exceptions import LowConfidenceError, SchemaValidationError
from models import IncidentPayload, RCAOutput, RCAResult, ReasoningStep
from prompt_registry import PromptRegistry

# structlog provides structured JSON output — every log entry is machine-parseable
# and queryable in production log aggregation tools (Datadog, CloudWatch, etc.).
log = structlog.get_logger(__name__)

# Maximum chars of compressed_context forwarded to the prompt.
# The LLM context window is finite; 4000 chars keeps the prompt well within limits
# while the Context Compressor (Step 9) handles the full compression upstream.
_MAX_CONTEXT_CHARS = 4000


# ---------------------------------------------------------------------------
# ToolDefinition — the schema the OpenAI API expects for function calling.
# ---------------------------------------------------------------------------


class ToolDefinition(TypedDict):
    """TypedDict describing one function available to the LLM via tool calling.
    Why TypedDict instead of a Pydantic model or plain dict?
    TypedDict provides static type checking (mypy, pyright) on the keys without
    runtime overhead. Plain dict silently accepts misspelled keys. Pydantic adds
    unnecessary instantiation cost for a structure that is only ever passed
    directly to the OpenAI API.
    The OpenAI tools parameter format wraps each ToolDefinition as:
      {"type": "function", "function": <ToolDefinition>}
    The wrapping is done in RCAAgent.run so the definition stays minimal here.
    """

    name: str
    description: str
    # parameters: a JSON Schema object describing the function's arguments.
    # Example: {"type": "object", "properties": {"service": {"type": "string"}},
    #           "required": ["service"]}
    parameters: dict


# ---------------------------------------------------------------------------
# RCAAgent — the ReAct reasoning engine.
# ---------------------------------------------------------------------------


class RCAAgent:
    """Autonomous root cause analysis agent using the ReAct loop pattern.
    ReAct (Reason + Act) alternates between:
      Thought — the LLM reasons about what it knows and what to do next
      Action — the LLM selects a tool and arguments (OpenAI tool calling)
      Observation — the tool returns its result; the LLM incorporates it
    The loop continues until either:
      a) The LLM returns a stop response with confidence >= threshold
      b) max_iterations is exhausted (raises LowConfidenceError)
    Why confidence threshold and not just max_iterations?
    Stopping early when confidence is high saves tokens and reduces latency for
    straightforward incidents. The threshold is configurable so operators can
    trade token cost against investigation thoroughness.
    Interface contract:
      - register_tool must be called before run for any tool the LLM may use.
      - set_stream_callback is optional; omitting it disables live streaming.
      - run is the only async method — all tool functions must also be async.
    """

    def __init__(
        self,
        openai_client: Any,
        prompt_registry: PromptRegistry,
        max_iterations: int = 15,
        confidence_threshold: float = 0.8,
    ) -> None:
        """Initialise the agent with injected dependencies.
        No I/O occurs here. The agent is stateless between run calls —
        token counts and reasoning steps are local to each run invocation.
        Args:
            openai_client: Any object with chat.completions.create
                                  (real openai.AsyncOpenAI or a test mock).
            prompt_registry: Loaded PromptRegistry for prompt rendering.
            max_iterations: Hard cap on ReAct loop iterations.
                                  After this many iterations without a stop
                                  response, LowConfidenceError is raised.
            confidence_threshold: Minimum confidence to accept a stop response.
                                  Values below this trigger another loop iteration
                                  (up to max_iterations).
        """
        # Injected dependencies — never instantiated internally.
        self._client = openai_client
        self._registry = prompt_registry

        self._max_iterations = max_iterations
        self._confidence_threshold = confidence_threshold

        # Tool registry: name → async callable. Populated via register_tool.
        # Using dict gives O(1) lookup during the hot path of the ReAct loop.
        self._tools: dict[str, Any] = {}

        # Tool schemas passed to the OpenAI API on every chat.completions.create call.
        # Kept in insertion order (dict in Python 3.7+ is ordered, but list is explicit).
        self._tool_schemas: list[ToolDefinition] = []

        # Optional streaming callback. None means streaming is disabled.
        # Using None (not a no-op lambda) allows callers to distinguish
        # "streaming configured" from "streaming not configured".
        self._stream_callback: Callable[[ReasoningStep], None] | None = None

    def register_tool(
        self,
        name: str,
        func: Any,
        schema: ToolDefinition,
    ) -> None:
        """Register an async callable as an agent tool.
        Tools must be async coroutines because all I/O (PostgreSQL queries,
        ChromaDB searches, HTTP calls) requires async execution. Registering
        a sync function will cause 'TypeError: object is not awaitable' at
        runtime when the LLM first calls that tool.
        Args:
            name: Tool name the LLM will use in tool_calls. Must match
                    schema['name'] exactly — the OpenAI API uses the name
                    from the tool_call to route the dispatch.
            func: Async callable invoked with **kwargs from LLM arguments.
            schema: ToolDefinition dict passed to OpenAI's tools parameter.
        """
        # Store func separately from schema to avoid coupling dispatch logic
        # to the schema structure. The schema goes to OpenAI; func stays local.
        self._tools[name] = func
        self._tool_schemas.append(schema)

        log.debug("tool_registered", tool_name=name)

    def set_stream_callback(
        self,
        callback: Callable[[ReasoningStep], None],
    ) -> None:
        """Set a function called after each ReasoningStep completes.
        The callback receives a fully-populated ReasoningStep (thought, action,
        action_input, observation all set). It is called synchronously inside
        run, so it should not block. The Step 13d consumer uses this to
        publish each step to the rca.stream Kafka topic for SSE delivery.
        Errors in the callback are swallowed (see _emit_step) — a streaming
        failure must never abort an ongoing RCA investigation.
        """
        self._stream_callback = callback

    def _emit_step(self, step: ReasoningStep) -> None:
        """Invoke the stream callback if one is registered, swallowing errors.
        Why swallow callback errors here instead of propagating them?
        Streaming is a best-effort feature. An RCA investigation succeeding
        but not streaming its steps is far better than an investigation failing
        because a Kafka publish raised a connection error at iteration 12.
        The primary goal (RCAResult written to rca_results) must not be blocked
        by the secondary goal (live step streaming).
        Errors are logged at WARN so they are visible in production without
        causing alert fatigue. A persistent streaming failure (repeated WARNs)
        signals that the Kafka topic or consumer needs investigation.
        """
        if self._stream_callback is None:
            # No callback registered — streaming is disabled for this agent.
            return

        try:
            self._stream_callback(step)
        except Exception as exc:
            # WARN not ERROR: streaming failure is recoverable. The RCA result
            # will still be written to rca_results and agent.results.
            log.warning(
                "stream_callback_failed",
                error=str(exc),
                step_number=step.step_number,
            )

    async def run(self, incident: IncidentPayload) -> RCAResult:
        """Execute the ReAct loop for one incident and return a validated RCAResult.
        The loop proceeds as follows:
          1. Render the system prompt from the PromptRegistry.
          2. Call OpenAI with the current message history and tool schemas.
          3. If finish_reason='tool_calls': dispatch the tool, record the step,
             append the observation to messages, continue to next iteration.
          4. If finish_reason='stop': parse and validate JSON output as RCAOutput.
             If confidence >= threshold, return RCAResult. Otherwise ask the LLM
             to investigate further and continue.
          5. After max_iterations without a final answer, raise LowConfidenceError.
        Args:
            incident: Validated IncidentPayload from the incidents.ready topic.
        Returns:
            RCAResult: fully validated result ready to persist and publish.
        Raises:
            SchemaValidationError: LLM produced unparseable or invalid JSON output.
            LowConfidenceError: max_iterations exhausted without a stop response.
        """
        # time.monotonic is a wall-clock timer that never goes backwards
        # (unlike time.time which can jump on NTP adjustments). Use it
        # exclusively for measuring durations — never for absolute timestamps.
        start_time = time.monotonic()

        # --- Render system prompt from the PromptRegistry ---
        # The prompt variant ('v1' or 'v2') was assigned randomly by the Model
        # Router for A/B testing. All prompt variables are injected here so the
        # rendered string is ready to send without further processing.
        system_prompt = self._registry.load(
            "rca_agent",
            incident.prompt_variant,
            variables={
                "service": (
                    incident.affected_services[0]
                    if incident.affected_services
                    else "unknown"
                ),
                # anomaly_type maps to severity in the prompt template variable.
                "anomaly_type": incident.severity,
                # Truncate to _MAX_CONTEXT_CHARS to stay within context window.
                # The Context Compressor already reduced tokens; this is a safety cap.
                "compressed_context": incident.compressed_context[:_MAX_CONTEXT_CHARS],
            },
        )

        # --- Initialise the conversation message history ---
        # OpenAI's chat completions API is stateless — the full conversation
        # history must be sent on every call. We maintain it in this list and
        # append each assistant message and tool result after each iteration.
        messages: list[dict] = [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": (
                    f"Investigate this incident.\n"
                    f"Affected services: {', '.join(incident.affected_services)}\n"
                    f"Severity: {incident.severity}\n"
                    f"Incident ID: {incident.incident_id}"
                ),
            },
        ]

        # --- Accumulators for the full loop ---
        reasoning_steps: list[ReasoningStep] = []
        total_input_tokens = 0
        total_output_tokens = 0
        total_tool_latency_ms = 0
        iteration = 0
        # rca_output is populated on the first 'stop' response. Initialised to
        # None so LowConfidenceError can report confidence=0.0 if the loop
        # exits without ever receiving a stop response (all tool_calls).
        rca_output: RCAOutput | None = None

        # --- ReAct loop ---
        # The condition is < (not <=) so iteration reaches exactly max_iterations
        # on the final pass and the `iteration == self._max_iterations` check
        # inside correctly identifies the last allowed attempt.
        while iteration < self._max_iterations:
            iteration += 1

            log.debug(
                "react_iteration_start",
                iteration=iteration,
                max_iterations=self._max_iterations,
                tenant_id=incident.tenant_id,
                incident_id=incident.incident_id,
            )

            # --- LLM call ---
            # temperature=0.0: deterministic output for reproducible debugging.
            # tool_schemas are wrapped in the OpenAI required format here so
            # ToolDefinition stays clean (no 'type': 'function' key duplication).
            response = await self._client.chat.completions.create(
                model=incident.model_id,
                messages=messages,
                tools=[
                    {"type": "function", "function": s}
                    for s in self._tool_schemas
                ],
                temperature=0.0,
            )

            # --- Accumulate token usage ---
            # Input and output tokens are billed at different rates (e.g. GPT-4:
            # $10/M input, $30/M output). Tracking them separately allows the
            # Evaluation Harness to compute precise cost_usd per investigation.
            total_input_tokens += response.usage.prompt_tokens
            total_output_tokens += response.usage.completion_tokens

            choice = response.choices[0]

            # ----------------------------------------------------------------
            # Branch A: LLM requests one or more tool calls
            # ----------------------------------------------------------------
            if choice.finish_reason == "tool_calls":
                # Append the assistant message FIRST — OpenAI requires the
                # assistant message (containing all tool_calls) to appear in the
                # conversation before any role="tool" response messages.
                messages.append(choice.message)

                # Loop over EVERY tool_call in the message. OpenAI may request
                # multiple tools in a single turn (parallel tool use). Every
                # tool_call_id in the assistant message MUST receive a matching
                # role="tool" response — missing any id causes:
                #   400 "tool_call_ids did not have response messages".
                for tool_call in choice.message.tool_calls:
                    tool_name = tool_call.function.name

                    # json.loads (not eval): always parse LLM-provided JSON safely.
                    # eval would execute arbitrary code — never acceptable.
                    tool_args: dict = json.loads(tool_call.function.arguments)

                    # Build the step record. observation is set after the tool runs.
                    step = ReasoningStep(
                        step_number=iteration,
                        # content is the LLM's reasoning text before the tool call.
                        # Falls back to a descriptive string if the LLM omits it.
                        thought=choice.message.content or f"Using tool: {tool_name}",
                        action=tool_name,
                        action_input=tool_args,
                    )

                    # --- Tool dispatch ---
                    tool_start = time.monotonic()

                    if tool_name in self._tools:
                        try:
                            # All registered tools are async coroutines. Using await
                            # here means tool execution is non-blocking — other async
                            # tasks in the event loop can run while we wait for I/O.
                            observation = str(
                                await self._tools[tool_name](**tool_args)
                            )
                        except Exception as exc:
                            # Recover from individual tool failures by returning the
                            # error as an observation. The LLM can reason about it:
                            # "The query failed — let me try a different approach."
                            observation = f"Tool error: {str(exc)}"
                            log.warning(
                                "tool_execution_error",
                                tool_name=tool_name,
                                error=str(exc),
                                tenant_id=incident.tenant_id,
                            )
                    else:
                        # Unknown tool — LLM hallucinated a name not in the registry.
                        # Return the error as an observation so the agent can
                        # self-correct in the next iteration.
                        observation = (
                            f"Unknown tool: '{tool_name}'. "
                            f"Available tools: {list(self._tools.keys())}"
                        )
                        log.warning(
                            "unknown_tool_requested",
                            tool_name=tool_name,
                            available_tools=list(self._tools.keys()),
                        )

                    # Accumulate tool time separately from LLM time for Grafana panel 13.
                    total_tool_latency_ms += int(
                        (time.monotonic() - tool_start) * 1000
                    )

                    step.observation = observation
                    reasoning_steps.append(step)

                    # Emit to streaming callback (errors swallowed inside _emit_step).
                    self._emit_step(step)

                    # Append one tool response per tool_call_id — OpenAI validates
                    # that every id in the assistant message has a matching response.
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": observation,
                        }
                    )

            # ----------------------------------------------------------------
            # Branch B: LLM produced a final answer (finish_reason='stop')
            # ----------------------------------------------------------------
            elif choice.finish_reason in ("stop", "length"):
                raw_content = choice.message.content or ""

                # --- Extract JSON from possible markdown code fences ---
                # LLMs frequently wrap JSON in ```json ... ``` even when told
                # not to. Stripping the fence is more robust than fighting the
                # LLM with increasingly strict prompts.
                try:
                    json_str = raw_content
                    if "```json" in raw_content:
                        # Split on ```json, take the part after it, then split
                        # on ``` to remove the closing fence.
                        json_str = (
                            raw_content.split("```json")[1].split("```")[0].strip()
                        )
                    elif "```" in raw_content:
                        # Unfenced code block — extract between first pair of ```.
                        json_str = (
                            raw_content.split("```")[1].split("```")[0].strip()
                        )

                    parsed = json.loads(json_str)
                    # model_validate raises ValidationError if any field violates
                    # the RCAOutput schema (min_length, ge/le bounds, etc.).
                    rca_output = RCAOutput.model_validate(parsed)

                except (json.JSONDecodeError, ValueError) as exc:
                    # Non-retryable: the same prompt will produce the same broken
                    # output. Route to DLQ immediately via SchemaValidationError.
                    raise SchemaValidationError(
                        f"LLM output failed validation: {exc}",
                        raw_content,
                        [str(exc)],
                    )

                # --- Confidence gate ---
                # Return early if confidence is high enough OR this is the final
                # allowed iteration (avoid infinite loops on stubborn LLMs).
                if (
                    rca_output.confidence >= self._confidence_threshold
                    or iteration == self._max_iterations
                ):
                    # time.monotonic - start_time gives total elapsed seconds.
                    # Convert to milliseconds for the latency fields in rca_results.
                    total_latency_ms = int(
                        (time.monotonic() - start_time) * 1000
                    )

                    log.info(
                        "rca_agent_completed",
                        confidence=rca_output.confidence,
                        iterations=iteration,
                        total_latency_ms=total_latency_ms,
                        tenant_id=incident.tenant_id,
                        incident_id=incident.incident_id,
                    )

                    return RCAResult(
                        tenant_id=incident.tenant_id,
                        incident_id=incident.incident_id,
                        root_cause=rca_output.root_cause,
                        confidence=rca_output.confidence,
                        recommendations=rca_output.recommendations,
                        reasoning_steps=reasoning_steps,
                        model_used=incident.model_id,
                        prompt_version=incident.prompt_variant,
                        input_tokens=total_input_tokens,
                        output_tokens=total_output_tokens,
                        compression_ratio=incident.compression_ratio,
                        total_latency_ms=total_latency_ms,
                        # llm_latency = total minus tool time. Subtraction is safe
                        # because tool_latency is always <= total_latency.
                        llm_latency_ms=total_latency_ms - total_tool_latency_ms,
                        tool_latency_ms=total_tool_latency_ms,
                    )

                # --- Confidence below threshold — request further investigation ---
                # Append the assistant's stop message and a user nudge so the LLM
                # has context for why it is continuing rather than stopping.
                messages.append(choice.message)
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            f"Your confidence is {rca_output.confidence:.2f}, "
                            f"below the required {self._confidence_threshold:.2f}. "
                            f"Please gather more evidence before concluding."
                        ),
                    }
                )

                log.debug(
                    "confidence_below_threshold",
                    confidence=rca_output.confidence,
                    threshold=self._confidence_threshold,
                    iteration=iteration,
                )

            # ----------------------------------------------------------------
            # Branch C: unexpected finish_reason (content_filter, etc.)
            # ----------------------------------------------------------------
            else:
                # Log but do not raise — the loop will retry or exhaust iterations.
                log.warning(
                    "unexpected_finish_reason",
                    finish_reason=choice.finish_reason,
                    iteration=iteration,
                    tenant_id=incident.tenant_id,
                )

        # --- Loop exhausted without a valid stop response ---
        # This happens when all max_iterations returned 'tool_calls' and no
        # 'stop' response was ever produced. rca_output may be None if the LLM
        # never stopped, so we default final_confidence to 0.0.
        raise LowConfidenceError(
            final_confidence=rca_output.confidence if rca_output is not None else 0.0,
            iterations=iteration,
        )
