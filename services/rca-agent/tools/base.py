"""
Tool base class — the bridge between the LLM's tool-call decisions and real Python functions.
=============================================================
BACKGROUND: WHAT IS THE ReAct LOOP?
=============================================================
ReAct stands for Reasoning + Acting. It is a prompting strategy that turns a passive
LLM (one that only answers questions) into an active investigator that gathers real
evidence before concluding.
The loop alternates between three phases on every iteration:
    Thought → the LLM reasons in plain text about what it knows and what to do next
                  "I see connection errors. I should check the logs to understand frequency."
    Action → the LLM DECIDES to call a tool (via OpenAI function calling).
                  It does NOT execute anything — it just declares its intent.
                  "Call QueryLogs with service='payment-service', level='ERROR'"
    Observation → YOUR code executes the real Python function and feeds the result back.
                  "=== ERROR logs: 47 errors 'connection pool exhausted' in last 30 min ==="
The LLM sees the observation and loops again: another Thought → Action → Observation.
When it has enough evidence, it produces a Final Answer instead of another Action.
    Iteration 1: Thought → QueryLogs → "47 ERROR logs: pool exhausted"
    Iteration 2: Thought → GetDependencies→ "auth-service shares 12 traces"
    Iteration 3: Thought → BuildTimeline → "payment-service failed first at 10:05"
    Iteration 4: Thought → FINAL ANSWER → {"root_cause": "...", "confidence": 0.92}
This is the core intelligence of the RCA Agent. Without tools, the LLM can only
reason about what was in its initial prompt. With tools, it discovers real evidence
from the live database on every iteration.
=============================================================
HOW DOES OpenAI FUNCTION CALLING FIT IN?
=============================================================
OpenAI function calling is the mechanism that makes the "Action" step work.
Here is the exact flow, step by step:
--- STEP 1: YOU send your tool schemas to OpenAI ---
    response = await client.chat.completions.create(
        model="gpt-4-turbo",
        messages=[system_prompt, user_message],
        tools=[
            {"type": "function", "function": QUERY_LOGS_SCHEMA},
            {"type": "function", "function": GET_DEPENDENCIES_SCHEMA},
            {"type": "function", "function": BUILD_TIMELINE_SCHEMA},
        ]
    )
    Each schema has three fields the LLM reads:
      name: "QueryLogs"
      description: "Query recent logs for a specific service and level..."
                   ↑ THE LLM READS THIS to decide WHEN to use this tool
      parameters: {"type": "object", "properties": {"service": ...}, "required": ["service"]}
                   ↑ THE LLM READS THIS to know WHAT ARGUMENTS to provide
    If you provide 10 schemas, the LLM reads all 10 descriptions and selects
    whichever tool best matches its current Thought. It may also call multiple
    tools in one response (but our loop processes them one at a time).
--- STEP 2: THE LLM responds with finish_reason="tool_calls" ---
    choice.finish_reason = "tool_calls" ← not "stop" — not a final answer yet
    choice.message.tool_calls = [
        {
            "id": "call_abc123", ← unique ID for THIS specific call
            "type": "function",
            "function": {
                "name": "QueryLogs", ← which tool the LLM chose
                "arguments": '{"service": "payment-service", "level": "ERROR"}'
                             ↑ JSON string — the LLM filled in args using your schema
            }
        }
    ]
    The LLM has NOT run QueryLogs. It has only said "I want to call QueryLogs
    with these arguments." YOUR code must execute the actual function.
--- STEP 3: YOU execute the Python function and feed the result back ---
    tool_name = tool_call.function.name # "QueryLogs"
    tool_args = json.loads(tool_call.function.arguments) # {"service": "payment-service", ...}
    # Look up the registered callable by name:
    func = self._tools["QueryLogs"]
    # func is: functools.partial(query_logs, tenant_id="abc123", db_pool=pool)
    # (tenant_id and db_pool were pre-bound at registration — LLM never sees them)
    # Execute with LLM-provided args only:
    observation = await func(**tool_args)
    # → calls query_logs(tenant_id="abc123", db_pool=pool, service="payment-service", level="ERROR")
    # → returns: "=== ERROR logs for 'payment-service' ===\n[10:23:41] ERROR pool exhausted..."
    # Append BOTH messages to history in this exact order (OpenAI requires this):
    messages.append(choice.message) # assistant message with tool_calls
    messages.append({
        "role": "tool",
        "tool_call_id": "call_abc123", ← MUST match the id from the LLM's response
        "content": observation, ← the real database result
    })
--- STEP 4: YOU call OpenAI again with the updated history ---
    The LLM now sees its own tool_call request + the real observation.
    It reasons about the data and either calls another tool or produces
    finish_reason="stop" with a final JSON answer.
=============================================================
WHY DOES tool_call.id MATTER?
=============================================================
OpenAI can return multiple tool calls in one response:
    choice.message.tool_calls = [
        {"id": "call_abc123", "function": {"name": "QueryLogs", "arguments": "..."}},
        {"id": "call_def456", "function": {"name": "GetDependencies", "arguments": "..."}},
    ]
When you send back results, OpenAI uses the id to match each result to its request:
    messages.append({"role":"tool", "tool_call_id":"call_abc123", "content": "logs output"})
    messages.append({"role":"tool", "tool_call_id":"call_def456", "content": "deps output"})
Without the matching id, OpenAI raises a 400 error:
    "tool result does not have a corresponding tool_call_id"
In our ReAct loop (agent.py) we process one tool call per iteration (tool_calls[0])
for a linear, auditable reasoning chain. The id still matters for that single call.
=============================================================
HOW DO THE Tool CLASS AND func FIELD FIT IN?
=============================================================
`func` is just the Python async function that implements this tool's real capability.
Each Tool instance pairs ONE schema (what OpenAI reads) with ONE function (what you run):
    Tool(schema=QUERY_LOGS_SCHEMA, func=query_logs)
    Tool(schema=GET_DEPENDENCIES_SCHEMA, func=get_dependencies)
    Tool(schema=BUILD_TIMELINE_SCHEMA, func=build_timeline)
    ┌─────────────────────────────────────────────────────────┐
    │ Tool instance │
    │ ┌──────────────────────┐ ┌───────────────────────┐ │
    │ │ schema │ │ func │ │
    │ │ name: "QueryLogs" │ │ async def query_logs │ │
    │ │ description: "..." │ │ tenant_id (bound) │ │
    │ │ parameters: {...} │ │ db_pool (bound) │ │
    │ │ ↑ OpenAI reads this │ │ service ← LLM arg │ │
    │ │ to decide to call │ │ level ← LLM arg │ │
    │ └──────────────────────┘ └───────────────────────┘ │
    └─────────────────────────────────────────────────────────┘
The agent does not care which function func points to. It always calls:
    observation = await tool.execute(**llm_provided_args)
    → which calls: await self.func(**llm_provided_args)
In production, func is a functools.partial with infrastructure args pre-filled:
    func = functools.partial(query_logs, tenant_id="abc123", db_pool=pool)
    Calling func(service="payment-service") is equivalent to:
    query_logs(tenant_id="abc123", db_pool=pool, service="payment-service")
The LLM only sees and provides: service, level, minutes_back, limit.
It never knows tenant_id or db_pool exist. This is Dependency Inversion:
the LLM depends only on the schema (the abstraction), not the implementation.
=============================================================
DESIGN PRINCIPLES (Standards 3, 4, 5)
=============================================================
this module only defines the Tool dataclass
and ToolSchema TypedDict. No database calls, no LLM calls, no business logic.
If either type changes shape, this is the only file that changes.
Tool.execute is the sole contract all
tools must fulfill. A tool function only needs to be async and return something
str-able — it does not need to inherit from a class or implement a protocol.
each tool function is a Strategy implementation
behind a uniform Tool wrapper. The RCAAgent dispatches any Tool via execute
without knowing what the underlying function does. Adding a new investigation
strategy means creating a new async function and wrapping it in Tool — zero
changes to the agent code (Open/Closed Principle).
Tool.execute is the single safety boundary.
Any exception from func is caught here and returned as an error string.
This keeps the ReAct loop alive even when a database query times out or a
service dependency is temporarily unavailable.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, TypedDict

import structlog

# structlog produces structured JSON logs — every entry is machine-parseable
# and queryable in production log aggregation systems (Datadog, CloudWatch, etc.).
log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# ToolSchema — the shape the OpenAI function calling API expects.
# ---------------------------------------------------------------------------


class ToolSchema(TypedDict):
    """JSON Schema descriptor for one agent tool, as required by the OpenAI API.
    Why TypedDict and not a Pydantic model or plain dict?
    TypedDict provides static type checking (mypy/pyright) with zero runtime
    overhead. A plain dict silently accepts misspelled keys. Pydantic would
    add unnecessary validation cost for a structure that is only ever passed
    directly to the OpenAI API as a dict — no serialisation or deserialization
    is needed on our side.
    OpenAI wraps each ToolSchema as:
        {"type": "function", "function": <ToolSchema>}
    The wrapping is done in the agent's run method, not here, so this
    TypedDict stays minimal and reusable across any LLM client.
    """

    name: str
    description: str
    # parameters: a JSON Schema object describing the function's arguments.
    # Must have shape: {"type": "object", "properties": {...}, "required": [...]}
    # The LLM reads this schema to know which kwargs to provide when calling
    # the tool. Missing or wrong schema → the LLM guesses args incorrectly.
    parameters: dict


# ---------------------------------------------------------------------------
# Tool — pairs a ToolSchema (for OpenAI) with an async callable (for dispatch).
# ---------------------------------------------------------------------------


@dataclass
class Tool:
    """Wraps an async callable with its OpenAI schema and a safe execute boundary.
    Why wrap both together instead of storing them separately?
    The RCAAgent needs two things per tool: the JSON schema to send to OpenAI,
    and the callable to invoke when OpenAI returns a tool_calls response.
    Storing them as a pair in one dataclass eliminates any chance of a
    schema/callable mismatch in the tool registry.
    Tool instances are interchangeable strategies.
    The agent dispatches any Tool via execute without knowing its internals.
    New investigation capabilities (e.g., SearchRunbooks, CheckDeployments) are
    added by creating new Tool instances — no agent code changes required.
    execute is the contract that guarantees
    string output. Any exception from func is caught here and returned as a
    descriptive error string. The agent loop must never crash because a single
    tool encountered a database timeout or a network error.
    """

    schema: ToolSchema

    # func is the actual Python async function implementing this tool's capability.
    # In production the consumer binds it via functools.partial before registering:
    #   Tool(schema=QUERY_LOGS_SCHEMA, func=partial(query_logs, tenant_id=t, db_pool=p))
    #   Tool(schema=GET_DEPENDENCIES_SCHEMA, func=partial(get_dependencies, tenant_id=t, db_pool=p))
    #   Tool(schema=BUILD_TIMELINE_SCHEMA, func=partial(build_timeline, tenant_id=t, db_pool=p))
    # The agent/runtime does not care which function func points to.
    # It always executes: await tool.execute(**llm_args) → await self.func(**llm_args)
    # func must be an async coroutine. The RCAAgent uses `await execute` —
    # passing a sync function causes "TypeError: object is not a coroutine"
    # at the first tool call, not at registration time.
    func: Callable[..., Any]

    async def execute(self, **kwargs: Any) -> str:
        """Execute the wrapped tool and always return a string result.
        Why always return str and never raise?
        Tool results are appended to the OpenAI message history as "tool" role
        messages. The OpenAI Chat Completions API requires string content in
        those messages. More critically: if execute raises, the exception
        propagates up to the RCAAgent's ReAct loop and crashes the entire
        investigation. Returning an error string instead lets the LLM reason:
        "the database query failed — let me try a different service or time range."
        Args:
            **kwargs: Arguments the LLM provided via OpenAI tool calling.
                      The schema['parameters'] JSON Schema defines valid kwargs.
                      These come from json.loads(tool_call.function.arguments)
                      inside the agent's ReAct loop.
        Returns:
            str: Tool output on success, or "Tool error (<name>): <message>"
                 on failure. The "Tool error" prefix is recognised by tests and
                 can be used by the LLM to detect failure without parsing the
                 entire message.
        """
        try:
            result = await self.func(**kwargs)
            # str handles any return type: str, list, None, custom objects.
            # The LLM receives the string representation regardless of what
            # func returned — a non-string return is not an error condition.
            return str(result)

        except Exception as exc:
            # WARN not ERROR: a single tool failure is recoverable. The
            # investigation continues — the agent will try another approach.
            # ERROR is reserved for unrecoverable service-level failures.
            log.warning(
                "tool_execute_error",
                tool_name=self.schema["name"],
                error=str(exc),
                # error_type enables filtering WARNs by exception class in Grafana
                # (e.g., asyncpg.TooManyConnectionsError vs. ValueError).
                error_type=type(exc).__name__,
            )
            # Prefix with "Tool error (<name>)" so test assertions are unambiguous
            # and the LLM can detect the failure mode in its next Thought step.
            return f"Tool error ({self.schema['name']}): {str(exc)}"
