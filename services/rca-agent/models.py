"""
Pydantic v2 data models for the RCA Agent service.
this module only defines data shapes.
No business logic, no database calls, no Kafka publishing lives here.
Why Pydantic on LLM output?
LLMs are not reliable JSON serialisers. They hallucinate field names, omit
required fields, and return confidence as "high" instead of 0.85. Pydantic
validates every field at the system boundary between the LLM and the rest of
the platform. If model_validate passes, all downstream code can trust the
types and constraints without defensive checks.
Why separate RCAOutput from RCAResult?
RCAOutput is what the LLM must return — minimal, strict, described in the
prompt. RCAResult is what the system stores — RCAOutput plus metadata added
by the agent (token counts, latency, tenant context). Keeping them separate
means the LLM prompt never leaks internal system fields, and the storage
schema never constrains what we ask the LLM to output.
Why generate rca_id in Python rather than the database?
Generating the UUID in Python means the Kafka message and the database row
share the same rca_id before any DB write occurs. If the INSERT fails, the
DLQ consumer can correlate the Kafka message to the failed row by rca_id
without needing a SELECT.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# ReasoningStep — one Thought / Action / Observation cycle.
# ---------------------------------------------------------------------------


class ReasoningStep(BaseModel):
    """Records a single Thought → Action → Observation iteration of the ReAct loop.
    Stored in rca_results.reasoning_steps as a JSONB array so the UI can
    replay the full agent reasoning chain after the investigation completes.
    Why capture timestamp per step?
    Individual step timestamps enable per-tool latency analysis in Grafana.
    If BuildTimeline consistently takes 5 seconds but SearchKnowledgeBase
    takes 50 ms, that difference drives targeted optimisation decisions.
    """

    # str_strip_whitespace: trims leading/trailing whitespace from all string
    # fields automatically. Protects against LLM outputs with trailing newlines.
    model_config = ConfigDict(str_strip_whitespace=True)

    # ge=1: step_number=0 would create ambiguity with 0-indexed iteration
    # counters. Enforcing >= 1 keeps the UI display and log correlation clear.
    step_number: int = Field(ge=1)

    thought: str = Field(min_length=1)
    action: str = Field(min_length=1)

    # action_input can be a plain string (simple arg) or a structured dict
    # matching the tool's JSON Schema. OpenAI always returns tool arguments
    # as a JSON object, so dict is the common case; str supports edge cases.
    action_input: str | dict

    # observation is None until the tool call returns its result. The agent
    # sets this field after executing the tool and before emitting the step.
    observation: str | None = None

    # datetime.now(timezone.utc) returns a timezone-aware UTC
    # datetime. .isoformat appends the +00:00 offset explicitly.
    # Never: datetime.utcnow which returns a naive datetime with no tz info.
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


# ---------------------------------------------------------------------------
# RCAOutput — the exact JSON structure the LLM must emit.
# ---------------------------------------------------------------------------


class RCAOutput(BaseModel):
    """Minimal schema that the LLM is required to produce as its final answer.
    Kept deliberately small so the prompt can describe it in a few lines.
    The agent adds all additional metadata (tokens, latency, IDs) after
    validation — the LLM only needs to fill three fields.
    Why min_length=20 on root_cause?
    Forces a complete sentence, not a useless two-word response like "database
    error". Operators responding to a 3 AM incident alert need actionable text,
    not a label. The constraint is enforced at the Pydantic boundary before any
    downstream consumer sees the value.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    root_cause: str = Field(min_length=20)

    # ge=0.0, le=1.0: rejects hallucinated values like confidence=1.5 or
    # confidence=-0.1 that would corrupt the faithfulness scoring downstream.
    confidence: float = Field(ge=0.0, le=1.0)

    # min_length=1 ensures at least one recommendation is present. An empty
    # recommendations list is not actionable and indicates the LLM gave up.
    recommendations: list[str] = Field(min_length=1)


# ---------------------------------------------------------------------------
# RCAResult — the full persisted record after a successful investigation.
# ---------------------------------------------------------------------------


class RCAResult(BaseModel):
    """Complete RCA output stored in rca_results and published to agent.results.
    Combines LLM output (root_cause, confidence, recommendations) with
    agent-level metadata: token usage, latencies, cache/compression context,
    and the full reasoning chain. This is the authoritative record of one
    investigation run.
    """

    model_config = ConfigDict(
        str_strip_whitespace=True,
        # model_used starts with model_ which pydantic reserves for its own
        # config fields. Clearing protected_namespaces silences the warning.
        protected_namespaces=(),
    )

    # uuid4 generates a cryptographically random UUID. str formats it as
    # the standard 8-4-4-4-12 hyphenated representation expected by PostgreSQL.
    rca_id: str = Field(default_factory=lambda: str(uuid4()))

    tenant_id: str
    incident_id: str

    root_cause: str = Field(min_length=20)
    confidence: float = Field(ge=0.0, le=1.0)
    recommendations: list[str] = Field(min_length=1)

    # default_factory=list avoids the mutable-default-argument Python trap where
    # all instances share the same list object if `default=[]` were used instead.
    reasoning_steps: list[ReasoningStep] = Field(default_factory=list)

    model_used: str
    prompt_version: str

    # ge=0 on token counts: negative token values are physically impossible
    # and would corrupt per-tenant cost calculations downstream.
    input_tokens: int = Field(ge=0, default=0)
    output_tokens: int = Field(ge=0, default=0)

    cache_hit: bool = False

    # gt=0.0: a ratio of exactly 0 would imply infinite compression — not
    # physically possible. 1.0 means no compression was applied.
    compression_ratio: float = Field(gt=0.0, default=1.0)

    # Literal enforces the exact valid set at model validation time. Without it,
    # a typo like status='FAILED' would silently persist and break UI filters.
    status: Literal["success", "failed", "retried"] = "success"

    # failure_reason is None for successful investigations, populated by the
    # Step 13d consumer before writing to rca_results when status='failed'.
    failure_reason: str | None = None

    # Latency fields track where time was spent: total = llm + tool + overhead.
    # Separate tracking enables Grafana panels for per-component latency p95.
    total_latency_ms: int = Field(ge=0, default=0)
    llm_latency_ms: int = Field(ge=0, default=0)
    tool_latency_ms: int = Field(ge=0, default=0)

    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_db_dict(self) -> dict:
        """Return a dict suitable for an asyncpg parameterised INSERT statement.
        Converts reasoning_steps from list[ReasoningStep] to a JSON string
        because asyncpg requires JSONB column parameters to be provided as
        strings — the driver does not accept Python lists for JSONB columns
        without an explicit cast. The database handles deserialization.
        Returns:
            dict with all RCAResult fields, reasoning_steps as a JSON string.
        """
        d = self.model_dump()

        # --- Serialise reasoning steps for the JSONB column ---
        # model_dump returns a list of dicts, but asyncpg needs a string
        # for JSONB parameters. json.dumps produces the correct encoding.
        d["reasoning_steps"] = json.dumps(
            [step.model_dump() for step in self.reasoning_steps]
        )

        # --- Parse created_at string → datetime for asyncpg ---
        # The field is stored as an ISO 8601 string (str field type) but
        # asyncpg requires a datetime.datetime instance for TIMESTAMPTZ columns.
        # fromisoformat handles the +00:00 suffix produced by datetime.isoformat.
        if isinstance(d["created_at"], str):
            d["created_at"] = datetime.fromisoformat(d["created_at"])

        return d


# ---------------------------------------------------------------------------
# IncidentPayload — message arriving from incidents.ready via Kafka.
# ---------------------------------------------------------------------------


class IncidentPayload(BaseModel):
    """Represents one message consumed from the incidents.ready Kafka topic.
    The Model Router enriches each incident with model_id and prompt_variant
    before publishing to incidents.ready. The RCA Agent consumes this payload
    as its sole input — no other data source is consulted at startup.
    Why validate the Kafka message with Pydantic?
    A malformed payload cannot be investigated. Catching schema errors here
    (in the Kafka consumer before calling RCAAgent.run) routes the message
    immediately to rca.dlq with failure_reason='schema_validation_error',
    rather than causing a cryptic AttributeError deep inside the ReAct loop.
    """

    model_config = ConfigDict(
        str_strip_whitespace=True,
        # model_id starts with model_ which pydantic reserves for its own
        # config fields. Clearing protected_namespaces silences the warning.
        protected_namespaces=(),
    )

    incident_id: str
    tenant_id: str
    alert_ids: list[str]
    affected_services: list[str]
    is_cascade: bool

    # severity: one of LOW, MEDIUM, HIGH, CRITICAL — validated by Model Router
    # before publishing. Not re-validated here to avoid duplicate constraint.
    severity: str

    # model_id: the OpenAI model name selected by the Model Router.
    # Examples: 'gpt-4-turbo' (CRITICAL+premium), 'gpt-3.5-turbo' (standard).
    model_id: str

    # prompt_variant: 'v1' or 'v2' — selects the A/B prompt template.
    # Randomly assigned 50/50 by the Model Router for statistical validity.
    prompt_variant: str

    # compressed_context: log lines for the affected services, already
    # compressed by the Context Compressor if token count exceeded 6000.
    compressed_context: str

    # compression_ratio: 1.0 if no compression was applied; > 1.0 indicates
    # compression occurred. Forwarded to RCAResult for Grafana panel 17.
    compression_ratio: float = 1.0

    incident_description: str

    # created_at: UTC ISO 8601 timestamp assigned by the Alert Correlator.
    created_at: str

    # rca_id_hint: pre-generated UUID from the trigger endpoint (POST /investigations/trigger).
    # When set, the Kafka handler uses this value as rca_id instead of generating a new one.
    # This allows the UI to navigate to /investigations/{rca_id} immediately after triggering,
    # before the agent has completed its investigation. None for incidents arriving from the
    # normal model-router pipeline where the agent generates its own rca_id.
    rca_id_hint: str | None = None
