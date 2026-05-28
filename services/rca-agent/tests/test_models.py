"""
Unit tests for services/rca-agent/models.py.
every test mocks no external dependencies (none are needed here —
models.py is pure Pydantic with no I/O). All tests run in-memory with zero
infrastructure.
Test naming convention:
  test_{class_or_function}_{expected_behaviour}
Each test covers exactly one behaviour: either the happy path OR one specific
failure path. Combined tests that check both "valid input works" and "invalid
input raises" in the same function are split here into separate tests so that
a single failure does not mask another assertion.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

# --- sys.path setup ---
# Insert the rca-agent directory so that 'from models import ...' resolves
# when pytest is invoked from any working directory.
sys.path.insert(0, str(Path(__file__).parent.parent))

from models import RCAOutput, RCAResult, ReasoningStep


# ---------------------------------------------------------------------------
# Fixtures — shared valid inputs reused across multiple tests.
# ---------------------------------------------------------------------------


@pytest.fixture()
def valid_rca_kwargs() -> dict:
    """Return the minimum valid keyword arguments for constructing an RCAResult.
    Using a fixture (not a module-level dict) prevents shared mutable state
    between tests — each test gets a fresh copy via fixture injection.
    """
    return {
        "tenant_id": "tenant-abc",
        "incident_id": "incident-xyz",
        "root_cause": "Database connection pool exhausted due to long-running queries",
        "confidence": 0.85,
        "recommendations": ["Increase pool size to 20", "Add query timeout of 30s"],
        "reasoning_steps": [],
        "model_used": "gpt-4-turbo",
        "prompt_version": "v1",
    }


@pytest.fixture()
def valid_reasoning_step() -> ReasoningStep:
    """Return a fully populated ReasoningStep for tests that need a real step."""
    return ReasoningStep(
        step_number=1,
        thought="I should query recent logs to understand the error pattern.",
        action="QueryLogs",
        action_input={"service": "payment-service", "level": "ERROR"},
        observation="Found 47 ERROR entries in the last 5 minutes.",
    )


# ---------------------------------------------------------------------------
# Test: RCAResult happy path
# ---------------------------------------------------------------------------


def test_valid_rca_result_created_successfully(valid_rca_kwargs: dict) -> None:
    """RCAResult is constructed without error when all required fields are valid."""
    result = RCAResult(**valid_rca_kwargs)

    # Verify the fields were stored correctly — not just that no exception was raised.
    assert result.tenant_id == "tenant-abc"
    assert result.incident_id == "incident-xyz"
    assert result.confidence == 0.85
    assert result.status == "success"
    assert result.cache_hit is False
    assert result.compression_ratio == 1.0

    # rca_id must be auto-generated (non-empty UUID-format string).
    assert len(result.rca_id) == 36
    assert result.rca_id.count("-") == 4


# ---------------------------------------------------------------------------
# Test: root_cause min_length constraint
# ---------------------------------------------------------------------------


def test_root_cause_min_length_enforced(valid_rca_kwargs: dict) -> None:
    """RCAResult raises ValidationError when root_cause is shorter than 20 characters."""
    # "DB error" is 8 chars — well below the 20-char minimum that forces a
    # complete sentence useful to an on-call engineer.
    valid_rca_kwargs["root_cause"] = "DB error"

    with pytest.raises(ValidationError) as exc_info:
        RCAResult(**valid_rca_kwargs)

    # Verify the error targets the correct field, not a different constraint.
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("root_cause",) for e in errors)


# ---------------------------------------------------------------------------
# Test: confidence range constraints
# ---------------------------------------------------------------------------


def test_confidence_out_of_range_raises_when_above_one(
    valid_rca_kwargs: dict,
) -> None:
    """RCAResult raises ValidationError when confidence exceeds 1.0."""
    valid_rca_kwargs["confidence"] = 1.5

    with pytest.raises(ValidationError) as exc_info:
        RCAResult(**valid_rca_kwargs)

    errors = exc_info.value.errors()
    assert any(e["loc"] == ("confidence",) for e in errors)


def test_confidence_out_of_range_raises_when_negative(
    valid_rca_kwargs: dict,
) -> None:
    """RCAResult raises ValidationError when confidence is negative."""
    valid_rca_kwargs["confidence"] = -0.1

    with pytest.raises(ValidationError) as exc_info:
        RCAResult(**valid_rca_kwargs)

    errors = exc_info.value.errors()
    assert any(e["loc"] == ("confidence",) for e in errors)


# ---------------------------------------------------------------------------
# Test: recommendations must be non-empty
# ---------------------------------------------------------------------------


def test_empty_recommendations_raises(valid_rca_kwargs: dict) -> None:
    """RCAResult raises ValidationError when recommendations list is empty."""
    valid_rca_kwargs["recommendations"] = []

    with pytest.raises(ValidationError) as exc_info:
        RCAResult(**valid_rca_kwargs)

    errors = exc_info.value.errors()
    assert any(e["loc"] == ("recommendations",) for e in errors)


# ---------------------------------------------------------------------------
# Test: created_at is UTC ISO 8601 format
# ---------------------------------------------------------------------------


def test_created_at_is_utc_iso8601(valid_rca_kwargs: dict) -> None:
    """RCAResult.created_at contains a UTC ISO 8601 timestamp with timezone offset."""
    result = RCAResult(**valid_rca_kwargs)

    # isoformat on a UTC-aware datetime produces "+00:00" suffix.
    # Verify this is present so consumers know the timezone explicitly.
    assert "+00:00" in result.created_at or result.created_at.endswith("Z"), (
        f"created_at '{result.created_at}' does not contain a UTC offset"
    )

    # Verify the string is parseable as a real datetime — not just any string.
    # fromisoformat raises ValueError if the format is invalid.
    parsed = datetime.fromisoformat(result.created_at)
    assert parsed.tzinfo is not None, "created_at must be timezone-aware"

    # The offset must be UTC (zero offset).
    utc_offset = parsed.utcoffset()
    from datetime import timedelta
    assert utc_offset == timedelta(0), (
        f"created_at offset {utc_offset} is not UTC"
    )


# ---------------------------------------------------------------------------
# Test: to_db_dict serialises reasoning_steps as a JSON string
# ---------------------------------------------------------------------------


def test_to_db_dict_serialises_reasoning_steps_as_json_string(
    valid_rca_kwargs: dict,
    valid_reasoning_step: ReasoningStep,
) -> None:
    """to_db_dict converts reasoning_steps from list[ReasoningStep] to a JSON string.
    asyncpg requires JSONB column parameters to be passed as strings, not as
    Python lists. This test verifies the conversion is correct and reversible.
    """
    valid_rca_kwargs["reasoning_steps"] = [valid_reasoning_step]
    result = RCAResult(**valid_rca_kwargs)

    db_dict = result.to_db_dict()

    # reasoning_steps must be a string (for asyncpg JSONB parameter binding).
    assert isinstance(db_dict["reasoning_steps"], str), (
        f"Expected str, got {type(db_dict['reasoning_steps'])}"
    )

    # The string must be valid JSON that round-trips to the original step data.
    parsed_steps = json.loads(db_dict["reasoning_steps"])
    assert len(parsed_steps) == 1
    assert parsed_steps[0]["step_number"] == 1
    assert parsed_steps[0]["action"] == "QueryLogs"
    assert parsed_steps[0]["observation"] == "Found 47 ERROR entries in the last 5 minutes."


# ---------------------------------------------------------------------------
# Test: compression_ratio must be strictly positive
# ---------------------------------------------------------------------------


def test_compression_ratio_must_be_positive(valid_rca_kwargs: dict) -> None:
    """RCAResult raises ValidationError when compression_ratio is zero or negative."""
    valid_rca_kwargs["compression_ratio"] = 0.0

    with pytest.raises(ValidationError) as exc_info:
        RCAResult(**valid_rca_kwargs)

    errors = exc_info.value.errors()
    assert any(e["loc"] == ("compression_ratio",) for e in errors)


def test_compression_ratio_negative_raises(valid_rca_kwargs: dict) -> None:
    """RCAResult raises ValidationError when compression_ratio is negative."""
    valid_rca_kwargs["compression_ratio"] = -1.5

    with pytest.raises(ValidationError) as exc_info:
        RCAResult(**valid_rca_kwargs)

    errors = exc_info.value.errors()
    assert any(e["loc"] == ("compression_ratio",) for e in errors)


# ---------------------------------------------------------------------------
# Test: ReasoningStep timestamp is UTC
# ---------------------------------------------------------------------------


def test_reasoning_step_timestamp_is_utc() -> None:
    """ReasoningStep.timestamp is auto-generated as a UTC ISO 8601 string."""
    from datetime import datetime, timedelta

    step = ReasoningStep(
        step_number=1,
        thought="Analysing the error pattern in logs.",
        action="QueryLogs",
        action_input={"service": "auth-service"},
    )

    # Timestamp must contain the UTC offset indicator.
    assert "+00:00" in step.timestamp, (
        f"timestamp '{step.timestamp}' is missing UTC offset"
    )

    # Must be parseable as a real datetime.
    parsed = datetime.fromisoformat(step.timestamp)
    assert parsed.tzinfo is not None

    # Offset must be exactly zero (UTC).
    assert parsed.utcoffset() == timedelta(0)


# ---------------------------------------------------------------------------
# Test: RCAOutput min_length on root_cause
# ---------------------------------------------------------------------------


def test_rca_output_root_cause_min_length_enforced() -> None:
    """RCAOutput raises ValidationError when root_cause is too short."""
    with pytest.raises(ValidationError) as exc_info:
        RCAOutput(
            root_cause="short",  # 5 chars — below 20-char minimum
            confidence=0.9,
            recommendations=["Fix the database connection pool"],
        )

    errors = exc_info.value.errors()
    assert any(e["loc"] == ("root_cause",) for e in errors)
