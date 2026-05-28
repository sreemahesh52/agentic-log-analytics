"""
Unit tests for PromptRegistry.
All tests use pytest's tmp_path fixture — real temporary directories, no
mocking of Path or open. This validates actual filesystem interactions
while keeping tests hermetic (no shared state, no external services).
Test naming convention: test_{what}_{expected_behaviour_under_what_condition}
Every test covers the happy path OR a specific failure path — never both.
"""

from __future__ import annotations

import pytest
from pathlib import Path

# Insert the rca-agent directory into sys.path before importing so that
# 'from prompt_registry import ...' resolves correctly regardless of where
# pytest is invoked from.
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from prompt_registry import PromptLoadError, PromptNotFoundError, PromptRegistry


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

def _write_prompt(
    base: Path,
    name: str,
    version: str,
    body: str,
    header: str | None = None,
) -> Path:
    """Helper: write a prompt file into the tmp_path-rooted directory.
    Creates the name/ subdirectory automatically so tests do not repeat
    the mkdir boilerplate.
    """
    # Construct the standard header if not explicitly supplied.
    if header is None:
        header = (
            f"# model: gpt-4-turbo\n"
            f"# version: {version}\n"
            f"# purpose: test prompt\n"
        )

    prompt_dir = base / name
    prompt_dir.mkdir(parents=True, exist_ok=True)

    file_path = prompt_dir / f"{version}.txt"
    # Write header + separator + body — mirrors real prompt file format.
    file_path.write_text(f"{header}---\n{body}", encoding="utf-8")
    return file_path


@pytest.fixture()
def prompts_dir(tmp_path: Path) -> Path:
    """Provide a clean, empty prompts root directory for each test."""
    return tmp_path / "prompts"


@pytest.fixture()
def registry_with_rca_v1(prompts_dir: Path) -> PromptRegistry:
    """Registry pre-loaded with a single rca_agent/v1.txt prompt."""
    prompts_dir.mkdir()
    _write_prompt(
        prompts_dir,
        "rca_agent",
        "v1",
        "Analyse {service} anomaly type {anomaly_type}.\nContext: {compressed_context}",
    )
    return PromptRegistry(prompts_dir)


# ---------------------------------------------------------------------------
# Happy path tests
# ---------------------------------------------------------------------------

def test_load_returns_rendered_prompt_with_variables(
    registry_with_rca_v1: PromptRegistry,
) -> None:
    """Variables passed to load are substituted into the prompt body."""
    result = registry_with_rca_v1.load(
        "rca_agent",
        "v1",
        {
            "service": "payment-service",
            "anomaly_type": "error_rate_spike",
            "compressed_context": "47 ERROR logs in 5 minutes",
        },
    )
    assert "payment-service" in result
    assert "error_rate_spike" in result
    assert "47 ERROR logs in 5 minutes" in result
    # Ensure no raw placeholder leaks remain.
    assert "{service}" not in result
    assert "{anomaly_type}" not in result
    assert "{compressed_context}" not in result


def test_load_returns_template_unchanged_when_no_variables(
    registry_with_rca_v1: PromptRegistry,
) -> None:
    """Calling load with variables=None returns the raw template with placeholders."""
    result = registry_with_rca_v1.load("rca_agent", "v1")
    # Placeholders must survive untouched when no variables are provided.
    assert "{service}" in result
    assert "{anomaly_type}" in result


def test_load_strips_header_before_dashes(prompts_dir: Path) -> None:
    """The metadata header (lines before '---') must not appear in the returned string."""
    prompts_dir.mkdir()
    _write_prompt(
        prompts_dir,
        "rca_agent",
        "v1",
        "Prompt body here.",
        header="# model: gpt-4-turbo\n# version: v1\n# purpose: strip test\n",
    )
    registry = PromptRegistry(prompts_dir)
    result = registry.load("rca_agent", "v1")

    # Header metadata must not appear in the prompt sent to the LLM.
    assert "# model:" not in result
    assert "# version:" not in result
    assert "# purpose:" not in result
    # The separator itself must not appear in the returned body.
    assert result.strip()[:3] != "---"
    # The body text must be present.
    assert "Prompt body here." in result


def test_load_returns_full_content_when_no_separator(prompts_dir: Path) -> None:
    """Files without a '---' separator are returned in full as the prompt body."""
    prompts_dir.mkdir()
    prompt_dir = prompts_dir / "bare_prompt"
    prompt_dir.mkdir()
    (prompt_dir / "v1.txt").write_text("No header here. Just prompt.", encoding="utf-8")

    registry = PromptRegistry(prompts_dir)
    result = registry.load("bare_prompt", "v1")
    assert result == "No header here. Just prompt."


def test_list_versions_returns_sorted_list(prompts_dir: Path) -> None:
    """list_versions returns stems of all .txt files, sorted alphabetically."""
    prompts_dir.mkdir()
    for version in ("v2", "v1", "v3"):
        _write_prompt(prompts_dir, "rca_agent", version, f"Body for {version}")

    registry = PromptRegistry(prompts_dir)
    versions = registry.list_versions("rca_agent")
    assert versions == ["v1", "v2", "v3"]


def test_list_versions_returns_empty_for_missing_name(prompts_dir: Path) -> None:
    """list_versions returns [] (not an error) when the name directory is absent."""
    prompts_dir.mkdir()
    registry = PromptRegistry(prompts_dir)
    # No exception should be raised — empty list signals 'no versions found'.
    assert registry.list_versions("nonexistent_prompt") == []


# ---------------------------------------------------------------------------
# Safe substitution tests
# ---------------------------------------------------------------------------

def test_safe_substitution_leaves_unmatched_placeholders_unchanged(
    prompts_dir: Path,
) -> None:
    """Providing only some variables must leave unmatched {placeholders} intact.
    Standard str.format raises KeyError for missing keys. SafeDict prevents
    this so partial variable sets are usable — critical when the same template
    is shared across pipeline stages that supply different context subsets.
    """
    prompts_dir.mkdir()
    _write_prompt(
        prompts_dir,
        "rca_agent",
        "v1",
        "Service: {service}. Type: {anomaly_type}. Unknown: {unknown_key}",
    )
    registry = PromptRegistry(prompts_dir)

    # Only supply {service} — {anomaly_type} and {unknown_key} are not provided.
    result = registry.load("rca_agent", "v1", {"service": "auth-service"})

    assert "auth-service" in result
    # Unmatched placeholders must survive unchanged — no KeyError, no blank substitution.
    assert "{anomaly_type}" in result
    assert "{unknown_key}" in result


# ---------------------------------------------------------------------------
# Error path tests
# ---------------------------------------------------------------------------

def test_load_raises_prompt_not_found_for_missing_version(
    registry_with_rca_v1: PromptRegistry,
) -> None:
    """Loading a non-existent version raises PromptNotFoundError with name+version set."""
    with pytest.raises(PromptNotFoundError) as exc_info:
        registry_with_rca_v1.load("rca_agent", "v99")

    assert exc_info.value.name == "rca_agent"
    assert exc_info.value.version == "v99"
    assert "rca_agent/v99" in str(exc_info.value)


def test_load_raises_prompt_not_found_for_missing_name(
    registry_with_rca_v1: PromptRegistry,
) -> None:
    """Loading a completely unknown prompt name raises PromptNotFoundError."""
    with pytest.raises(PromptNotFoundError) as exc_info:
        registry_with_rca_v1.load("nonexistent_prompt", "v1")

    assert exc_info.value.name == "nonexistent_prompt"
    assert exc_info.value.version == "v1"


def test_raises_prompt_load_error_on_missing_directory() -> None:
    """Constructing PromptRegistry with a non-existent directory raises PromptLoadError.
    This enforces fail-fast behaviour at startup: misconfigured PROMPTS_DIR
    is caught immediately, not on the first LLM call minutes into service operation.
    """
    with pytest.raises(PromptLoadError) as exc_info:
        PromptRegistry("/tmp/this_path_absolutely_does_not_exist_12345")

    assert "not found" in str(exc_info.value).lower()


# ---------------------------------------------------------------------------
# Caching tests
# ---------------------------------------------------------------------------

def test_caches_loaded_prompt_reads_file_only_once(prompts_dir: Path) -> None:
    """After the first load, modifying the file does not change the returned content.
    The in-memory cache exists precisely to avoid file I/O on every LLM call.
    This test verifies that the cache is actually used — the stale content from
    before the file modification is returned on the second call.
    """
    prompts_dir.mkdir()
    file_path = _write_prompt(prompts_dir, "rca_agent", "v1", "Original body.")
    registry = PromptRegistry(prompts_dir)

    # First load reads from disk and caches the result.
    first_result = registry.load("rca_agent", "v1")
    assert "Original body." in first_result

    # Overwrite the file on disk.
    file_path.write_text(
        "# model: gpt-4-turbo\n---\nModified body.", encoding="utf-8"
    )

    # Second load must return the cached (original) content, not the new file content.
    second_result = registry.load("rca_agent", "v1")
    assert "Original body." in second_result
    assert "Modified body." not in second_result


def test_invalidate_cache_forces_reload(prompts_dir: Path) -> None:
    """After invalidate_cache, the next load re-reads from disk.
    This is the only intended mechanism for picking up prompt changes during
    development without restarting the service process.
    """
    prompts_dir.mkdir()
    file_path = _write_prompt(prompts_dir, "rca_agent", "v1", "Original body.")
    registry = PromptRegistry(prompts_dir)

    # Prime the cache.
    first_result = registry.load("rca_agent", "v1")
    assert "Original body." in first_result

    # Modify the file, then invalidate the cache.
    file_path.write_text(
        "# model: gpt-4-turbo\n---\nModified body after invalidation.", encoding="utf-8"
    )
    registry.invalidate_cache()

    # After invalidation, load must re-read from disk and return the new content.
    after_invalidation = registry.load("rca_agent", "v1")
    assert "Modified body after invalidation." in after_invalidation
    assert "Original body." not in after_invalidation
