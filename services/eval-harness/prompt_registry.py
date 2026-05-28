"""
PromptRegistry — loads, caches, and renders versioned prompt templates.
Every LLM call in the platform goes through this module. Prompts live in
the prompts/ directory as plain text files with a metadata header section
separated from the prompt body by a '---' line.
Design rationale: keeping prompts in files (not code) means they can be
version-controlled, diff'd, and updated without touching Python source.
The registry adds in-memory caching so repeated LLM calls in a single
agent run never touch the filesystem more than once per prompt file.
"""

from __future__ import annotations

import re
import structlog
from pathlib import Path
from typing import Optional

# structlog provides structured JSON output — every log entry is machine-parseable.
logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Custom exceptions — typed so callers can handle each failure mode distinctly.
# ---------------------------------------------------------------------------

class PromptNotFoundError(Exception):
    """Raised when the requested prompt name/version file does not exist.
    Typed separately from PromptLoadError so callers can distinguish
    'the file is missing' from 'the file exists but could not be read'.
    """

    def __init__(self, name: str, version: str) -> None:
        # Store name/version as attributes so callers can log or retry them.
        super().__init__(f"Prompt '{name}/{version}' not found")
        self.name = name
        self.version = version


class PromptLoadError(Exception):
    """Raised when a prompt directory or file exists but cannot be read.
    Wraps the underlying OSError so the root cause is never silently lost.
    """


# ---------------------------------------------------------------------------
# PromptRegistry
# ---------------------------------------------------------------------------

class PromptRegistry:
    """Loads versioned prompt templates from the filesystem with in-memory caching.
    Interface segregation: this class only loads and renders prompts.
    It has no knowledge of OpenAI, Kafka, or PostgreSQL.
    Dependency inversion: callers receive a PromptRegistry instance via
    constructor injection — no service instantiates this directly.
    File layout expected on disk:
        prompts/
          {name}/
            {version}.txt ← metadata header + '---' separator + prompt body
    """

    def __init__(self, prompts_dir: str | Path) -> None:
        """Resolve and validate the prompts directory at construction time.
        Failing fast here (rather than on first load) means a misconfigured
        PROMPTS_DIR environment variable is caught at service startup, not
        during the first RCA investigation.
        Args:
            prompts_dir: Path to the directory containing prompt subdirectories.
        Raises:
            PromptLoadError: If the directory does not exist.
        """
        # .resolve converts relative paths and symlinks to an absolute path.
        self._prompts_dir = Path(prompts_dir).resolve()

        if not self._prompts_dir.exists():
            raise PromptLoadError(
                f"Prompts directory not found: {self._prompts_dir}"
            )

        # Cache keyed by (name, version) tuple — tuples are hashable, strings are not.
        self._cache: dict[tuple[str, str], str] = {}

        logger.info(
            "prompt_registry_initialised",
            prompts_dir=str(self._prompts_dir),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(
        self,
        name: str,
        version: str,
        variables: Optional[dict] = None,
    ) -> str:
        """Return the rendered prompt body for (name, version).
        The metadata header (lines before '---') is stripped; only the body
        below the separator is returned. Variable placeholders in the body
        are substituted using SafeDict so unmatched keys remain intact.
        Args:
            name: Subdirectory name under prompts_dir (e.g. 'rca_agent').
            version: Filename stem without extension (e.g. 'v1').
            variables: Optional dict of placeholder substitutions.
        Returns:
            Rendered prompt string with placeholders filled.
        Raises:
            PromptNotFoundError: If the file does not exist.
            PromptLoadError: If the file exists but cannot be read.
        """
        cache_key = (name, version)

        # --- Cache lookup ---
        # On a cache miss we read from disk exactly once; subsequent calls
        # return the cached template string.
        if cache_key not in self._cache:
            self._load_into_cache(name, version, cache_key)

        template = self._cache[cache_key]

        # --- Variable substitution ---
        if variables is None:
            # Return the raw template unchanged — caller may substitute later.
            return template

        # Regex replacement instead of str.format_map:
        # format_map raises ValueError on any literal {text} in the template
        # (e.g. JSON examples in prompts like {"confidence": 0.0-1.0}).
        # The regex \{(\w+)\} only matches single-word identifiers — {service},
        # {anomaly_type}, {compressed_context} — and leaves JSON curly braces
        # like {"key": value} completely untouched.
        def _replace(match: re.Match) -> str:
            key = match.group(1)
            return str(variables[key]) if key in variables else match.group(0)

        return re.sub(r"\{(\w+)\}", _replace, template)

    def list_versions(self, name: str) -> list[str]:
        """Return sorted list of available version stems for a prompt name.
        Returns an empty list (not an error) if the name directory does not
        exist, so callers can check availability without exception handling.
        Args:
            name: Prompt subdirectory name (e.g. 'rca_agent').
        Returns:
            Sorted list of version strings (e.g. ['v1', 'v2']).
        """
        dir_path = self._prompts_dir / name

        if not dir_path.exists():
            # Empty list signals 'no versions' without raising — callers can
            # distinguish this from PromptNotFoundError on a specific version.
            return []

        # .stem strips the .txt extension; sort gives deterministic ordering.
        return sorted(p.stem for p in dir_path.glob("*.txt"))

    def invalidate_cache(self) -> None:
        """Clear the in-memory cache so subsequent loads re-read from disk.
        Call this during development when prompt files change between calls.
        In production the cache persists for the service lifetime; restating
        the service is the intended mechanism for picking up prompt changes.
        """
        self._cache.clear()
        logger.info("prompt_cache_invalidated")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _load_into_cache(
        self, name: str, version: str, cache_key: tuple[str, str]
    ) -> None:
        """Read the prompt file from disk, strip its header, and cache the body.
        Args:
            name: Prompt subdirectory name.
            version: Version stem.
            cache_key: Pre-built tuple key for self._cache.
        Raises:
            PromptNotFoundError: If the .txt file does not exist.
            PromptLoadError: If the file exists but read fails.
        """
        # Build the expected path: prompts_dir / name / version.txt
        path = self._prompts_dir / name / f"{version}.txt"

        if not path.exists():
            logger.warning(
                "prompt_not_found",
                name=name,
                version=version,
                path=str(path),
            )
            raise PromptNotFoundError(name, version)

        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            # Wrap OSError so callers see PromptLoadError, not a bare OS exception.
            raise PromptLoadError(
                f"Failed to read prompt file {path}: {exc}"
            ) from exc

        # --- Header stripping ---
        # Prompt files use a YAML-like header separated from the body by '---\n'.
        # The header documents model, temperature, and purpose — it is NOT part
        # of the executable prompt sent to the LLM.
        if "---\n" in raw:
            # split(..., 1) stops at the first separator so '---' in prompt body is safe.
            body = raw.split("---\n", 1)[1]
        else:
            # No separator found — treat the entire file as the prompt body.
            body = raw

        self._cache[cache_key] = body

        logger.debug(
            "prompt_loaded",
            name=name,
            version=version,
            body_length=len(body),
        )
