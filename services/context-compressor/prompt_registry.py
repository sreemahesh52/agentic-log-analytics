"""Minimal prompt registry for the context-compressor service.
This is a lightweight implementation that covers this service's needs.
Step 12 builds the full PromptRegistry module shared across all services.
This module will be replaced in Step 12 — do NOT add new features here.
Prompt file format:
  Lines starting with '#' are metadata headers (skipped by the loader).
  A single '---' separator marks the end of headers and start of content.
  Content uses Python .format(**variables) placeholders: {variable_name}.
Example prompt file (prompts/context_compressor/v1.txt):
  # model: gpt-3.5-turbo
  # temperature: 0.0
  # version: v1
  # purpose: Compress log context
  ---
  Compress these logs: {log_text}
"""

import os

import structlog

logger = structlog.get_logger(__name__)


class PromptRegistry:
    """File-based prompt loader. Reads from prompts_dir/{name}/{version}.txt.
    Interface designed to match the full PromptRegistry coming in Step 12:
      load(name, version, variables) — same signature, compatible replacement.
    Dependency Inversion: ContextCompressor depends on this interface, not on
    any specific file system layout. Step 12 swaps this implementation without
    changing ContextCompressor at all.
    """

    def __init__(self, prompts_dir: str) -> None:
        """Accept the root prompts directory path.
        Args:
            prompts_dir: absolute path to the prompts root, e.g. '/app/prompts'.
                         Each prompt lives at {prompts_dir}/{name}/{version}.txt.
        """
        # Store root directory. Never resolve paths lazily — catch misconfiguration
        # at construction time rather than on the first load call.
        self._prompts_dir = prompts_dir
        logger.debug("prompt_registry_initialised", prompts_dir=prompts_dir)

    def load(self, name: str, version: str, variables: dict) -> str:
        """Load a prompt file, strip headers, and render variable placeholders.
        Args:
            name: prompt name, e.g. 'context_compressor'. Maps to subdirectory.
            version: version string, e.g. 'v1'. Maps to filename {version}.txt.
            variables: dict of placeholder values. '{log_text}' → variables['log_text'].
        Returns:
            Rendered prompt string with all {placeholders} filled in.
        Raises:
            FileNotFoundError: if the prompt file does not exist.
            KeyError: if a required {placeholder} is missing from variables.
        """
        # os.path.join builds the path safely — no SQL or shell injection risk.
        prompt_path = os.path.join(self._prompts_dir, name, f"{version}.txt")

        try:
            with open(prompt_path, "r", encoding="utf-8") as fh:
                raw_content = fh.read()
        except FileNotFoundError:
            logger.error(
                "prompt_file_not_found",
                name=name,
                version=version,
                path=prompt_path,
            )
            raise

        # --- Parse: find the '---' separator, keep only body lines ---
        # Lines before '---' are metadata (# key: value) — discard them.
        # Lines after '---' are the prompt content — keep them.
        lines = raw_content.splitlines()
        body_lines: list[str] = []
        past_separator = False

        for line in lines:
            if not past_separator:
                # '---' marks the boundary between headers and prompt content.
                if line.strip() == "---":
                    past_separator = True
                continue
            body_lines.append(line)

        # Fallback: if no separator found, treat the entire file as the body.
        if not past_separator:
            body_lines = lines

        prompt_body = "\n".join(body_lines).strip()

        # --- Render variable placeholders ---
        # str.format(**variables) replaces {log_text}, etc.
        # KeyError propagates if a placeholder is missing — a programming error,
        # not a runtime condition to silently skip.
        rendered = prompt_body.format(**variables)

        logger.debug("prompt_loaded", name=name, version=version)
        return rendered
