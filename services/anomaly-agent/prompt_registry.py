"""Minimal prompt registry for the anomaly-agent service.
This is a lightweight implementation that covers the anomaly-agent's needs.
Step 12 builds the full PromptRegistry module shared across all services.
This module will be replaced in Step 12 — do NOT add new features here.
Prompt file format:
  Lines starting with '#' are metadata headers (skipped by the loader).
  A single '---' separator marks the end of headers and start of content.
  Content uses Python .format(**variables) placeholders: {variable_name}.
Example prompt file (prompts/anomaly_verifier/v1.txt):
  # model: gpt-3.5-turbo
  # temperature: 0.0
  # version: v1
  # purpose: Filter false positives
  ---
  Service: {service}
  Is this a real anomaly? Answer YES or NO.
"""

import os
import structlog

logger = structlog.get_logger(__name__)


class PromptRegistry:
    """File-based prompt loader. Reads from prompts_dir/{name}/{version}.txt.
    Interface designed to match the full PromptRegistry coming in Step 12:
      load(name, version, variables) — same signature, compatible replacement.
    Dependency Inversion: LLMVerifier depends on this interface, not on
    any specific file system layout or prompt format. Step 12 swaps this
    implementation without changing LLMVerifier at all.
    """

    def __init__(self, prompts_dir: str) -> None:
        """Accept the root prompts directory path.
        Args:
            prompts_dir: absolute path to the prompts root, e.g. '/app/prompts'.
                         Each prompt lives at {prompts_dir}/{name}/{version}.txt.
        """
        # Store the root directory — never resolve paths lazily to catch
        # misconfiguration at construction time, not at first load call.
        self._prompts_dir = prompts_dir
        logger.debug("prompt_registry_initialised", prompts_dir=prompts_dir)

    def load(self, name: str, version: str, variables: dict) -> str:
        """Load a prompt file, strip headers, and render variable placeholders.
        Args:
            name: prompt name, e.g. 'anomaly_verifier'. Maps to subdirectory.
            version: version string, e.g. 'v1'. Maps to filename {version}.txt.
            variables: dict of placeholder values. '{service}' → variables['service'].
        Returns:
            Rendered prompt string with all {placeholders} filled in.
        Raises:
            FileNotFoundError: if the prompt file does not exist.
            KeyError: if a required {placeholder} is missing from variables.
        """
        # Build the full path to the prompt file.
        # os.path.join is safe here — no SQL or shell injection risk from controlled inputs.
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

        # --- Parse: skip header lines, find the --- separator ---
        # Lines before '---' are metadata (# key: value) — strip them.
        # Lines after '---' are the prompt content — keep them.
        lines = raw_content.splitlines()
        body_lines: list[str] = []
        past_separator = False

        for line in lines:
            if not past_separator:
                # '---' marks the boundary between headers and prompt content.
                if line.strip() == "---":
                    past_separator = True
                # Skip header lines and the separator itself.
                continue
            body_lines.append(line)

        # If no separator found, treat the entire file as the prompt body.
        # This is a fallback for hand-written prompts that omit the header block.
        if not past_separator:
            body_lines = lines

        # Join and strip leading/trailing blank lines from the body.
        prompt_body = "\n".join(body_lines).strip()

        # --- Render variable placeholders ---
        # str.format(**variables) replaces {service}, {anomaly_description}, etc.
        # KeyError is raised (and propagated) if a placeholder is not in variables —
        # a missing variable is a programming error, not a runtime condition to swallow.
        rendered = prompt_body.format(**variables)

        logger.debug("prompt_loaded", name=name, version=version)
        return rendered

    def list_versions(self, name: str) -> list[str]:
        """Return available version strings for a named prompt.
        Returns an empty list if the prompt directory does not exist.
        Used for validation and discovery — not required for normal operation.
        """
        prompt_dir = os.path.join(self._prompts_dir, name)
        if not os.path.isdir(prompt_dir):
            return []
        # Collect filenames ending in .txt, strip the extension to get the version.
        # sorted ensures deterministic ordering for display and testing.
        return sorted(
            fname[:-4]  # strip '.txt'
            for fname in os.listdir(prompt_dir)
            if fname.endswith(".txt")
        )
