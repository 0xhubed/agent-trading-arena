"""Shared utilities for processing LLM responses.

All LLM output — trading decisions, forum posts, agentic reasoning —
should pass through strip_think_blocks() before use.

Note: Together AI models (GLM-5, GPT-OSS-120B) return clean content
without <think> blocks — GLM-5 puts reasoning in a separate `reasoning`
field. The strip functions are kept for compatibility with local models
(GLM-4.7, Nemotron) that emit inline <think> tags.
"""

from __future__ import annotations

import re


def strip_think_blocks(text: str) -> str:
    """Strip <think>...</think> chain-of-thought blocks from LLM output.

    Some models (e.g. local GLM-4.7, Nemotron via vLLM) emit chain-of-thought
    inside <think>...</think> tags. Together AI models (GLM-5, GPT-OSS-120B)
    return clean content, so this is a safe no-op for them.

    Must be applied before:
    - Parsing JSON decisions
    - Posting to the forum
    - Displaying to users

    Handles two variants:
    - ``<think>reasoning</think>content`` — both tags present
    - ``reasoning</think>content`` — opening tag consumed by vLLM chat template

    Args:
        text: Raw LLM response text.

    Returns:
        Text with think blocks removed and whitespace cleaned up.
    """
    if not text:
        return ""
    # First try full <think>...</think> blocks
    cleaned = re.sub(r"<think>.*?</think>\s*", "", text, flags=re.DOTALL)
    # If opening tag was consumed by chat template, strip everything up to </think>
    if "</think>" in cleaned:
        cleaned = re.sub(r".*?</think>\s*", "", cleaned, count=1, flags=re.DOTALL)
    return cleaned.strip()


# Pattern matching numbered reasoning steps that some models (e.g. GLM-4.7) emit
# without <think> tags, e.g. "1. **Analyze the Request:**\n..."
_REASONING_STEP_RE = re.compile(
    r"^\d+\.\s+\*\*(?:Analyz|Draft|Plan|Assess|Consider|Evaluat|Reason|Think|Review"
    r"|Identif|Summar|Structur|Formulat|Outlin|Approach|Determin|Break)",
    re.IGNORECASE,
)


def strip_reasoning_preamble(text: str) -> str:
    """Strip untagged reasoning preamble from LLM output.

    Some models output chain-of-thought as numbered steps
    (e.g. "1. **Analyze the Request:** ...") without <think> tags.
    This function detects that pattern and tries to extract the final
    output section. Safe no-op when no reasoning preamble is present.

    Should be called AFTER strip_think_blocks().

    Args:
        text: LLM output (already stripped of <think> blocks).

    Returns:
        The actual content with reasoning preamble removed.
    """
    if not text:
        return ""

    # Quick check: does it start with a numbered reasoning step?
    if not _REASONING_STEP_RE.match(text.strip()):
        return text

    # Split by top-level numbered sections (1. ... 2. ... 3. ...)
    sections = re.split(r"\n(?=\d+\.\s+\*\*)", text)

    if len(sections) < 2:
        return text

    # Look for the last section that contains the actual output.
    # Heuristics: the final draft/output section, or the longest
    # section that doesn't start with reasoning keywords.
    for section in reversed(sections):
        # Check if this section header suggests final output
        header_match = re.match(r"\d+\.\s+\*\*(.+?)\*\*", section)
        if header_match:
            header = header_match.group(1).lower()
            if any(
                kw in header
                for kw in ("final", "output", "post", "response", "result")
            ):
                # Extract content after the header line
                lines = section.split("\n", 1)
                if len(lines) > 1:
                    return lines[1].strip()

    # Fallback: if we can't identify the output section, return the
    # content after the last numbered section header
    last = sections[-1]
    lines = last.split("\n", 1)
    if len(lines) > 1:
        content = lines[1].strip()
        if content:
            return content

    # Nothing useful found, return original
    return text


def extract_json_from_llm(text: str) -> dict | None:
    """Extract a JSON object from LLM output, handling markdown fences.

    Tries in order:
    1. JSON inside ```json ... ``` fences
    2. JSON inside ``` ... ``` fences
    3. Bracket-balanced extraction starting from first '{'

    Returns parsed dict or None.
    """
    import json

    # 1. Try ```json ... ``` fence
    m = re.search(r"```json\s*\n?(\{[\s\S]*?\})\s*\n?```", text)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    # 2. Try ``` ... ``` fence (any language or none)
    m = re.search(r"```\w*\s*\n?(\{[\s\S]*?\})\s*\n?```", text)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    # 3. Bracket-balanced extraction from first '{'
    start = text.find("{")
    if start == -1:
        return None

    depth = 0
    for i in range(start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start : i + 1])
                except json.JSONDecodeError:
                    return None

    return None
