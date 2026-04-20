"""Codegen orchestrator — uses Claude Sonnet to generate targeted code fixes.

Receives structured findings from the journal analysis, builds context,
and drives an Anthropic tool_use loop to make minimal, surgical edits.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import anthropic

from agent_arena.codegen.findings import Finding
from agent_arena.codegen.tools import TOOL_SCHEMAS, process_tool_call

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-6"
ESCALATION_MODEL = "claude-opus-4-6"
MAX_TOOL_ROUNDS = 10

SYSTEM_PROMPT = """\
You are a code-maintenance agent for Agent Arena, an AI crypto-trading \
simulation platform.

You receive a structured **finding** from the Observer Journal — a \
recurring problem detected across multiple daily journal entries.  Your \
job is to make minimal, surgical code changes that address the finding.

## Rules

1. **Read before edit** — always read a target file before modifying it.
2. **Minimal changes only** — tweak thresholds, adjust character prompts, \
   tune weights.  Never add new files or delete existing ones.
3. **Cite evidence** — when you explain your changes, reference the \
   specific journal numbers from the evidence provided.
4. **Protected files** — the edit_file tool will refuse writes to core \
   infrastructure (arena, runner, storage, api, cli, codegen).  Do not \
   attempt to edit them.
5. **One finding at a time** — focus on the single finding provided.
6. **Do NOT over-engineer** — if a 2-line tweak solves it, stop there.

## Finding types and suggested fixes

- **overtrading**: Add "patience" language to agent character prompts \
  in configs/production.yaml, or increase hold bias.
- **high_conf_bad_pnl**: Lower default confidence or add hedging \
  language to the agent character prompt.
- **rr_inversion**: Adjust default stop-loss / take-profit ratios in \
  evolution/genome.py.
- **skill_underperform**: Adjust skill loading weights or prompt \
  emphasis in agents/skill_aware_llm.py.
- **forum_echo**: Reduce forum influence weight or add contrarian \
  weighting in agents/forum_aware_llm.py.

After making your changes, output a brief summary of what you changed \
and why, citing the journal evidence.
"""

ESCALATION_SYSTEM_PROMPT = """\
You are diagnosing why automated code fixes have failed for an AI \
crypto-trading simulation called Agent Arena.

A codegen agent has attempted to fix a recurring problem by tweaking \
character prompts, thresholds, and weights — but the problem persists \
after {prior_fix_count} attempts.  Prompt-level changes are clearly \
insufficient.

Your job is to:
1. **Diagnose** why prompt-level fixes failed (LLMs ignore character \
   instructions? Threshold is in the wrong place? The root cause is \
   architectural?)
2. **Suggest a structural fix** — e.g., adding a gate in the runner, \
   injecting data into context, modifying the decision pipeline, \
   changing the agent architecture.
3. **Be specific** — name files, functions, and the kind of change \
   needed.  Do NOT suggest more prompt tweaks.

Output a concise diagnosis (2-4 paragraphs) with:
- Why prompt fixes don't work for this problem
- What structural change would actually fix it
- Which files/functions to modify
"""


@dataclass
class CodeChange:
    """Record of a single file edit."""

    file_path: str
    description: str


@dataclass
class CodegenResult:
    """Result from a codegen run for one finding."""

    finding_id: str
    changes: list[CodeChange] = field(default_factory=list)
    summary: str = ""
    error: str = ""


@dataclass
class CodegenEscalation:
    """Result from diagnosing a stale fix that needs human intervention."""

    finding_id: str
    stale_finding_id: str  # the original finding that keeps recurring
    reason: str
    evidence: list[str] = field(default_factory=list)
    prior_fix_count: int = 0
    suggested_approach: str = ""  # LLM-generated structural fix suggestion


class CodegenAgent:
    """Drives Claude Sonnet to fix a single journal finding."""

    def __init__(self, *, project_root: str = "."):
        self.client = anthropic.Anthropic()
        self.project_root = project_root

    def run(self, finding: Finding) -> CodegenResult:
        """Execute the tool-use loop for a single finding."""
        result = CodegenResult(finding_id=finding.finding_id)

        user_prompt = self._build_prompt(finding)
        messages: list[dict] = [{"role": "user", "content": user_prompt}]

        for _ in range(MAX_TOOL_ROUNDS):
            try:
                response = self.client.messages.create(
                    model=MODEL,
                    max_tokens=4096,
                    system=SYSTEM_PROMPT,
                    tools=TOOL_SCHEMAS,
                    messages=messages,
                )
            except Exception as exc:
                result.error = f"Anthropic API error: {exc}"
                logger.error("Codegen API error: %s", exc)
                break

            # Collect text blocks for the summary
            text_parts: list[str] = []
            tool_uses: list[dict] = []
            for block in response.content:
                if block.type == "text":
                    text_parts.append(block.text)
                elif block.type == "tool_use":
                    tool_uses.append({
                        "id": block.id,
                        "name": block.name,
                        "input": block.input,
                    })

            # If no tool calls, we're done
            if not tool_uses:
                result.summary = "\n".join(text_parts)
                break

            # Append assistant message with all content blocks
            messages.append({"role": "assistant", "content": response.content})

            # Process each tool call and collect results
            tool_results = []
            for tc in tool_uses:
                tool_output = process_tool_call(
                    tc["name"],
                    tc["input"],
                    project_root=self.project_root,
                )
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tc["id"],
                    "content": tool_output,
                })

                # Track edits
                if tc["name"] == "edit_file" and tool_output.startswith("OK"):
                    result.changes.append(CodeChange(
                        file_path=tc["input"]["path"],
                        description=tool_output,
                    ))

            messages.append({"role": "user", "content": tool_results})
        else:
            # Exhausted rounds — capture whatever text we have
            if not result.summary:
                result.summary = "(max tool rounds reached)"

        return result

    def escalate(self, finding: Finding) -> CodegenEscalation:
        """Diagnose a stale fix and produce an escalation for human review."""
        escalation = CodegenEscalation(
            finding_id=finding.finding_id,
            stale_finding_id=finding.stale_finding_id,
            evidence=finding.evidence,
            prior_fix_count=finding.prior_fix_count,
            reason=(
                f"'{finding.stale_finding_id}' has been fixed "
                f"{finding.prior_fix_count} times by codegen but "
                f"still recurs."
            ),
        )

        prompt = (
            f"## Stale Fix: {finding.stale_finding_id}\n\n"
            f"This finding has been 'fixed' {finding.prior_fix_count} "
            f"times by codegen (character prompt tweaks, threshold "
            f"adjustments) but it keeps recurring.\n\n"
            f"### Evidence\n"
        )
        for ev in finding.evidence[:8]:
            prompt += f"- {ev}\n"
        prompt += (
            f"\n### Affected agents\n"
            f"{', '.join(finding.agent_ids) or 'system-wide'}\n\n"
            f"Diagnose why prompt-level fixes failed and suggest a "
            f"structural alternative."
        )

        system = ESCALATION_SYSTEM_PROMPT.format(
            prior_fix_count=finding.prior_fix_count,
        )

        try:
            response = self.client.messages.create(
                model=ESCALATION_MODEL,
                max_tokens=2048,
                system=system,
                messages=[{"role": "user", "content": prompt}],
            )
            text = "\n".join(
                b.text for b in response.content if b.type == "text"
            )
            escalation.suggested_approach = text
        except Exception as exc:
            logger.error("Escalation LLM call failed: %s", exc)
            escalation.suggested_approach = (
                f"(LLM call failed: {exc}. Manual diagnosis required.)"
            )

        return escalation

    @staticmethod
    def _build_prompt(finding: Finding) -> str:
        """Build the user message for the codegen LLM."""
        lines = [
            f"## Finding: {finding.finding_id}",
            f"**Severity:** {finding.severity}",
            f"**Affected agents:** {', '.join(finding.agent_ids) or 'system-wide'}",
            f"**Detected in entries:** {', '.join(finding.entry_dates)}",
            "",
            "### Evidence",
        ]
        for ev in finding.evidence:
            lines.append(f"- {ev}")

        lines.append("")
        lines.append("### Target files")
        for tf in finding.target_files:
            lines.append(f"- `{tf}`")

        lines.append("")
        lines.append(
            "Read the target files, then make minimal edits to address "
            "this finding.  Explain your changes when done."
        )
        return "\n".join(lines)
