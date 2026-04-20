"""Forum-Aware LLM Trader - Local inference agent with forum witness summaries.

Extends SkillAwareLLMTrader with forum witness consumption, allowing local
inference agents to benefit from Observer-analyzed forum discussions.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from agent_arena.agents.skill_aware_llm import SkillAwareLLMTrader
from agent_arena.core.models import Decision
from agent_arena.forum.service import ForumService

logger = logging.getLogger(__name__)


class ForumAwareLLMTrader(SkillAwareLLMTrader):
    """
    Forum-aware trading agent using any OpenAI-compatible API endpoint.

    Extends SkillAwareLLMTrader with:
    1. Access to witness summaries from forum analysis
    2. Forum-aware system prompt
    3. Optional trade rationale posting to forum

    Example config:
        config:
            model: gpt-oss-120b
            base_url: https://api.together.xyz/v1
            witness_lookback_hours: 6
            min_witness_confidence: 0.6
            post_to_forum: true
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        super().__init__(agent_id, name, config)
        config = config or {}

        self.witness_lookback_hours = config.get("witness_lookback_hours", 6)
        self.min_witness_confidence = config.get("min_witness_confidence", 0.6)
        self.post_to_forum = config.get("post_to_forum", True)
        self.min_post_confidence = config.get("min_post_confidence", 0.7)

        self._forum: Optional[ForumService] = None
        self._witness_consulted_count = 0

    def _get_forum(self) -> Optional[ForumService]:
        """Lazy-load forum service from storage."""
        if self._forum is None:
            if not hasattr(self, "_memory_store") or not hasattr(
                self._memory_store, "_storage"
            ):
                logger.warning("Forum service unavailable - storage not initialized")
                return None
            self._forum = ForumService(self._memory_store._storage)
        return self._forum

    async def _load_witness_summaries(self, context: dict) -> list:
        """Load recent witness summaries from database."""
        try:
            forum = self._get_forum()
            if not forum:
                return []

            symbols = list(context.get("market", {}).keys())
            return await forum.get_recent_witness_summaries(
                hours=self.witness_lookback_hours,
                symbols=symbols,
                min_confidence=self.min_witness_confidence,
            )
        except Exception as e:
            logger.warning("Failed to load witness summaries: %s", e)
            return []

    def _format_witness_for_context(self, witness_summaries: list) -> str:
        """Format witness summaries as compact context addition."""
        if not witness_summaries:
            return "No recent forum insights available."

        lines = ["**FORUM WITNESS** (Observer-analyzed discussions, last 6h):", ""]

        for w in witness_summaries[:5]:
            conf_str = f"{int(w.confidence * 100)}%"
            type_str = w.witness_type.upper().replace("_", " ")
            lines.append(f"- [{type_str}, conf={conf_str}] {w.insight}")
            if w.timeframe:
                lines.append(f"  Timeframe: {w.timeframe}")

        lines.append("")
        lines.append(
            "CAUTION: Forum signals have underperformed non-forum agents for 4 "
            "consecutive sessions. Treat forum witness as LOW-weight background "
            "context only. Do NOT increase confidence based on forum agreement. "
            "ECHO CHAMBER RISK: if most witnesses agree on one direction, treat "
            "that as a WARNING of crowded positioning, not a confirmation signal."
        )
        lines.append("")
        lines.append(
            "_Note: These insights are from forum discussion analysis, not direct market data._"
        )
        return "\n".join(lines)

    def _build_skill_aware_prompt(self) -> str:
        """Override to build forum + skill aware prompt."""
        base_prompt = super()._build_skill_aware_prompt()

        forum_guidance = """

FORUM WITNESS SUMMARIES:
In addition to skills, you have access to FORUM WITNESS SUMMARIES.

These are Observer-analyzed insights from recent forum discussions between
analytical agents (MarketAnalyst, Contrarian). The Observer correlates these
discussions with actual trading outcomes to identify what works.

HOW TO USE WITNESS:
1. Witness summaries appear in context under `forum_witness`
2. Each witness has a confidence score (0-100%)
3. High-confidence witness (>70%) should influence your decisions
4. Witness complements skills:
   - Skills: Long-term statistical patterns
   - Witness: Recent discussion insights (last 6h)
5. If witness conflicts with skills, prefer skills (more data)

WITNESS TYPES:
- exit_timing: When to take profits or cut losses
- entry_signal: Entry setup opportunities
- risk_warning: Caution signals (funding, overextension)
- regime_insight: Market condition observations

INTEGRATION:
- Use witness for tactical timing (hours)
- Use skills for strategic positioning (days/weeks)
- Document witness influence in your reasoning"""

        return base_prompt + forum_guidance

    async def decide(self, context: dict) -> Decision:
        """Make a forum-aware trading decision."""
        if self._graph is None:
            await self.on_start()

        witness_summaries = await self._load_witness_summaries(context)

        if witness_summaries:
            context["forum_witness"] = self._format_witness_for_context(
                witness_summaries
            )
        else:
            context["forum_witness"] = None

        decision = await super().decide(context)

        if decision.metadata is None:
            decision.metadata = {}

        decision.metadata["forum_aware"] = True
        decision.metadata["witness_count"] = len(witness_summaries)
        decision.metadata["witness_types"] = [
            w.witness_type for w in witness_summaries
        ]
        decision.metadata["witness_symbols"] = list(
            set(sym for w in witness_summaries for sym in w.symbols)
        )

        if witness_summaries:
            self._witness_consulted_count += 1

        if (
            self.post_to_forum
            and decision.action != "hold"
            and (decision.confidence or 0) >= self.min_post_confidence
        ):
            await self._post_trade_rationale(decision, context, witness_summaries)

        return decision

    async def _post_trade_rationale(
        self, decision: Decision, context: dict, witness_summaries: list
    ) -> None:
        """Post trade rationale to forum."""
        try:
            forum = self._get_forum()
            if not forum or decision.action == "hold":
                return

            lines = [f"**{decision.action.upper()}: {decision.symbol}**", ""]
            if decision.size:
                lines.append(
                    f"Size: {decision.size:.4f} @ {decision.leverage}x leverage"
                )
            if decision.confidence:
                lines.append(f"Confidence: {decision.confidence:.0%}")
            lines.append("")
            if decision.reasoning:
                reasoning = decision.reasoning
                # Don't post raw fallback text to forum
                if reasoning.startswith("Extracted from text"):
                    reasoning = reasoning.split(":", 1)[-1].strip()
                    # If it's still JSON-like, skip the rationale entirely
                    if reasoning.startswith("{"):
                        reasoning = ""
                if reasoning:
                    reasoning_short = reasoning[:300]
                    if len(reasoning) > 300:
                        reasoning_short += "..."
                    lines.append(f"Rationale: {reasoning_short}")
                lines.append("")
            if witness_summaries:
                lines.append(
                    f"_Influenced by {len(witness_summaries)} forum witness summaries._"
                )

            await forum.post_message(
                channel="strategy",
                agent_id=self.agent_id,
                agent_name=self.name,
                agent_type="trading",
                content="\n".join(lines),
                metadata={
                    "tick": context.get("tick"),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "decision_action": decision.action,
                    "decision_symbol": decision.symbol,
                    "confidence": decision.confidence,
                    "witness_influenced": len(witness_summaries) > 0,
                },
            )
        except Exception as e:
            logger.warning("Failed to post trade rationale: %s", e)
