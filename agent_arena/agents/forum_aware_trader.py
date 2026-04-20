"""Forum-Aware Trader - Agent that uses forum witness summaries + skills.

This agent extends SkillAwareTrader to include forum witness consumption,
allowing it to benefit from real-time forum discussions analyzed by the Observer.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from agent_arena.agents.skill_aware_trader import SkillAwareTrader
from agent_arena.core.models import Decision
from agent_arena.forum.service import ForumService


class ForumAwareTradingAgent(SkillAwareTrader):
    """
    Trading agent that leverages both skills and forum witness summaries.

    This agent extends SkillAwareTrader with:
    1. Access to witness summaries from forum analysis
    2. Witness-aware system prompt
    3. Decision metadata tracking witness influence
    4. Optional trade rationale posting to forum

    The agent receives compact witness summaries (200-300 tokens) that
    distill forum discussions into actionable insights.

    Example config:
        config:
            model: claude-sonnet-4-20250514
            max_iterations: 4
            skills_dir: ".claude/skills"
            witness_lookback_hours: 6  # How far back to load witness
            min_witness_confidence: 0.6  # Minimum confidence threshold
            post_to_forum: true  # Post trade rationale back to forum
            character: "Analytical trader who learns from peer discussions"
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        super().__init__(agent_id, name, config)
        config = config or {}

        # Witness configuration
        self.witness_lookback_hours = config.get("witness_lookback_hours", 6)
        self.min_witness_confidence = config.get("min_witness_confidence", 0.6)
        self.post_to_forum = config.get("post_to_forum", True)

        # Forum service (initialized on first use)
        self._forum: Optional[ForumService] = None

        # Track witness usage
        self._witness_consulted_count = 0

    def _get_forum(self) -> Optional[ForumService]:
        """Lazy-load forum service."""
        if self._forum is None:
            # Get storage from memory store (available after graph init)
            if not hasattr(self, "_memory_store") or not hasattr(
                self._memory_store, "_storage"
            ):
                logger = logging.getLogger(__name__)
                logger.warning("Forum service unavailable - storage not initialized")
                return None
            self._forum = ForumService(self._memory_store._storage)
        return self._forum

    async def decide(self, context: dict) -> Decision:
        """Make a forum-aware trading decision.

        Loads witness summaries and injects them into context before
        making a decision via parent SkillAwareTrader.

        Args:
            context: Trading context dict

        Returns:
            Decision with witness metadata
        """
        # Ensure graph is created
        if self._graph is None:
            await self.on_start()

        # Load recent witness summaries
        witness_summaries = await self._load_witness_summaries(context)

        # Format witness for context injection
        if witness_summaries:
            context["forum_witness"] = self._format_witness_for_context(
                witness_summaries
            )
        else:
            context["forum_witness"] = None

        # Make decision using parent class (SkillAwareTrader)
        decision = await super().decide(context)

        # Add forum-aware metadata (ensure metadata dict exists)
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

        # Track witness consultation
        if witness_summaries:
            self._witness_consulted_count += 1

        # Optionally post trade rationale to forum
        if self.post_to_forum and decision.action != "hold":
            await self._post_trade_rationale(decision, context, witness_summaries)

        return decision

    async def _load_witness_summaries(self, context: dict) -> list:
        """Load recent witness summaries from database.

        Args:
            context: Trading context with market symbols

        Returns:
            List of WitnessSummary objects
        """
        try:
            # Get forum service
            forum = self._get_forum()
            if not forum:
                return []

            # Get symbols from context
            symbols = list(context.get("market", {}).keys())

            # Load witness summaries
            summaries = await forum.get_recent_witness_summaries(
                hours=self.witness_lookback_hours,
                symbols=symbols,
                min_confidence=self.min_witness_confidence,
            )

            return summaries

        except Exception as e:
            # Don't fail trading decision if witness loading fails
            print(f"Warning: Failed to load witness summaries: {e}")
            return []

    def _format_witness_for_context(self, witness_summaries: list) -> str:
        """Format witness summaries as compact context addition.

        Args:
            witness_summaries: List of WitnessSummary objects

        Returns:
            Formatted string for context injection (~200-300 tokens)
        """
        if not witness_summaries:
            return "No recent forum insights available."

        lines = ["**FORUM WITNESS** (Observer-analyzed discussions, last 6h):"]
        lines.append("")

        for w in witness_summaries[:5]:  # Top 5 witness by confidence
            # Format: [TYPE, conf=XX%] Insight text
            conf_str = f"{int(w.confidence * 100)}%"
            type_str = w.witness_type.upper().replace("_", " ")

            lines.append(f"- [{type_str}, conf={conf_str}] {w.insight}")

            # Add timeframe if available
            if w.timeframe:
                lines.append(f"  Timeframe: {w.timeframe}")

        lines.append("")
        lines.append(
            "_Note: These insights are from forum discussion analysis, not direct market data._"
        )

        return "\n".join(lines)

    def _build_skill_aware_prompt(self) -> str:
        """Override to build forum + skill aware prompt."""
        # Get base skill-aware prompt from parent
        base_prompt = super()._build_skill_aware_prompt()

        # Append forum witness guidance
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

    async def _post_trade_rationale(
        self,
        decision: Decision,
        context: dict,
        witness_summaries: list,
    ) -> None:
        """Post trade rationale to forum (optional).

        Args:
            decision: The trading decision made
            context: Trading context
            witness_summaries: Witness summaries used
        """
        try:
            forum = self._get_forum()
            if not forum:
                return

            # Only post on non-hold actions
            if decision.action == "hold":
                return

            # Format rationale message
            content = self._format_trade_rationale(
                decision, context, witness_summaries
            )

            # Post to strategy channel
            await forum.post_message(
                channel="strategy",
                agent_id=self.agent_id,
                agent_name=self.name,
                agent_type="trading",
                content=content,
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
            # Don't fail decision if posting fails
            print(f"Warning: Failed to post trade rationale: {e}")

    def _format_trade_rationale(
        self,
        decision: Decision,
        context: dict,
        witness_summaries: list,
    ) -> str:
        """Format trade decision as forum message.

        Args:
            decision: Trading decision
            context: Trading context
            witness_summaries: Witness summaries used

        Returns:
            Formatted message content
        """
        lines = [f"**{decision.action.upper()}: {decision.symbol}**"]
        lines.append("")

        # Basic decision info
        if decision.size:
            lines.append(f"Size: {decision.size:.4f} @ {decision.leverage}x leverage")

        if decision.confidence:
            lines.append(f"Confidence: {decision.confidence:.0%}")

        lines.append("")

        # Reasoning (truncated)
        if decision.reasoning:
            reasoning_short = decision.reasoning[:300]
            if len(decision.reasoning) > 300:
                reasoning_short += "..."
            lines.append(f"Rationale: {reasoning_short}")
            lines.append("")

        # Witness influence
        if witness_summaries:
            lines.append(
                f"_Influenced by {len(witness_summaries)} forum witness summaries._"
            )

        return "\n".join(lines)
