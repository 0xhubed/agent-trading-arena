"""Journal-Aware LLM Trader - extends ForumAwareLLMTrader with daily journal.

Receives a personalized daily briefing from the Observer's journal:
- Market overview (shared across agents)
- Personal report card (agent-specific critique + recommendations)
- Forum quality assessment (shared)
"""

from __future__ import annotations

import logging
from datetime import date as date_type
from datetime import datetime, timezone
from typing import Optional

from agent_arena.agents.forum_aware_llm import ForumAwareLLMTrader
from agent_arena.core.models import Decision

logger = logging.getLogger(__name__)


class JournalAwareLLMTrader(ForumAwareLLMTrader):
    """
    Forum-aware trader that also consumes the Observer's daily journal.

    The journal provides a personalized report card with critique and
    recommendations, plus shared sections on market conditions and
    forum quality.

    Example config:
        config:
            model: gpt-oss-120b
            base_url: https://api.together.xyz/v1
            journal_lookback_days: 1
            witness_lookback_hours: 6
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        super().__init__(agent_id, name, config)
        config = config or {}
        self.journal_lookback_days = config.get("journal_lookback_days", 1)
        self._journal_consulted_count = 0

    async def _load_journal_briefing(self) -> str:
        """Load the latest journal briefing for this agent."""
        try:
            # Access storage through the memory store (same pattern as forum)
            if not hasattr(self, "_memory_store") or not hasattr(
                self._memory_store, "_storage"
            ):
                return ""

            storage = self._memory_store._storage

            if not hasattr(storage, "get_latest_journal_entry"):
                return ""

            entry_dict = await storage.get_latest_journal_entry()
            if not entry_dict:
                return ""

            # Build personalized briefing (static method, no instantiation)
            from agent_arena.journal.models import JournalEntry
            from agent_arena.journal.service import JournalService

            journal_date = entry_dict["journal_date"]
            if isinstance(journal_date, str):
                journal_date = date_type.fromisoformat(journal_date)

            entry = JournalEntry(
                id=entry_dict.get("id", ""),
                journal_date=journal_date,
                generated_at=datetime.now(timezone.utc),
                lookback_hours=entry_dict.get("lookback_hours", 24),
                full_markdown=entry_dict.get("full_markdown", ""),
                market_summary=entry_dict.get("market_summary", ""),
                forum_summary=entry_dict.get("forum_summary", ""),
                learning_summary=entry_dict.get("learning_summary", ""),
                recommendations=entry_dict.get("recommendations", ""),
                agent_reports=entry_dict.get("agent_reports", {}),
                metrics=entry_dict.get("metrics", {}),
            )

            return JournalService.get_agent_briefing(entry, self.agent_id)

        except Exception as e:
            logger.warning("Failed to load journal briefing: %s", e)
            return ""

    def _build_skill_aware_prompt(self) -> str:
        """Override to add journal guidance to the system prompt."""
        base_prompt = super()._build_skill_aware_prompt()

        journal_guidance = """

DAILY JOURNAL BRIEFING:
In addition to skills and forum witness, you receive a DAILY JOURNAL from the
Observer agent. This is a critical, data-driven editorial analyzing competition
performance over the last 24 hours.

HOW TO USE THE JOURNAL:
1. Journal briefing appears in context under `journal_briefing`
2. Your personal REPORT CARD contains specific critique and recommendations
3. Pay attention to the Observer's assessment of your trading patterns
4. If the report says you're overtrading, reduce activity
5. If it identifies missed opportunities, adjust your criteria

PRIORITY ORDER:
1. Position advisories (immediate action required)
2. Journal report card (strategic adjustment)
3. Skills (long-term patterns)
4. Forum witness (tactical timing)"""

        return base_prompt + journal_guidance

    async def decide(self, context: dict) -> Decision:
        """Make a journal-aware trading decision."""
        if self._graph is None:
            await self.on_start()

        # Load journal briefing
        journal = await self._load_journal_briefing()
        if journal:
            context["journal_briefing"] = journal
            self._journal_consulted_count += 1
        else:
            context["journal_briefing"] = None

        decision = await super().decide(context)

        if decision.metadata is None:
            decision.metadata = {}

        decision.metadata["journal_aware"] = True
        decision.metadata["journal_consulted"] = bool(journal)

        return decision
