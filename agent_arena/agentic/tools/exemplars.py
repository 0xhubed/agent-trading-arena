"""Trade Exemplar Tool — retrieves relevant past trade examples for agentic agents."""

from __future__ import annotations

import logging
from typing import Any, Optional, Type

from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ExemplarInput(BaseModel):
    """Input for the trade exemplar tool."""

    symbol: str = Field(default="", description="Symbol to filter exemplars by (e.g., PF_XBTUSD)")
    regime: str = Field(default="", description="Market regime to filter by (e.g., trending_up)")


class TradeExemplarTool(BaseTool):
    """Retrieves relevant past trade reflections as few-shot examples.

    Returns 2 winning + 2 losing trade examples with lessons learned,
    filtered by symbol and regime when available.
    """

    name: str = "trade_exemplars"
    description: str = (
        "Retrieve lessons from past trades. Returns winning and losing "
        "trade examples with what went right/wrong and key lessons. "
        "Optionally filter by symbol or market regime."
    )
    args_schema: Type[BaseModel] = ExemplarInput

    # Instance attributes
    storage: Any = None
    agent_id: str = ""

    class Config:
        arbitrary_types_allowed = True

    def _run(self, symbol: str = "", regime: str = "") -> str:
        """Sync wrapper — not used in async agentic flow."""
        return "Use async version"

    async def _arun(self, symbol: str = "", regime: str = "") -> str:
        """Retrieve trade exemplars."""
        if not self.storage or not self.agent_id:
            return "No trade history available yet."

        try:
            from agent_arena.reflexion.exemplars import ExemplarBuilder

            builder = ExemplarBuilder(self.storage)
            exemplars = await builder.get_exemplars(
                agent_id=self.agent_id,
                symbol=symbol or None,
                regime=regime or None,
            )

            if not exemplars:
                return "No trade reflections available yet. Continue trading to build history."

            return builder.format_for_prompt(exemplars)

        except Exception as e:
            logger.exception("Exemplar tool failed")
            return f"Failed to retrieve exemplars: {e}"
