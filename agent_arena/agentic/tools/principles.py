"""Trading Principles Tool — queries abstract principles for agentic agents."""

from __future__ import annotations

import logging
from typing import Any, Type

from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class PrincipleInput(BaseModel):
    """Input for the trading principles tool."""

    regime: str = Field(default="", description="Market regime to filter by (e.g., trending_up, ranging)")
    symbol: str = Field(default="", description="Symbol to filter by (e.g., PF_XBTUSD)")


class TradingPrinciplesTool(BaseTool):
    """Retrieves abstract trading principles distilled from past experience.

    These principles are compressed from many individual trade reflections
    into actionable rules, via the metabolic memory system.
    """

    name: str = "trading_principles"
    description: str = (
        "Retrieve abstract trading principles learned from past experience. "
        "These are distilled from many individual trades into general rules. "
        "Optionally filter by market regime or symbol."
    )
    args_schema: Type[BaseModel] = PrincipleInput

    storage: Any = None
    agent_id: str = ""

    class Config:
        arbitrary_types_allowed = True

    def _run(self, regime: str = "", symbol: str = "") -> str:
        return "Use async version"

    async def _arun(self, regime: str = "", symbol: str = "") -> str:
        """Retrieve trading principles."""
        if not self.storage or not self.agent_id:
            return "No principles available yet."

        if not hasattr(self.storage, "pool"):
            return "Principles require PostgreSQL backend."

        try:
            async with self.storage.pool.acquire() as conn:
                conditions = ["agent_id = $1", "is_active = TRUE"]
                params: list[Any] = [self.agent_id]
                idx = 2

                if regime:
                    conditions.append(f"(regime = ${idx} OR regime = 'all')")
                    params.append(regime)
                    idx += 1

                if symbol:
                    conditions.append(f"(symbol = ${idx} OR symbol IS NULL OR symbol = '')")
                    params.append(symbol)
                    idx += 1

                params.append(10)  # limit
                where = " AND ".join(conditions)

                rows = await conn.fetch(
                    f"""
                    SELECT principle, regime, confidence, application_count
                    FROM abstract_principles
                    WHERE {where}
                    ORDER BY confidence DESC, application_count DESC
                    LIMIT ${idx}
                    """,
                    *params,
                )

                if not rows:
                    return "No trading principles found yet. Continue trading to build experience."

                # Format principles
                lines = ["## Trading Principles\n"]
                for r in rows:
                    conf = r["confidence"]
                    regime_tag = f" [{r['regime']}]" if r["regime"] and r["regime"] != "all" else ""
                    lines.append(
                        f"- {r['principle']}{regime_tag} "
                        f"(confidence: {conf:.0%}, applied {r['application_count']}x)"
                    )

                # Log access
                for r in rows:
                    await conn.execute(
                        """
                        INSERT INTO memory_access_log (memory_type, memory_id, agent_id)
                        SELECT 'abstract_principle', id, agent_id
                        FROM abstract_principles
                        WHERE agent_id = $1 AND principle = $2
                        LIMIT 1
                        """,
                        self.agent_id, r["principle"],
                    )

                return "\n".join(lines)

        except Exception as e:
            logger.exception("Principles tool failed")
            return f"Failed to retrieve principles: {e}"
