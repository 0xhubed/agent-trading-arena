"""Reflexion Service — generates structured LLM reflections for closed trades."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from agent_arena.reflexion.models import TradeReflection
from agent_arena.llm_utils import extract_json_from_llm, strip_think_blocks

logger = logging.getLogger(__name__)

REFLECTION_PROMPT = """You are a trading coach analyzing a completed trade.

Trade details:
- Agent: {agent_id}
- Symbol: {symbol}
- Side: {side}
- Entry price: ${entry_price:.2f}
- Exit price: ${exit_price:.2f}
- Realized P&L: ${realized_pnl:.2f}
- Market regime: {market_regime}
- Original reasoning: {reasoning}

Analyze this trade and respond in JSON:
{{
  "entry_signal": "What signal/reasoning led to the entry",
  "what_went_right": "What aspects of the trade were correct",
  "what_went_wrong": "What mistakes were made (empty string if none)",
  "lesson": "One concise, actionable lesson for future trades",
  "confidence": 0.0-1.0
}}

Be specific and actionable. Focus on what can be improved."""


class ReflexionService:
    """Generates structured trade reflections using LLM analysis."""

    def __init__(
        self,
        storage: Any,
        model: str = "claude-sonnet-4-6",
    ):
        self.storage = storage
        self.model = model

    async def reflect_on_closed_trades(
        self,
        agent_id: str,
        lookback_hours: int = 24,
    ) -> list[TradeReflection]:
        """Generate reflections for recently closed trades.

        Called during journal generation phase.
        """
        trades = await self._get_recent_closed_trades(agent_id, lookback_hours)
        if not trades:
            logger.debug("No closed trades for %s in last %dh", agent_id, lookback_hours)
            return []

        # Skip trades that already have reflections
        existing = await self._get_existing_reflection_trade_ids(agent_id)
        new_trades = [t for t in trades if t.get("id") not in existing]

        if not new_trades:
            logger.debug("All trades for %s already reflected", agent_id)
            return []

        reflections = []
        for trade in new_trades[:10]:  # Limit to 10 per cycle
            try:
                reflection = await self._generate_reflection(agent_id, trade)
                if reflection:
                    await self._save_reflection(reflection)
                    reflections.append(reflection)
            except Exception:
                logger.exception("Failed to reflect on trade %s", trade.get("id"))

        logger.info(
            "Generated %d reflections for %s from %d trades",
            len(reflections), agent_id, len(new_trades),
        )
        return reflections

    async def get_reflections(
        self,
        agent_id: str,
        limit: int = 20,
        outcome: Optional[str] = None,
    ) -> list[TradeReflection]:
        """Retrieve stored reflections for an agent."""
        if not hasattr(self.storage, "pool"):
            return []

        try:
            async with self.storage.pool.acquire() as conn:
                if outcome:
                    rows = await conn.fetch(
                        """
                        SELECT * FROM trade_reflections
                        WHERE agent_id = $1 AND outcome = $2
                        ORDER BY created_at DESC LIMIT $3
                        """,
                        agent_id, outcome, limit,
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT * FROM trade_reflections
                        WHERE agent_id = $1
                        ORDER BY created_at DESC LIMIT $2
                        """,
                        agent_id, limit,
                    )
                return [TradeReflection.from_row(r) for r in rows]
        except Exception:
            logger.exception("Failed to get reflections for %s", agent_id)
            return []

    async def _generate_reflection(
        self, agent_id: str, trade: dict,
    ) -> Optional[TradeReflection]:
        """Generate a single trade reflection via LLM."""
        from langchain_anthropic import ChatAnthropic
        from langchain_core.messages import HumanMessage

        # Determine outcome
        pnl = float(trade.get("realized_pnl", 0))
        outcome = "win" if pnl > 0 else "loss" if pnl < 0 else "breakeven"

        # Get decision reasoning
        reasoning = await self._get_trade_reasoning(trade)

        # Detect market regime (simplified)
        regime = await self._detect_regime(trade.get("symbol", ""), trade.get("timestamp"))

        prompt = REFLECTION_PROMPT.format(
            agent_id=agent_id,
            symbol=trade.get("symbol", "?"),
            side=trade.get("side", "?"),
            entry_price=float(trade.get("price", 0)),
            exit_price=float(trade.get("exit_price", trade.get("price", 0))),
            realized_pnl=pnl,
            market_regime=regime,
            reasoning=reasoning[:500] if reasoning else "Not available",
        )

        try:
            llm = ChatAnthropic(model=self.model, max_tokens=500, temperature=0.3)
            response = await llm.ainvoke([HumanMessage(content=prompt)])
            content = strip_think_blocks(response.content)

            # Parse JSON from response
            parsed = extract_json_from_llm(content)
            if not parsed:
                return None

            return TradeReflection(
                agent_id=agent_id,
                trade_id=trade.get("id", ""),
                symbol=trade.get("symbol", ""),
                side=trade.get("side", ""),
                entry_price=float(trade.get("price", 0)),
                exit_price=float(trade.get("exit_price", trade.get("price", 0))),
                realized_pnl=pnl,
                market_regime=regime,
                entry_signal=parsed.get("entry_signal", ""),
                outcome=outcome,
                what_went_right=parsed.get("what_went_right", ""),
                what_went_wrong=parsed.get("what_went_wrong", ""),
                lesson=parsed.get("lesson", ""),
                confidence=parsed.get("confidence", 0.5),
            )
        except Exception:
            logger.exception("LLM reflection failed for trade %s", trade.get("id"))
            return None

    async def _get_recent_closed_trades(
        self, agent_id: str, lookback_hours: int,
    ) -> list[dict]:
        """Get recently closed trades (trades with realized_pnl != 0)."""
        if not hasattr(self.storage, "pool"):
            return []

        try:
            async with self.storage.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT * FROM trades
                    WHERE agent_id = $1
                    AND realized_pnl IS NOT NULL
                    AND realized_pnl != 0
                    AND timestamp >= NOW() - INTERVAL '1 hour' * $2
                    ORDER BY timestamp DESC
                    LIMIT 20
                    """,
                    agent_id, lookback_hours,
                )
                return [dict(r) for r in rows]
        except Exception:
            return []

    async def _get_existing_reflection_trade_ids(self, agent_id: str) -> set[str]:
        """Get trade IDs that already have reflections."""
        if not hasattr(self.storage, "pool"):
            return set()

        try:
            async with self.storage.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT trade_id FROM trade_reflections WHERE agent_id = $1",
                    agent_id,
                )
                return {r["trade_id"] for r in rows}
        except Exception:
            return set()

    async def _get_trade_reasoning(self, trade: dict) -> str:
        """Get the decision reasoning for a trade."""
        decision_id = trade.get("decision_id")
        if not decision_id or not hasattr(self.storage, "pool"):
            return ""

        try:
            async with self.storage.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT reasoning FROM decisions WHERE id = $1",
                    decision_id,
                )
                return row["reasoning"] if row and row["reasoning"] else ""
        except Exception:
            return ""

    async def _detect_regime(
        self, symbol: str, timestamp: Any,
    ) -> str:
        """Simple regime detection based on recent price action."""
        # Simplified — in production, use core/regime.py
        return "unknown"

    async def _save_reflection(self, reflection: TradeReflection) -> None:
        """Save a reflection to the database."""
        if not hasattr(self.storage, "pool"):
            return

        try:
            async with self.storage.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO trade_reflections (
                        agent_id, trade_id, decision_id, symbol, side,
                        entry_price, exit_price, realized_pnl,
                        market_regime, entry_signal, what_went_right,
                        what_went_wrong, lesson, outcome, confidence
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                    """,
                    reflection.agent_id,
                    reflection.trade_id,
                    reflection.decision_id,
                    reflection.symbol,
                    reflection.side,
                    reflection.entry_price,
                    reflection.exit_price,
                    reflection.realized_pnl,
                    reflection.market_regime,
                    reflection.entry_signal,
                    reflection.what_went_right,
                    reflection.what_went_wrong,
                    reflection.lesson,
                    reflection.outcome,
                    reflection.confidence,
                )
        except Exception:
            logger.exception("Failed to save reflection")

