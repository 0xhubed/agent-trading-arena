"""Exemplar Builder — selects relevant past trades as few-shot examples for agent prompts."""

from __future__ import annotations

import logging
from typing import Any, Optional

from agent_arena.reflexion.models import TradeReflection

logger = logging.getLogger(__name__)


class ExemplarBuilder:
    """Selects relevant past trade reflections as few-shot prompt exemplars.

    Retrieves 2 winning + 2 losing trade reflections for a given agent,
    filtered by symbol/regime when available, and formats them for
    prompt injection.
    """

    def __init__(self, storage: Any):
        self.storage = storage

    async def get_exemplars(
        self,
        agent_id: str,
        symbol: Optional[str] = None,
        regime: Optional[str] = None,
        wins: int = 2,
        losses: int = 2,
    ) -> list[TradeReflection]:
        """Get balanced win/loss exemplars for an agent.

        Args:
            agent_id: Agent to get exemplars for.
            symbol: Optional symbol filter (e.g., PF_XBTUSD).
            regime: Optional market regime filter.
            wins: Number of winning exemplars.
            losses: Number of losing exemplars.

        Returns:
            List of TradeReflection objects (wins first, then losses).
        """
        if not hasattr(self.storage, "pool"):
            return []

        win_exemplars = await self._query_exemplars(
            agent_id, "win", symbol, regime, wins
        )
        loss_exemplars = await self._query_exemplars(
            agent_id, "loss", symbol, regime, losses
        )

        # Log access for metabolic scoring
        all_exemplars = win_exemplars + loss_exemplars
        await self._log_access(all_exemplars)

        return all_exemplars

    def format_for_prompt(self, exemplars: list[TradeReflection]) -> str:
        """Format exemplars as a prompt section.

        Returns a string suitable for injection into agent prompts.
        """
        if not exemplars:
            return ""

        lines = ["## Lessons from Recent Trades\n"]

        for i, ex in enumerate(exemplars, 1):
            emoji = "+" if ex.outcome == "win" else "-"
            lines.append(
                f"{emoji} **{ex.symbol} {ex.side}** "
                f"(PnL: ${ex.realized_pnl:+.2f})"
            )
            if ex.lesson:
                lines.append(f"  Lesson: {ex.lesson}")
            if ex.what_went_wrong and ex.outcome == "loss":
                lines.append(f"  Mistake: {ex.what_went_wrong}")
            if ex.what_went_right and ex.outcome == "win":
                lines.append(f"  Right: {ex.what_went_right}")
            lines.append("")

        return "\n".join(lines)

    async def _query_exemplars(
        self,
        agent_id: str,
        outcome: str,
        symbol: Optional[str],
        regime: Optional[str],
        limit: int,
    ) -> list[TradeReflection]:
        """Query reflections from DB with optional filters."""
        if not hasattr(self.storage, "pool"):
            return []

        try:
            async with self.storage.pool.acquire() as conn:
                # Build query with optional filters
                conditions = ["agent_id = $1", "outcome = $2", "is_digested = FALSE"]
                params: list[Any] = [agent_id, outcome]
                idx = 3

                if symbol:
                    conditions.append(f"symbol = ${idx}")
                    params.append(symbol)
                    idx += 1

                if regime:
                    conditions.append(f"market_regime = ${idx}")
                    params.append(regime)
                    idx += 1

                params.append(limit)
                where = " AND ".join(conditions)

                rows = await conn.fetch(
                    f"""
                    SELECT * FROM trade_reflections
                    WHERE {where}
                    ORDER BY created_at DESC
                    LIMIT ${idx}
                    """,
                    *params,
                )

                # If filtered query returned too few, fall back to unfiltered
                if len(rows) < limit and (symbol or regime):
                    rows = await conn.fetch(
                        """
                        SELECT * FROM trade_reflections
                        WHERE agent_id = $1 AND outcome = $2 AND is_digested = FALSE
                        ORDER BY created_at DESC
                        LIMIT $3
                        """,
                        agent_id, outcome, limit,
                    )

                return [TradeReflection.from_row(r) for r in rows]
        except Exception:
            logger.exception("Failed to query exemplars for %s", agent_id)
            return []

    async def _log_access(self, reflections: list[TradeReflection]) -> None:
        """Log memory access for metabolic scoring (batched)."""
        if not reflections or not hasattr(self.storage, "pool"):
            return

        trade_ids = [r.trade_id for r in reflections if r.trade_id]
        if not trade_ids:
            return

        agent_id = reflections[0].agent_id

        try:
            async with self.storage.pool.acquire() as conn:
                # Batch update access counts
                await conn.execute(
                    """
                    UPDATE trade_reflections
                    SET access_count = access_count + 1,
                        last_accessed = NOW()
                    WHERE trade_id = ANY($1) AND agent_id = $2
                    """,
                    trade_ids, agent_id,
                )
        except Exception:
            logger.debug("Failed to log exemplar access (table may not exist yet)")

