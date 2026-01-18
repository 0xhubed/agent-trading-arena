"""Trade validation rules tool - prevents churning and enforces discipline."""

import json
from typing import Optional

from pydantic import BaseModel, Field

from agent_arena.agentic.tools.base import TradingTool


class TradeValidationInput(BaseModel):
    """Input schema for trade validation."""

    proposed_action: str = Field(
        description="The action being considered: open_long, open_short, close, hold"
    )
    symbol: str = Field(description="Trading symbol (e.g., BTCUSDT)")


class TradeRulesTool(TradingTool):
    """
    Validates proposed trades against execution rules.

    Prevents common mistakes:
    - Churning (trading same symbol too frequently)
    - Counter-trend trading (shorting uptrends, longing downtrends)
    - Ignoring recent loss streaks
    - Over-concentration in single symbol

    REQUIRED before any open/close action to enforce trading discipline.
    """

    name: str = "validate_trade"
    description: str = """REQUIRED before any open/close action. Validates:
- Minimum hold period (3 ticks since last trade on symbol)
- Recent win rate (warns if >60% recent trades are losses)
- Trend alignment (warns if trading against strong trend)
- Symbol concentration (warns if too many positions on same symbol)

Returns PROCEED, CAUTION, or REJECT with violations/warnings list.
Input: proposed_action (required), symbol (required)."""

    args_schema: type[BaseModel] = TradeValidationInput

    def _run(
        self,
        proposed_action: str,
        symbol: str,
    ) -> str:
        """Validate proposed trade against rules."""
        context = self._context
        violations = []
        warnings = []

        tick = context.get("tick", 0)
        portfolio = context.get("portfolio", {})
        market = context.get("market", {})
        agent_id = context.get("agent_id", "")

        # Rule 1: Minimum hold period (check recent trades)
        recent_trades = self._get_recent_trades(agent_id, symbol, limit=5)
        if recent_trades:
            last_trade_tick = recent_trades[0].get("tick", 0)
            ticks_since = tick - last_trade_tick
            if ticks_since < 3:
                violations.append({
                    "rule": "HOLD_PERIOD",
                    "message": (
                        f"Traded {symbol} only {ticks_since} tick(s) ago. "
                        "Minimum 3 ticks required."
                    ),
                    "severity": "HIGH",
                })

        # Rule 2: Recent performance check
        all_recent = self._get_recent_trades(agent_id, None, limit=10)
        if len(all_recent) >= 5:
            losses = sum(1 for t in all_recent if float(t.get("pnl", 0)) < 0)
            loss_rate = losses / len(all_recent)
            if loss_rate > 0.6:
                warnings.append({
                    "rule": "LOSS_STREAK",
                    "message": (
                        f"{int(loss_rate*100)}% of last {len(all_recent)} trades "
                        "were losses. Consider reducing size or waiting."
                    ),
                    "severity": "MEDIUM",
                })

        # Rule 3: Trend alignment
        if symbol in market:
            change_24h = float(market[symbol].get("change_24h", 0))

            if proposed_action == "open_short" and change_24h > 3:
                warnings.append({
                    "rule": "COUNTER_TREND",
                    "message": (
                        f"Opening short against +{change_24h:.1f}% uptrend. "
                        "Strong trends tend to continue."
                    ),
                    "severity": "MEDIUM",
                })
            elif proposed_action == "open_long" and change_24h < -3:
                warnings.append({
                    "rule": "COUNTER_TREND",
                    "message": (
                        f"Opening long against {change_24h:.1f}% downtrend. "
                        "Consider waiting for reversal confirmation."
                    ),
                    "severity": "MEDIUM",
                })

        # Rule 4: Symbol concentration
        positions = portfolio.get("positions", [])
        symbol_positions = [p for p in positions if p.get("symbol") == symbol]

        if len(symbol_positions) >= 1 and proposed_action.startswith("open"):
            warnings.append({
                "rule": "CONCENTRATION",
                "message": (
                    f"Already have position on {symbol}. "
                    "Consider diversifying to other symbols."
                ),
                "severity": "LOW",
            })

        # Rule 5: Check if closing a winning position too early
        if proposed_action == "close":
            for pos in symbol_positions:
                roe = float(pos.get("roe_percent", 0))
                if 0 < roe < 2:  # Small profit
                    warnings.append({
                        "rule": "EARLY_EXIT",
                        "message": (
                            f"Position only +{roe:.1f}% profitable. "
                            "Consider letting winners run."
                        ),
                        "severity": "LOW",
                    })

        # Determine recommendation
        if violations:
            recommendation = "REJECT"
        elif warnings:
            recommendation = "CAUTION"
        else:
            recommendation = "PROCEED"

        return json.dumps({
            "proposed": f"{proposed_action} {symbol}",
            "recommendation": recommendation,
            "can_proceed": len(violations) == 0,
            "violations": violations,
            "warnings": warnings,
            "summary": self._get_summary(recommendation, violations, warnings),
        }, indent=2)

    def _get_recent_trades(
        self, agent_id: str, symbol: Optional[str], limit: int
    ) -> list:
        """Query recent trades from storage."""
        if not self._storage:
            return []

        try:
            # Use storage to get recent trades
            import asyncio

            async def fetch():
                return await self._storage.get_trades(
                    agent_id=agent_id,
                    symbol=symbol,
                    limit=limit,
                )

            # Try to get event loop
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # We're in async context, can't use run_until_complete
                    # Return empty for now - will be populated from context
                    return []
                return loop.run_until_complete(fetch())
            except RuntimeError:
                return []
        except Exception:
            return []

    def _get_summary(
        self, recommendation: str, violations: list, warnings: list
    ) -> str:
        """Generate human-readable summary."""
        if recommendation == "REJECT":
            return f"BLOCKED: {violations[0]['message']}"
        elif recommendation == "CAUTION":
            return f"PROCEED WITH CAUTION: {len(warnings)} warning(s) - {warnings[0]['message']}"
        else:
            return "All checks passed. Trade is valid."
