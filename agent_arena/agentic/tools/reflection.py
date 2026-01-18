"""Reflection tool - analyzes trading patterns and learns from mistakes."""

import json
from collections import Counter
from typing import Optional

from pydantic import BaseModel

from agent_arena.agentic.tools.base import TradingTool


class ReflectionTool(TradingTool):
    """
    Analyzes recent trading performance and identifies patterns.

    Detects:
    - Churning patterns (opening/closing too quickly)
    - Symbol bias (over-trading one symbol)
    - Counter-trend losses
    - Win/loss patterns by symbol and action type

    Call this at the START of each decision to learn from history
    and avoid repeating mistakes.
    """

    name: str = "reflect_on_performance"
    description: str = """Analyzes last 20 trades and recent decisions to identify:
- Churning patterns (opening/closing too quickly)
- Symbol bias (over-trading one symbol)
- Win/loss patterns by symbol
- Counter-trend mistakes
- Suggested adjustments

Call this FIRST in each decision to learn from your trading history.
No input required."""

    # No args needed
    args_schema: Optional[type[BaseModel]] = None

    def _run(self) -> str:
        """Analyze recent trading performance."""
        context = self._context
        agent_id = context.get("agent_id", "")
        portfolio = context.get("portfolio", {})

        # Get trade and decision history
        trades = self._get_trade_history(agent_id, limit=20)
        decisions = self._get_decision_history(agent_id, limit=30)

        analysis = {
            "patterns_detected": [],
            "symbol_performance": {},
            "action_distribution": {},
            "recommendations": [],
            "current_status": {
                "equity": float(portfolio.get("equity", 10000)),
                "pnl_percent": float(portfolio.get("pnl_percent", 0)),
                "open_positions": len(portfolio.get("positions", [])),
            },
        }

        # Pattern 1: Churning detection
        churning = self._detect_churning(decisions)
        if churning:
            analysis["patterns_detected"].append({
                "pattern": "CHURNING",
                "description": f"Detected {churning['count']} quick open/close cycles",
                "severity": "HIGH",
                "symbols_affected": churning.get("symbols", []),
                "fix": "Enforce minimum 5-tick hold period before closing",
            })
            analysis["recommendations"].append(
                "STOP trading the same symbol within 5 ticks of last trade"
            )

        # Pattern 2: Symbol concentration
        symbol_bias = self._detect_symbol_bias(decisions)
        if symbol_bias:
            analysis["patterns_detected"].append({
                "pattern": "SYMBOL_BIAS",
                "description": (
                    f"Over-trading {symbol_bias['symbol']} "
                    f"({symbol_bias['pct']}% of trades)"
                ),
                "severity": "MEDIUM",
                "fix": "Diversify across available symbols",
            })
            analysis["recommendations"].append(
                f"Consider other symbols besides {symbol_bias['symbol']}"
            )

        # Pattern 3: Counter-trend losses
        counter_trend = self._detect_counter_trend_losses(trades)
        if counter_trend:
            analysis["patterns_detected"].append({
                "pattern": "COUNTER_TREND_LOSSES",
                "description": f"{counter_trend['count']} losses from trading against trend",
                "severity": "HIGH",
                "total_loss": f"${counter_trend['total_loss']:.2f}",
                "fix": "Only trade in direction of 24h trend",
            })
            analysis["recommendations"].append(
                "Avoid shorts when 24h change > +2%, avoid longs when < -2%"
            )

        # Pattern 4: Loss streak
        loss_streak = self._detect_loss_streak(trades)
        if loss_streak:
            analysis["patterns_detected"].append({
                "pattern": "LOSS_STREAK",
                "description": f"{loss_streak['count']} consecutive losing trades",
                "severity": "HIGH",
                "total_loss": f"${loss_streak['total_loss']:.2f}",
                "fix": "Take a break, reduce position sizes",
            })
            analysis["recommendations"].append(
                "Reduce position size by 50% until you have 2 consecutive wins"
            )

        # Symbol-by-symbol performance
        symbol_groups = self._group_by_symbol(trades)
        for symbol, symbol_trades in symbol_groups.items():
            wins = sum(1 for t in symbol_trades if float(t.get("pnl", 0)) > 0)
            total_pnl = sum(float(t.get("pnl", 0)) for t in symbol_trades)
            analysis["symbol_performance"][symbol] = {
                "trades": len(symbol_trades),
                "wins": wins,
                "win_rate": f"{wins/len(symbol_trades)*100:.0f}%" if symbol_trades else "N/A",
                "total_pnl": f"${total_pnl:+.2f}",
                "recommendation": self._get_symbol_recommendation(
                    wins, len(symbol_trades), total_pnl
                ),
            }

        # Action distribution
        action_counts = Counter(d.get("action", "hold") for d in decisions)
        total_decisions = len(decisions)
        if total_decisions > 0:
            analysis["action_distribution"] = {
                action: {
                    "count": count,
                    "percentage": f"{count/total_decisions*100:.0f}%",
                }
                for action, count in action_counts.items()
            }

            # Check for over-trading (low hold rate)
            hold_rate = action_counts.get("hold", 0) / total_decisions
            if hold_rate < 0.3:
                analysis["patterns_detected"].append({
                    "pattern": "OVER_TRADING",
                    "description": f"Only {hold_rate*100:.0f}% hold decisions (should be >50%)",
                    "severity": "MEDIUM",
                    "fix": "Be more selective, wait for high-conviction setups",
                })
                analysis["recommendations"].append(
                    "Increase hold rate to at least 50% - only trade best setups"
                )

        # Overall assessment
        if not analysis["patterns_detected"]:
            analysis["overall"] = "No concerning patterns detected. Continue current strategy."
        else:
            patterns = analysis["patterns_detected"]
            high_severity = sum(1 for p in patterns if p["severity"] == "HIGH")
            analysis["overall"] = (
                f"Found {len(patterns)} pattern(s) to address "
                f"({high_severity} high severity)."
            )

        return json.dumps(analysis, indent=2)

    def _get_trade_history(self, agent_id: str, limit: int) -> list:
        """Get trade history from storage or context."""
        # First try context (set by runner)
        if "trade_history" in self._context:
            return self._context["trade_history"][:limit]

        # Fallback to storage
        if not self._storage:
            return []

        try:
            import asyncio

            async def fetch():
                return await self._storage.get_trades(agent_id=agent_id, limit=limit)

            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    return []
                return loop.run_until_complete(fetch())
            except RuntimeError:
                return []
        except Exception:
            return []

    def _get_decision_history(self, agent_id: str, limit: int) -> list:
        """Get decision history from storage or context."""
        if "decision_history" in self._context:
            return self._context["decision_history"][:limit]

        if not self._storage:
            return []

        try:
            import asyncio

            async def fetch():
                return await self._storage.get_decisions(agent_id=agent_id, limit=limit)

            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    return []
                return loop.run_until_complete(fetch())
            except RuntimeError:
                return []
        except Exception:
            return []

    def _detect_churning(self, decisions: list) -> Optional[dict]:
        """Detect if agent is opening and closing too quickly."""
        if len(decisions) < 5:
            return None

        quick_cycles = []
        symbols_affected = set()

        for i, decision in enumerate(decisions):
            if decision.get("action") == "close":
                symbol = decision.get("symbol")
                close_tick = decision.get("tick", 0)

                # Look for matching open in recent history
                for j in range(i + 1, min(i + 5, len(decisions))):
                    prev = decisions[j]
                    if prev.get("action", "").startswith("open") and prev.get("symbol") == symbol:
                        open_tick = prev.get("tick", 0)
                        ticks_held = close_tick - open_tick
                        if 0 < ticks_held <= 3:
                            quick_cycles.append({
                                "symbol": symbol,
                                "ticks_held": ticks_held,
                            })
                            symbols_affected.add(symbol)
                        break

        if len(quick_cycles) >= 3:
            return {
                "count": len(quick_cycles),
                "symbols": list(symbols_affected),
            }
        return None

    def _detect_symbol_bias(self, decisions: list) -> Optional[dict]:
        """Detect if one symbol dominates trading."""
        trade_decisions = [
            d for d in decisions
            if d.get("action") in ["open_long", "open_short", "close"]
        ]

        if len(trade_decisions) < 5:
            return None

        symbols = [d.get("symbol") for d in trade_decisions if d.get("symbol")]
        if not symbols:
            return None

        counts = Counter(symbols)
        most_common = counts.most_common(1)[0]
        pct = most_common[1] / len(symbols) * 100

        if pct > 40:  # More than 40% on one symbol
            return {"symbol": most_common[0], "pct": int(pct)}
        return None

    def _detect_counter_trend_losses(self, trades: list) -> Optional[dict]:
        """Detect losses from trading against the trend."""
        # This requires market data context
        market = self._context.get("market", {})
        counter_trend_losses = []

        for trade in trades:
            if float(trade.get("pnl", 0)) >= 0:
                continue

            symbol = trade.get("symbol")
            side = trade.get("side")

            if symbol in market:
                change_24h = float(market[symbol].get("change_24h", 0))

                # Short in uptrend or long in downtrend
                if (side == "short" and change_24h > 2) or (side == "long" and change_24h < -2):
                    counter_trend_losses.append(trade)

        if len(counter_trend_losses) >= 2:
            total_loss = sum(float(t.get("pnl", 0)) for t in counter_trend_losses)
            return {
                "count": len(counter_trend_losses),
                "total_loss": abs(total_loss),
            }
        return None

    def _detect_loss_streak(self, trades: list) -> Optional[dict]:
        """Detect consecutive losing trades."""
        if len(trades) < 3:
            return None

        # Count consecutive losses from most recent
        streak = 0
        total_loss = 0

        for trade in trades:
            pnl = float(trade.get("pnl", 0))
            if pnl < 0:
                streak += 1
                total_loss += abs(pnl)
            else:
                break

        if streak >= 3:
            return {"count": streak, "total_loss": total_loss}
        return None

    def _group_by_symbol(self, trades: list) -> dict[str, list]:
        """Group trades by symbol."""
        groups: dict[str, list] = {}
        for trade in trades:
            symbol = trade.get("symbol")
            if symbol:
                if symbol not in groups:
                    groups[symbol] = []
                groups[symbol].append(trade)
        return groups

    def _get_symbol_recommendation(self, wins: int, total: int, total_pnl: float) -> str:
        """Get recommendation for a symbol based on performance."""
        if total < 3:
            return "Insufficient data"

        win_rate = wins / total

        if win_rate >= 0.6 and total_pnl > 0:
            return "CONTINUE - Strong performance"
        elif win_rate >= 0.5 and total_pnl > 0:
            return "CONTINUE - Positive but monitor"
        elif win_rate < 0.4 or total_pnl < -50:
            return "AVOID - Poor performance on this symbol"
        else:
            return "REDUCE - Mixed results, trade smaller"
