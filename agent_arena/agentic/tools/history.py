"""Trade History tool - query past trades, win rate, P&L analysis."""

import asyncio
import json
from typing import Optional

from pydantic import BaseModel, Field

from agent_arena.agentic.tools.base import TradingTool


class TradeHistoryInput(BaseModel):
    """Input schema for trade history queries."""

    query_type: str = Field(
        default="summary",
        description="Query type: 'summary', 'recent', 'by_symbol', 'performance'",
    )
    symbol: Optional[str] = Field(default=None, description="Filter by symbol (optional)")
    limit: int = Field(default=10, description="Number of trades to return")


class TradeHistoryTool(TradingTool):
    """
    Query past trading history and performance metrics.

    Provides:
    - Recent trade history
    - Win rate and P&L statistics
    - Performance by symbol
    - Decision patterns analysis
    """

    name: str = "trade_history"
    description: str = """Query your past trades and performance metrics.
Use this to analyze your trading patterns and learn from past decisions.
Input: query_type (summary/recent/by_symbol/performance), symbol (optional), limit.
Returns: Trade history and statistics based on query type."""

    args_schema: type[BaseModel] = TradeHistoryInput

    def _run(
        self,
        query_type: str = "summary",
        symbol: Optional[str] = None,
        limit: int = 10,
    ) -> str:
        """Sync wrapper - runs async method."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # We're in an async context, create a future
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as pool:
                    future = pool.submit(
                        asyncio.run, self._arun(query_type, symbol, limit)
                    )
                    return future.result()
            else:
                return loop.run_until_complete(self._arun(query_type, symbol, limit))
        except Exception:
            # Fallback for when we can't get an event loop
            return asyncio.run(self._arun(query_type, symbol, limit))

    async def _arun(
        self,
        query_type: str = "summary",
        symbol: Optional[str] = None,
        limit: int = 10,
    ) -> str:
        """Query trade history from storage."""
        if not self._storage:
            return json.dumps({
                "error": "Storage not available",
                "hint": "Trade history requires database connection",
            })

        agent_id = self._context.get("agent_id", "unknown")

        if query_type == "summary":
            return await self._get_summary(agent_id)
        elif query_type == "recent":
            return await self._get_recent_trades(agent_id, limit)
        elif query_type == "by_symbol":
            return await self._get_by_symbol(agent_id, symbol)
        elif query_type == "performance":
            return await self._get_performance_metrics(agent_id)
        else:
            return json.dumps({
                "error": f"Unknown query type: {query_type}",
                "valid_types": ["summary", "recent", "by_symbol", "performance"],
            })

    async def _get_summary(self, agent_id: str) -> str:
        """Get overall trading summary."""
        try:
            trades = await self._storage.get_agent_trades(agent_id, limit=100)
        except Exception as e:
            return json.dumps({"error": f"Failed to fetch trades: {str(e)}"})

        if not trades:
            return json.dumps({
                "total_trades": 0,
                "message": "No trading history yet. This is the first session or no trades executed.",
                "suggestion": "Focus on analyzing current market conditions.",
            })

        # Calculate statistics
        pnl_values = []
        for t in trades:
            if t.get("realized_pnl"):
                try:
                    pnl_values.append(float(t["realized_pnl"]))
                except (ValueError, TypeError):
                    pass

        wins = [p for p in pnl_values if p > 0]
        losses = [p for p in pnl_values if p < 0]
        total_pnl = sum(pnl_values)

        return json.dumps({
            "total_trades": len(trades),
            "trades_with_pnl": len(pnl_values),
            "winning_trades": len(wins),
            "losing_trades": len(losses),
            "win_rate_pct": round(len(wins) / len(pnl_values) * 100, 1) if pnl_values else 0,
            "total_realized_pnl": round(total_pnl, 2),
            "avg_win": round(sum(wins) / len(wins), 2) if wins else 0,
            "avg_loss": round(sum(losses) / len(losses), 2) if losses else 0,
            "largest_win": round(max(wins), 2) if wins else 0,
            "largest_loss": round(min(losses), 2) if losses else 0,
        }, indent=2)

    async def _get_recent_trades(self, agent_id: str, limit: int) -> str:
        """Get recent trades."""
        try:
            trades = await self._storage.get_agent_trades(agent_id, limit=limit)
        except Exception as e:
            return json.dumps({"error": f"Failed to fetch trades: {str(e)}"})

        if not trades:
            return json.dumps({
                "recent_trades": [],
                "message": "No trades found",
            })

        formatted = []
        for t in trades:
            formatted.append({
                "symbol": t.get("symbol"),
                "side": t.get("side"),
                "size": t.get("size"),
                "price": t.get("price"),
                "realized_pnl": t.get("realized_pnl"),
                "timestamp": t.get("timestamp"),
            })

        return json.dumps({"recent_trades": formatted, "count": len(formatted)}, indent=2)

    async def _get_by_symbol(self, agent_id: str, symbol: Optional[str]) -> str:
        """Get performance breakdown by symbol."""
        try:
            trades = await self._storage.get_agent_trades(agent_id, limit=200)
        except Exception as e:
            return json.dumps({"error": f"Failed to fetch trades: {str(e)}"})

        if not trades:
            return json.dumps({"performance_by_symbol": {}, "message": "No trades found"})

        # Group by symbol
        by_symbol: dict = {}
        for t in trades:
            sym = t.get("symbol", "UNKNOWN")
            if symbol and sym != symbol:
                continue

            if sym not in by_symbol:
                by_symbol[sym] = {"trades": 0, "wins": 0, "losses": 0, "total_pnl": 0}

            by_symbol[sym]["trades"] += 1
            if t.get("realized_pnl"):
                pnl = float(t["realized_pnl"])
                by_symbol[sym]["total_pnl"] += pnl
                if pnl > 0:
                    by_symbol[sym]["wins"] += 1
                elif pnl < 0:
                    by_symbol[sym]["losses"] += 1

        # Calculate win rates
        for sym in by_symbol:
            total = by_symbol[sym]["wins"] + by_symbol[sym]["losses"]
            if total > 0:
                by_symbol[sym]["win_rate_pct"] = round(
                    by_symbol[sym]["wins"] / total * 100, 1
                )
            else:
                by_symbol[sym]["win_rate_pct"] = 0
            by_symbol[sym]["total_pnl"] = round(by_symbol[sym]["total_pnl"], 2)

        return json.dumps({"performance_by_symbol": by_symbol}, indent=2)

    async def _get_performance_metrics(self, agent_id: str) -> str:
        """Get detailed performance metrics."""
        try:
            trades = await self._storage.get_agent_trades(agent_id, limit=100)
            decisions = await self._storage.get_recent_decisions(agent_id, limit=100)
        except Exception as e:
            return json.dumps({"error": f"Failed to fetch data: {str(e)}"})

        pnl_values = []
        for t in trades:
            if t.get("realized_pnl"):
                try:
                    pnl_values.append(float(t["realized_pnl"]))
                except (ValueError, TypeError):
                    pass

        if not pnl_values:
            return json.dumps({
                "message": "Insufficient data for performance metrics",
                "total_decisions": len(decisions) if decisions else 0,
                "total_trades": len(trades) if trades else 0,
            })

        # Calculate metrics
        total_pnl = sum(pnl_values)
        avg_pnl = total_pnl / len(pnl_values)
        max_win = max(pnl_values)
        max_loss = min(pnl_values)

        # Profit factor
        gross_profit = sum(p for p in pnl_values if p > 0)
        gross_loss = abs(sum(p for p in pnl_values if p < 0))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        # Consecutive wins/losses
        max_consecutive_wins = 0
        max_consecutive_losses = 0
        current_wins = 0
        current_losses = 0

        for pnl in pnl_values:
            if pnl > 0:
                current_wins += 1
                current_losses = 0
                max_consecutive_wins = max(max_consecutive_wins, current_wins)
            elif pnl < 0:
                current_losses += 1
                current_wins = 0
                max_consecutive_losses = max(max_consecutive_losses, current_losses)

        return json.dumps({
            "total_realized_pnl": round(total_pnl, 2),
            "average_trade_pnl": round(avg_pnl, 2),
            "max_win": round(max_win, 2),
            "max_loss": round(max_loss, 2),
            "profit_factor": round(profit_factor, 2) if profit_factor != float("inf") else "infinite",
            "max_consecutive_wins": max_consecutive_wins,
            "max_consecutive_losses": max_consecutive_losses,
            "total_decisions": len(decisions) if decisions else 0,
            "total_executed_trades": len(trades) if trades else 0,
            "trade_execution_rate_pct": round(
                len(trades) / len(decisions) * 100, 1
            ) if decisions else 0,
        }, indent=2)
