"""Tool for analyzing agent performance (meta-learning)."""

from __future__ import annotations

from typing import Optional, Type

from pydantic import BaseModel, Field

from .base import TradingTool


class AgentPerformanceInput(BaseModel):
    """Input schema for agent performance tool."""

    regime: Optional[str] = Field(
        default=None,
        description=(
            "Market regime to analyze. If not provided, uses current regime. "
            "Options: trending_up, trending_down, ranging, volatile"
        )
    )
    symbol: Optional[str] = Field(
        default=None,
        description="Filter by trading symbol (e.g., BTCUSDT)"
    )
    min_trades: int = Field(
        default=10,
        description="Minimum number of trades required for inclusion"
    )


class AgentPerformanceTool(TradingTool):
    """
    Analyze which trading agents perform best in current conditions.

    This enables meta-learning: learning from the strategies and
    behaviors of other successful agents in similar market conditions.
    """

    name: str = "analyze_agent_performance"
    description: str = """Analyze which trading agents perform best in the current market regime.
Returns performance metrics for each agent in similar conditions.
Use this to identify successful strategies for the current market.

Returns list of agents ranked by performance with:
- agent_id, agent_name
- sharpe_ratio: Risk-adjusted returns
- win_rate: Percentage of profitable trades
- total_pnl: Total profit/loss in this regime
- trade_count: Number of trades analyzed"""

    args_schema: Type[BaseModel] = AgentPerformanceInput

    def _run(
        self,
        regime: Optional[str] = None,
        symbol: Optional[str] = None,
        min_trades: int = 10,
    ) -> str:
        """Synchronous fallback."""
        return "Agent performance analysis requires async execution."

    async def _arun(
        self,
        regime: Optional[str] = None,
        symbol: Optional[str] = None,
        min_trades: int = 10,
    ) -> str:
        """Get agent performance in specified regime."""
        if not self._storage:
            return "Error: Storage not available for performance analysis."

        try:
            # Use current regime if not specified
            if not regime:
                regime = self._context.get("regime", "unknown")

            if regime == "unknown":
                return (
                    "Cannot analyze performance: market regime is unknown. "
                    "Wait for more data or specify a regime manually."
                )

            # Get performance data from storage
            results = await self._storage.get_regime_performance(
                regime=regime,
                symbol=symbol,
                min_trades=min_trades,
            )

            if not results:
                return (
                    f"No agent performance data found for {regime} regime "
                    f"with at least {min_trades} trades."
                )

            # Sort by Sharpe ratio
            results.sort(
                key=lambda x: x.get("sharpe_ratio") or 0,
                reverse=True,
            )

            # Format output
            output_parts = [
                f"Agent Performance in {regime.upper()} Regime:\n"
                f"(Minimum {min_trades} trades required)\n"
            ]

            for i, agent in enumerate(results[:10], 1):  # Top 10
                agent_id = agent.get("agent_id", "unknown")
                sharpe = agent.get("sharpe_ratio")
                win_rate = agent.get("win_rate", 0)
                total_pnl = agent.get("total_pnl", 0)
                trades = agent.get("total_trades", 0)
                wins = agent.get("winning_trades", 0)

                # Format Sharpe ratio
                sharpe_str = f"{sharpe:.2f}" if sharpe else "N/A"

                # Determine performance tier
                if sharpe and sharpe > 1.5:
                    tier = "🌟"  # Excellent
                elif sharpe and sharpe > 1.0:
                    tier = "✅"  # Good
                elif sharpe and sharpe > 0:
                    tier = "➖"  # Neutral
                else:
                    tier = "⚠️"  # Poor

                output_parts.append(
                    f"\n{i}. {tier} {agent_id}\n"
                    f"   Sharpe: {sharpe_str} | "
                    f"Win Rate: {win_rate:.0%} ({wins}/{trades})\n"
                    f"   Total P&L: ${total_pnl:+,.2f}"
                )

            # Add insights
            if results:
                top_performer = results[0]
                top_id = top_performer.get("agent_id", "unknown")
                top_sharpe = top_performer.get("sharpe_ratio", 0)
                top_win_rate = top_performer.get("win_rate", 0)

                output_parts.append(
                    f"\n\n📈 Top Performer: {top_id}\n"
                    f"   In {regime} conditions, this agent achieves "
                    f"Sharpe {top_sharpe:.2f} with {top_win_rate:.0%} win rate."
                )

                # Average performance
                avg_sharpe = sum(
                    r.get("sharpe_ratio", 0) or 0 for r in results
                ) / len(results)
                avg_win_rate = sum(
                    r.get("win_rate", 0) for r in results
                ) / len(results)

                output_parts.append(
                    f"\n   Average across all agents: "
                    f"Sharpe {avg_sharpe:.2f}, Win Rate {avg_win_rate:.0%}"
                )

                # Regime-specific advice
                output_parts.append(f"\n\n💡 Strategy hints for {regime} regime:")
                if regime == "trending_up":
                    output_parts.append(
                        "   - Trend-following strategies tend to outperform\n"
                        "   - Consider longer holding periods\n"
                        "   - Look for pullback entries"
                    )
                elif regime == "trending_down":
                    output_parts.append(
                        "   - Short positions may be favorable\n"
                        "   - Tighter risk management recommended\n"
                        "   - Rally selling can be effective"
                    )
                elif regime == "ranging":
                    output_parts.append(
                        "   - Mean-reversion strategies work better\n"
                        "   - Trade support/resistance levels\n"
                        "   - Smaller position sizes recommended"
                    )
                elif regime == "volatile":
                    output_parts.append(
                        "   - Reduce position sizes significantly\n"
                        "   - Widen stop-losses or avoid trading\n"
                        "   - Wait for volatility to subside"
                    )

            return "\n".join(output_parts)

        except Exception as e:
            return f"Error analyzing agent performance: {str(e)}"
