"""FitnessEvaluator — converts backtest results into a scalar fitness score."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from agent_arena.backtest.results import AgentResult


@dataclass
class FitnessEvaluator:
    """Weighted multi-objective fitness function for agent evolution.

    Maps backtest AgentResult metrics to a 0-1 scalar fitness score.
    Default weights emphasize risk-adjusted returns (Sharpe + drawdown = 60%).
    """

    weights: dict[str, float] = field(default_factory=lambda: {
        "sharpe": 0.40,
        "return": 0.25,
        "win_rate": 0.15,
        "drawdown": 0.20,
    })

    # Minimum trades to avoid rewarding "hold" strategies
    min_trades: int = 5
    min_trade_penalty: float = 0.5

    def evaluate(self, result: AgentResult) -> float:
        """Compute scalar fitness from a backtest AgentResult.

        Returns a float roughly in [0, 1], though values slightly outside
        that range are possible for extreme performance.
        """
        sharpe_score = self._normalize_sharpe(result.sharpe_ratio)
        return_score = self._normalize_return(result.total_pnl_pct)
        win_score = result.win_rate
        dd_score = self._normalize_drawdown(result.max_drawdown_pct)

        fitness = (
            self.weights["sharpe"] * sharpe_score
            + self.weights["return"] * return_score
            + self.weights["win_rate"] * win_score
            + self.weights["drawdown"] * dd_score
        )

        # Penalty for too few trades — discourages passive "hold" genomes
        if result.total_trades < self.min_trades:
            fitness *= self.min_trade_penalty

        return round(fitness, 6)

    def evaluate_batch(self, results: dict[str, AgentResult]) -> dict[str, float]:
        """Evaluate fitness for a dict of {genome_id: AgentResult}."""
        return {gid: self.evaluate(r) for gid, r in results.items()}

    @staticmethod
    def _normalize_sharpe(sharpe: Optional[float]) -> float:
        """Map Sharpe ratio to [0, 1]. Sharpe 0 -> 0.25, Sharpe 2 -> 1.0."""
        s = max(-2.0, min(3.0, sharpe or 0.0))
        # Linear: -2 -> 0, 0 -> 0.4, 2 -> 0.8, 3 -> 1.0
        return max(0.0, min(1.0, (s + 2.0) / 5.0))

    @staticmethod
    def _normalize_return(total_pnl_pct: float) -> float:
        """Map return percentage to [0, 1]. 0% -> 0.5, +50% -> 1.0, -50% -> 0.0."""
        r = max(-50.0, min(50.0, total_pnl_pct))
        return (r + 50.0) / 100.0

    @staticmethod
    def _normalize_drawdown(max_dd_pct: float) -> float:
        """Map max drawdown to [0, 1]. 0% DD -> 1.0, 20%+ DD -> 0.0."""
        dd = max(0.0, min(20.0, abs(max_dd_pct)))
        return max(0.0, 1.0 - dd / 20.0)
