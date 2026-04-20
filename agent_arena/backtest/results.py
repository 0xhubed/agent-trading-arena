"""Backtest result dataclasses."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Optional


@dataclass
class TradeRecord:
    """Record of a single trade during backtest."""

    id: str
    tick: int
    timestamp: str
    symbol: str
    side: str  # "long" or "short"
    action: str  # "open" or "close"
    size: Decimal
    price: Decimal
    leverage: int
    fee: Decimal
    realized_pnl: Optional[Decimal] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "tick": self.tick,
            "timestamp": self.timestamp,
            "symbol": self.symbol,
            "side": self.side,
            "action": self.action,
            "size": float(self.size),
            "price": float(self.price),
            "leverage": self.leverage,
            "fee": float(self.fee),
            "realized_pnl": float(self.realized_pnl) if self.realized_pnl else None,
        }


@dataclass
class EquityPoint:
    """Single point on the equity curve."""

    tick: int
    timestamp: str
    equity: Decimal
    pnl: Decimal
    pnl_pct: float

    def to_dict(self) -> dict:
        return {
            "tick": self.tick,
            "timestamp": self.timestamp,
            "equity": float(self.equity),
            "pnl": float(self.pnl),
            "pnl_pct": self.pnl_pct,
        }


@dataclass
class AgentResult:
    """Results for a single agent in a backtest."""

    agent_id: str
    agent_name: str

    # Final metrics
    initial_capital: Decimal = Decimal("10000")
    final_equity: Decimal = Decimal("10000")
    total_pnl: Decimal = Decimal("0")
    total_pnl_pct: float = 0.0

    # Trade statistics
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0

    # Risk metrics
    sharpe_ratio: Optional[float] = None
    profit_factor: Optional[float] = None
    max_drawdown_pct: float = 0.0
    max_drawdown_amount: Decimal = Decimal("0")

    # Trade metrics
    avg_trade_pnl: Optional[float] = None
    largest_win: Optional[Decimal] = None
    largest_loss: Optional[Decimal] = None
    avg_holding_time: Optional[float] = None  # In ticks

    # Cost metrics
    total_fees: Decimal = Decimal("0")
    total_funding_paid: Decimal = Decimal("0")
    total_funding_received: Decimal = Decimal("0")

    # Detailed records
    equity_curve: list[EquityPoint] = field(default_factory=list)
    trades: list[TradeRecord] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "agent_name": self.agent_name,
            "initial_capital": float(self.initial_capital),
            "final_equity": float(self.final_equity),
            "total_pnl": float(self.total_pnl),
            "total_pnl_pct": self.total_pnl_pct,
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": self.win_rate,
            "sharpe_ratio": self.sharpe_ratio,
            "profit_factor": self.profit_factor,
            "max_drawdown_pct": self.max_drawdown_pct,
            "max_drawdown_amount": float(self.max_drawdown_amount),
            "avg_trade_pnl": self.avg_trade_pnl,
            "largest_win": float(self.largest_win) if self.largest_win else None,
            "largest_loss": float(self.largest_loss) if self.largest_loss else None,
            "avg_holding_time": self.avg_holding_time,
            "total_fees": float(self.total_fees),
            "total_funding_paid": float(self.total_funding_paid),
            "total_funding_received": float(self.total_funding_received),
        }

    @property
    def total_return(self) -> float:
        """Return as a decimal (0.10 = 10%)."""
        return self.total_pnl_pct / 100.0

    def to_summary_dict(self) -> dict:
        """Get a summary without detailed records."""
        return self.to_dict()

    def to_full_dict(self) -> dict:
        """Get full results including equity curve and trades."""
        result = self.to_dict()
        result["equity_curve"] = [p.to_dict() for p in self.equity_curve]
        result["trades"] = [t.to_dict() for t in self.trades]
        return result


@dataclass
class ComparisonResult:
    """Statistical comparison between an agent and a baseline."""

    agent_id: str
    baseline_id: str
    outperformance: float  # agent return - baseline return
    p_value: Optional[float] = None
    ci_low: Optional[float] = None
    ci_high: Optional[float] = None
    is_significant: bool = False

    def to_dict(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "baseline_id": self.baseline_id,
            "outperformance": self.outperformance,
            "p_value": self.p_value,
            "ci_low": self.ci_low,
            "ci_high": self.ci_high,
            "is_significant": self.is_significant,
        }


@dataclass
class BacktestResult:
    """Complete results from a backtest run."""

    run_id: str
    name: str
    start_date: str
    end_date: str
    tick_interval: str

    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None

    # Progress
    total_ticks: int = 0
    completed_ticks: int = 0
    status: str = "pending"  # pending, running, completed, failed
    error_message: Optional[str] = None

    # Cost tracking
    estimated_cost: Optional[float] = None
    actual_cost: Optional[float] = None

    # Results
    agent_results: dict[str, AgentResult] = field(default_factory=dict)
    comparisons: list[ComparisonResult] = field(default_factory=list)

    # Configuration used
    config: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        # Build agents array with full results for frontend
        agents = []
        for agent_id, result in self.agent_results.items():
            agent_dict = result.to_full_dict()
            # Add total_return as decimal (0.10 = 10%) for frontend
            agent_dict["total_return"] = result.total_return
            agents.append(agent_dict)

        # Sort by final equity (best first)
        agents.sort(key=lambda x: x["final_equity"], reverse=True)

        return {
            "run_id": self.run_id,
            "name": self.name,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "tick_interval": self.tick_interval,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "total_ticks": self.total_ticks,
            "completed_ticks": self.completed_ticks,
            "status": self.status,
            "error_message": self.error_message,
            "estimated_cost": self.estimated_cost,
            "actual_cost": self.actual_cost,
            # Frontend expects 'agents' as array, not 'agent_results' as dict
            "agents": agents,
            # Also keep agent_results for backwards compatibility
            "agent_results": {
                agent_id: result.to_summary_dict()
                for agent_id, result in self.agent_results.items()
            },
            "comparisons": [c.to_dict() for c in self.comparisons],
            # Frontend expects config with symbols
            "config": self.config or {
                "start_date": self.start_date,
                "end_date": self.end_date,
                "tick_interval": self.tick_interval,
                "symbols": [],
            },
        }

    def get_leaderboard(self) -> list[dict]:
        """Get sorted leaderboard of agent results."""
        sorted_results = sorted(
            self.agent_results.values(),
            key=lambda r: r.total_pnl,
            reverse=True,
        )
        return [
            {
                "rank": i + 1,
                "agent_id": r.agent_id,
                "agent_name": r.agent_name,
                "total_pnl": float(r.total_pnl),
                "total_pnl_pct": r.total_pnl_pct,
                "final_equity": float(r.final_equity),
                "win_rate": r.win_rate,
                "sharpe_ratio": r.sharpe_ratio,
                "max_drawdown_pct": r.max_drawdown_pct,
                "total_trades": r.total_trades,
            }
            for i, r in enumerate(sorted_results)
        ]

    @property
    def agents(self) -> list[AgentResult]:
        """Get list of agent results for convenient iteration."""
        return list(self.agent_results.values())
