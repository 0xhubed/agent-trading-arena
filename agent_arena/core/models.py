"""Core models for Agent Arena trading simulation."""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import ClassVar, Optional


class Side(Enum):
    """Position side."""

    LONG = "long"
    SHORT = "short"


class OrderType(Enum):
    """Order type for pending orders."""

    LIMIT_LONG = "limit_long"
    LIMIT_SHORT = "limit_short"


@dataclass
class Decision:
    """
    The standardized output from any agent.
    This is the ONLY thing agents must conform to.

    Actions:
    - "hold": Do nothing
    - "open_long": Open a long position (market order)
    - "open_short": Open a short position (market order)
    - "close": Close position (full or partial)
    - "limit_long": Place a limit order to open long
    - "limit_short": Place a limit order to open short
    - "set_stop_loss": Set stop-loss on existing position
    - "set_take_profit": Set take-profit on existing position
    - "cancel_order": Cancel a pending limit order
    """

    action: str
    symbol: Optional[str] = None
    size: Optional[Decimal] = None
    leverage: int = 1

    # Limit order fields
    limit_price: Optional[Decimal] = None  # Target price for limit orders

    # Stop-loss/Take-profit fields
    stop_loss_price: Optional[Decimal] = None  # Auto-close trigger for loss
    take_profit_price: Optional[Decimal] = None  # Auto-close trigger for profit

    confidence: float = 0.5  # 0.0 to 1.0
    reasoning: str = ""

    metadata: dict = field(default_factory=dict)
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)


@dataclass
class Position:
    """An open futures position."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str = ""
    side: Side = Side.LONG
    size: Decimal = Decimal("0")
    entry_price: Decimal = Decimal("0")
    leverage: int = 1
    margin: Decimal = Decimal("0")
    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    mark_price: Decimal = Decimal("0")

    # Stop-loss/Take-profit triggers
    stop_loss_price: Optional[Decimal] = None
    take_profit_price: Optional[Decimal] = None

    @property
    def notional(self) -> Decimal:
        """Current notional value."""
        return self.size * self.mark_price

    @property
    def unrealized_pnl(self) -> Decimal:
        """Unrealized profit/loss."""
        if self.side == Side.LONG:
            return (self.mark_price - self.entry_price) * self.size
        else:
            return (self.entry_price - self.mark_price) * self.size

    @property
    def roe_percent(self) -> float:
        """Return on equity (margin)."""
        if self.margin == 0:
            return 0.0
        return float((self.unrealized_pnl / self.margin) * 100)

    @property
    def liquidation_price(self) -> Decimal:
        """Approximate liquidation price."""
        mmr = Decimal("0.004")  # 0.4% maintenance margin
        if self.side == Side.LONG:
            return self.entry_price * (1 - Decimal("1") / self.leverage + mmr)
        else:
            return self.entry_price * (1 + Decimal("1") / self.leverage - mmr)


@dataclass
class Trade:
    """A completed trade."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    agent_id: str = ""
    symbol: str = ""
    side: Side = Side.LONG
    size: Decimal = Decimal("0")
    price: Decimal = Decimal("0")
    leverage: int = 1
    fee: Decimal = Decimal("0")
    realized_pnl: Optional[Decimal] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    decision_id: Optional[str] = None


@dataclass
class PendingOrder:
    """A pending limit order waiting to be triggered."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    agent_id: str = ""
    symbol: str = ""
    order_type: OrderType = OrderType.LIMIT_LONG
    size: Decimal = Decimal("0")
    limit_price: Decimal = Decimal("0")
    leverage: int = 1
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Optional SL/TP to set when order fills
    stop_loss_price: Optional[Decimal] = None
    take_profit_price: Optional[Decimal] = None

    def should_trigger(self, current_price: Decimal) -> bool:
        """Check if order should trigger at current price."""
        if self.order_type == OrderType.LIMIT_LONG:
            # Buy limit: trigger when price drops to or below limit
            return current_price <= self.limit_price
        elif self.order_type == OrderType.LIMIT_SHORT:
            # Sell limit: trigger when price rises to or above limit
            return current_price >= self.limit_price
        return False


@dataclass
class Portfolio:
    """Agent's trading portfolio."""

    MAX_TRADES_HISTORY: ClassVar[int] = 1000  # Limit trades list to prevent unbounded growth

    agent_id: str
    initial_capital: Decimal = Decimal("10000")
    available_margin: Decimal = Decimal("10000")
    positions: dict[str, Position] = field(default_factory=dict)
    trades: list[Trade] = field(default_factory=list)
    pending_orders: list[PendingOrder] = field(default_factory=list)
    realized_pnl: Decimal = Decimal("0")

    def add_trade(self, trade: Trade) -> None:
        """Add a trade and prune old trades if exceeding limit."""
        self.trades.append(trade)
        if len(self.trades) > self.MAX_TRADES_HISTORY:
            self.trades = self.trades[-self.MAX_TRADES_HISTORY:]

    @property
    def unrealized_pnl(self) -> Decimal:
        """Total unrealized P&L across all positions."""
        return sum(p.unrealized_pnl for p in self.positions.values())

    @property
    def total_pnl(self) -> Decimal:
        """Total realized + unrealized P&L."""
        return self.realized_pnl + self.unrealized_pnl

    @property
    def equity(self) -> Decimal:
        """Current equity value."""
        return self.initial_capital + self.total_pnl

    @property
    def equity_percent(self) -> float:
        """Equity change as percentage."""
        return float((self.equity / self.initial_capital - 1) * 100)

    @property
    def used_margin(self) -> Decimal:
        """Total margin currently used in positions."""
        return sum(p.margin for p in self.positions.values())

    @property
    def margin_utilization(self) -> float:
        """Percentage of equity used as margin."""
        if self.equity == 0:
            return 0.0
        return float((self.used_margin / self.equity) * 100)

    def to_context(self) -> dict:
        """Convert portfolio to context dict for agents."""
        now = datetime.now(timezone.utc)

        positions_ctx = []
        for p in self.positions.values():
            hold_seconds = (now - p.opened_at).total_seconds()
            hold_hours = hold_seconds / 3600
            roe = p.roe_percent
            liq_distance = self._liquidation_distance(p)

            # Position advisory signal
            advisory = self._position_advisory(roe, hold_hours, liq_distance, p)

            positions_ctx.append({
                "symbol": p.symbol,
                "side": p.side.value,
                "size": float(p.size),
                "entry_price": float(p.entry_price),
                "mark_price": float(p.mark_price),
                "liquidation_price": float(p.liquidation_price),
                "margin": float(p.margin),
                "unrealized_pnl": float(p.unrealized_pnl),
                "roe_percent": roe,
                "leverage": p.leverage,
                "opened_at": p.opened_at.isoformat(),
                "hold_hours": round(hold_hours, 1),
                "liq_distance_pct": round(liq_distance, 2),
                "stop_loss": float(p.stop_loss_price) if p.stop_loss_price else None,
                "take_profit": float(p.take_profit_price) if p.take_profit_price else None,
                "advisory": advisory,
            })

        # Recent trade performance (last 10 closed trades)
        recent_closed = [t for t in self.trades if t.realized_pnl is not None][-10:]
        trade_performance = self._trade_performance_summary(recent_closed)

        return {
            "equity": float(self.equity),
            "available_margin": float(self.available_margin),
            "used_margin": float(self.used_margin),
            "margin_utilization": self.margin_utilization,
            "positions": positions_ctx,
            "pending_orders": [
                {
                    "id": o.id,
                    "symbol": o.symbol,
                    "order_type": o.order_type.value,
                    "size": float(o.size),
                    "limit_price": float(o.limit_price),
                    "leverage": o.leverage,
                }
                for o in self.pending_orders
            ],
            "realized_pnl": float(self.realized_pnl),
            "total_pnl": float(self.total_pnl),
            "pnl_percent": self.equity_percent,
            "trade_performance": trade_performance,
        }

    @staticmethod
    def _liquidation_distance(p: Position) -> float:
        """Percentage distance from mark price to liquidation price."""
        if p.mark_price == 0:
            return 100.0
        return float(abs(p.mark_price - p.liquidation_price) / p.mark_price * 100)

    @staticmethod
    def _position_advisory(
        roe: float, hold_hours: float, liq_distance: float, p: Position
    ) -> str:
        """Generate a concise advisory signal for a position."""
        signals = []

        # Liquidation proximity (highest priority)
        if liq_distance < 2.0:
            signals.append("DANGER: near liquidation — reduce position or add margin")
        elif liq_distance < 5.0:
            signals.append("WARNING: liquidation within 5% — consider reducing size")

        # Profit-taking signals
        if roe > 50:
            signals.append(f"Strong profit (+{roe:.0f}% ROE) — consider taking partial profits")
        elif roe > 20:
            signals.append(f"Good profit (+{roe:.0f}% ROE) — consider setting a take-profit")
        elif roe > 10 and hold_hours > 12:
            signals.append("Moderate profit, held >12h — consider locking in gains")

        # Loss-cutting signals
        if roe < -30:
            signals.append(f"Deep loss ({roe:.0f}% ROE) — strongly consider cutting losses")
        elif roe < -15:
            signals.append(f"Significant loss ({roe:.0f}% ROE) — consider reducing position")
        elif roe < -8 and hold_hours > 6:
            signals.append("Loss growing over time — reassess thesis or cut partial")

        # Stale position (held too long with small movement)
        if hold_hours > 24 and abs(roe) < 3:
            signals.append("Held >24h with <3% move — capital may be better deployed elsewhere")

        # No stop-loss warning
        if not p.stop_loss_price and abs(roe) < 5:
            signals.append("No stop-loss set — consider adding one for risk management")

        return "; ".join(signals) if signals else "Position healthy"

    @staticmethod
    def _trade_performance_summary(recent_trades: list[Trade]) -> dict:
        """Summarize recent closed trade performance."""
        if not recent_trades:
            return {"total": 0, "summary": "No closed trades yet"}

        wins = [t for t in recent_trades if t.realized_pnl and t.realized_pnl > 0]
        losses = [t for t in recent_trades if t.realized_pnl and t.realized_pnl < 0]
        total_pnl = sum(float(t.realized_pnl) for t in recent_trades if t.realized_pnl)

        avg_win = (
            sum(float(t.realized_pnl) for t in wins) / len(wins) if wins else 0
        )
        avg_loss = (
            sum(float(t.realized_pnl) for t in losses) / len(losses) if losses else 0
        )

        return {
            "total": len(recent_trades),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": len(wins) / len(recent_trades) if recent_trades else 0,
            "total_pnl": round(total_pnl, 2),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "summary": (
                f"Last {len(recent_trades)} trades: "
                f"{len(wins)}W/{len(losses)}L, "
                f"avg win ${avg_win:+.2f}, avg loss ${avg_loss:+.2f}, "
                f"net ${total_pnl:+.2f}"
            ),
        }


@dataclass
class EquitySnapshot:
    """A snapshot of portfolio equity at a point in time."""

    tick: int
    timestamp: datetime
    equity: Decimal

    def to_dict(self) -> dict:
        return {
            "tick": self.tick,
            "timestamp": self.timestamp.isoformat(),
            "equity": float(self.equity),
        }


@dataclass
class PortfolioAnalytics:
    """Comprehensive performance analytics for a portfolio."""

    # Trade statistics
    total_trades: int = 0  # All trades (opens + closes)
    closed_trades: int = 0  # Only closing trades with realized P&L
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0  # winning / closed_trades

    # P&L metrics
    total_pnl: Decimal = Decimal("0")
    total_fees_paid: Decimal = Decimal("0")
    total_funding_paid: Decimal = Decimal("0")
    total_funding_received: Decimal = Decimal("0")
    net_funding: Decimal = Decimal("0")

    # Risk metrics
    max_drawdown: float = 0.0  # Maximum peak-to-trough decline (percentage)
    max_drawdown_amount: Decimal = Decimal("0")  # Maximum drawdown in currency
    max_drawdown_duration: int = 0  # Ticks in max drawdown period
    current_drawdown: float = 0.0  # Current drawdown percentage

    # Performance ratios
    sharpe_ratio: float = 0.0  # Risk-adjusted return (annualized)
    profit_factor: float = 0.0  # Gross profit / gross loss
    average_win: Decimal = Decimal("0")
    average_loss: Decimal = Decimal("0")
    largest_win: Decimal = Decimal("0")
    largest_loss: Decimal = Decimal("0")
    expectancy: Decimal = Decimal("0")  # Expected value per trade

    # Position metrics
    average_holding_ticks: float = 0.0  # Average ticks position held
    average_leverage_used: float = 0.0

    # Equity tracking
    equity_high: Decimal = Decimal("0")  # All-time high equity
    equity_low: Decimal = Decimal("0")  # All-time low equity
    equity_history: list[EquitySnapshot] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            # Trade statistics
            "total_trades": self.total_trades,
            "closed_trades": self.closed_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": round(self.win_rate, 4),
            # P&L metrics
            "total_pnl": float(self.total_pnl),
            "total_fees_paid": float(self.total_fees_paid),
            "total_funding_paid": float(self.total_funding_paid),
            "total_funding_received": float(self.total_funding_received),
            "net_funding": float(self.net_funding),
            # Risk metrics
            "max_drawdown": round(self.max_drawdown, 4),
            "max_drawdown_amount": float(self.max_drawdown_amount),
            "max_drawdown_duration": self.max_drawdown_duration,
            "current_drawdown": round(self.current_drawdown, 4),
            # Performance ratios
            "sharpe_ratio": round(self.sharpe_ratio, 4),
            "profit_factor": round(self.profit_factor, 4),
            "average_win": float(self.average_win),
            "average_loss": float(self.average_loss),
            "largest_win": float(self.largest_win),
            "largest_loss": float(self.largest_loss),
            "expectancy": float(self.expectancy),
            # Position metrics
            "average_holding_ticks": round(self.average_holding_ticks, 2),
            "average_leverage_used": round(self.average_leverage_used, 2),
            # Equity tracking
            "equity_high": float(self.equity_high),
            "equity_low": float(self.equity_low),
        }

    @classmethod
    def calculate(
        cls,
        portfolio: "Portfolio",
        equity_history: list[EquitySnapshot],
        funding_paid: Decimal = Decimal("0"),
        funding_received: Decimal = Decimal("0"),
        ticks_per_year: int = 17520,  # ~30-min ticks per year
    ) -> "PortfolioAnalytics":
        """
        Calculate comprehensive analytics from portfolio and equity history.

        Args:
            portfolio: The portfolio to analyze
            equity_history: List of equity snapshots over time
            funding_paid: Total funding paid (outgoing)
            funding_received: Total funding received (incoming)
            ticks_per_year: Number of ticks in a year (for Sharpe calculation)
        """
        analytics = cls()

        # Trade statistics
        trades = portfolio.trades
        analytics.total_trades = len(trades)

        if analytics.total_trades == 0:
            analytics.equity_high = portfolio.initial_capital
            analytics.equity_low = portfolio.initial_capital
            return analytics

        # Separate winning and losing trades (by realized P&L)
        closing_trades = [t for t in trades if t.realized_pnl is not None]
        winning = [t for t in closing_trades if t.realized_pnl > 0]
        losing = [t for t in closing_trades if t.realized_pnl < 0]

        analytics.closed_trades = len(closing_trades)
        analytics.winning_trades = len(winning)
        analytics.losing_trades = len(losing)
        analytics.win_rate = (
            len(winning) / len(closing_trades) if closing_trades else 0.0
        )

        # P&L metrics
        analytics.total_pnl = portfolio.total_pnl
        analytics.total_fees_paid = sum(t.fee for t in trades)
        analytics.total_funding_paid = funding_paid
        analytics.total_funding_received = funding_received
        analytics.net_funding = funding_received - funding_paid

        # Win/loss metrics
        if winning:
            gross_profit = sum(t.realized_pnl for t in winning)
            analytics.average_win = gross_profit / len(winning)
            analytics.largest_win = max(t.realized_pnl for t in winning)
        else:
            gross_profit = Decimal("0")

        if losing:
            gross_loss = abs(sum(t.realized_pnl for t in losing))
            analytics.average_loss = gross_loss / len(losing)
            analytics.largest_loss = abs(min(t.realized_pnl for t in losing))
        else:
            gross_loss = Decimal("0")

        # Profit factor
        if gross_loss > 0:
            analytics.profit_factor = float(gross_profit / gross_loss)
        elif gross_profit > 0:
            analytics.profit_factor = float("inf")

        # Expectancy (expected P&L per trade)
        if closing_trades:
            total_realized = sum(t.realized_pnl for t in closing_trades)
            analytics.expectancy = total_realized / len(closing_trades)

        # Position metrics - average leverage from opening trades
        opening_trades = [t for t in trades if t.realized_pnl is None]
        if opening_trades:
            total_leverage = sum(t.leverage for t in opening_trades)
            analytics.average_leverage_used = total_leverage / len(opening_trades)

        # Equity tracking and drawdown
        if equity_history:
            equities = [s.equity for s in equity_history]
            analytics.equity_high = max(equities)
            analytics.equity_low = min(equities)

            # Calculate max drawdown
            peak = equities[0]
            max_dd = Decimal("0")
            max_dd_pct = 0.0
            max_dd_duration = 0
            in_drawdown = False
            dd_start_tick = 0

            for i, eq in enumerate(equities):
                if eq > peak:
                    peak = eq
                    if in_drawdown:
                        # Exiting drawdown
                        dd_duration = i - dd_start_tick
                        if dd_duration > max_dd_duration:
                            max_dd_duration = dd_duration
                    in_drawdown = False
                else:
                    drawdown = peak - eq
                    drawdown_pct = float(drawdown / peak) if peak > 0 else 0.0
                    if not in_drawdown:
                        in_drawdown = True
                        dd_start_tick = i
                    if drawdown > max_dd:
                        max_dd = drawdown
                        max_dd_pct = drawdown_pct

            # Current drawdown
            current_peak = max(equities)
            current_eq = equities[-1]
            if current_eq < current_peak:
                analytics.current_drawdown = float((current_peak - current_eq) / current_peak)

            analytics.max_drawdown = max_dd_pct
            analytics.max_drawdown_amount = max_dd
            analytics.max_drawdown_duration = max_dd_duration

            # Sharpe ratio (risk-adjusted returns)
            # Require minimum 30 data points for meaningful Sharpe calculation
            min_sharpe_samples = 30
            if len(equities) >= min_sharpe_samples:
                # Calculate returns between ticks
                returns = []
                for i in range(1, len(equities)):
                    prev_eq = equities[i - 1]
                    curr_eq = equities[i]
                    if prev_eq > 0:
                        ret = float((curr_eq - prev_eq) / prev_eq)
                        returns.append(ret)

                if len(returns) >= min_sharpe_samples - 1:
                    mean_return = sum(returns) / len(returns)
                    variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)
                    std_dev = math.sqrt(variance) if variance > 0 else 0

                    if std_dev > 0:
                        # Annualized Sharpe (assuming risk-free rate = 0)
                        raw_sharpe = (mean_return / std_dev) * math.sqrt(ticks_per_year)
                        # Clamp to reasonable bounds (-10 to 10)
                        # Sharpe > 3 is already exceptional, > 10 is unrealistic
                        analytics.sharpe_ratio = max(-10.0, min(10.0, raw_sharpe))

        analytics.equity_history = equity_history
        return analytics
