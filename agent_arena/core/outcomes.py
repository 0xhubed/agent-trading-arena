"""Calculate and track decision outcomes for learning agents."""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


@dataclass
class DecisionOutcome:
    """Outcome metrics for a decision."""

    decision_id: int
    realized_pnl: Optional[Decimal]
    holding_duration_ticks: int
    max_drawdown_during: Decimal
    max_profit_during: Decimal
    exit_reason: str  # 'manual_close', 'stop_loss', 'take_profit', 'liquidation'
    outcome_score: float
    risk_adjusted_return: float

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return {
            "decision_id": self.decision_id,
            "realized_pnl": float(self.realized_pnl) if self.realized_pnl else None,
            "holding_duration_ticks": self.holding_duration_ticks,
            "max_drawdown_during": float(self.max_drawdown_during),
            "max_profit_during": float(self.max_profit_during),
            "exit_reason": self.exit_reason,
            "outcome_score": self.outcome_score,
            "risk_adjusted_return": self.risk_adjusted_return,
        }


def calculate_outcome_score(
    realized_pnl: Decimal,
    holding_duration: int,
    max_drawdown: Decimal,
    entry_capital: Decimal = Decimal("10000"),
) -> float:
    """
    Calculate normalized outcome score (-1 to 1).

    The score considers:
    - Return (primary factor)
    - Risk-adjusted return (reward/risk ratio)
    - Time efficiency (quicker profits are better)

    Args:
        realized_pnl: Final P&L of the trade.
        holding_duration: Number of ticks held.
        max_drawdown: Maximum drawdown during the trade.
        entry_capital: Starting capital for percentage calculation.

    Returns:
        Score between -1 (worst) and 1 (best).
    """
    if entry_capital == 0:
        return 0.0

    # Return component (normalized to reasonable range)
    return_pct = float(realized_pnl / entry_capital)
    return_score = math.tanh(return_pct * 10)  # Squash to (-1, 1)

    # Risk-adjusted component (reward/risk ratio)
    if max_drawdown > 0:
        risk_adjusted = float(realized_pnl / max_drawdown)
        risk_score = math.tanh(risk_adjusted)
    else:
        # No drawdown - perfect trade or small profit
        risk_score = 1.0 if realized_pnl > 0 else 0.0

    # Time efficiency (prefer quicker profits, longer holds for losses aren't penalized as much)
    # Normalize holding duration (assume 100 ticks is "standard")
    if realized_pnl > 0:
        # Quick profits are better
        time_factor = 1.0 / (1.0 + holding_duration / 50)
    else:
        # Cutting losses quickly is also good
        time_factor = 1.0 / (1.0 + holding_duration / 100)

    # Weighted combination
    outcome_score = (
        0.5 * return_score +  # 50% weight on raw return
        0.3 * risk_score +     # 30% weight on risk-adjusted return
        0.2 * (return_score * time_factor)  # 20% weight on time-adjusted return
    )

    return max(-1.0, min(1.0, outcome_score))


def calculate_risk_adjusted_return(
    realized_pnl: Decimal,
    max_drawdown: Decimal,
) -> float:
    """
    Calculate simple risk-adjusted return (profit / max drawdown).

    Args:
        realized_pnl: Final P&L of the trade.
        max_drawdown: Maximum drawdown during the trade.

    Returns:
        Risk-adjusted return ratio.
    """
    if max_drawdown <= 0:
        return float(realized_pnl) if realized_pnl != 0 else 0.0

    return float(realized_pnl / max_drawdown)


def classify_outcome(outcome: DecisionOutcome) -> str:
    """
    Classify an outcome into categories for pattern learning.

    Args:
        outcome: DecisionOutcome object.

    Returns:
        Classification string: 'excellent', 'good', 'neutral', 'poor', 'bad'
    """
    score = outcome.outcome_score

    if score >= 0.7:
        return "excellent"
    elif score >= 0.3:
        return "good"
    elif score >= -0.3:
        return "neutral"
    elif score >= -0.7:
        return "poor"
    else:
        return "bad"


def get_outcome_feedback(outcome: DecisionOutcome) -> str:
    """
    Generate human-readable feedback for an outcome.

    Args:
        outcome: DecisionOutcome object.

    Returns:
        Feedback string for learning.
    """
    classification = classify_outcome(outcome)
    pnl = float(outcome.realized_pnl) if outcome.realized_pnl else 0
    duration = outcome.holding_duration_ticks
    exit = outcome.exit_reason
    risk_adj = outcome.risk_adjusted_return

    lines = []

    if classification == "excellent":
        lines.append("Excellent trade execution.")
        if risk_adj > 2:
            lines.append(f"Outstanding risk/reward ratio of {risk_adj:.1f}.")
        if duration < 20 and pnl > 0:
            lines.append("Quick profit capture was effective.")
    elif classification == "good":
        lines.append("Good trade with positive outcome.")
        if pnl > 0:
            lines.append(f"Profitable by ${pnl:.2f}.")
    elif classification == "neutral":
        lines.append("Trade had minimal impact.")
        if duration > 50:
            lines.append("Consider tighter stop-loss for long holds.")
    elif classification == "poor":
        lines.append("Trade resulted in a loss.")
        if exit == "stop_loss":
            lines.append("Stop-loss triggered as expected.")
        elif exit == "liquidation":
            lines.append("Position was liquidated - review leverage usage.")
    else:  # bad
        lines.append("Significant loss on this trade.")
        if float(outcome.max_drawdown_during) > abs(pnl):
            lines.append("Trade experienced large drawdown before closing.")
        lines.append("Review entry conditions and risk management.")

    return " ".join(lines)


@dataclass
class PendingOutcome:
    """Tracks a decision awaiting outcome."""

    decision_id: int
    agent_id: str
    symbol: str
    action: str
    entry_tick: int
    entry_price: Decimal
    entry_equity: Decimal

    # Tracking during position hold
    max_equity: Decimal = Decimal("0")
    min_equity: Decimal = Decimal("999999999")
    peak_profit: Decimal = Decimal("0")
    max_drawdown: Decimal = Decimal("0")


class OutcomeTracker:
    """
    Tracks outcomes for open decisions.

    This class monitors positions from entry to exit, tracking:
    - Maximum profit achieved during the hold
    - Maximum drawdown experienced
    - Equity fluctuations
    """

    def __init__(self):
        # Map: (agent_id, symbol) -> PendingOutcome
        self.pending: dict[tuple[str, str], PendingOutcome] = {}

    def register_decision(
        self,
        decision_id: int,
        agent_id: str,
        symbol: str,
        action: str,
        tick: int,
        price: Decimal,
        equity: Decimal,
    ) -> None:
        """
        Register a new decision for outcome tracking.

        Args:
            decision_id: Database ID of the decision.
            agent_id: Agent making the decision.
            symbol: Trading symbol.
            action: Decision action (open_long, open_short, etc.).
            tick: Current tick number.
            price: Entry price.
            equity: Current equity at entry.
        """
        if action in ("open_long", "open_short", "limit_long", "limit_short"):
            key = (agent_id, symbol)
            self.pending[key] = PendingOutcome(
                decision_id=decision_id,
                agent_id=agent_id,
                symbol=symbol,
                action=action,
                entry_tick=tick,
                entry_price=price,
                entry_equity=equity,
                max_equity=equity,
                min_equity=equity,
            )

    def update_equity(
        self,
        agent_id: str,
        symbol: str,
        current_equity: Decimal,
        unrealized_pnl: Decimal,
    ) -> None:
        """
        Update tracking for open position.

        Call this on each tick to track equity and P&L fluctuations.

        Args:
            agent_id: Agent ID.
            symbol: Trading symbol.
            current_equity: Current total equity.
            unrealized_pnl: Current unrealized P&L of the position.
        """
        key = (agent_id, symbol)
        if key not in self.pending:
            return

        pending = self.pending[key]

        # Track equity extremes
        pending.max_equity = max(pending.max_equity, current_equity)
        pending.min_equity = min(pending.min_equity, current_equity)

        # Track P&L extremes
        pending.peak_profit = max(pending.peak_profit, unrealized_pnl)

        # Calculate drawdown from peak
        drawdown = pending.peak_profit - unrealized_pnl
        pending.max_drawdown = max(pending.max_drawdown, drawdown)

    def complete_outcome(
        self,
        agent_id: str,
        symbol: str,
        realized_pnl: Decimal,
        exit_tick: int,
        exit_reason: str,
    ) -> Optional[DecisionOutcome]:
        """
        Complete outcome calculation when position closes.

        Args:
            agent_id: Agent ID.
            symbol: Trading symbol.
            realized_pnl: Final realized P&L.
            exit_tick: Tick when position closed.
            exit_reason: Why position closed ('manual_close', 'stop_loss', etc.).

        Returns:
            DecisionOutcome object or None if no pending outcome found.
        """
        key = (agent_id, symbol)
        if key not in self.pending:
            return None

        pending = self.pending.pop(key)

        holding_duration = exit_tick - pending.entry_tick

        outcome_score = calculate_outcome_score(
            realized_pnl=realized_pnl,
            holding_duration=holding_duration,
            max_drawdown=pending.max_drawdown,
            entry_capital=pending.entry_equity,
        )

        risk_adjusted = calculate_risk_adjusted_return(
            realized_pnl=realized_pnl,
            max_drawdown=pending.max_drawdown,
        )

        return DecisionOutcome(
            decision_id=pending.decision_id,
            realized_pnl=realized_pnl,
            holding_duration_ticks=holding_duration,
            max_drawdown_during=pending.max_drawdown,
            max_profit_during=pending.peak_profit,
            exit_reason=exit_reason,
            outcome_score=outcome_score,
            risk_adjusted_return=risk_adjusted,
        )

    def get_pending(self, agent_id: str, symbol: str) -> Optional[PendingOutcome]:
        """Get pending outcome for a position if it exists."""
        return self.pending.get((agent_id, symbol))

    def has_pending(self, agent_id: str, symbol: str) -> bool:
        """Check if there's a pending outcome for a position."""
        return (agent_id, symbol) in self.pending

    def cancel_pending(self, agent_id: str, symbol: str) -> None:
        """Cancel tracking for a position (e.g., order cancelled)."""
        key = (agent_id, symbol)
        if key in self.pending:
            del self.pending[key]

    def get_all_pending(self, agent_id: Optional[str] = None) -> list[PendingOutcome]:
        """Get all pending outcomes, optionally filtered by agent."""
        if agent_id:
            return [p for p in self.pending.values() if p.agent_id == agent_id]
        return list(self.pending.values())
