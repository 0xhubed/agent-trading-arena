"""Trading arena for executing trades and tracking P&L."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from agent_arena.core.config import ConstraintsConfig, FeeConfig
from agent_arena.core.models import (
    Decision,
    EquitySnapshot,
    OrderType,
    PendingOrder,
    Portfolio,
    PortfolioAnalytics,
    Position,
    Side,
    Trade,
)


class TradingArena:
    """
    Executes trades and tracks P&L for all agents.
    Simulates futures trading with realistic mechanics.
    """

    FUNDING_INTERVAL_SECONDS = 28800  # 8 hours in seconds
    MAX_EQUITY_HISTORY = 1000  # Limit snapshots to prevent unbounded growth
    SECONDS_PER_YEAR = 365 * 24 * 60 * 60  # For Sharpe annualization

    def __init__(
        self,
        symbols: list[str],
        fees: Optional[FeeConfig] = None,
        constraints: Optional[ConstraintsConfig] = None,
        tick_interval_seconds: int = 60,
    ):
        self.symbols = symbols
        self.portfolios: dict[str, Portfolio] = {}
        self.current_prices: dict[str, Decimal] = {}
        self.tick_interval_seconds = tick_interval_seconds

        # Use provided config or defaults
        self.fees = fees or FeeConfig()
        self.constraints = constraints or ConstraintsConfig()

        # Analytics tracking
        self.equity_history: dict[str, list[EquitySnapshot]] = {}  # agent_id -> snapshots
        self.funding_paid: dict[str, Decimal] = {}  # agent_id -> total paid
        self.funding_received: dict[str, Decimal] = {}  # agent_id -> total received

    def register_agent(self, agent_id: str) -> Portfolio:
        """Create a portfolio for an agent."""
        starting_capital = self.constraints.starting_capital
        portfolio = Portfolio(
            agent_id=agent_id,
            initial_capital=starting_capital,
            available_margin=starting_capital,
        )
        self.portfolios[agent_id] = portfolio

        # Initialize analytics tracking
        self.equity_history[agent_id] = []
        self.funding_paid[agent_id] = Decimal("0")
        self.funding_received[agent_id] = Decimal("0")

        return portfolio

    def restore_portfolio_state(
        self,
        agent_id: str,
        state: dict,
        current_prices: dict[str, Decimal],
    ) -> Portfolio:
        """Restore a portfolio from saved state."""
        # Create portfolio with restored values
        portfolio = Portfolio(
            agent_id=agent_id,
            initial_capital=Decimal(str(state["initial_capital"])),
            available_margin=Decimal(str(state["available_margin"])),
            realized_pnl=Decimal(str(state["realized_pnl"])),
        )

        # Restore positions
        for pos_data in state.get("positions", []):
            position = Position(
                id=pos_data["id"],
                symbol=pos_data["symbol"],
                side=Side(pos_data["side"]),
                size=Decimal(str(pos_data["size"])),
                entry_price=Decimal(str(pos_data["entry_price"])),
                leverage=pos_data["leverage"],
                margin=Decimal(str(pos_data["margin"])),
                opened_at=pos_data["opened_at"],
                mark_price=current_prices.get(pos_data["symbol"], Decimal("0")),
            )
            if pos_data.get("stop_loss_price"):
                position.stop_loss_price = Decimal(str(pos_data["stop_loss_price"]))
            if pos_data.get("take_profit_price"):
                position.take_profit_price = Decimal(str(pos_data["take_profit_price"]))
            portfolio.positions[pos_data["symbol"]] = position

        # Restore pending orders
        for order_data in state.get("pending_orders", []):
            order = PendingOrder(
                id=order_data["id"],
                agent_id=agent_id,
                symbol=order_data["symbol"],
                order_type=OrderType(order_data["order_type"]),
                size=Decimal(str(order_data["size"])),
                limit_price=Decimal(str(order_data["limit_price"])),
                leverage=order_data["leverage"],
                created_at=order_data["created_at"],
            )
            if order_data.get("stop_loss_price"):
                order.stop_loss_price = Decimal(str(order_data["stop_loss_price"]))
            if order_data.get("take_profit_price"):
                order.take_profit_price = Decimal(str(order_data["take_profit_price"]))
            portfolio.pending_orders.append(order)

        self.portfolios[agent_id] = portfolio

        # Restore funding tracking
        self.equity_history[agent_id] = []
        self.funding_paid[agent_id] = Decimal(str(state.get("funding_paid", 0)))
        self.funding_received[agent_id] = Decimal(str(state.get("funding_received", 0)))

        return portfolio

    def update_prices(self, prices: dict[str, Decimal]) -> None:
        """Update market prices and mark all positions."""
        self.current_prices = prices
        for portfolio in self.portfolios.values():
            for position in portfolio.positions.values():
                if position.symbol in prices:
                    position.mark_price = prices[position.symbol]

    def apply_funding_payments(
        self,
        funding_rates: dict[str, Decimal],
        interval_seconds: int,
    ) -> list[dict]:
        """
        Apply funding rate payments to all open positions.

        Funding is prorated based on tick interval vs 8-hour funding period.
        Positive rate: longs pay shorts
        Negative rate: shorts pay longs
        """
        payments = []
        funding_ratio = Decimal(str(interval_seconds)) / Decimal(str(self.FUNDING_INTERVAL_SECONDS))

        for portfolio in self.portfolios.values():
            for symbol, position in portfolio.positions.items():
                funding_rate = funding_rates.get(symbol)
                if funding_rate is None or funding_rate == 0:
                    continue

                # Calculate funding payment
                notional = position.size * position.mark_price
                payment_amount = notional * funding_rate * funding_ratio

                # Determine direction: positive rate means longs pay, shorts receive
                if position.side == Side.LONG:
                    # Longs pay when rate > 0, receive when rate < 0
                    net_payment = -payment_amount  # Negative = outgoing for longs
                else:
                    # Shorts receive when rate > 0, pay when rate < 0
                    net_payment = payment_amount  # Positive = incoming for shorts

                # Apply to portfolio
                portfolio.available_margin += net_payment
                portfolio.realized_pnl += net_payment

                # Track funding for analytics
                if net_payment > 0:
                    self.funding_received[portfolio.agent_id] += net_payment
                else:
                    self.funding_paid[portfolio.agent_id] += abs(net_payment)

                payments.append({
                    "agent_id": portfolio.agent_id,
                    "symbol": symbol,
                    "side": position.side.value,
                    "funding_rate": float(funding_rate),
                    "notional": float(notional),
                    "amount": float(net_payment),
                    "direction": "received" if net_payment > 0 else "paid",
                })

        return payments

    def check_liquidations(self) -> list[dict]:
        """
        Check all positions for liquidation and close if triggered.

        Liquidation occurs when mark price crosses liquidation price.
        The position is closed with total loss of margin plus liquidation fee.
        """
        liquidations = []

        for portfolio in self.portfolios.values():
            # Iterate over copy since we'll modify during iteration
            for symbol in list(portfolio.positions.keys()):
                position = portfolio.positions[symbol]
                liq_price = position.liquidation_price

                # Check liquidation condition
                is_liquidated = False
                if position.side == Side.LONG and position.mark_price <= liq_price:
                    is_liquidated = True
                elif position.side == Side.SHORT and position.mark_price >= liq_price:
                    is_liquidated = True

                if not is_liquidated:
                    continue

                # Calculate liquidation losses
                notional = position.size * position.mark_price
                liq_fee = notional * self.fees.liquidation_fee
                total_loss = position.margin + liq_fee

                # Apply to portfolio - lose margin and pay fee
                # Note: margin was already deducted when position opened,
                # so we just need to deduct the fee and record the loss
                portfolio.available_margin -= liq_fee
                portfolio.realized_pnl -= (position.margin + liq_fee)

                # Record liquidation trade
                trade = Trade(
                    agent_id=portfolio.agent_id,
                    symbol=symbol,
                    side=Side.SHORT if position.side == Side.LONG else Side.LONG,
                    size=position.size,
                    price=position.mark_price,
                    leverage=position.leverage,
                    fee=liq_fee,
                    realized_pnl=-(position.margin + liq_fee),
                )
                portfolio.add_trade(trade)

                # Remove position
                del portfolio.positions[symbol]

                liquidations.append({
                    "agent_id": portfolio.agent_id,
                    "symbol": symbol,
                    "side": position.side.value,
                    "size": float(position.size),
                    "entry_price": float(position.entry_price),
                    "liquidation_price": float(liq_price),
                    "mark_price": float(position.mark_price),
                    "margin_lost": float(position.margin),
                    "fee": float(liq_fee),
                    "total_loss": float(total_loss),
                })

        return liquidations

    def check_stop_loss_take_profit(self) -> list[dict]:
        """
        Check all positions for stop-loss/take-profit triggers.

        SL/TP are checked BEFORE liquidation check in the runner.
        """
        triggered = []

        for portfolio in self.portfolios.values():
            for symbol in list(portfolio.positions.keys()):
                position = portfolio.positions[symbol]
                mark_price = position.mark_price

                trigger_type = None
                trigger_price = None

                # Check stop-loss
                if position.stop_loss_price is not None:
                    if position.side == Side.LONG:
                        # Long SL triggers when price falls to or below SL
                        if mark_price <= position.stop_loss_price:
                            trigger_type = "stop_loss"
                            trigger_price = position.stop_loss_price
                    else:
                        # Short SL triggers when price rises to or above SL
                        if mark_price >= position.stop_loss_price:
                            trigger_type = "stop_loss"
                            trigger_price = position.stop_loss_price

                # Check take-profit (only if SL not triggered)
                if trigger_type is None and position.take_profit_price is not None:
                    if position.side == Side.LONG:
                        # Long TP triggers when price rises to or above TP
                        if mark_price >= position.take_profit_price:
                            trigger_type = "take_profit"
                            trigger_price = position.take_profit_price
                    else:
                        # Short TP triggers when price falls to or below TP
                        if mark_price <= position.take_profit_price:
                            trigger_type = "take_profit"
                            trigger_price = position.take_profit_price

                if trigger_type is None:
                    continue

                # Close the position
                trade = self._close_position(portfolio, symbol)
                if trade is None:
                    continue

                triggered.append({
                    "agent_id": portfolio.agent_id,
                    "symbol": symbol,
                    "side": position.side.value,
                    "trigger_type": trigger_type,
                    "trigger_price": float(trigger_price),
                    "mark_price": float(mark_price),
                    "size": float(trade.size),
                    "realized_pnl": float(trade.realized_pnl) if trade.realized_pnl else 0,
                    "fee": float(trade.fee),
                })

        return triggered

    def check_pending_orders(self) -> list[dict]:
        """
        Check and execute triggered pending limit orders.

        Returns list of triggered order events.
        """
        triggered = []

        for portfolio in self.portfolios.values():
            remaining_orders = []

            for order in portfolio.pending_orders:
                price = self.current_prices.get(order.symbol)
                if price is None:
                    remaining_orders.append(order)
                    continue

                if not order.should_trigger(price):
                    remaining_orders.append(order)
                    continue

                # Order triggered - execute it
                side = Side.LONG if order.order_type == OrderType.LIMIT_LONG else Side.SHORT

                # Create a decision to execute
                decision = Decision(
                    action="open_long" if side == Side.LONG else "open_short",
                    symbol=order.symbol,
                    size=order.size,
                    leverage=order.leverage,
                    stop_loss_price=order.stop_loss_price,
                    take_profit_price=order.take_profit_price,
                )

                trade = self._open_position(portfolio, decision, side)

                if trade is not None:
                    # Set SL/TP on the new position if specified
                    position = portfolio.positions.get(order.symbol)
                    if position and order.stop_loss_price:
                        position.stop_loss_price = order.stop_loss_price
                    if position and order.take_profit_price:
                        position.take_profit_price = order.take_profit_price

                    triggered.append({
                        "agent_id": portfolio.agent_id,
                        "order_id": order.id,
                        "symbol": order.symbol,
                        "order_type": order.order_type.value,
                        "limit_price": float(order.limit_price),
                        "fill_price": float(trade.price),
                        "size": float(trade.size),
                        "leverage": trade.leverage,
                        "fee": float(trade.fee),
                    })
                else:
                    # Order couldn't be executed (insufficient margin, etc.)
                    # Keep it pending or cancel? For now, cancel it.
                    triggered.append({
                        "agent_id": portfolio.agent_id,
                        "order_id": order.id,
                        "symbol": order.symbol,
                        "order_type": order.order_type.value,
                        "status": "cancelled",
                        "reason": "insufficient_margin",
                    })

            portfolio.pending_orders = remaining_orders

        return triggered

    def execute(self, agent_id: str, decision: Decision) -> Optional[Trade]:
        """Execute an agent's decision."""
        portfolio = self.portfolios.get(agent_id)
        if not portfolio:
            return None

        if decision.action == "hold":
            return None

        if decision.action == "open_long":
            trade = self._open_position(portfolio, decision, Side.LONG)
            # Set SL/TP if provided with market order
            if trade and decision.symbol:
                position = portfolio.positions.get(decision.symbol)
                if position:
                    if decision.stop_loss_price:
                        position.stop_loss_price = decision.stop_loss_price
                    if decision.take_profit_price:
                        position.take_profit_price = decision.take_profit_price
            return trade

        if decision.action == "open_short":
            trade = self._open_position(portfolio, decision, Side.SHORT)
            # Set SL/TP if provided with market order
            if trade and decision.symbol:
                position = portfolio.positions.get(decision.symbol)
                if position:
                    if decision.stop_loss_price:
                        position.stop_loss_price = decision.stop_loss_price
                    if decision.take_profit_price:
                        position.take_profit_price = decision.take_profit_price
            return trade

        if decision.action == "close":
            return self._close_position(portfolio, decision.symbol, decision.size)

        if decision.action == "limit_long":
            self._place_limit_order(portfolio, decision, OrderType.LIMIT_LONG)
            return None  # No immediate trade

        if decision.action == "limit_short":
            self._place_limit_order(portfolio, decision, OrderType.LIMIT_SHORT)
            return None  # No immediate trade

        if decision.action == "set_stop_loss":
            self._set_stop_loss(portfolio, decision)
            return None

        if decision.action == "set_take_profit":
            self._set_take_profit(portfolio, decision)
            return None

        if decision.action == "cancel_order":
            self._cancel_pending_order(portfolio, decision)
            return None

        return None

    def _place_limit_order(
        self,
        portfolio: Portfolio,
        decision: Decision,
        order_type: OrderType,
    ) -> Optional[PendingOrder]:
        """Place a limit order."""
        if not decision.symbol or not decision.limit_price:
            return None

        size = decision.size or Decimal("0")
        if size <= 0:
            return None

        order = PendingOrder(
            agent_id=portfolio.agent_id,
            symbol=decision.symbol,
            order_type=order_type,
            size=size,
            limit_price=decision.limit_price,
            leverage=min(decision.leverage, self.constraints.max_leverage),
            stop_loss_price=decision.stop_loss_price,
            take_profit_price=decision.take_profit_price,
        )
        portfolio.pending_orders.append(order)
        return order

    def _set_stop_loss(self, portfolio: Portfolio, decision: Decision) -> bool:
        """Set stop-loss on an existing position."""
        if not decision.symbol or decision.symbol not in portfolio.positions:
            return False

        position = portfolio.positions[decision.symbol]
        position.stop_loss_price = decision.stop_loss_price
        return True

    def _set_take_profit(self, portfolio: Portfolio, decision: Decision) -> bool:
        """Set take-profit on an existing position."""
        if not decision.symbol or decision.symbol not in portfolio.positions:
            return False

        position = portfolio.positions[decision.symbol]
        position.take_profit_price = decision.take_profit_price
        return True

    def _cancel_pending_order(self, portfolio: Portfolio, decision: Decision) -> bool:
        """Cancel a pending order by symbol or order_id in metadata."""
        order_id = decision.metadata.get("order_id")

        if order_id:
            # Cancel by order ID
            portfolio.pending_orders = [
                o for o in portfolio.pending_orders if o.id != order_id
            ]
            return True
        elif decision.symbol:
            # Cancel all orders for symbol
            portfolio.pending_orders = [
                o for o in portfolio.pending_orders if o.symbol != decision.symbol
            ]
            return True

        return False

    def _open_position(
        self,
        portfolio: Portfolio,
        decision: Decision,
        side: Side,
    ) -> Optional[Trade]:
        """Open a new position or add to an existing one (same direction only)."""
        symbol = decision.symbol
        if not symbol or symbol not in self.current_prices:
            return None

        price = self.current_prices[symbol]
        size = decision.size or Decimal("0")
        leverage = min(decision.leverage, self.constraints.max_leverage)

        if size <= 0:
            return None

        # Check if adding to existing position
        existing_position = portfolio.positions.get(symbol)
        if existing_position is not None:
            if existing_position.side != side:
                # Cannot add to position in opposite direction
                return None
            # Use existing leverage when adding to position
            leverage = existing_position.leverage

        # Calculate margin required
        notional = size * price
        margin_required = notional / leverage
        fee = notional * self.fees.taker_fee

        # Check constraints
        if margin_required + fee > portfolio.available_margin:
            # Reduce size to fit available margin
            max_notional = (portfolio.available_margin - fee) * leverage
            size = max_notional / price
            if size <= 0:
                return None
            margin_required = (size * price) / leverage
            fee = size * price * self.fees.taker_fee

        # Check max position size (for new positions or total after adding)
        current_margin = existing_position.margin if existing_position else Decimal("0")
        total_margin = current_margin + margin_required
        max_margin = portfolio.equity * self.constraints.max_position_pct

        if total_margin > max_margin:
            # Reduce to max position size
            allowed_margin = max_margin - current_margin
            if allowed_margin <= 0:
                return None
            margin_required = allowed_margin
            size = (margin_required * leverage) / price
            fee = size * price * self.fees.taker_fee

        if size <= 0:
            return None

        # Update or create position
        if existing_position is not None:
            # Add to existing position - calculate new average entry price
            new_size = existing_position.size + size
            new_entry = (
                existing_position.entry_price * existing_position.size + price * size
            ) / new_size
            existing_position.size = new_size
            existing_position.entry_price = new_entry
            existing_position.margin += margin_required
            existing_position.mark_price = price
        else:
            # Create new position
            position = Position(
                symbol=symbol,
                side=side,
                size=size,
                entry_price=price,
                leverage=leverage,
                margin=margin_required,
                mark_price=price,
            )
            portfolio.positions[symbol] = position

        # Update portfolio
        portfolio.available_margin -= margin_required + fee

        # Record trade
        trade = Trade(
            agent_id=portfolio.agent_id,
            symbol=symbol,
            side=side,
            size=size,
            price=price,
            leverage=leverage,
            fee=fee,
        )
        portfolio.add_trade(trade)

        return trade

    def _close_position(
        self,
        portfolio: Portfolio,
        symbol: Optional[str],
        size: Optional[Decimal] = None,
    ) -> Optional[Trade]:
        """Close an existing position (fully or partially)."""
        if not symbol or symbol not in portfolio.positions:
            return None

        position = portfolio.positions[symbol]
        price = self.current_prices.get(symbol)

        if not price:
            return None

        # Determine close size (full or partial)
        close_size = size if size is not None and size < position.size else position.size
        is_partial = close_size < position.size

        if close_size <= 0:
            return None

        # Calculate proportional P&L for the closed portion
        close_ratio = close_size / position.size
        partial_pnl = position.unrealized_pnl * close_ratio
        partial_margin = position.margin * close_ratio
        fee = close_size * price * self.fees.taker_fee
        net_pnl = partial_pnl - fee

        if is_partial:
            # Partial close - reduce position
            position.size -= close_size
            position.margin -= partial_margin
            position.mark_price = price  # Update mark price to current
            # Note: entry_price stays the same for remaining position
        else:
            # Full close - remove position
            del portfolio.positions[symbol]

        # Update portfolio
        portfolio.available_margin += partial_margin + net_pnl
        portfolio.realized_pnl += net_pnl

        # Record trade (closing trade is opposite side)
        trade = Trade(
            agent_id=portfolio.agent_id,
            symbol=symbol,
            side=Side.SHORT if position.side == Side.LONG else Side.LONG,
            size=close_size,
            price=price,
            leverage=position.leverage,
            fee=fee,
            realized_pnl=net_pnl,
        )
        portfolio.add_trade(trade)

        return trade

    def get_leaderboard(self) -> list[dict]:
        """Get current standings."""
        standings = []
        for agent_id, portfolio in self.portfolios.items():
            standings.append({
                "agent_id": agent_id,
                "equity": float(portfolio.equity),
                "pnl": float(portfolio.total_pnl),
                "pnl_percent": portfolio.equity_percent,
                "positions": len(portfolio.positions),
                "trades": len(portfolio.trades),
            })
        return sorted(standings, key=lambda x: x["equity"], reverse=True)

    def get_portfolio(self, agent_id: str) -> Optional[Portfolio]:
        """Get portfolio for an agent."""
        return self.portfolios.get(agent_id)

    def reset(self) -> None:
        """Reset all portfolios to starting capital."""
        starting_capital = self.constraints.starting_capital
        for agent_id in list(self.portfolios.keys()):
            self.portfolios[agent_id] = Portfolio(
                agent_id=agent_id,
                initial_capital=starting_capital,
                available_margin=starting_capital,
            )
            # Reset analytics tracking
            self.equity_history[agent_id] = []
            self.funding_paid[agent_id] = Decimal("0")
            self.funding_received[agent_id] = Decimal("0")

    def record_equity_snapshot(self, tick: int, timestamp: datetime) -> None:
        """Record equity snapshot for all agents at current tick."""
        for agent_id, portfolio in self.portfolios.items():
            snapshot = EquitySnapshot(
                tick=tick,
                timestamp=timestamp,
                equity=portfolio.equity,
            )
            history = self.equity_history[agent_id]
            history.append(snapshot)
            # Prune old snapshots to prevent unbounded growth
            if len(history) > self.MAX_EQUITY_HISTORY:
                self.equity_history[agent_id] = history[-self.MAX_EQUITY_HISTORY:]

    def get_analytics(
        self,
        agent_id: str,
        ticks_per_year: Optional[int] = None,
    ) -> Optional[PortfolioAnalytics]:
        """
        Calculate and return comprehensive analytics for an agent.

        Args:
            agent_id: The agent to get analytics for
            ticks_per_year: Number of ticks per year for Sharpe calculation.
                            If None, calculated from tick_interval_seconds.

        Returns:
            PortfolioAnalytics or None if agent not found
        """
        portfolio = self.portfolios.get(agent_id)
        if not portfolio:
            return None

        # Calculate ticks per year from actual tick interval
        if ticks_per_year is None:
            ticks_per_year = self.SECONDS_PER_YEAR // self.tick_interval_seconds

        equity_history = self.equity_history.get(agent_id, [])
        funding_paid = self.funding_paid.get(agent_id, Decimal("0"))
        funding_received = self.funding_received.get(agent_id, Decimal("0"))

        return PortfolioAnalytics.calculate(
            portfolio=portfolio,
            equity_history=equity_history,
            funding_paid=funding_paid,
            funding_received=funding_received,
            ticks_per_year=ticks_per_year,
        )

    def get_all_analytics(
        self,
        ticks_per_year: Optional[int] = None,
    ) -> dict[str, PortfolioAnalytics]:
        """Get analytics for all agents."""
        return {
            agent_id: self.get_analytics(agent_id, ticks_per_year)
            for agent_id in self.portfolios.keys()
        }
