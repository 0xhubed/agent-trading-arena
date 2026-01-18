"""Technical Analysis trading agent - rule-based, no LLM."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from typing import Optional

from agent_arena.core.agent import BaseAgent
from agent_arena.core.models import Decision


class TATrader(BaseAgent):
    """
    Technical Analysis trader using classic indicators.

    Implements:
    - RSI (Relative Strength Index) for overbought/oversold signals
    - SMA crossover for trend detection
    - Momentum for confirmation

    No LLM - pure rule-based trading.
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        super().__init__(agent_id, name, config)
        config = config or {}

        self.model = "technical-analysis"  # Not an LLM

        # Strategy parameters
        self.rsi_period = config.get("rsi_period", 14)
        self.rsi_oversold = config.get("rsi_oversold", 30)
        self.rsi_overbought = config.get("rsi_overbought", 70)
        self.sma_short = config.get("sma_short", 5)
        self.sma_long = config.get("sma_long", 20)
        self.position_size_pct = Decimal(str(config.get("position_size_pct", 0.15)))
        self.leverage = config.get("leverage", 3)

        # Price history per symbol
        self._price_history: dict[str, list[float]] = defaultdict(list)
        self._max_history = 50  # Keep last 50 prices

    async def decide(self, context: dict) -> Decision:
        """Make trading decision based on technical indicators."""
        market = context.get("market", {})
        portfolio = context.get("portfolio", {})

        # Update price history
        for symbol, data in market.items():
            price = float(data.get("price", 0))
            if price > 0:
                self._price_history[symbol].append(price)
                # Trim to max history
                if len(self._price_history[symbol]) > self._max_history:
                    self._price_history[symbol] = self._price_history[symbol][-self._max_history:]

        # Check current positions
        current_positions = {pos["symbol"]: pos for pos in portfolio.get("positions", [])}

        # Analyze each symbol and find best opportunity
        best_signal = None
        best_score = 0

        for symbol, data in market.items():
            prices = self._price_history.get(symbol, [])

            # Need enough history for indicators
            if len(prices) < self.sma_long:
                continue

            # Calculate indicators
            rsi = self._calculate_rsi(prices)
            sma_short = self._calculate_sma(prices, self.sma_short)
            sma_long = self._calculate_sma(prices, self.sma_long)
            momentum = self._calculate_momentum(prices, 5)

            current_price = prices[-1]

            # Check if we have a position in this symbol
            has_position = symbol in current_positions

            # Generate signals
            signal, score, reasoning = self._generate_signal(
                symbol=symbol,
                rsi=rsi,
                sma_short=sma_short,
                sma_long=sma_long,
                momentum=momentum,
                current_price=current_price,
                has_position=has_position,
                position=current_positions.get(symbol),
            )

            if signal and score > best_score:
                best_signal = (signal, symbol, reasoning, score)
                best_score = score

        # Execute best signal or hold
        if best_signal:
            action, symbol, reasoning, score = best_signal
            equity = Decimal(str(portfolio.get("equity", 10000)))
            price = Decimal(str(market[symbol]["price"]))

            if action == "close":
                return Decision(
                    action="close",
                    symbol=symbol,
                    confidence=min(score / 100, 1.0),
                    reasoning=reasoning,
                    timestamp=datetime.utcnow(),
                )

            # Calculate position size
            position_value = equity * self.position_size_pct
            size = position_value / price

            return Decision(
                action=action,
                symbol=symbol,
                size=size,
                leverage=self.leverage,
                confidence=min(score / 100, 1.0),
                reasoning=reasoning,
                timestamp=datetime.utcnow(),
            )

        # No signal - hold
        return Decision(
            action="hold",
            confidence=0.5,
            reasoning="No strong TA signals detected. Waiting for setup.",
            timestamp=datetime.utcnow(),
        )

    def _calculate_rsi(self, prices: list[float], period: int = None) -> float:
        """Calculate RSI (Relative Strength Index)."""
        period = period or self.rsi_period
        if len(prices) < period + 1:
            return 50.0  # Neutral

        # Calculate price changes
        changes = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        recent_changes = changes[-(period):]

        gains = [c for c in recent_changes if c > 0]
        losses = [-c for c in recent_changes if c < 0]

        avg_gain = sum(gains) / period if gains else 0
        avg_loss = sum(losses) / period if losses else 0

        if avg_loss == 0:
            return 100.0
        if avg_gain == 0:
            return 0.0

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def _calculate_sma(self, prices: list[float], period: int) -> float:
        """Calculate Simple Moving Average."""
        if len(prices) < period:
            return prices[-1] if prices else 0
        return sum(prices[-period:]) / period

    def _calculate_momentum(self, prices: list[float], period: int = 5) -> float:
        """Calculate momentum (percent change over period)."""
        if len(prices) < period + 1:
            return 0.0
        old_price = prices[-period-1]
        current_price = prices[-1]
        if old_price == 0:
            return 0.0
        return ((current_price - old_price) / old_price) * 100

    def _generate_signal(
        self,
        symbol: str,
        rsi: float,
        sma_short: float,
        sma_long: float,
        momentum: float,
        current_price: float,
        has_position: bool,
        position: Optional[dict],
    ) -> tuple[Optional[str], float, str]:
        """
        Generate trading signal based on indicators.

        Returns: (action, score, reasoning)
        """
        signals = []
        score = 0

        # RSI signals
        if rsi < self.rsi_oversold:
            signals.append(f"RSI oversold ({rsi:.1f})")
            score += 30
        elif rsi > self.rsi_overbought:
            signals.append(f"RSI overbought ({rsi:.1f})")
            score -= 30

        # SMA crossover signals
        if sma_short > sma_long:
            signals.append(f"SMA bullish (short {sma_short:.0f} > long {sma_long:.0f})")
            score += 20
        else:
            signals.append(f"SMA bearish (short {sma_short:.0f} < long {sma_long:.0f})")
            score -= 20

        # Momentum confirmation
        if momentum > 2:
            signals.append(f"Strong upward momentum ({momentum:.1f}%)")
            score += 15
        elif momentum < -2:
            signals.append(f"Strong downward momentum ({momentum:.1f}%)")
            score -= 15

        reasoning = f"{symbol}: " + "; ".join(signals)

        # Decision logic
        if has_position and position:
            # Check if we should close
            pos_side = position.get("side", "long")
            roe = position.get("roe_percent", 0)

            # Take profit at 5% ROE or cut loss at -3%
            if roe >= 5:
                return ("close", 80, f"Taking profit at {roe:.1f}% ROE. {reasoning}")
            if roe <= -3:
                return ("close", 70, f"Cutting loss at {roe:.1f}% ROE. {reasoning}")

            # Close if signals reversed
            if pos_side == "long" and score < -30:
                return ("close", 60, f"Bearish reversal detected. {reasoning}")
            if pos_side == "short" and score > 30:
                return ("close", 60, f"Bullish reversal detected. {reasoning}")

            return (None, 0, "")

        else:
            # Look for entry
            if score >= 40:
                return ("open_long", score, f"Bullish setup. {reasoning}")
            if score <= -40:
                return ("open_short", abs(score), f"Bearish setup. {reasoning}")

            return (None, 0, "")
