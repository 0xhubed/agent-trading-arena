"""Baseline agents for backtesting comparison."""

from __future__ import annotations

import random
from decimal import Decimal
from typing import Optional

from agent_arena.core.agent import BaseAgent
from agent_arena.core.models import Decision


def _positions_to_dict(positions: list | dict) -> dict:
    """Convert positions list to dict keyed by symbol."""
    if isinstance(positions, dict):
        return positions
    return {p["symbol"]: p for p in positions} if positions else {}


class RandomAgent(BaseAgent):
    """
    Random trading baseline.

    Makes random decisions at a configurable frequency.
    Used to establish statistical baseline for comparison.
    Any agent that can't beat this consistently has no edge.
    """

    def __init__(
        self,
        agent_id: str,
        name: str,
        config: Optional[dict] = None,
    ):
        super().__init__(agent_id, name, config)
        self.trade_frequency = config.get("trade_frequency", 0.2) if config else 0.2
        self.position_size = Decimal(str(config.get("position_size", 0.1))) if config else Decimal("0.1")
        self.max_leverage = config.get("max_leverage", 5) if config else 5

    async def decide(self, context: dict) -> Decision:
        """Make a random trading decision."""
        portfolio = context.get("portfolio", {})
        symbols = list(context.get("market", {}).keys())

        if not symbols:
            return Decision(action="hold", reasoning="No market data available")

        # Check if we have a position to potentially close
        positions = _positions_to_dict(portfolio.get("positions", []))

        # Random chance to close existing position
        if positions:
            for symbol, pos in positions.items():
                if random.random() < self.trade_frequency:
                    return Decision(
                        action="close",
                        symbol=symbol,
                        confidence=0.5,
                        reasoning="Random close decision",
                        metadata={"strategy": "random"},
                    )

        # Random chance to open new position
        if random.random() < self.trade_frequency:
            symbol = random.choice(symbols)

            # Skip if already have position in this symbol
            if symbol in positions:
                return Decision(action="hold", reasoning="Already have position in symbol")

            action = random.choice(["open_long", "open_short"])
            leverage = random.randint(2, self.max_leverage)

            return Decision(
                action=action,
                symbol=symbol,
                size=self.position_size,
                leverage=leverage,
                confidence=0.5,
                reasoning=f"Random {action.replace('open_', '')} trade",
                metadata={"strategy": "random"},
            )

        return Decision(
            action="hold",
            reasoning="Random hold decision",
            metadata={"strategy": "random"},
        )


class SMAAgent(BaseAgent):
    """
    Simple Moving Average crossover strategy.

    - Long when price > SMA
    - Flat when price < SMA

    A classic trend-following baseline that should capture
    strong trends but lose in choppy markets.
    """

    def __init__(
        self,
        agent_id: str,
        name: str,
        config: Optional[dict] = None,
    ):
        super().__init__(agent_id, name, config)
        self.sma_period = config.get("sma_period", 50) if config else 50
        self.position_size = Decimal(str(config.get("position_size", 0.15))) if config else Decimal("0.15")
        self.leverage = config.get("leverage", 3) if config else 3
        self.primary_symbol = config.get("primary_symbol", "PF_XBTUSD") if config else "PF_XBTUSD"

    async def decide(self, context: dict) -> Decision:
        """Make decision based on SMA crossover."""
        candles = context.get("candles", {})
        market = context.get("market", {})
        portfolio = context.get("portfolio", {})

        symbol = self.primary_symbol
        if symbol not in market:
            # Fallback to first available symbol
            symbol = next(iter(market.keys()), None)
            if not symbol:
                return Decision(action="hold", reasoning="No market data")

        # Get current price
        current_price = market[symbol]["price"]

        # Calculate SMA from candles
        sma = self._calculate_sma(candles, symbol)
        if sma is None:
            return Decision(
                action="hold",
                reasoning=f"Insufficient candle data for SMA{self.sma_period}",
                metadata={"strategy": "sma_crossover"},
            )

        positions = _positions_to_dict(portfolio.get("positions", []))
        has_position = symbol in positions

        # Trading logic
        if current_price > sma:
            # Bullish: should be long
            if not has_position:
                return Decision(
                    action="open_long",
                    symbol=symbol,
                    size=self.position_size,
                    leverage=self.leverage,
                    confidence=0.6,
                    reasoning=f"Price {current_price:.2f} > SMA{self.sma_period} {sma:.2f}",
                    metadata={
                        "strategy": "sma_crossover",
                        "sma": float(sma),
                        "price": float(current_price),
                    },
                )
            elif positions[symbol].get("side") == "short":
                return Decision(
                    action="close",
                    symbol=symbol,
                    confidence=0.6,
                    reasoning=f"Close short: Price crossed above SMA{self.sma_period}",
                    metadata={"strategy": "sma_crossover"},
                )
        else:
            # Bearish: should be flat (or short if configured)
            if has_position and positions[symbol].get("side") == "long":
                return Decision(
                    action="close",
                    symbol=symbol,
                    confidence=0.6,
                    reasoning=f"Close long: Price {current_price:.2f} < SMA{self.sma_period} {sma:.2f}",
                    metadata={"strategy": "sma_crossover"},
                )

        return Decision(
            action="hold",
            reasoning=f"SMA signal unchanged (price={current_price:.2f}, SMA={sma:.2f})",
            metadata={
                "strategy": "sma_crossover",
                "sma": float(sma),
                "price": float(current_price),
            },
        )

    def _calculate_sma(self, candles: dict, symbol: str) -> Optional[Decimal]:
        """Calculate SMA from candle data."""
        symbol_candles = candles.get(symbol, {})

        # Try different intervals
        for interval in ["1h", "4h", "15m"]:
            interval_candles = symbol_candles.get(interval, [])
            if len(interval_candles) >= self.sma_period:
                closes = [c["close"] for c in interval_candles[-self.sma_period:]]
                return sum(closes) / len(closes)

        return None


class MomentumAgent(BaseAgent):
    """
    Momentum factor baseline.

    - Long the best 24h performer
    - Short the worst 24h performer
    - Rebalances periodically

    A simple momentum strategy that bets on continuation of trends.
    """

    def __init__(
        self,
        agent_id: str,
        name: str,
        config: Optional[dict] = None,
    ):
        super().__init__(agent_id, name, config)
        self.rebalance_ticks = config.get("rebalance_ticks", 24) if config else 24
        self.position_size = Decimal(str(config.get("position_size", 0.1))) if config else Decimal("0.1")
        self.leverage = config.get("leverage", 3) if config else 3
        self.long_only = config.get("long_only", True) if config else True

        self._ticks_since_rebalance = 0

    async def decide(self, context: dict) -> Decision:
        """Make decision based on momentum."""
        market = context.get("market", {})
        portfolio = context.get("portfolio", {})

        if not market:
            return Decision(action="hold", reasoning="No market data")

        self._ticks_since_rebalance += 1

        # Check if we should rebalance
        if self._ticks_since_rebalance < self.rebalance_ticks:
            return Decision(
                action="hold",
                reasoning=f"Waiting for rebalance ({self._ticks_since_rebalance}/{self.rebalance_ticks} ticks)",
                metadata={"strategy": "momentum"},
            )

        self._ticks_since_rebalance = 0

        # Find best and worst performers
        performances = {
            symbol: data.get("change_24h", 0)
            for symbol, data in market.items()
        }

        if not performances:
            return Decision(action="hold", reasoning="No performance data")

        best_symbol = max(performances, key=performances.get)
        worst_symbol = min(performances, key=performances.get)

        best_change = performances[best_symbol]
        worst_change = performances[worst_symbol]

        positions = _positions_to_dict(portfolio.get("positions", []))

        # Close positions that are no longer the best/worst
        for symbol in list(positions.keys()):
            pos = positions[symbol]
            if pos.get("side") == "long" and symbol != best_symbol:
                return Decision(
                    action="close",
                    symbol=symbol,
                    confidence=0.6,
                    reasoning=f"Closing old long: {symbol} no longer best performer",
                    metadata={"strategy": "momentum"},
                )
            if pos.get("side") == "short" and symbol != worst_symbol:
                return Decision(
                    action="close",
                    symbol=symbol,
                    confidence=0.6,
                    reasoning=f"Closing old short: {symbol} no longer worst performer",
                    metadata={"strategy": "momentum"},
                )

        # Open new long on best performer
        if best_symbol not in positions and best_change > 0:
            return Decision(
                action="open_long",
                symbol=best_symbol,
                size=self.position_size,
                leverage=self.leverage,
                confidence=0.6,
                reasoning=f"Long best performer: {best_symbol} ({best_change:+.2f}% 24h)",
                metadata={
                    "strategy": "momentum",
                    "performance_24h": best_change,
                },
            )

        # Open short on worst performer (if not long-only)
        if not self.long_only and worst_symbol not in positions and worst_change < 0:
            return Decision(
                action="open_short",
                symbol=worst_symbol,
                size=self.position_size,
                leverage=self.leverage,
                confidence=0.6,
                reasoning=f"Short worst performer: {worst_symbol} ({worst_change:+.2f}% 24h)",
                metadata={
                    "strategy": "momentum",
                    "performance_24h": worst_change,
                },
            )

        return Decision(
            action="hold",
            reasoning="No momentum trades available",
            metadata={
                "strategy": "momentum",
                "best_symbol": best_symbol,
                "worst_symbol": worst_symbol,
            },
        )


class BuyAndHoldAgent(BaseAgent):
    """
    Simple buy-and-hold baseline.

    Opens a long position in the primary symbol at the start
    and holds indefinitely. The ultimate passive benchmark.
    """

    def __init__(
        self,
        agent_id: str,
        name: str,
        config: Optional[dict] = None,
    ):
        super().__init__(agent_id, name, config)
        self.primary_symbol = config.get("primary_symbol", "PF_XBTUSD") if config else "PF_XBTUSD"
        self.position_size = Decimal(str(config.get("position_size", 0.5))) if config else Decimal("0.5")
        self.leverage = config.get("leverage", 1) if config else 1
        self._opened = False

    async def decide(self, context: dict) -> Decision:
        """Buy once and hold."""
        if self._opened:
            return Decision(
                action="hold",
                reasoning="Holding long position",
                metadata={"strategy": "buy_and_hold"},
            )

        market = context.get("market", {})
        symbol = self.primary_symbol

        if symbol not in market:
            symbol = next(iter(market.keys()), None)
            if not symbol:
                return Decision(action="hold", reasoning="No market data")

        self._opened = True

        return Decision(
            action="open_long",
            symbol=symbol,
            size=self.position_size,
            leverage=self.leverage,
            confidence=1.0,
            reasoning=f"Buy and hold: Opening long position in {symbol}",
            metadata={"strategy": "buy_and_hold"},
        )


class MeanReversionAgent(BaseAgent):
    """
    Mean reversion strategy baseline.

    - Buy when RSI is oversold (< 30)
    - Sell when RSI is overbought (> 70)

    Counter-trend strategy that works in ranging markets
    but loses in strong trends.
    """

    def __init__(
        self,
        agent_id: str,
        name: str,
        config: Optional[dict] = None,
    ):
        super().__init__(agent_id, name, config)
        self.rsi_oversold = config.get("rsi_oversold", 30) if config else 30
        self.rsi_overbought = config.get("rsi_overbought", 70) if config else 70
        self.position_size = Decimal(str(config.get("position_size", 0.1))) if config else Decimal("0.1")
        self.leverage = config.get("leverage", 3) if config else 3
        self.primary_symbol = config.get("primary_symbol", "PF_XBTUSD") if config else "PF_XBTUSD"

    async def decide(self, context: dict) -> Decision:
        """Make decision based on RSI mean reversion."""
        candles = context.get("candles", {})
        market = context.get("market", {})
        portfolio = context.get("portfolio", {})

        symbol = self.primary_symbol
        if symbol not in market:
            symbol = next(iter(market.keys()), None)
            if not symbol:
                return Decision(action="hold", reasoning="No market data")

        # Calculate RSI
        rsi = self._calculate_rsi(candles, symbol)
        if rsi is None:
            return Decision(
                action="hold",
                reasoning="Insufficient data for RSI calculation",
                metadata={"strategy": "mean_reversion"},
            )

        positions = _positions_to_dict(portfolio.get("positions", []))
        has_position = symbol in positions

        # Trading logic
        if rsi < self.rsi_oversold:
            # Oversold: buy signal
            if not has_position:
                return Decision(
                    action="open_long",
                    symbol=symbol,
                    size=self.position_size,
                    leverage=self.leverage,
                    confidence=0.65,
                    reasoning=f"RSI oversold at {rsi:.1f}",
                    metadata={"strategy": "mean_reversion", "rsi": rsi},
                )
            elif positions[symbol].get("side") == "short":
                return Decision(
                    action="close",
                    symbol=symbol,
                    confidence=0.65,
                    reasoning=f"Close short: RSI oversold at {rsi:.1f}",
                    metadata={"strategy": "mean_reversion", "rsi": rsi},
                )

        elif rsi > self.rsi_overbought:
            # Overbought: sell signal
            if has_position and positions[symbol].get("side") == "long":
                return Decision(
                    action="close",
                    symbol=symbol,
                    confidence=0.65,
                    reasoning=f"Close long: RSI overbought at {rsi:.1f}",
                    metadata={"strategy": "mean_reversion", "rsi": rsi},
                )

        return Decision(
            action="hold",
            reasoning=f"RSI neutral at {rsi:.1f}",
            metadata={"strategy": "mean_reversion", "rsi": rsi},
        )

    def _calculate_rsi(self, candles: dict, symbol: str, period: int = 14) -> Optional[float]:
        """Calculate RSI from candle data."""
        symbol_candles = candles.get(symbol, {})

        # Try different intervals
        for interval in ["1h", "4h", "15m"]:
            interval_candles = symbol_candles.get(interval, [])
            if len(interval_candles) >= period + 1:
                closes = [float(c["close"]) for c in interval_candles[-(period + 1):]]

                gains = []
                losses = []
                for i in range(1, len(closes)):
                    change = closes[i] - closes[i - 1]
                    if change > 0:
                        gains.append(change)
                        losses.append(0)
                    else:
                        gains.append(0)
                        losses.append(abs(change))

                avg_gain = sum(gains) / period
                avg_loss = sum(losses) / period

                if avg_loss == 0:
                    return 100.0

                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
                return rsi

        return None
