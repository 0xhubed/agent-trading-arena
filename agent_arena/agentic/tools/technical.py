"""Technical Analysis tool - RSI, SMA, MACD, Bollinger Bands."""

import json
from typing import Optional

from pydantic import BaseModel, Field, PrivateAttr

from agent_arena.agentic.tools.base import TradingTool


class TechnicalAnalysisInput(BaseModel):
    """Input schema for technical analysis."""

    symbol: str = Field(description="Trading symbol (e.g., PF_XBTUSD)")
    indicators: list[str] = Field(
        default=["rsi", "sma", "macd", "bollinger"],
        description="Indicators to calculate: rsi, sma, macd, bollinger",
    )
    period: int = Field(default=14, description="Period for calculations")


class TechnicalAnalysisTool(TradingTool):
    """
    Calculate technical indicators for a symbol.

    Available indicators:
    - RSI (Relative Strength Index): Overbought > 70, Oversold < 30
    - SMA (Simple Moving Average): Short-term vs long-term crossovers
    - MACD (Moving Average Convergence Divergence): Momentum and trend
    - Bollinger Bands: Volatility and price extremes
    """

    name: str = "technical_analysis"
    description: str = """Calculate technical indicators for a cryptocurrency.
Use this to identify overbought/oversold conditions, trend direction, and momentum.
Input: symbol (required), indicators (optional list), period (optional int).
Returns: Dictionary with indicator values and interpretation."""

    args_schema: type[BaseModel] = TechnicalAnalysisInput

    # Price history (per-instance, across calls within a session)
    _price_history: dict[str, list[float]] = PrivateAttr(default_factory=dict)

    def _run(
        self,
        symbol: str,
        indicators: Optional[list[str]] = None,
        period: int = 14,
    ) -> str:
        """Calculate requested indicators."""
        indicators = indicators or ["rsi", "sma", "macd", "bollinger"]

        # Get price history from context or internal state
        prices = self._get_prices(symbol)

        if len(prices) < 2:
            return json.dumps(
                {
                    "symbol": symbol,
                    "error": f"Insufficient price history ({len(prices)} points). Need at least 2.",
                    "suggestion": "Wait a few more ticks to gather price data.",
                }
            )

        results = {"symbol": symbol, "price_history_points": len(prices), "indicators": {}}

        if "rsi" in indicators:
            rsi = self._calculate_rsi(prices, period)
            signal = "oversold" if rsi < 30 else "overbought" if rsi > 70 else "neutral"
            results["indicators"]["rsi"] = {
                "value": round(rsi, 2),
                "signal": signal,
                "interpretation": self._interpret_rsi(rsi),
            }

        if "sma" in indicators:
            sma_short = self._calculate_sma(prices, 5)
            sma_long = self._calculate_sma(prices, 20)
            trend = "bullish" if sma_short > sma_long else "bearish"
            results["indicators"]["sma"] = {
                "short_5": round(sma_short, 2),
                "long_20": round(sma_long, 2),
                "signal": trend,
                "interpretation": f"Short-term SMA {'above' if trend == 'bullish' else 'below'} long-term - {trend} trend",
            }

        if "macd" in indicators:
            macd_line, signal_line, histogram = self._calculate_macd(prices)
            macd_signal = "bullish" if histogram > 0 else "bearish"
            results["indicators"]["macd"] = {
                "macd_line": round(macd_line, 4),
                "signal_line": round(signal_line, 4),
                "histogram": round(histogram, 4),
                "signal": macd_signal,
                "interpretation": f"MACD histogram {'positive' if histogram > 0 else 'negative'} - {macd_signal} momentum",
            }

        if "bollinger" in indicators:
            upper, middle, lower = self._calculate_bollinger(prices, period)
            current_price = prices[-1] if prices else 0
            if current_price < lower:
                bb_signal = "oversold"
            elif current_price > upper:
                bb_signal = "overbought"
            else:
                bb_signal = "neutral"
            results["indicators"]["bollinger"] = {
                "upper": round(upper, 2),
                "middle": round(middle, 2),
                "lower": round(lower, 2),
                "current_price": round(current_price, 2),
                "signal": bb_signal,
                "interpretation": f"Price at {'lower band (oversold)' if bb_signal == 'oversold' else 'upper band (overbought)' if bb_signal == 'overbought' else 'middle of bands'}",
            }

        # Add overall summary
        signals = [v.get("signal") for v in results["indicators"].values()]
        bullish = signals.count("bullish") + signals.count("oversold")
        bearish = signals.count("bearish") + signals.count("overbought")
        results["summary"] = {
            "bullish_signals": bullish,
            "bearish_signals": bearish,
            "neutral_signals": len(signals) - bullish - bearish,
            "bias": "bullish" if bullish > bearish else "bearish" if bearish > bullish else "neutral",
        }

        return json.dumps(results, indent=2)

    def _get_prices(self, symbol: str) -> list[float]:
        """Get price history, updating from context."""
        # Update from current market data
        market = self._context.get("market", {})
        if symbol in market:
            price = float(market[symbol].get("price", 0))
            if price > 0:
                if symbol not in self._price_history:
                    self._price_history[symbol] = []
                self._price_history[symbol].append(price)
                # Keep last 100 prices
                self._price_history[symbol] = self._price_history[symbol][-100:]

        return self._price_history.get(symbol, [])

    def _calculate_rsi(self, prices: list[float], period: int = 14) -> float:
        """Calculate Relative Strength Index."""
        if len(prices) < period + 1:
            return 50.0  # Neutral when insufficient data

        changes = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
        recent_changes = changes[-period:]

        gains = [c for c in recent_changes if c > 0]
        losses = [-c for c in recent_changes if c < 0]

        avg_gain = sum(gains) / period if gains else 0
        avg_loss = sum(losses) / period if losses else 0

        if avg_loss == 0:
            return 100.0
        if avg_gain == 0:
            return 0.0

        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def _calculate_sma(self, prices: list[float], period: int) -> float:
        """Calculate Simple Moving Average."""
        if len(prices) < period:
            return prices[-1] if prices else 0
        return sum(prices[-period:]) / period

    def _calculate_ema(self, prices: list[float], period: int) -> float:
        """Calculate Exponential Moving Average."""
        if len(prices) < period:
            return prices[-1] if prices else 0

        multiplier = 2 / (period + 1)
        ema = sum(prices[:period]) / period

        for price in prices[period:]:
            ema = (price - ema) * multiplier + ema

        return ema

    def _calculate_macd(self, prices: list[float]) -> tuple[float, float, float]:
        """Calculate MACD (12, 26, 9)."""
        if len(prices) < 26:
            return 0, 0, 0

        ema_12 = self._calculate_ema(prices, 12)
        ema_26 = self._calculate_ema(prices, 26)
        macd_line = ema_12 - ema_26

        # Signal line is 9-period EMA of MACD line (simplified approximation)
        signal_line = macd_line * 0.9
        histogram = macd_line - signal_line

        return macd_line, signal_line, histogram

    def _calculate_bollinger(
        self, prices: list[float], period: int = 20
    ) -> tuple[float, float, float]:
        """Calculate Bollinger Bands."""
        if len(prices) < period:
            price = prices[-1] if prices else 0
            return price, price, price

        recent = prices[-period:]
        middle = sum(recent) / period
        variance = sum((p - middle) ** 2 for p in recent) / period
        std_dev = variance**0.5

        upper = middle + (2 * std_dev)
        lower = middle - (2 * std_dev)

        return upper, middle, lower

    def _interpret_rsi(self, rsi: float) -> str:
        """Interpret RSI value."""
        if rsi < 20:
            return "Extremely oversold - strong buy signal (contrarian)"
        elif rsi < 30:
            return "Oversold - potential buying opportunity"
        elif rsi < 45:
            return "Slightly bearish momentum"
        elif rsi < 55:
            return "Neutral momentum"
        elif rsi < 70:
            return "Slightly bullish momentum"
        elif rsi < 80:
            return "Overbought - potential selling opportunity"
        else:
            return "Extremely overbought - strong sell signal (contrarian)"
