"""Multi-timeframe analysis tool - validates trend alignment across timeframes."""

import json

from pydantic import BaseModel, Field

from agent_arena.agentic.tools.base import TradingTool


class MultiTimeframeInput(BaseModel):
    """Input schema for multi-timeframe analysis."""

    symbol: str = Field(description="Trading symbol to analyze (e.g., PF_XBTUSD)")


class MultiTimeframeTool(TradingTool):
    """
    Analyzes trend alignment across multiple timeframes.

    Checks 15m, 1h, and 4h timeframes to determine if trends are aligned.
    Strong signals occur when all timeframes agree on direction.

    Use this to:
    - Confirm trend direction before entering
    - Avoid counter-trend trades
    - Identify high-conviction setups
    """

    name: str = "multi_timeframe_analysis"
    description: str = """Analyzes trend direction on 15m, 1h, and 4h timeframes.
Returns whether trends are aligned (all same direction).
STRONG signal when all timeframes agree, WAIT when mixed.
Input: symbol (required)."""

    args_schema: type[BaseModel] = MultiTimeframeInput

    def _run(self, symbol: str) -> str:
        """Analyze trend across multiple timeframes."""
        context = self._context
        candles = context.get("candles", {}).get(symbol, {})

        if not candles:
            return json.dumps({
                "symbol": symbol,
                "error": "No candle data available for this symbol",
                "suggestion": "Ensure candles are enabled in competition config",
            }, indent=2)

        signals = {}
        details = {}

        for interval in ["15m", "1h", "4h"]:
            if interval in candles and len(candles[interval]) >= 10:
                trend, strength, metrics = self._calculate_trend(candles[interval])
                signals[interval] = trend
                details[interval] = {
                    "trend": trend,
                    "strength": round(strength, 2),
                    "candles_analyzed": len(candles[interval]),
                    **metrics,
                }
            elif interval in candles:
                details[interval] = {
                    "trend": "insufficient_data",
                    "candles_available": len(candles[interval]),
                    "candles_required": 10,
                }

        if not signals:
            return json.dumps({
                "symbol": symbol,
                "error": "Insufficient candle data across all timeframes",
                "available_intervals": list(candles.keys()),
            }, indent=2)

        # Check alignment
        trend_values = list(signals.values())
        unique_trends = set(trend_values)
        aligned = len(unique_trends) == 1 and len(signals) >= 2

        # Determine recommendation
        if aligned:
            direction = trend_values[0]
            if direction == "bullish":
                recommendation = "STRONG_BULLISH"
                action_hint = "High conviction for long positions. Consider larger size."
            elif direction == "bearish":
                recommendation = "STRONG_BEARISH"
                action_hint = "High conviction for short positions. Consider larger size."
            else:
                recommendation = "NEUTRAL_ALIGNED"
                action_hint = "All timeframes neutral. Wait for clearer direction."
        elif "bullish" in unique_trends and "bearish" not in unique_trends:
            recommendation = "MODERATE_BULLISH"
            action_hint = "Bullish bias but not all timeframes aligned. Use smaller size."
        elif "bearish" in unique_trends and "bullish" not in unique_trends:
            recommendation = "MODERATE_BEARISH"
            action_hint = "Bearish bias but not all timeframes aligned. Use smaller size."
        else:
            recommendation = "CONFLICTING"
            action_hint = "Timeframes disagree on direction. Best to wait or hold."

        # Calculate overall momentum
        momentum_score = self._calculate_momentum_score(signals, details)

        return json.dumps({
            "symbol": symbol,
            "timeframes": details,
            "alignment": {
                "aligned": aligned,
                "unique_trends": list(unique_trends),
                "timeframes_analyzed": len(signals),
            },
            "recommendation": recommendation,
            "action_hint": action_hint,
            "momentum_score": momentum_score,
            "suggested_actions": self._get_suggested_actions(recommendation),
        }, indent=2)

    def _calculate_trend(self, candles: list) -> tuple[str, float, dict]:
        """
        Calculate trend from OHLCV candles.

        Returns:
            tuple: (trend direction, strength 0-1, metrics dict)
        """
        if len(candles) < 10:
            return "neutral", 0.0, {}

        # Extract closes
        closes = [float(c.get("close", 0)) for c in candles[-20:]]

        # Calculate SMAs
        sma5 = sum(closes[-5:]) / 5 if len(closes) >= 5 else closes[-1]
        sma10 = sum(closes[-10:]) / 10 if len(closes) >= 10 else closes[-1]
        sma20 = sum(closes) / len(closes)

        # Calculate price position relative to SMAs
        current_price = closes[-1]

        # Calculate momentum (price change over period)
        price_change_pct = (closes[-1] - closes[0]) / closes[0] * 100 if closes[0] else 0

        # Calculate higher highs / lower lows
        highs = [float(c.get("high", 0)) for c in candles[-10:]]
        lows = [float(c.get("low", 0)) for c in candles[-10:]]

        recent_high = max(highs[-3:]) if len(highs) >= 3 else highs[-1]
        prev_high = max(highs[:-3]) if len(highs) > 3 else highs[0]
        recent_low = min(lows[-3:]) if len(lows) >= 3 else lows[-1]
        prev_low = min(lows[:-3]) if len(lows) > 3 else lows[0]

        higher_highs = recent_high > prev_high
        higher_lows = recent_low > prev_low
        lower_highs = recent_high < prev_high
        lower_lows = recent_low < prev_low

        # Score bullish/bearish signals
        bullish_score = 0
        bearish_score = 0

        # SMA alignment
        if sma5 > sma10 > sma20:
            bullish_score += 2
        elif sma5 < sma10 < sma20:
            bearish_score += 2

        # Price vs SMAs
        if current_price > sma5 > sma10:
            bullish_score += 1
        elif current_price < sma5 < sma10:
            bearish_score += 1

        # Higher highs/lows
        if higher_highs and higher_lows:
            bullish_score += 2
        elif lower_highs and lower_lows:
            bearish_score += 2

        # Momentum
        if price_change_pct > 2:
            bullish_score += 1
        elif price_change_pct < -2:
            bearish_score += 1

        # Determine trend
        total_score = bullish_score + bearish_score
        if total_score == 0:
            return "neutral", 0.0, {"sma5": sma5, "sma20": sma20}

        if bullish_score > bearish_score:
            strength = bullish_score / 6  # Max possible score
            return "bullish", min(strength, 1.0), {
                "sma5": round(sma5, 2),
                "sma20": round(sma20, 2),
                "price_change_pct": round(price_change_pct, 2),
                "higher_highs": higher_highs,
                "higher_lows": higher_lows,
            }
        elif bearish_score > bullish_score:
            strength = bearish_score / 6
            return "bearish", min(strength, 1.0), {
                "sma5": round(sma5, 2),
                "sma20": round(sma20, 2),
                "price_change_pct": round(price_change_pct, 2),
                "lower_highs": lower_highs,
                "lower_lows": lower_lows,
            }
        else:
            return "neutral", 0.0, {"sma5": round(sma5, 2), "sma20": round(sma20, 2)}

    def _calculate_momentum_score(self, signals: dict, details: dict) -> dict:
        """Calculate overall momentum score from all timeframes."""
        bullish_count = sum(1 for s in signals.values() if s == "bullish")
        bearish_count = sum(1 for s in signals.values() if s == "bearish")
        total = len(signals)

        if total == 0:
            return {"score": 0, "direction": "neutral"}

        # Calculate weighted strength
        total_strength = 0
        for interval, detail in details.items():
            if isinstance(detail, dict) and "strength" in detail:
                # Weight longer timeframes more
                weight = {"15m": 1, "1h": 2, "4h": 3}.get(interval, 1)
                direction_mult = 1 if signals.get(interval) == "bullish" else -1
                total_strength += detail["strength"] * weight * direction_mult

        max_weighted = 6  # 1 + 2 + 3
        normalized_score = total_strength / max_weighted

        if normalized_score > 0.2:
            direction = "bullish"
        elif normalized_score < -0.2:
            direction = "bearish"
        else:
            direction = "neutral"

        return {
            "score": round(normalized_score, 2),
            "direction": direction,
            "bullish_timeframes": bullish_count,
            "bearish_timeframes": bearish_count,
        }

    def _get_suggested_actions(self, recommendation: str) -> list[str]:
        """Get suggested actions based on recommendation."""
        actions = {
            "STRONG_BULLISH": [
                "Open long with higher leverage (3-5x)",
                "Scale into existing long positions",
                "Avoid shorts entirely",
            ],
            "STRONG_BEARISH": [
                "Open short with higher leverage (3-5x)",
                "Close long positions",
                "Scale into existing shorts",
            ],
            "MODERATE_BULLISH": [
                "Open long with lower leverage (2-3x)",
                "Keep existing longs",
                "Avoid new shorts",
            ],
            "MODERATE_BEARISH": [
                "Consider short with lower leverage (2-3x)",
                "Tighten stops on longs",
                "Avoid adding to longs",
            ],
            "CONFLICTING": [
                "Wait for clearer direction",
                "Reduce position sizes",
                "Consider closing positions",
            ],
            "NEUTRAL_ALIGNED": [
                "Wait for breakout",
                "Look at other symbols",
                "Hold existing positions",
            ],
        }
        return actions.get(recommendation, ["Analyze further"])
