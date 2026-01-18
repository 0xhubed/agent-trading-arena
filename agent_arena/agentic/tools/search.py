"""Market Search tool - news, sentiment, Fear & Greed Index."""

import asyncio
import json
from typing import ClassVar, Optional

import httpx
from pydantic import BaseModel, Field

from agent_arena.agentic.tools.base import TradingTool


class MarketSearchInput(BaseModel):
    """Input schema for market search."""

    query: str = Field(description="Search query (e.g., 'BTC', 'ETH sentiment', 'market fear')")
    source: str = Field(
        default="all",
        description="Data source: 'fear_greed', 'sentiment', 'funding', 'all'",
    )


class MarketSearchTool(TradingTool):
    """
    Search for market sentiment and external indicators.

    Sources:
    - Fear & Greed Index (real API)
    - Funding rate analysis (from context)
    - Price-based sentiment (derived from market data)
    """

    name: str = "market_search"
    description: str = """Search for crypto market sentiment and indicators.
Use this to understand market conditions and crowd sentiment before trading.
Input: query (search term like 'BTC' or 'market'), source (fear_greed/sentiment/funding/all).
Returns: Sentiment scores, Fear & Greed Index, funding rate analysis."""

    args_schema: type[BaseModel] = MarketSearchInput

    # Fear & Greed Index API
    FEAR_GREED_URL: ClassVar[str] = "https://api.alternative.me/fng/"

    def _run(
        self,
        query: str,
        source: str = "all",
    ) -> str:
        """Sync wrapper."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as pool:
                    future = pool.submit(asyncio.run, self._arun(query, source))
                    return future.result()
            else:
                return loop.run_until_complete(self._arun(query, source))
        except Exception:
            return asyncio.run(self._arun(query, source))

    async def _arun(
        self,
        query: str,
        source: str = "all",
    ) -> str:
        """Search for market information."""
        results = {"query": query, "source": source}

        if source in ["all", "fear_greed"]:
            results["fear_greed"] = await self._get_fear_greed()

        if source in ["all", "sentiment"]:
            results["sentiment"] = self._get_price_sentiment(query)

        if source in ["all", "funding"]:
            results["funding_analysis"] = self._get_funding_analysis(query)

        # Add overall market assessment
        results["market_assessment"] = self._assess_market(results)

        return json.dumps(results, indent=2)

    async def _get_fear_greed(self) -> dict:
        """Fetch Fear & Greed Index from API."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(self.FEAR_GREED_URL, timeout=10)
                data = response.json()

                if data.get("data"):
                    current = data["data"][0]
                    value = int(current["value"])
                    return {
                        "value": value,
                        "classification": current["value_classification"],
                        "timestamp": current["timestamp"],
                        "interpretation": self._interpret_fear_greed(value),
                        "trading_implication": self._fear_greed_trading_signal(value),
                    }
        except httpx.TimeoutException:
            return {"error": "Fear & Greed API timeout", "fallback": "Use other indicators"}
        except Exception as e:
            return {"error": f"Could not fetch Fear & Greed: {str(e)}"}

        return {"error": "No data available"}

    def _interpret_fear_greed(self, value: int) -> str:
        """Interpret Fear & Greed value."""
        if value <= 25:
            return "Extreme Fear - Market is very scared, potential capitulation"
        elif value <= 45:
            return "Fear - Market is cautious, sentiment is negative"
        elif value <= 55:
            return "Neutral - Market is balanced, no strong directional bias"
        elif value <= 75:
            return "Greed - Market is optimistic, risk appetite is high"
        else:
            return "Extreme Greed - Market is euphoric, potential bubble territory"

    def _fear_greed_trading_signal(self, value: int) -> str:
        """Get trading signal from Fear & Greed."""
        if value <= 25:
            return "CONTRARIAN BUY - Extreme fear often marks bottoms"
        elif value <= 35:
            return "ACCUMULATE - Fear presents buying opportunities"
        elif value <= 65:
            return "NEUTRAL - Trade based on technicals, not sentiment"
        elif value <= 80:
            return "CAUTIOUS - Greed can precede corrections"
        else:
            return "CONTRARIAN SELL - Extreme greed often marks tops"

    def _get_price_sentiment(self, query: str) -> dict:
        """Derive sentiment from price action in context."""
        market = self._context.get("market", {})
        query_lower = query.lower()

        sentiment_data = {}
        for symbol, data in market.items():
            symbol_base = symbol.lower().replace("usdt", "")
            if query_lower in symbol_base or symbol_base in query_lower or query_lower in ["all", "market"]:
                change = data.get("change_24h", 0)

                # Determine sentiment from price change
                if change > 5:
                    price_sentiment = "very_bullish"
                elif change > 2:
                    price_sentiment = "bullish"
                elif change > -2:
                    price_sentiment = "neutral"
                elif change > -5:
                    price_sentiment = "bearish"
                else:
                    price_sentiment = "very_bearish"

                sentiment_data[symbol] = {
                    "price_sentiment": price_sentiment,
                    "change_24h_pct": round(change, 2),
                    "current_price": float(data.get("price", 0)),
                    "volume_24h": float(data.get("volume_24h", 0)),
                }

        if not sentiment_data:
            return {"message": f"No market data found matching '{query}'"}

        return sentiment_data

    def _get_funding_analysis(self, query: str) -> dict:
        """Analyze funding rates from context."""
        market = self._context.get("market", {})
        query_lower = query.lower()

        funding_data = {}
        for symbol, data in market.items():
            symbol_base = symbol.lower().replace("usdt", "")
            if query_lower in symbol_base or symbol_base in query_lower or query_lower in ["all", "market"]:
                funding_rate = data.get("funding_rate")
                if funding_rate is not None:
                    rate = float(funding_rate) * 100  # Convert to percentage

                    # Interpret funding rate
                    if rate > 0.05:
                        interpretation = "Very positive - Longs paying shorts heavily"
                        signal = "Crowded long - potential short opportunity"
                    elif rate > 0.01:
                        interpretation = "Positive - Longs paying shorts"
                        signal = "Bullish bias in market"
                    elif rate > -0.01:
                        interpretation = "Neutral - Balanced market"
                        signal = "No strong directional pressure"
                    elif rate > -0.05:
                        interpretation = "Negative - Shorts paying longs"
                        signal = "Bearish bias in market"
                    else:
                        interpretation = "Very negative - Shorts paying longs heavily"
                        signal = "Crowded short - potential long opportunity"

                    funding_data[symbol] = {
                        "funding_rate_pct": round(rate, 4),
                        "interpretation": interpretation,
                        "trading_signal": signal,
                        "annualized_rate_pct": round(rate * 3 * 365, 2),  # 8hr funding * 3 * 365
                    }

        if not funding_data:
            return {"message": f"No funding data found matching '{query}'"}

        return funding_data

    def _assess_market(self, results: dict) -> dict:
        """Create overall market assessment."""
        signals = []

        # Fear & Greed signal
        fg = results.get("fear_greed", {})
        if isinstance(fg, dict) and "value" in fg:
            value = fg["value"]
            if value <= 30:
                signals.append(("fear_greed", "bullish", "Extreme fear = buying opportunity"))
            elif value >= 70:
                signals.append(("fear_greed", "bearish", "Extreme greed = caution advised"))
            else:
                signals.append(("fear_greed", "neutral", "Balanced sentiment"))

        # Funding signals
        funding = results.get("funding_analysis", {})
        if isinstance(funding, dict):
            for symbol, data in funding.items():
                if isinstance(data, dict) and "funding_rate_pct" in data:
                    rate = data["funding_rate_pct"]
                    if rate > 0.03:
                        signals.append((f"{symbol}_funding", "bearish", "High positive funding"))
                    elif rate < -0.03:
                        signals.append((f"{symbol}_funding", "bullish", "High negative funding"))

        # Price sentiment signals
        sentiment = results.get("sentiment", {})
        if isinstance(sentiment, dict):
            for symbol, data in sentiment.items():
                if isinstance(data, dict) and "price_sentiment" in data:
                    ps = data["price_sentiment"]
                    if ps in ["very_bullish", "bullish"]:
                        signals.append((f"{symbol}_price", "bullish", f"Strong uptrend ({data.get('change_24h_pct')}%)"))
                    elif ps in ["very_bearish", "bearish"]:
                        signals.append((f"{symbol}_price", "bearish", f"Strong downtrend ({data.get('change_24h_pct')}%)"))

        # Count signals
        bullish = sum(1 for s in signals if s[1] == "bullish")
        bearish = sum(1 for s in signals if s[1] == "bearish")
        neutral = sum(1 for s in signals if s[1] == "neutral")

        if bullish > bearish + 1:
            overall = "BULLISH"
            recommendation = "Favor long positions, but use proper risk management"
        elif bearish > bullish + 1:
            overall = "BEARISH"
            recommendation = "Favor short positions or stay in cash"
        else:
            overall = "MIXED"
            recommendation = "Wait for clearer signals or trade smaller size"

        return {
            "overall_bias": overall,
            "bullish_signals": bullish,
            "bearish_signals": bearish,
            "neutral_signals": neutral,
            "recommendation": recommendation,
            "signal_details": [{"source": s[0], "direction": s[1], "reason": s[2]} for s in signals],
        }
