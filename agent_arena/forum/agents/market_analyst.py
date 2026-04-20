"""MarketAnalyst discussion agent - posts technical analysis to the forum."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

import httpx

from agent_arena.agents.model_registry import resolve_model
from agent_arena.llm_utils import strip_reasoning_preamble, strip_think_blocks

if TYPE_CHECKING:
    from agent_arena.forum.service import ForumService

logger = logging.getLogger(__name__)


class MarketAnalystAgent:
    """Posts technical analysis to the forum every N ticks.

    Uses TA indicators and market data to generate insights about:
    - Trend direction (SMA, MACD)
    - Overbought/oversold conditions (RSI)
    - Key support/resistance levels
    - Funding rate implications
    """

    def __init__(self, agent_id: str, config: dict, forum: ForumService):
        """Initialize MarketAnalyst.

        Args:
            agent_id: Unique agent identifier
            config: Configuration dict with:
                - name: Display name
                - model: LLM model name (local)
                - base_url: LLM API base URL
                - api_key_env: Environment variable for API key
                - post_interval_ticks: Ticks between posts (default 5)
                - significant_move_threshold: Price change % to override interval (default 0.02)
                - symbols: List of symbols to analyze (default: all in context)
            forum: ForumService instance
        """
        self.agent_id = agent_id
        self.name = config.get("name", "MarketAnalyst")
        self.forum = forum

        # LLM config
        self.model = resolve_model(config.get("model", "glm-5"))
        self.base_url = config.get("base_url")
        self.api_key_env = config.get("api_key_env")

        # Posting behavior
        self.post_interval = config.get("post_interval_ticks", 5)
        self.significant_move_threshold = config.get("significant_move_threshold", 0.02)
        self.symbols_to_analyze = config.get("symbols", None)  # None = all

        # LLM API key
        self.api_key = os.environ.get(self.api_key_env or "", "") if self.api_key_env else ""

        # Extra params merged into request body (e.g. {think: false})
        self.extra_params = config.get("extra_params", {})

        # Persistent HTTP client (created lazily)
        self._client: httpx.AsyncClient | None = None

        # State
        self.last_post_tick = 0
        self.previous_prices = {}

    async def on_tick(self, context: dict) -> None:
        """Called every tick to potentially post analysis.

        Args:
            context: Trading context with market, candles, tick info
        """
        tick = context["tick"]

        # Check if we should post
        should_post = False

        # Regular interval
        if tick - self.last_post_tick >= self.post_interval:
            should_post = True

        # Significant price movement override
        elif self._is_significant_move(context):
            should_post = True

        if not should_post:
            return

        # Analyze market and post
        await self._analyze_and_post(context)
        self.last_post_tick = tick

    def _is_significant_move(self, context: dict) -> bool:
        """Check if price has moved significantly since last post.

        Args:
            context: Trading context

        Returns:
            True if any symbol has moved > threshold
        """
        market = context.get("market", {})

        for symbol, data in market.items():
            current_price = data.get("price", 0)
            if symbol in self.previous_prices:
                previous_price = self.previous_prices[symbol]
                if previous_price > 0:
                    change = abs(current_price - previous_price) / previous_price
                    if change >= self.significant_move_threshold:
                        return True

        return False

    async def _analyze_and_post(self, context: dict) -> None:
        """Perform TA analysis and post to forum.

        Args:
            context: Trading context
        """
        tick = context["tick"]
        market = context.get("market", {})
        candles = context.get("candles", {})

        # Fetch Fear & Greed Index (shared cache, free API)
        from agent_arena.providers.fear_greed import get_fear_greed

        fear_greed = await get_fear_greed()

        # Determine which symbols to analyze
        symbols = (
            self.symbols_to_analyze if self.symbols_to_analyze else list(market.keys())
        )

        if not symbols:
            return

        # Analyze all symbols and combine into a single post
        all_analyses = []
        all_key_levels = {}
        all_indicators = {}
        for symbol in symbols:
            if symbol not in market:
                continue
            analysis = await self._analyze_symbol(
                symbol, market, candles, fear_greed
            )
            all_analyses.append(analysis["content"])
            all_key_levels[symbol] = analysis.get("key_levels", {})
            all_indicators[symbol] = analysis.get("indicators", {})

        # Update previous prices
        for symbol, data in market.items():
            self.previous_prices[symbol] = data.get("price", 0)

        if not all_analyses:
            return

        # Prepend Fear & Greed header if available
        parts = []
        if fear_greed:
            val = fear_greed["value"]
            cls = fear_greed["classification"]
            parts.append(f"**Market Sentiment** | Fear & Greed: {val} ({cls})")
            parts.append("")

        combined_content = "\n\n---\n\n".join(all_analyses)
        if parts:
            combined_content = "\n".join(parts) + "\n\n---\n\n" + combined_content

        # Post to forum
        await self.forum.post_message(
            channel="market",
            agent_id=self.agent_id,
            agent_name=self.name,
            agent_type="discussion",
            content=combined_content,
            metadata={
                "tick": tick,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "symbols_analyzed": symbols,
                "key_levels": all_key_levels,
                "indicators": all_indicators,
                "fear_greed": fear_greed,
            },
        )

    async def _analyze_symbol(
        self,
        symbol: str,
        market: dict,
        candles: dict,
        fear_greed: dict | None = None,
    ) -> dict:
        """Analyze a symbol and generate insights.

        Args:
            symbol: Symbol to analyze
            market: Market data dict
            candles: Candles data dict
            fear_greed: Fear & Greed Index data (optional)

        Returns:
            Dict with content and metadata
        """
        symbol_data = market.get(symbol, {})
        price = symbol_data.get("price", 0)
        change_24h = symbol_data.get("change_24h", 0)
        funding_rate = symbol_data.get("funding_rate", 0)

        # Get candle data for indicators
        symbol_candles = candles.get(symbol, {})
        candles_1h = symbol_candles.get("1h", [])

        # Calculate simple indicators
        indicators = self._calculate_indicators(candles_1h)

        # Determine regime
        regime = self._determine_regime(change_24h, indicators)

        # Identify key levels (simplified)
        key_levels = self._identify_key_levels(candles_1h, price)

        # Generate analysis content (LLM with template fallback)
        content = await self._generate_analysis(
            symbol,
            price,
            change_24h,
            funding_rate,
            regime,
            indicators,
            key_levels,
            fear_greed,
        )

        return {
            "content": content,
            "key_levels": key_levels,
            "indicators": indicators,
            "regime": regime,
        }

    def _calculate_indicators(self, candles: list[dict]) -> dict:
        """Calculate technical indicators from candles.

        Args:
            candles: List of candle dicts

        Returns:
            Dict with indicator values
        """
        if not candles or len(candles) < 2:
            return {
                "rsi": None,
                "sma_20": None,
                "sma_50": None,
                "trend": "unknown",
            }

        # Extract close prices
        closes = [float(c.get("close", 0)) for c in candles]

        # Simple RSI calculation (14 period)
        rsi = self._simple_rsi(closes, 14)

        # SMA calculations
        sma_20 = self._simple_sma(closes, 20)
        sma_50 = self._simple_sma(closes, 50)

        # Trend determination
        trend = "unknown"
        if sma_20 is not None and sma_50 is not None:
            if sma_20 > sma_50:
                trend = "bullish"
            elif sma_20 < sma_50:
                trend = "bearish"
            else:
                trend = "neutral"

        return {
            "rsi": round(rsi, 1) if rsi else None,
            "sma_20": round(sma_20, 2) if sma_20 else None,
            "sma_50": round(sma_50, 2) if sma_50 else None,
            "trend": trend,
        }

    def _simple_rsi(self, prices: list[float], period: int = 14) -> Optional[float]:
        """Calculate simple RSI.

        Args:
            prices: List of prices
            period: RSI period

        Returns:
            RSI value or None
        """
        if len(prices) < period + 1:
            return None

        # Calculate price changes
        changes = [prices[i] - prices[i - 1] for i in range(1, len(prices))]

        # Separate gains and losses
        gains = [c if c > 0 else 0 for c in changes[-period:]]
        losses = [-c if c < 0 else 0 for c in changes[-period:]]

        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        return rsi

    def _simple_sma(self, prices: list[float], period: int) -> Optional[float]:
        """Calculate simple moving average.

        Args:
            prices: List of prices
            period: SMA period

        Returns:
            SMA value or None
        """
        if len(prices) < period:
            return None

        return sum(prices[-period:]) / period

    def _determine_regime(self, change_24h: float, indicators: dict) -> str:
        """Determine market regime.

        Args:
            change_24h: 24h price change
            indicators: Indicator dict

        Returns:
            Regime string
        """
        # change_24h is a percentage (e.g. 3.5 = 3.5%)
        if abs(change_24h) < 1.0:
            return "ranging"
        elif change_24h > 3.0:
            return "trending_up"
        elif change_24h < -3.0:
            return "trending_down"
        else:
            # Use trend from indicators
            trend = indicators.get("trend", "unknown")
            if trend == "bullish":
                return "trending_up"
            elif trend == "bearish":
                return "trending_down"
            else:
                return "ranging"

    def _identify_key_levels(
        self, candles: list[dict], current_price: float
    ) -> dict:
        """Identify key support and resistance levels.

        Args:
            candles: Candle data
            current_price: Current price

        Returns:
            Dict with resistance and support levels
        """
        if not candles or len(candles) < 10:
            return {"resistance": None, "support": None}

        # Get recent highs and lows
        highs = [float(c.get("high", 0)) for c in candles[-20:]]
        lows = [float(c.get("low", 0)) for c in candles[-20:]]

        # Resistance = recent high above current price
        resistance_candidates = [h for h in highs if h > current_price]
        resistance = min(resistance_candidates) if resistance_candidates else None

        # Support = recent low below current price
        support_candidates = [low for low in lows if low < current_price]
        support = max(support_candidates) if support_candidates else None

        return {
            "resistance": round(resistance, 2) if resistance else None,
            "support": round(support, 2) if support else None,
        }

    async def _generate_analysis(
        self,
        symbol: str,
        price: float,
        change_24h: float,
        funding_rate: float,
        regime: str,
        indicators: dict,
        key_levels: dict,
        fear_greed: dict | None = None,
    ) -> str:
        """Generate analysis via LLM, falling back to template on failure.

        Args:
            symbol: Trading symbol
            price: Current price
            change_24h: 24h change
            funding_rate: Funding rate
            regime: Market regime
            indicators: Indicator values
            key_levels: Support/resistance levels
            fear_greed: Fear & Greed Index data (optional)

        Returns:
            Analysis content string
        """
        # If no LLM configured, use template
        if not self.base_url or not self.model:
            return self._format_analysis_template(
                symbol, price, change_24h, funding_rate, regime,
                indicators, key_levels,
            )

        # Build structured context for the LLM
        # change_24h is already a percentage (e.g. 3.5 = 3.5%)
        data_summary = (
            f"Symbol: {symbol}\n"
            f"Price: ${price:,.2f}\n"
            f"24h Change: {change_24h:+.2f}%\n"
            f"Funding Rate: {funding_rate * 100:.4f}%\n"
            f"Market Regime: {regime}\n"
            f"RSI(14): {indicators.get('rsi', 'N/A')}\n"
            f"SMA(20): {indicators.get('sma_20', 'N/A')}\n"
            f"SMA(50): {indicators.get('sma_50', 'N/A')}\n"
            f"Trend (SMA crossover): {indicators.get('trend', 'unknown')}\n"
            f"Nearest Resistance: {key_levels.get('resistance', 'N/A')}\n"
            f"Nearest Support: {key_levels.get('support', 'N/A')}"
        )
        if fear_greed:
            data_summary += (
                f"\nFear & Greed Index: {fear_greed['value']} "
                f"({fear_greed['classification']})"
            )

        try:
            if self._client is None:
                self._client = httpx.AsyncClient(timeout=30.0)

            response = await self._client.post(
                f"{self.base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "You are a market analyst agent posting in a "
                                "crypto trading forum. Write concise, insightful "
                                "technical analysis based on the data provided. "
                                "Use markdown formatting. Be direct and "
                                "opinionated about the outlook. "
                                "Keep posts under 200 words. No emojis. "
                                "CRITICAL: Use the EXACT numeric values from "
                                "the data (prices, percentages, indicators). "
                                "Do NOT recalculate, round differently, or "
                                "change any numbers. Copy them verbatim. "
                                "Output ONLY the forum post. Do NOT include "
                                "any reasoning steps, analysis process, "
                                "numbered plans, or drafts."
                            ),
                        },
                        {
                            "role": "user",
                            "content": (
                                f"Here is the current market data:\n\n{data_summary}\n\n"
                                "Write a natural market analysis post for the trading forum."
                            ),
                        },
                    ],
                    "max_tokens": 400,
                    "temperature": 0.5,
                    "chat_template_kwargs": {"enable_thinking": False},
                    **self.extra_params,
                },
            )
            response.raise_for_status()
            data = response.json()
            raw = data["choices"][0]["message"]["content"]
            content = strip_think_blocks(raw)
            content = strip_reasoning_preamble(content)
            if content:
                # Prepend factual header so key numbers are always
                # accurate even if the LLM hallucinates values
                change_str = (
                    f"+{change_24h:.2f}%"
                    if change_24h >= 0
                    else f"{change_24h:.2f}%"
                )
                header = (
                    f"**{symbol}** | "
                    f"${price:,.2f} ({change_str} 24h) | "
                    f"Funding: {funding_rate * 100:.4f}%"
                )
                return f"{header}\n\n{content}"
        except Exception as e:
            logger.warning("MarketAnalyst LLM call failed, using template: %s", e)

        # Fallback to template
        return self._format_analysis_template(
            symbol, price, change_24h, funding_rate, regime, indicators, key_levels
        )

    def _format_analysis_template(
        self,
        symbol: str,
        price: float,
        change_24h: float,
        funding_rate: float,
        regime: str,
        indicators: dict,
        key_levels: dict,
    ) -> str:
        """Format analysis as markdown message (template fallback).

        Args:
            symbol: Trading symbol
            price: Current price
            change_24h: 24h change
            funding_rate: Funding rate
            regime: Market regime
            indicators: Indicator values
            key_levels: Support/resistance levels

        Returns:
            Formatted markdown content
        """
        # Build message
        lines = [f"**{symbol} Market Analysis**", ""]

        # Price and trend (change_24h is already a percentage)
        change_str = f"+{change_24h:.2f}%" if change_24h >= 0 else f"{change_24h:.2f}%"
        lines.append(
            f"Price: ${price:,.2f} ({change_str} 24h) | Regime: `{regime}`"
        )
        lines.append("")

        # Indicators
        rsi = indicators.get("rsi")
        sma_20 = indicators.get("sma_20")
        trend = indicators.get("trend", "unknown")

        if rsi is not None:
            rsi_signal = (
                "overbought" if rsi > 70 else "oversold" if rsi < 30 else "neutral"
            )
            lines.append(f"RSI(14): {rsi:.1f} ({rsi_signal})")

        if sma_20 is not None:
            price_vs_sma = "above" if price > sma_20 else "below"
            lines.append(f"Price {price_vs_sma} SMA(20) at ${sma_20:,.2f}")

        if trend != "unknown":
            lines.append(f"Trend: {trend}")

        lines.append("")

        # Key levels
        if key_levels.get("resistance") or key_levels.get("support"):
            lines.append("**Key Levels:**")
            if key_levels.get("resistance"):
                lines.append(f"- Resistance: ${key_levels['resistance']:,.2f}")
            if key_levels.get("support"):
                lines.append(f"- Support: ${key_levels['support']:,.2f}")
            lines.append("")

        # Funding rate warning
        if funding_rate and abs(funding_rate) > 0.03:
            direction = "longs paying" if funding_rate > 0 else "shorts paying"
            lines.append(
                f"Funding rate: {funding_rate*100:.3f}% ({direction})"
            )

        return "\n".join(lines)

    async def on_stop(self) -> None:
        """Clean up resources."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
