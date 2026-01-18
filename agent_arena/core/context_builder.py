"""Build enriched context for storage and RAG."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from .indicators import compute_all_indicators
from .regime import calculate_volatility_percentile, classify_regime


class ContextBuilder:
    """
    Builds enriched market context for decisions.

    This context is used for:
    1. Storage in decision_contexts table
    2. Embedding generation for RAG retrieval
    3. Agent prompting with additional insights
    """

    def __init__(self, primary_symbol: str = "BTCUSDT"):
        """
        Initialize context builder.

        Args:
            primary_symbol: Symbol to use for regime classification (default: BTCUSDT).
        """
        self.primary_symbol = primary_symbol

    def build_context(
        self,
        market_data: dict,
        candles: dict,
        portfolio: dict,
        tick: int,
        timestamp: datetime,
    ) -> dict:
        """
        Build full enriched context from raw market data.

        Args:
            market_data: Dict of symbol -> market info (price, change_24h, etc.).
            candles: Dict of symbol -> interval -> list of candle dicts.
            portfolio: Portfolio state dict (equity, positions, etc.).
            tick: Current tick number.
            timestamp: Current timestamp.

        Returns:
            Enriched context dict with indicators, regime, and volatility.
        """
        # Compute indicators for each symbol
        indicators = {}
        for symbol, symbol_candles in candles.items():
            # Use 1h candles for indicator calculation if available
            hourly_candles = symbol_candles.get("1h", symbol_candles.get("15m", []))
            if hourly_candles:
                indicators[symbol] = compute_all_indicators(hourly_candles)

        # Classify regime using primary symbol (BTC as market proxy)
        primary_candles = candles.get(self.primary_symbol, {}).get("1h", [])
        primary_indicators = indicators.get(self.primary_symbol, {})

        regime = classify_regime(primary_candles, primary_indicators)
        volatility_pct = calculate_volatility_percentile(primary_candles)

        return {
            "market_prices": market_data,
            "candles": candles,
            "indicators": indicators,
            "portfolio_state": portfolio,
            "regime": regime,
            "volatility_percentile": volatility_pct,
            "tick": tick,
            "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime) else timestamp,
        }

    def summarize_context(self, context: dict) -> str:
        """
        Create text summary for embedding generation.

        Args:
            context: Enriched context dict.

        Returns:
            Text summary suitable for embedding.
        """
        parts = []

        # Market prices
        parts.append("=== Market Prices ===")
        for symbol, data in context.get("market_prices", {}).items():
            price = data.get("price", 0)
            change = data.get("change_24h", 0)
            funding = data.get("funding_rate", 0)
            parts.append(
                f"{symbol}: ${price:,.2f} ({change:+.2f}%) "
                f"funding={funding:.4f}%"
            )

        # Technical indicators
        parts.append("")
        parts.append("=== Technical Indicators ===")
        for symbol, ind in context.get("indicators", {}).items():
            rsi = ind.get("rsi_14")
            sma_pct = ind.get("price_vs_sma20")
            macd = ind.get("macd")
            bb = ind.get("bollinger")

            if rsi:
                signal = ind.get("rsi_signal", "neutral")
                parts.append(f"{symbol} RSI(14): {rsi:.1f} ({signal})")

            if sma_pct:
                direction = "above" if sma_pct > 0 else "below"
                parts.append(f"{symbol} Price vs SMA20: {sma_pct:+.1f}% ({direction})")

            if macd:
                macd_val = macd.get("histogram", 0)
                if macd_val:
                    momentum = "bullish" if macd_val > 0 else "bearish"
                    parts.append(f"{symbol} MACD: {momentum} (histogram={macd_val:.2f})")

            if bb:
                percent_b = bb.get("percent_b", 0.5)
                if percent_b > 0.8:
                    bb_signal = "near upper band (overbought)"
                elif percent_b < 0.2:
                    bb_signal = "near lower band (oversold)"
                else:
                    bb_signal = "within bands"
                parts.append(f"{symbol} Bollinger: {bb_signal}")

        # Portfolio state
        parts.append("")
        parts.append("=== Portfolio ===")
        portfolio = context.get("portfolio_state", {})
        equity = portfolio.get("equity", 10000)
        pnl_pct = portfolio.get("pnl_percent", 0)
        positions = portfolio.get("positions", [])
        available = portfolio.get("available_margin", equity)

        parts.append(f"Equity: ${equity:,.2f}")
        parts.append(f"P&L: {pnl_pct:+.2f}%")
        parts.append(f"Available margin: ${available:,.2f}")
        parts.append(f"Open positions: {len(positions)}")

        if positions:
            for pos in positions[:5]:  # Limit to 5 positions
                symbol = pos.get("symbol", "?")
                side = pos.get("side", "?")
                size = pos.get("size", 0)
                pnl = pos.get("unrealized_pnl", 0)
                parts.append(f"  - {symbol} {side}: {size} (P&L: ${pnl:+.2f})")

        # Regime and volatility
        parts.append("")
        parts.append("=== Market Conditions ===")
        regime = context.get("regime", "unknown")
        vol_pct = context.get("volatility_percentile", 50)
        parts.append(f"Market regime: {regime}")
        parts.append(f"Volatility percentile: {vol_pct:.0f}%")

        if vol_pct > 80:
            parts.append("WARNING: High volatility environment")
        elif vol_pct < 20:
            parts.append("NOTE: Low volatility environment")

        return "\n".join(parts)

    def get_key_conditions(self, context: dict) -> dict:
        """
        Extract key conditions for pattern matching.

        Args:
            context: Enriched context dict.

        Returns:
            Dict of condition -> value for pattern matching.
        """
        conditions = {
            "regime": context.get("regime"),
            "volatility_percentile": context.get("volatility_percentile"),
        }

        # Portfolio conditions
        portfolio = context.get("portfolio_state", {})
        positions = portfolio.get("positions", [])
        conditions["has_positions"] = len(positions) > 0
        conditions["position_count"] = len(positions)
        conditions["pnl_percent"] = portfolio.get("pnl_percent", 0)

        equity = portfolio.get("equity", 10000)
        starting = 10000  # Assume $10k starting
        conditions["is_profitable"] = equity > starting
        conditions["equity_change_pct"] = ((equity / starting) - 1) * 100

        # Indicator conditions for primary symbol
        primary_ind = context.get("indicators", {}).get(self.primary_symbol, {})

        rsi = primary_ind.get("rsi_14")
        if rsi:
            conditions["btc_rsi"] = rsi
            conditions["rsi_oversold"] = rsi < 30
            conditions["rsi_overbought"] = rsi > 70

        sma_pct = primary_ind.get("price_vs_sma20")
        if sma_pct:
            conditions["btc_vs_sma20"] = sma_pct
            conditions["above_sma20"] = sma_pct > 0

        macd = primary_ind.get("macd")
        if macd:
            hist = macd.get("histogram", 0)
            conditions["macd_bullish"] = hist > 0 if hist else None

        adx = primary_ind.get("adx")
        if adx:
            conditions["adx"] = adx.get("adx")
            conditions["strong_trend"] = adx.get("adx", 0) > 25

        return conditions

    def format_for_prompt(self, context: dict) -> str:
        """
        Format context for inclusion in LLM prompts.

        Args:
            context: Enriched context dict.

        Returns:
            Formatted string for LLM prompt.
        """
        lines = []

        # Market overview
        lines.append("## Current Market State")
        lines.append("")

        for symbol, data in context.get("market_prices", {}).items():
            price = data.get("price", 0)
            change = data.get("change_24h", 0)
            lines.append(f"- **{symbol}**: ${price:,.2f} ({change:+.2f}% 24h)")

        # Regime
        lines.append("")
        regime = context.get("regime", "unknown")
        vol_pct = context.get("volatility_percentile", 50)
        lines.append(f"**Market Regime**: {regime}")
        lines.append(f"**Volatility**: {vol_pct:.0f}th percentile")

        # Key indicators
        lines.append("")
        lines.append("## Key Indicators")
        lines.append("")

        for symbol, ind in context.get("indicators", {}).items():
            rsi = ind.get("rsi_14")
            rsi_signal = ind.get("rsi_signal", "")
            sma_pct = ind.get("price_vs_sma20")
            ma_trend = ind.get("ma_trend", "")

            indicator_parts = []
            if rsi:
                indicator_parts.append(f"RSI={rsi:.1f}")
                if rsi_signal:
                    indicator_parts.append(f"({rsi_signal})")
            if sma_pct:
                indicator_parts.append(f"vs SMA20={sma_pct:+.1f}%")
            if ma_trend:
                indicator_parts.append(f"trend={ma_trend}")

            if indicator_parts:
                lines.append(f"- {symbol}: {' '.join(indicator_parts)}")

        # Portfolio
        lines.append("")
        lines.append("## Portfolio Status")
        lines.append("")

        portfolio = context.get("portfolio_state", {})
        equity = portfolio.get("equity", 10000)
        pnl_pct = portfolio.get("pnl_percent", 0)
        available = portfolio.get("available_margin", equity)
        positions = portfolio.get("positions", [])

        lines.append(f"- Equity: ${equity:,.2f} ({pnl_pct:+.2f}%)")
        lines.append(f"- Available Margin: ${available:,.2f}")
        lines.append(f"- Open Positions: {len(positions)}")

        if positions:
            lines.append("")
            for pos in positions:
                symbol = pos.get("symbol", "?")
                side = pos.get("side", "?")
                size = pos.get("size", 0)
                pnl = pos.get("unrealized_pnl", 0)
                leverage = pos.get("leverage", 1)
                lines.append(
                    f"  - {symbol} {side.upper()} x{leverage}: "
                    f"{size} (P&L: ${pnl:+.2f})"
                )

        return "\n".join(lines)


# Module-level convenience instance
_default_builder: Optional[ContextBuilder] = None


def get_context_builder(primary_symbol: str = "BTCUSDT") -> ContextBuilder:
    """Get or create the default context builder instance."""
    global _default_builder
    if _default_builder is None or _default_builder.primary_symbol != primary_symbol:
        _default_builder = ContextBuilder(primary_symbol)
    return _default_builder
