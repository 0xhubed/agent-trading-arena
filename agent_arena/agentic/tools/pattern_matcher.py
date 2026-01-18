"""Tool for matching learned trading patterns."""

from __future__ import annotations

from typing import Optional, Type

from pydantic import BaseModel, Field

from .base import TradingTool


class PatternMatcherInput(BaseModel):
    """Input schema for pattern matcher tool."""

    pattern_types: Optional[list[str]] = Field(
        default=None,
        description=(
            "Filter by pattern type: 'entry_signal', 'exit_signal', "
            "'risk_rule', 'regime_rule'"
        )
    )
    min_confidence: float = Field(
        default=0.5,
        description="Minimum pattern confidence (0-1)"
    )


class PatternMatcherTool(TradingTool):
    """
    Check if current conditions match any learned trading patterns.

    Patterns are rules discovered from historical trading outcomes,
    such as "RSI below 30 in trending_up regime often leads to profitable longs".
    """

    name: str = "check_patterns"
    description: str = """Check if current market conditions match any learned trading patterns.
Returns matching patterns with their historical success rates.
Use this to identify high-probability trading setups.

Returns list of matching patterns with:
- pattern_type: entry_signal, exit_signal, risk_rule, or regime_rule
- conditions: What conditions define the pattern
- lesson: The learned insight/recommendation
- confidence: Pattern confidence (0-1)
- success_rate: Historical win rate
- sample_size: Number of supporting trades"""

    args_schema: Type[BaseModel] = PatternMatcherInput

    def _run(
        self,
        pattern_types: Optional[list[str]] = None,
        min_confidence: float = 0.5,
    ) -> str:
        """Synchronous fallback."""
        return "Pattern matching requires async execution."

    async def _arun(
        self,
        pattern_types: Optional[list[str]] = None,
        min_confidence: float = 0.5,
    ) -> str:
        """Find patterns matching current conditions."""
        if not self._storage:
            return "Error: Storage not available for pattern lookup."

        if not self._context:
            return "Error: No current market context available."

        try:
            # Extract current conditions from context
            current_conditions = self._extract_conditions()

            # Get active patterns from storage
            patterns = await self._storage.get_active_patterns(
                pattern_types=pattern_types,
                min_confidence=min_confidence,
            )

            if not patterns:
                return "No learned patterns found matching the criteria."

            # Match patterns against current conditions
            matches = []
            for pattern in patterns:
                if self._matches_conditions(current_conditions, pattern.get("conditions", {})):
                    matches.append(pattern)

            if not matches:
                return (
                    f"No patterns match current conditions. "
                    f"Checked {len(patterns)} patterns against: {current_conditions}"
                )

            # Sort by confidence
            matches.sort(key=lambda x: x.get("confidence", 0), reverse=True)

            # Format output
            output_parts = [f"Found {len(matches)} matching patterns:\n"]

            for i, pattern in enumerate(matches[:5], 1):  # Limit to top 5
                p_type = pattern.get("pattern_type", "unknown")
                lesson = pattern.get("pattern_description", "No description")
                action = pattern.get("recommended_action")
                confidence = pattern.get("confidence", 0)
                success_rate = pattern.get("success_rate", 0)
                sample_size = pattern.get("sample_size", 0)
                conditions = pattern.get("conditions", {})

                output_parts.append(
                    f"\n{i}. [{p_type.upper()}] (confidence: {confidence:.0%})\n"
                    f"   Pattern: {lesson}\n"
                    f"   Success rate: {success_rate:.0%} ({sample_size} trades)\n"
                    f"   Conditions: {conditions}"
                )
                if action:
                    output_parts.append(f"\n   Recommended action: {action}")

            # Add overall guidance
            if matches:
                high_conf = [m for m in matches if m.get("confidence", 0) >= 0.7]
                if high_conf:
                    output_parts.append(
                        f"\n\n⚡ {len(high_conf)} high-confidence patterns detected. "
                        "Consider their recommendations strongly."
                    )

            return "\n".join(output_parts)

        except Exception as e:
            return f"Error matching patterns: {str(e)}"

    def _extract_conditions(self) -> dict:
        """Extract conditions from current context for pattern matching."""
        conditions = {}

        # Market regime
        if "regime" in self._context:
            conditions["regime"] = self._context["regime"]

        # Portfolio state
        portfolio = self._context.get("portfolio", {})
        positions = portfolio.get("positions", [])
        conditions["has_positions"] = len(positions) > 0
        conditions["position_count"] = len(positions)

        equity = portfolio.get("equity", 10000)
        starting = 10000
        pnl_pct = ((equity / starting) - 1) * 100
        conditions["pnl_percent"] = pnl_pct
        conditions["is_profitable"] = pnl_pct > 0

        # Market indicators (look for pre-computed indicators in context)
        indicators = self._context.get("indicators", {})
        for symbol, ind in indicators.items():
            # RSI conditions
            rsi = ind.get("rsi_14")
            if rsi:
                conditions[f"{symbol}_rsi"] = rsi
                conditions["rsi_oversold"] = rsi < 30
                conditions["rsi_overbought"] = rsi > 70

            # SMA conditions
            sma_pct = ind.get("price_vs_sma20")
            if sma_pct:
                conditions[f"{symbol}_vs_sma20"] = sma_pct
                conditions["above_sma20"] = sma_pct > 0

            # MACD
            macd = ind.get("macd")
            if macd and macd.get("histogram"):
                conditions["macd_bullish"] = macd["histogram"] > 0

            # ADX trend strength
            adx = ind.get("adx")
            if adx:
                conditions["adx"] = adx.get("adx")
                conditions["strong_trend"] = adx.get("adx", 0) > 25

        # Volatility
        vol_pct = self._context.get("volatility_percentile")
        if vol_pct:
            conditions["volatility_percentile"] = vol_pct
            conditions["high_volatility"] = vol_pct > 70
            conditions["low_volatility"] = vol_pct < 30

        return conditions

    def _matches_conditions(
        self,
        current: dict,
        pattern_conditions: dict,
    ) -> bool:
        """
        Check if current conditions match pattern conditions.

        Pattern conditions can be:
        - Exact values: {"regime": "trending_up"}
        - Ranges: {"rsi": {"gt": 70}} or {"rsi": {"lt": 30, "gt": 20}}
        - Lists: {"regime": ["trending_up", "ranging"]}
        """
        for key, expected in pattern_conditions.items():
            actual = current.get(key)

            # Skip if condition key not in current context
            if actual is None:
                continue

            # Handle different condition types
            if isinstance(expected, dict):
                # Range conditions
                if "gt" in expected and not (actual > expected["gt"]):
                    return False
                if "gte" in expected and not (actual >= expected["gte"]):
                    return False
                if "lt" in expected and not (actual < expected["lt"]):
                    return False
                if "lte" in expected and not (actual <= expected["lte"]):
                    return False
                if "eq" in expected and actual != expected["eq"]:
                    return False
                if "ne" in expected and actual == expected["ne"]:
                    return False

            elif isinstance(expected, list):
                # Multiple valid values
                if actual not in expected:
                    return False

            else:
                # Exact match
                if actual != expected:
                    return False

        return True
