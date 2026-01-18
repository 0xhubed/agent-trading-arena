"""Market regime classification for learning agents."""

from __future__ import annotations

from typing import Optional

from .indicators import calculate_adx, calculate_atr, calculate_sma


def classify_regime(
    candles: list[dict],
    indicators: Optional[dict] = None,
) -> str:
    """
    Classify current market regime.

    Uses multiple signals to determine market state:
    - Price position relative to moving averages
    - Moving average alignment
    - Recent momentum
    - Volatility levels
    - ADX trend strength

    Args:
        candles: List of candle dicts with OHLCV data.
        indicators: Pre-computed indicators dict (optional).

    Returns:
        One of: 'trending_up', 'trending_down', 'ranging', 'volatile', 'unknown'
    """
    if not candles or len(candles) < 50:
        return "unknown"

    closes = [float(c["close"]) for c in candles]
    current_price = closes[-1]

    indicators = indicators or {}

    # Get or calculate key indicators
    sma_20 = indicators.get("sma_20") or calculate_sma(closes, 20)
    sma_50 = indicators.get("sma_50") or calculate_sma(closes, 50)
    atr = indicators.get("atr_14") or calculate_atr(candles, 14)
    adx_data = indicators.get("adx") or calculate_adx(candles, 14)

    if sma_20 is None or sma_50 is None:
        return "unknown"

    # Calculate volatility percentage
    atr_pct = (atr / current_price) * 100 if atr and current_price else 0

    # Calculate recent momentum
    returns_5 = ((closes[-1] / closes[-5]) - 1) * 100 if len(closes) >= 5 else 0
    returns_20 = ((closes[-1] / closes[-20]) - 1) * 100 if len(closes) >= 20 else 0

    # Moving average signals
    above_sma20 = current_price > sma_20
    above_sma50 = current_price > sma_50
    sma20_above_sma50 = sma_20 > sma_50

    # ADX trend strength
    adx_value = adx_data.get("adx") if adx_data else None
    strong_trend = adx_value and adx_value > 25

    # Volatility classification
    is_high_volatility = atr_pct > 3.0
    is_extreme_volatility = atr_pct > 5.0

    # Extreme volatility overrides other classifications
    if is_extreme_volatility and abs(returns_5) > 10:
        return "volatile"

    # Strong trend up: aligned bullish signals
    if (
        above_sma20
        and above_sma50
        and sma20_above_sma50
        and returns_20 > 5
        and (not adx_value or strong_trend)
    ):
        return "trending_up"

    # Strong trend down: aligned bearish signals
    if (
        not above_sma20
        and not above_sma50
        and not sma20_above_sma50
        and returns_20 < -5
        and (not adx_value or strong_trend)
    ):
        return "trending_down"

    # High volatility with no clear trend
    if is_high_volatility:
        return "volatile"

    # Low ADX indicates ranging market
    if adx_value and adx_value < 20:
        return "ranging"

    # Mixed signals indicate ranging
    return "ranging"


def calculate_volatility_percentile(
    candles: list[dict],
    lookback: int = 100,
) -> float:
    """
    Calculate current volatility as percentile of recent history.

    Args:
        candles: List of candle dicts.
        lookback: Number of periods to compare against.

    Returns:
        Percentile (0-100) indicating current volatility relative to history.
    """
    if len(candles) < lookback:
        return 50.0  # Default to median if insufficient data

    # Calculate ATR for each point in history
    atrs = []
    for i in range(14, len(candles)):
        atr = calculate_atr(candles[:i + 1], 14)
        if atr:
            atrs.append(atr)

    if not atrs:
        return 50.0

    current_atr = atrs[-1]
    comparison_window = atrs[-lookback:] if len(atrs) >= lookback else atrs

    # Calculate percentile rank
    sorted_atrs = sorted(comparison_window)
    rank = sum(1 for a in sorted_atrs if a <= current_atr)

    return (rank / len(sorted_atrs)) * 100


def get_regime_characteristics(regime: str) -> dict:
    """
    Get trading characteristics for a market regime.

    Args:
        regime: Market regime string.

    Returns:
        Dict with recommended trading parameters for the regime.
    """
    characteristics = {
        "trending_up": {
            "description": "Strong bullish trend with aligned moving averages",
            "preferred_actions": ["open_long", "hold"],
            "avoid_actions": ["open_short"],
            "suggested_leverage": "moderate (3-5x)",
            "stop_loss_strategy": "trailing",
            "position_size": "larger",
            "entry_timing": "pullbacks to SMA20",
        },
        "trending_down": {
            "description": "Strong bearish trend with aligned moving averages",
            "preferred_actions": ["open_short", "hold"],
            "avoid_actions": ["open_long"],
            "suggested_leverage": "moderate (3-5x)",
            "stop_loss_strategy": "trailing",
            "position_size": "larger",
            "entry_timing": "rallies to SMA20",
        },
        "ranging": {
            "description": "Sideways movement with no clear direction",
            "preferred_actions": ["hold", "limit_long", "limit_short"],
            "avoid_actions": [],
            "suggested_leverage": "low (2-3x)",
            "stop_loss_strategy": "fixed",
            "position_size": "smaller",
            "entry_timing": "support/resistance levels",
        },
        "volatile": {
            "description": "High volatility with unpredictable movements",
            "preferred_actions": ["hold"],
            "avoid_actions": ["open_long", "open_short"],
            "suggested_leverage": "very low (1-2x)",
            "stop_loss_strategy": "wide fixed",
            "position_size": "minimal",
            "entry_timing": "wait for volatility to subside",
        },
        "unknown": {
            "description": "Insufficient data to classify regime",
            "preferred_actions": ["hold"],
            "avoid_actions": ["open_long", "open_short"],
            "suggested_leverage": "low (1-2x)",
            "stop_loss_strategy": "fixed",
            "position_size": "minimal",
            "entry_timing": "wait for clarity",
        },
    }

    return characteristics.get(regime, characteristics["unknown"])


def detect_regime_change(
    current_regime: str,
    previous_regime: str,
    confidence_threshold: float = 0.7,
) -> Optional[dict]:
    """
    Detect if a significant regime change has occurred.

    Args:
        current_regime: Current market regime.
        previous_regime: Previous market regime.
        confidence_threshold: Minimum confidence to report change.

    Returns:
        Dict with change details or None if no significant change.
    """
    if current_regime == previous_regime:
        return None

    # Define regime transition significance
    significant_transitions = {
        ("ranging", "trending_up"): {
            "significance": "high",
            "description": "Market breaking out into uptrend",
            "action_hint": "Consider long positions",
        },
        ("ranging", "trending_down"): {
            "significance": "high",
            "description": "Market breaking down into downtrend",
            "action_hint": "Consider short positions or exit longs",
        },
        ("trending_up", "trending_down"): {
            "significance": "critical",
            "description": "Major trend reversal from bullish to bearish",
            "action_hint": "Close longs, consider shorts",
        },
        ("trending_down", "trending_up"): {
            "significance": "critical",
            "description": "Major trend reversal from bearish to bullish",
            "action_hint": "Close shorts, consider longs",
        },
        ("trending_up", "volatile"): {
            "significance": "high",
            "description": "Uptrend becoming unstable",
            "action_hint": "Reduce position size, tighten stops",
        },
        ("trending_down", "volatile"): {
            "significance": "high",
            "description": "Downtrend becoming unstable",
            "action_hint": "Reduce position size, tighten stops",
        },
        ("volatile", "ranging"): {
            "significance": "medium",
            "description": "Volatility subsiding into consolidation",
            "action_hint": "Start looking for new setups",
        },
        ("trending_up", "ranging"): {
            "significance": "medium",
            "description": "Uptrend stalling into consolidation",
            "action_hint": "Take partial profits, move stops to breakeven",
        },
        ("trending_down", "ranging"): {
            "significance": "medium",
            "description": "Downtrend stalling into consolidation",
            "action_hint": "Take partial profits, move stops to breakeven",
        },
    }

    transition_key = (previous_regime, current_regime)
    transition_info = significant_transitions.get(transition_key)

    if transition_info:
        return {
            "from_regime": previous_regime,
            "to_regime": current_regime,
            "significance": transition_info["significance"],
            "description": transition_info["description"],
            "action_hint": transition_info["action_hint"],
        }

    # Default for any other transition
    return {
        "from_regime": previous_regime,
        "to_regime": current_regime,
        "significance": "low",
        "description": f"Market regime changed from {previous_regime} to {current_regime}",
        "action_hint": "Monitor for further developments",
    }
