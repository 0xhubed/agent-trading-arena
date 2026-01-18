"""Technical indicator calculations for learning agents."""

from __future__ import annotations

from typing import Optional


def calculate_rsi(closes: list[float], period: int = 14) -> Optional[float]:
    """
    Calculate Relative Strength Index (RSI).

    Args:
        closes: List of closing prices (oldest to newest).
        period: RSI period (default: 14).

    Returns:
        RSI value (0-100) or None if insufficient data.
    """
    if len(closes) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]
        gains.append(max(0, change))
        losses.append(max(0, -change))

    # Use Wilder's smoothing method
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_sma(values: list[float], period: int) -> Optional[float]:
    """
    Calculate Simple Moving Average.

    Args:
        values: List of values (oldest to newest).
        period: SMA period.

    Returns:
        SMA value or None if insufficient data.
    """
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def calculate_ema(values: list[float], period: int) -> Optional[float]:
    """
    Calculate Exponential Moving Average.

    Args:
        values: List of values (oldest to newest).
        period: EMA period.

    Returns:
        EMA value or None if insufficient data.
    """
    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    # Initialize EMA with SMA
    ema = sum(values[:period]) / period

    # Calculate EMA for remaining values
    for value in values[period:]:
        ema = (value * multiplier) + (ema * (1 - multiplier))

    return ema


def calculate_macd(
    closes: list[float],
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> Optional[dict]:
    """
    Calculate MACD (Moving Average Convergence Divergence).

    Args:
        closes: List of closing prices (oldest to newest).
        fast: Fast EMA period (default: 12).
        slow: Slow EMA period (default: 26).
        signal: Signal line period (default: 9).

    Returns:
        Dict with 'macd', 'signal', 'histogram' or None if insufficient data.
    """
    if len(closes) < slow + signal:
        return None

    # Calculate EMAs for all points to build MACD history
    macd_values = []

    for i in range(slow, len(closes) + 1):
        subset = closes[:i]
        ema_fast = calculate_ema(subset, fast)
        ema_slow = calculate_ema(subset, slow)
        if ema_fast is not None and ema_slow is not None:
            macd_values.append(ema_fast - ema_slow)

    if len(macd_values) < signal:
        return None

    macd_line = macd_values[-1]
    signal_line = calculate_ema(macd_values, signal)

    if signal_line is None:
        return None

    return {
        "macd": macd_line,
        "signal": signal_line,
        "histogram": macd_line - signal_line,
    }


def calculate_bollinger_bands(
    closes: list[float],
    period: int = 20,
    std_dev: float = 2.0,
) -> Optional[dict]:
    """
    Calculate Bollinger Bands.

    Args:
        closes: List of closing prices (oldest to newest).
        period: SMA period (default: 20).
        std_dev: Standard deviation multiplier (default: 2.0).

    Returns:
        Dict with 'upper', 'middle', 'lower', 'bandwidth', 'percent_b'
        or None if insufficient data.
    """
    if len(closes) < period:
        return None

    sma = calculate_sma(closes, period)
    if sma is None:
        return None

    # Calculate standard deviation
    variance = sum((c - sma) ** 2 for c in closes[-period:]) / period
    std = variance ** 0.5

    upper = sma + (std * std_dev)
    lower = sma - (std * std_dev)
    current_price = closes[-1]

    # Bandwidth: (Upper - Lower) / Middle
    bandwidth = (upper - lower) / sma if sma > 0 else 0

    # Percent B: (Price - Lower) / (Upper - Lower)
    band_width = upper - lower
    percent_b = (current_price - lower) / band_width if band_width > 0 else 0.5

    return {
        "upper": upper,
        "middle": sma,
        "lower": lower,
        "bandwidth": bandwidth,
        "percent_b": percent_b,
    }


def calculate_atr(candles: list[dict], period: int = 14) -> Optional[float]:
    """
    Calculate Average True Range (ATR).

    Args:
        candles: List of candle dicts with 'high', 'low', 'close' keys.
        period: ATR period (default: 14).

    Returns:
        ATR value or None if insufficient data.
    """
    if len(candles) < period + 1:
        return None

    true_ranges = []
    for i in range(1, len(candles)):
        high = float(candles[i]["high"])
        low = float(candles[i]["low"])
        prev_close = float(candles[i - 1]["close"])

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        true_ranges.append(tr)

    # Use Wilder's smoothing for ATR
    atr = sum(true_ranges[:period]) / period
    for tr in true_ranges[period:]:
        atr = (atr * (period - 1) + tr) / period

    return atr


def calculate_stochastic(
    candles: list[dict],
    k_period: int = 14,
    d_period: int = 3,
) -> Optional[dict]:
    """
    Calculate Stochastic Oscillator.

    Args:
        candles: List of candle dicts with 'high', 'low', 'close' keys.
        k_period: %K period (default: 14).
        d_period: %D smoothing period (default: 3).

    Returns:
        Dict with 'k' and 'd' values or None if insufficient data.
    """
    if len(candles) < k_period + d_period:
        return None

    k_values = []

    for i in range(k_period - 1, len(candles)):
        window = candles[i - k_period + 1:i + 1]
        highest_high = max(float(c["high"]) for c in window)
        lowest_low = min(float(c["low"]) for c in window)
        current_close = float(candles[i]["close"])

        if highest_high == lowest_low:
            k = 50.0
        else:
            k = ((current_close - lowest_low) / (highest_high - lowest_low)) * 100

        k_values.append(k)

    if len(k_values) < d_period:
        return None

    # %D is SMA of %K
    d = sum(k_values[-d_period:]) / d_period

    return {
        "k": k_values[-1],
        "d": d,
    }


def calculate_obv(candles: list[dict]) -> Optional[float]:
    """
    Calculate On-Balance Volume (OBV).

    Args:
        candles: List of candle dicts with 'close' and 'volume' keys.

    Returns:
        OBV value or None if insufficient data.
    """
    if len(candles) < 2:
        return None

    obv = 0
    for i in range(1, len(candles)):
        current_close = float(candles[i]["close"])
        prev_close = float(candles[i - 1]["close"])
        volume = float(candles[i]["volume"])

        if current_close > prev_close:
            obv += volume
        elif current_close < prev_close:
            obv -= volume
        # If equal, OBV unchanged

    return obv


def calculate_vwap(candles: list[dict]) -> Optional[float]:
    """
    Calculate Volume Weighted Average Price (VWAP).

    Args:
        candles: List of candle dicts with 'high', 'low', 'close', 'volume' keys.

    Returns:
        VWAP value or None if insufficient data.
    """
    if not candles:
        return None

    cumulative_tp_volume = 0
    cumulative_volume = 0

    for candle in candles:
        high = float(candle["high"])
        low = float(candle["low"])
        close = float(candle["close"])
        volume = float(candle["volume"])

        typical_price = (high + low + close) / 3
        cumulative_tp_volume += typical_price * volume
        cumulative_volume += volume

    if cumulative_volume == 0:
        return None

    return cumulative_tp_volume / cumulative_volume


def calculate_adx(candles: list[dict], period: int = 14) -> Optional[dict]:
    """
    Calculate Average Directional Index (ADX).

    Args:
        candles: List of candle dicts with 'high', 'low', 'close' keys.
        period: ADX period (default: 14).

    Returns:
        Dict with 'adx', 'plus_di', 'minus_di' or None if insufficient data.
    """
    if len(candles) < period * 2:
        return None

    plus_dm_list = []
    minus_dm_list = []
    tr_list = []

    for i in range(1, len(candles)):
        high = float(candles[i]["high"])
        low = float(candles[i]["low"])
        prev_high = float(candles[i - 1]["high"])
        prev_low = float(candles[i - 1]["low"])
        prev_close = float(candles[i - 1]["close"])

        # True Range
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        tr_list.append(tr)

        # Directional Movement
        up_move = high - prev_high
        down_move = prev_low - low

        plus_dm = up_move if up_move > down_move and up_move > 0 else 0
        minus_dm = down_move if down_move > up_move and down_move > 0 else 0

        plus_dm_list.append(plus_dm)
        minus_dm_list.append(minus_dm)

    # Smooth the values using Wilder's method
    def wilder_smooth(values: list, period: int) -> list:
        smoothed = [sum(values[:period])]
        for v in values[period:]:
            smoothed.append(smoothed[-1] - (smoothed[-1] / period) + v)
        return smoothed

    smoothed_tr = wilder_smooth(tr_list, period)
    smoothed_plus_dm = wilder_smooth(plus_dm_list, period)
    smoothed_minus_dm = wilder_smooth(minus_dm_list, period)

    if not smoothed_tr or smoothed_tr[-1] == 0:
        return None

    # Calculate +DI and -DI
    plus_di = (smoothed_plus_dm[-1] / smoothed_tr[-1]) * 100
    minus_di = (smoothed_minus_dm[-1] / smoothed_tr[-1]) * 100

    # Calculate DX values
    dx_list = []
    for i in range(len(smoothed_tr)):
        if smoothed_tr[i] == 0:
            continue
        pdi = (smoothed_plus_dm[i] / smoothed_tr[i]) * 100
        mdi = (smoothed_minus_dm[i] / smoothed_tr[i]) * 100
        if pdi + mdi == 0:
            dx_list.append(0)
        else:
            dx_list.append(abs(pdi - mdi) / (pdi + mdi) * 100)

    if len(dx_list) < period:
        return None

    # ADX is smoothed DX
    adx = sum(dx_list[-period:]) / period

    return {
        "adx": adx,
        "plus_di": plus_di,
        "minus_di": minus_di,
    }


def compute_all_indicators(candles: list[dict]) -> dict:
    """
    Compute all available indicators for a symbol.

    Args:
        candles: List of candle dicts with OHLCV data.

    Returns:
        Dict with all computed indicator values.
    """
    if not candles:
        return {}

    closes = [float(c["close"]) for c in candles]
    current_price = closes[-1] if closes else None

    result = {
        "current_price": current_price,
        "rsi_14": calculate_rsi(closes, 14),
        "sma_20": calculate_sma(closes, 20),
        "sma_50": calculate_sma(closes, 50),
        "sma_200": calculate_sma(closes, 200),
        "ema_12": calculate_ema(closes, 12),
        "ema_26": calculate_ema(closes, 26),
        "macd": calculate_macd(closes),
        "bollinger": calculate_bollinger_bands(closes),
        "atr_14": calculate_atr(candles, 14),
        "stochastic": calculate_stochastic(candles),
        "obv": calculate_obv(candles),
        "vwap": calculate_vwap(candles),
        "adx": calculate_adx(candles),
    }

    # Add derived metrics
    sma_20 = result["sma_20"]
    if sma_20 and current_price:
        result["price_vs_sma20"] = ((current_price / sma_20) - 1) * 100

    sma_50 = result["sma_50"]
    if sma_50 and current_price:
        result["price_vs_sma50"] = ((current_price / sma_50) - 1) * 100

    # Trend strength based on moving average alignment
    if sma_20 and sma_50:
        result["ma_trend"] = "bullish" if sma_20 > sma_50 else "bearish"

    # RSI classification
    rsi = result["rsi_14"]
    if rsi:
        if rsi < 30:
            result["rsi_signal"] = "oversold"
        elif rsi > 70:
            result["rsi_signal"] = "overbought"
        else:
            result["rsi_signal"] = "neutral"

    return result
