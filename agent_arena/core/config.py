"""Competition configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional


@dataclass
class FeeConfig:
    """Fee configuration for trading."""

    taker_fee: Decimal = Decimal("0.0004")  # 0.04% default
    maker_fee: Decimal = Decimal("0.0002")  # 0.02% for limit orders
    liquidation_fee: Decimal = Decimal("0.005")  # 0.5% default


@dataclass
class ConstraintsConfig:
    """Trading constraints configuration."""

    max_leverage: int = 10
    max_position_pct: Decimal = Decimal("0.25")  # Max 25% of equity per position
    starting_capital: Decimal = Decimal("10000")
    max_trades_per_window: int = 8  # Max non-hold decisions per rolling window
    trade_window_ticks: int = 20  # Rolling window size in ticks (~5h at 15min)


@dataclass
class CandleConfig:
    """Historical candle data configuration."""

    enabled: bool = True  # Whether to fetch candles
    intervals: list[str] = field(default_factory=lambda: ["1h", "15m"])
    limit: int = 100  # Number of candles per interval


@dataclass
class CompetitionConfig:
    """Competition settings."""

    name: str
    symbols: list[str] = field(default_factory=lambda: ["PF_XBTUSD", "PF_ETHUSD"])
    interval_seconds: int = 1800  # 30 minutes default
    duration_seconds: Optional[int] = None  # None = run until stopped
    initial_capital: Decimal = Decimal("10000")
    agent_timeout_seconds: float = 60.0  # Timeout for agent decisions
    fees: FeeConfig = field(default_factory=FeeConfig)
    constraints: ConstraintsConfig = field(default_factory=ConstraintsConfig)
    candles: CandleConfig = field(default_factory=CandleConfig)
    raw_config: dict = field(default_factory=dict)  # Full YAML config for extensions (M3 forum)
