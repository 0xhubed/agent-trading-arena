"""Shared config parsing utilities for YAML competition configs."""

from decimal import Decimal

from agent_arena.core.config import CandleConfig, ConstraintsConfig, FeeConfig


def parse_fees_config(config_data: dict) -> FeeConfig:
    """Parse fee configuration from YAML."""
    fees_data = config_data.get("fees", {})
    return FeeConfig(
        taker_fee=Decimal(str(fees_data.get("taker_fee", "0.0004"))),
        maker_fee=Decimal(str(fees_data.get("maker_fee", "0.0002"))),
        liquidation_fee=Decimal(str(fees_data.get("liquidation_fee", "0.005"))),
    )


def parse_constraints_config(config_data: dict) -> ConstraintsConfig:
    """Parse constraints configuration from YAML."""
    constraints_data = config_data.get("constraints", {})
    return ConstraintsConfig(
        max_leverage=constraints_data.get("max_leverage", 10),
        max_position_pct=Decimal(str(constraints_data.get("max_position_pct", "0.25"))),
        starting_capital=Decimal(str(constraints_data.get("starting_capital", "10000"))),
        max_trades_per_window=constraints_data.get("max_trades_per_window", 8),
        trade_window_ticks=constraints_data.get("trade_window_ticks", 20),
    )


def parse_candle_config(config_data: dict) -> CandleConfig:
    """Parse candle configuration from YAML."""
    candles_data = config_data.get("candles", {})
    return CandleConfig(
        enabled=candles_data.get("enabled", True),
        intervals=candles_data.get("intervals", ["1h", "15m"]),
        limit=candles_data.get("limit", 100),
    )
