"""Historical data provider for backtesting."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from agent_arena.providers.base import DataProvider

logger = logging.getLogger(__name__)


def parse_date(date_str: str) -> datetime:
    """Parse date string in YYYY-MM-DD format."""
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def date_to_ms(dt: datetime) -> int:
    """Convert datetime to milliseconds timestamp."""
    return int(dt.timestamp() * 1000)


def ms_to_datetime(ms: int) -> datetime:
    """Convert milliseconds timestamp to datetime."""
    return datetime.utcfromtimestamp(ms / 1000).replace(tzinfo=timezone.utc)


# Interval to milliseconds mapping
INTERVAL_MS = {
    "1m": 60 * 1000,
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "12h": 12 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
    "1w": 7 * 24 * 60 * 60 * 1000,
}


class HistoricalProvider(DataProvider):
    """
    Replay historical candle data from database as if live.

    Supports:
    - Reading from SQLite/PostgreSQL candles table
    - Variable replay speed
    - Jump to specific timestamps
    - Automatic data validation (warns if gaps exist)
    """

    def __init__(
        self,
        storage,
        start_date: str,
        end_date: str,
        symbols: list[str],
        tick_interval: str = "1h",
        candle_intervals: list[str] = None,
        candle_limit: int = 100,
    ):
        """
        Initialize historical data provider.

        Args:
            storage: Storage instance with database connection
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            symbols: List of trading pairs
            tick_interval: Interval between ticks (e.g., "1h", "4h")
            candle_intervals: Intervals to include in context (default: [tick_interval])
            candle_limit: Number of historical candles to include in context
        """
        self._storage = storage
        self._candle_storage: Optional[CandleStorage] = None

        self._start_date = parse_date(start_date)
        self._end_date = parse_date(end_date)
        self._symbols = symbols
        self._tick_interval = tick_interval
        self._tick_interval_ms = INTERVAL_MS.get(tick_interval, 3600000)
        self._candle_intervals = candle_intervals or [tick_interval]
        self._candle_limit = candle_limit

        # Current replay position
        self._current_time_ms = date_to_ms(self._start_date)
        self._end_time_ms = date_to_ms(self._end_date)
        self._tick_count = 0

        # Preloaded data for faster access
        self._price_data: dict[str, list[dict]] = {}
        self._data_validated = False

        # Will be set to storage instance that has candle methods
        self._candle_storage = None

    @property
    def name(self) -> str:
        return "historical"

    @property
    def current_timestamp(self) -> datetime:
        """Get current replay timestamp."""
        return ms_to_datetime(self._current_time_ms)

    @property
    def current_timestamp_ms(self) -> int:
        """Get current replay timestamp in milliseconds."""
        return self._current_time_ms

    @property
    def progress(self) -> float:
        """Get replay progress as percentage (0-100)."""
        total = self._end_time_ms - date_to_ms(self._start_date)
        current = self._current_time_ms - date_to_ms(self._start_date)
        return (current / total) * 100 if total > 0 else 0

    @property
    def total_ticks(self) -> int:
        """Get total number of ticks in the replay."""
        total_ms = self._end_time_ms - date_to_ms(self._start_date)
        return total_ms // self._tick_interval_ms

    @property
    def is_finished(self) -> bool:
        """Check if replay has reached the end."""
        return self._current_time_ms >= self._end_time_ms

    async def start(self) -> None:
        """Initialize and validate data availability."""
        # Check if storage has candle methods (PostgresStorage) or needs wrapper (SQLiteStorage)
        if hasattr(self._storage, "get_candles_at_time"):
            # PostgresStorage has candle methods directly
            self._candle_storage = self._storage
        elif hasattr(self._storage, "_connection"):
            # SQLiteStorage needs CandleStorage wrapper
            from agent_arena.storage.candles import CandleStorage
            self._candle_storage = CandleStorage(self._storage._connection)

        # Validate data availability
        if self._candle_storage:
            await self._validate_data()

    async def stop(self) -> None:
        """Cleanup resources."""
        self._candle_storage = None
        self._price_data.clear()

    async def _validate_data(self) -> None:
        """Validate that required data exists in the database."""
        if not self._candle_storage:
            return

        missing = []
        for symbol in self._symbols:
            for interval in self._candle_intervals:
                start, end, count = await self._candle_storage.get_data_range(
                    symbol, interval
                )

                if count == 0:
                    missing.append(f"{symbol}/{interval}: No data")
                    continue

                # Check if data covers our range
                start_needed = date_to_ms(self._start_date)
                end_needed = date_to_ms(self._end_date)

                if start and start > start_needed:
                    gap_days = (start - start_needed) / (24 * 60 * 60 * 1000)
                    missing.append(
                        f"{symbol}/{interval}: Missing {gap_days:.1f} days at start"
                    )

                if end and end < end_needed:
                    gap_days = (end_needed - end) / (24 * 60 * 60 * 1000)
                    missing.append(
                        f"{symbol}/{interval}: Missing {gap_days:.1f} days at end"
                    )

        if missing:
            logger.warning("Historical data gaps detected:")
            for msg in missing:
                logger.warning(f"  - {msg}")

        self._data_validated = True

    def reset(self) -> None:
        """Reset replay to start."""
        self._current_time_ms = date_to_ms(self._start_date)
        self._tick_count = 0

    def jump_to(self, timestamp: datetime) -> None:
        """Jump replay to specific point in time."""
        target_ms = date_to_ms(timestamp)
        if target_ms < date_to_ms(self._start_date):
            target_ms = date_to_ms(self._start_date)
        elif target_ms > self._end_time_ms:
            target_ms = self._end_time_ms

        self._current_time_ms = target_ms

    def advance_tick(self) -> bool:
        """
        Advance to next tick.

        Returns:
            True if successfully advanced, False if reached end
        """
        if self.is_finished:
            return False

        self._current_time_ms += self._tick_interval_ms
        self._tick_count += 1
        return not self.is_finished

    async def get_data(self, symbols: list[str]) -> dict:
        """
        Get market data for current replay timestamp.

        Returns dict to merge into agent context with:
        - market: Current prices and 24h stats
        - funding_rate: Current funding rate (simulated)
        """
        if not self._candle_storage:
            return {"market": {}}

        market = {}
        for symbol in symbols:
            # Get the candle at current time for price
            candle = await self._get_price_candle(symbol)
            if candle:
                # Calculate 24h change from candles
                change_24h = await self._calculate_24h_change(symbol, candle["close"])

                market[symbol] = {
                    "price": candle["close"],
                    "change_24h": change_24h,
                    "volume_24h": candle["volume"],
                    "high_24h": candle["high"],
                    "low_24h": candle["low"],
                    "funding_rate": Decimal("0.0001"),  # Simulated funding rate
                }

        return {"market": market}

    async def _get_price_candle(self, symbol: str) -> Optional[dict]:
        """Get the candle at or just before current time."""
        if not self._candle_storage:
            return None

        # Use the tick interval for price data
        candles = await self._candle_storage.get_candles_at_time(
            symbol,
            self._tick_interval,
            self._current_time_ms,
            limit=1,
        )

        if candles:
            return candles[-1]
        return None

    async def _calculate_24h_change(
        self, symbol: str, current_price: Decimal
    ) -> float:
        """Calculate 24h price change percentage."""
        if not self._candle_storage:
            return 0.0

        # Get price from 24 hours ago
        time_24h_ago = self._current_time_ms - (24 * 60 * 60 * 1000)

        candles = await self._candle_storage.get_candles_at_time(
            symbol,
            self._tick_interval,
            time_24h_ago,
            limit=1,
        )

        if candles:
            old_price = candles[-1]["close"]
            if old_price > 0:
                change = ((current_price - old_price) / old_price) * 100
                return float(change)

        return 0.0

    async def get_candles(
        self,
        symbol: str,
        interval: str = "1h",
        limit: int = 100,
    ) -> list[dict]:
        """
        Get historical candles up to current replay time.

        Args:
            symbol: Trading pair
            interval: Candle interval
            limit: Number of candles to return

        Returns:
            List of candles ending at or before current time
        """
        if not self._candle_storage:
            return []

        return await self._candle_storage.get_candles_at_time(
            symbol,
            interval,
            self._current_time_ms,
            limit,
        )

    async def get_candles_multi(
        self,
        symbols: list[str],
        intervals: list[str] = None,
        limit: int = 100,
    ) -> dict[str, dict[str, list[dict]]]:
        """
        Get candles for multiple symbols and intervals.

        Returns:
            Dict mapping symbol -> interval -> candles list
        """
        if intervals is None:
            intervals = self._candle_intervals

        result: dict[str, dict[str, list[dict]]] = {s: {} for s in symbols}

        for symbol in symbols:
            for interval in intervals:
                candles = await self.get_candles(symbol, interval, limit)
                result[symbol][interval] = candles

        return result

    async def get_price_at_time(
        self, symbol: str, timestamp_ms: int
    ) -> Optional[Decimal]:
        """Get price at a specific timestamp."""
        if not self._candle_storage:
            return None

        candles = await self._candle_storage.get_candles_at_time(
            symbol,
            self._tick_interval,
            timestamp_ms,
            limit=1,
        )

        if candles:
            return candles[-1]["close"]
        return None

    def get_status(self) -> dict:
        """Get current replay status."""
        return {
            "start_date": self._start_date.isoformat(),
            "end_date": self._end_date.isoformat(),
            "current_time": self.current_timestamp.isoformat(),
            "tick_interval": self._tick_interval,
            "tick_count": self._tick_count,
            "total_ticks": self.total_ticks,
            "progress_pct": round(self.progress, 2),
            "is_finished": self.is_finished,
            "symbols": self._symbols,
            "data_validated": self._data_validated,
        }
