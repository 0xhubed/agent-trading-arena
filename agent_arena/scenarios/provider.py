"""Scenario provider — replays JSON scenario files tick-by-tick."""

from __future__ import annotations

import bisect
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

from agent_arena.providers.base import DataProvider
from agent_arena.providers.historical import INTERVAL_MS
from agent_arena.scenarios.models import Scenario

logger = logging.getLogger(__name__)

BASE_DIR = Path("data/scenarios")


class ScenarioProvider(DataProvider):
    """
    Replay a saved JSON scenario tick-by-tick.

    Same interface as HistoricalProvider but reads from
    memory-loaded JSON instead of a database.
    """

    def __init__(
        self,
        scenario_id: str,
        candle_limit: int = 100,
        base_dir: str | Path = BASE_DIR,
    ):
        self._base_dir = Path(base_dir)
        self._scenario_id = scenario_id
        self._candle_limit = candle_limit

        # Loaded on start()
        self._scenario: Optional[Scenario] = None
        # {symbol/interval: [(timestamp_ms, candle_dict), ...]} sorted by timestamp
        self._candles: dict[str, list[tuple[int, dict]]] = {}
        # Just the timestamps per key, for bisect
        self._timestamps: dict[str, list[int]] = {}

        self._current_time_ms: int = 0
        self._end_time_ms: int = 0
        self._tick_interval_ms: int = 0
        self._tick_count: int = 0

    @property
    def name(self) -> str:
        return f"scenario:{self._scenario_id}"

    @property
    def scenario(self) -> Optional[Scenario]:
        return self._scenario

    @property
    def current_timestamp(self) -> datetime:
        return datetime.fromtimestamp(self._current_time_ms / 1000, tz=timezone.utc)

    @property
    def current_timestamp_ms(self) -> int:
        return self._current_time_ms

    @property
    def progress(self) -> float:
        start = self._current_time_ms - self._tick_count * self._tick_interval_ms
        total = self._end_time_ms - start
        current = self._current_time_ms - start
        return (current / total) * 100 if total > 0 else 0

    @property
    def total_ticks(self) -> int:
        if self._scenario:
            return self._scenario.total_ticks
        return 0

    @property
    def is_finished(self) -> bool:
        return self._current_time_ms >= self._end_time_ms

    async def start(self) -> None:
        """Load scenario data into memory."""
        scenario_dir = self._base_dir / self._scenario_id
        meta_path = scenario_dir / "metadata.json"
        candles_path = scenario_dir / "candles.json"

        if not meta_path.exists():
            raise FileNotFoundError(f"Scenario '{self._scenario_id}' not found")

        self._scenario = Scenario.from_json(meta_path.read_text(encoding="utf-8"))

        # Parse dates
        start_dt = datetime.strptime(self._scenario.start_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        end_dt = datetime.strptime(self._scenario.end_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        self._current_time_ms = int(start_dt.timestamp() * 1000)
        self._end_time_ms = int(end_dt.timestamp() * 1000)
        self._tick_interval_ms = INTERVAL_MS.get(self._scenario.interval, 3600000)
        self._tick_count = 0

        # Load candles
        raw = json.loads(candles_path.read_text(encoding="utf-8"))
        for key, candle_list in raw.items():
            entries = []
            ts_list = []
            for c in candle_list:
                ts = c["timestamp"]
                # Convert string values back to Decimal
                parsed = {
                    "timestamp": ts,
                    "open": Decimal(str(c["open"])),
                    "high": Decimal(str(c["high"])),
                    "low": Decimal(str(c["low"])),
                    "close": Decimal(str(c["close"])),
                    "volume": Decimal(str(c["volume"])),
                }
                # Preserve optional fields
                if "close_time" in c:
                    parsed["close_time"] = c["close_time"]
                if "quote_volume" in c:
                    parsed["quote_volume"] = Decimal(str(c["quote_volume"]))
                if "trades" in c:
                    parsed["trades"] = c["trades"]

                entries.append((ts, parsed))
                ts_list.append(ts)

            self._candles[key] = entries
            self._timestamps[key] = ts_list

        total_candles = sum(len(v) for v in self._candles.values())
        logger.info(
            f"Loaded scenario '{self._scenario_id}': "
            f"{len(self._candles)} series, {total_candles} candles"
        )

    async def stop(self) -> None:
        self._candles.clear()
        self._timestamps.clear()
        self._scenario = None

    def reset(self) -> None:
        if self._scenario:
            start_dt = datetime.strptime(
                self._scenario.start_date, "%Y-%m-%d"
            ).replace(tzinfo=timezone.utc)
            self._current_time_ms = int(start_dt.timestamp() * 1000)
            self._tick_count = 0

    def advance_tick(self) -> bool:
        if self.is_finished:
            return False
        self._current_time_ms += self._tick_interval_ms
        self._tick_count += 1
        return not self.is_finished

    async def get_data(self, symbols: list[str]) -> dict:
        """Get market data for current tick."""
        market = {}
        for symbol in symbols:
            key = f"{symbol}/{self._scenario.interval}" if self._scenario else None
            if not key or key not in self._candles:
                continue

            candle = self._get_candle_at(key, self._current_time_ms)
            if candle:
                change_24h = self._calc_24h_change(key, candle["close"])
                market[symbol] = {
                    "price": candle["close"],
                    "change_24h": change_24h,
                    "volume_24h": candle["volume"],
                    "high_24h": candle["high"],
                    "low_24h": candle["low"],
                    "funding_rate": Decimal("0.0001"),
                }

        return {"market": market}

    async def get_candles(
        self,
        symbol: str,
        interval: str = "1h",
        limit: int = 100,
    ) -> list[dict]:
        """Get candles up to current time."""
        key = f"{symbol}/{interval}"
        return self._get_candles_up_to(key, self._current_time_ms, limit)

    async def get_candles_multi(
        self,
        symbols: list[str],
        intervals: list[str] | None = None,
        limit: int = 100,
    ) -> dict[str, dict[str, list[dict]]]:
        """Get candles for multiple symbols and intervals."""
        if intervals is None and self._scenario:
            intervals = self._scenario.candle_intervals
        elif intervals is None:
            intervals = []

        result: dict[str, dict[str, list[dict]]] = {s: {} for s in symbols}
        for symbol in symbols:
            for interval in intervals:
                result[symbol][interval] = await self.get_candles(
                    symbol, interval, limit
                )
        return result

    def _get_candle_at(self, key: str, time_ms: int) -> Optional[dict]:
        """Get the candle at or just before the given time."""
        ts_list = self._timestamps.get(key)
        if not ts_list:
            return None

        idx = bisect.bisect_right(ts_list, time_ms) - 1
        if idx < 0:
            return None

        return self._candles[key][idx][1]

    def _get_candles_up_to(
        self, key: str, time_ms: int, limit: int
    ) -> list[dict]:
        """Get up to `limit` candles ending at or before time_ms."""
        ts_list = self._timestamps.get(key)
        if not ts_list:
            return []

        end_idx = bisect.bisect_right(ts_list, time_ms)
        start_idx = max(0, end_idx - limit)

        return [self._candles[key][i][1] for i in range(start_idx, end_idx)]

    def _calc_24h_change(self, key: str, current_price: Decimal) -> float:
        """Calculate 24h price change percentage."""
        time_24h_ago = self._current_time_ms - (24 * 60 * 60 * 1000)
        candle = self._get_candle_at(key, time_24h_ago)
        if candle and candle["close"] > 0:
            change = ((current_price - candle["close"]) / candle["close"]) * 100
            return float(change)
        return 0.0

    def get_status(self) -> dict:
        return {
            "scenario_id": self._scenario_id,
            "scenario_name": self._scenario.name if self._scenario else None,
            "current_time": self.current_timestamp.isoformat(),
            "tick_interval": self._scenario.interval if self._scenario else None,
            "tick_count": self._tick_count,
            "total_ticks": self.total_ticks,
            "progress_pct": round(self.progress, 2),
            "is_finished": self.is_finished,
            "symbols": self._scenario.symbols if self._scenario else [],
        }
