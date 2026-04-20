"""Scenario curator — fetches Kraken Futures data and saves as replayable JSON files."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import httpx

from agent_arena.data.fetch_historical import fetch_symbol_interval
from agent_arena.providers.historical import INTERVAL_MS
from agent_arena.scenarios.models import Scenario

logger = logging.getLogger(__name__)

BASE_DIR = Path("data/scenarios")


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that converts Decimals to strings."""

    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super().default(o)


class ScenarioCurator:
    """Fetches market data from Kraken Futures and saves as deterministic JSON scenarios."""

    def __init__(self, base_dir: str | Path = BASE_DIR):
        self.base_dir = Path(base_dir)

    async def curate(
        self,
        scenario_id: str,
        name: str,
        description: str,
        start_date: str,
        end_date: str,
        symbols: list[str],
        interval: str = "5m",
        candle_intervals: list[str] | None = None,
        progress_callback=None,
    ) -> Scenario:
        """
        Fetch candle data from Kraken Futures and save as a scenario.

        Args:
            scenario_id: Unique identifier (used as directory name)
            name: Human-readable name
            description: What this scenario tests
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD
            symbols: Trading pairs to fetch
            interval: Tick interval
            candle_intervals: Additional candle intervals to fetch (defaults to [interval])
            progress_callback: Optional callback(symbol, interval, fetched, total)

        Returns:
            Scenario metadata dataclass
        """
        if candle_intervals is None:
            candle_intervals = [interval]

        # Ensure tick interval is included
        if interval not in candle_intervals:
            candle_intervals = [interval] + candle_intervals

        scenario_dir = self.base_dir / scenario_id
        scenario_dir.mkdir(parents=True, exist_ok=True)

        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)

        # Fetch all candles
        all_candles: dict[str, list[dict]] = {}

        async with httpx.AsyncClient(timeout=30.0) as client:
            for symbol in symbols:
                for ci in candle_intervals:
                    key = f"{symbol}/{ci}"
                    logger.info(f"Fetching {key} ({start_date} to {end_date})")

                    candles = await fetch_symbol_interval(
                        client,
                        symbol,
                        ci,
                        start_ms,
                        end_ms,
                        progress_callback=progress_callback,
                    )

                    # Convert Decimals to strings for JSON safety
                    serializable = []
                    for c in candles:
                        serializable.append(
                            {
                                k: str(v) if isinstance(v, Decimal) else v
                                for k, v in c.items()
                            }
                        )
                    all_candles[key] = serializable
                    logger.info(f"  {key}: {len(candles)} candles")

        # Write candles.json
        candles_path = scenario_dir / "candles.json"
        candles_json = json.dumps(all_candles, cls=DecimalEncoder, separators=(",", ":"))
        candles_path.write_text(candles_json, encoding="utf-8")

        # Compute checksum
        checksum = hashlib.sha256(candles_json.encode("utf-8")).hexdigest()

        # Compute total ticks
        tick_ms = INTERVAL_MS.get(interval, 3600000)
        total_ticks = (end_ms - start_ms) // tick_ms

        # Build scenario metadata
        scenario = Scenario(
            scenario_id=scenario_id,
            name=name,
            description=description,
            symbols=symbols,
            interval=interval,
            candle_intervals=candle_intervals,
            start_date=start_date,
            end_date=end_date,
            total_ticks=total_ticks,
            checksum=checksum,
        )

        # Write metadata.json
        metadata_path = scenario_dir / "metadata.json"
        metadata_path.write_text(scenario.to_json(), encoding="utf-8")

        logger.info(
            f"Scenario '{scenario_id}' saved: {len(all_candles)} series, "
            f"{total_ticks} ticks, checksum={checksum[:12]}..."
        )

        return scenario
