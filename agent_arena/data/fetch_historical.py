"""Fetch historical data from Kraken Futures and store in database."""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Optional

import httpx

from agent_arena.providers.historical import INTERVAL_MS, date_to_ms, parse_date

logger = logging.getLogger(__name__)

KRAKEN_CHARTS_URL = "https://futures.kraken.com/api/charts/v1/trade"
MAX_CANDLES_PER_REQUEST = 5000

# Kraken public limit is ~15 req/s; 200ms is safely under.
REQUEST_DELAY_SECONDS = 0.2
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5.0

# Kraken-supported intervals are exactly the keys of INTERVAL_MS; resolution
# strings are identical to the interval strings, so no mapping dict is needed.
_SUPPORTED_INTERVALS = frozenset(INTERVAL_MS)


def estimate_data_size(
    symbols: list[str],
    start_date: str,
    end_date: str,
    intervals: list[str],
) -> dict:
    """Estimate how much data will be fetched."""
    start = parse_date(start_date)
    end = parse_date(end_date)
    duration_ms = date_to_ms(end) - date_to_ms(start)

    estimates: dict = {}
    total_candles = 0
    for symbol in symbols:
        estimates[symbol] = {}
        for interval in intervals:
            interval_ms = INTERVAL_MS.get(interval)
            if interval_ms:
                candle_count = duration_ms // interval_ms
                estimates[symbol][interval] = candle_count
                total_candles += candle_count

    estimated_size_mb = (total_candles * 200) / (1024 * 1024)
    return {
        "symbols": symbols,
        "intervals": intervals,
        "start_date": start_date,
        "end_date": end_date,
        "estimates": estimates,
        "total_candles": total_candles,
        "estimated_size_mb": round(estimated_size_mb, 2),
    }


async def fetch_candles_from_kraken(
    client: httpx.AsyncClient,
    symbol: str,
    interval: str,
    start_time_ms: int,
    end_time_ms: int,
) -> list[dict]:
    """Fetch candles from Kraken Futures charts API with retry logic.

    Kraken's from/to params are Unix seconds; the response `time` field is
    already in milliseconds.
    """
    if interval not in _SUPPORTED_INTERVALS:
        raise ValueError(
            f"Interval {interval!r} not supported by Kraken. "
            f"Supported: {sorted(_SUPPORTED_INTERVALS)}"
        )
    url = f"{KRAKEN_CHARTS_URL}/{symbol}/{interval}"
    params = {
        "from": start_time_ms // 1000,
        "to": end_time_ms // 1000,
    }

    last_err: Optional[Exception] = None
    for attempt in range(MAX_RETRIES):
        try:
            response = await client.get(url, params=params, timeout=30.0)
            if response.status_code == 429:
                logger.warning("Kraken rate-limited; backing off")
                await asyncio.sleep(RETRY_DELAY_SECONDS)
                continue
            response.raise_for_status()
            data = response.json()
            candles: list[dict] = []
            for c in data.get("candles", []):
                candles.append({
                    "timestamp": int(c["time"]),
                    "open": Decimal(str(c["open"])),
                    "high": Decimal(str(c["high"])),
                    "low": Decimal(str(c["low"])),
                    "close": Decimal(str(c["close"])),
                    "volume": Decimal(str(c["volume"])),
                })
            return candles
        except httpx.HTTPError as exc:
            last_err = exc
            logger.warning(
                "Kraken fetch attempt %d/%d failed for %s/%s: %s",
                attempt + 1, MAX_RETRIES, symbol, interval, exc,
            )
            await asyncio.sleep(RETRY_DELAY_SECONDS)
    if last_err:
        raise last_err
    return []


async def fetch_symbol_interval(
    client: httpx.AsyncClient,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    progress_callback=None,
) -> list[dict]:
    """Fetch all candles for one symbol/interval over a date range.

    Paginates in MAX_CANDLES_PER_REQUEST chunks.
    """
    interval_ms = INTERVAL_MS[interval]
    chunk_ms = MAX_CANDLES_PER_REQUEST * interval_ms

    all_candles: list[dict] = []
    cursor = start_ms

    while cursor < end_ms:
        chunk_end = min(cursor + chunk_ms, end_ms)
        chunk = await fetch_candles_from_kraken(
            client, symbol, interval, cursor, chunk_end
        )
        if not chunk:
            break
        all_candles.extend(chunk)
        last_ts = chunk[-1]["timestamp"]
        cursor = last_ts + interval_ms

        if progress_callback:
            progress_callback(symbol, interval, len(all_candles), None)

        await asyncio.sleep(REQUEST_DELAY_SECONDS)

    return all_candles


async def fetch_all(
    storage,
    symbols: list[str],
    start_date: str,
    end_date: str,
    intervals: list[str],
    progress_callback=None,
) -> dict:
    """Fetch candles for all symbol/interval combinations and write to storage."""
    start_ms = date_to_ms(parse_date(start_date))
    end_ms = date_to_ms(parse_date(end_date))

    total_candles = 0
    by_symbol: dict = {}

    async with httpx.AsyncClient(timeout=30.0) as client:
        for symbol in symbols:
            by_symbol[symbol] = {}
            for interval in intervals:
                candles = await fetch_symbol_interval(
                    client, symbol, interval, start_ms, end_ms,
                    progress_callback=progress_callback,
                )
                if hasattr(storage, "save_candles"):
                    await storage.save_candles(symbol, interval, candles)
                elif hasattr(storage, "_connection"):
                    from agent_arena.storage.candles import CandleStorage
                    cs = CandleStorage(storage._connection)
                    await cs.save_candles(symbol, interval, candles)
                by_symbol[symbol][interval] = len(candles)
                total_candles += len(candles)

    return {
        "by_symbol": by_symbol,
        "total_candles": total_candles,
    }


async def fetch_and_store_historical(
    symbols: list[str],
    start_date: str,
    end_date: str,
    intervals: list[str] = None,
    storage=None,
    progress_callback=None,
) -> dict:
    """Backward-compat wrapper around fetch_all() used by the CLI."""
    if intervals is None:
        intervals = ["1h", "4h"]

    return await fetch_all(
        storage=storage,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        intervals=intervals,
        progress_callback=progress_callback,
    )
