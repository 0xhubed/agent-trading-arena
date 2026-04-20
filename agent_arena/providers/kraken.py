"""Kraken Futures market data provider."""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal

import httpx

from agent_arena.providers.base import DataProvider
from agent_arena.providers.historical import INTERVAL_MS

logger = logging.getLogger(__name__)

# Derived from the canonical INTERVAL_MS in providers/historical.py so all
# three modules (provider, fetcher, historical replay) agree on the supported
# set. Kraken Futures charts API uses these strings directly as the `resolution`
# path segment, so no translation map is needed.
_INTERVAL_SECONDS = {k: v // 1000 for k, v in INTERVAL_MS.items()}


class KrakenProvider(DataProvider):
    """Real-time market data from Kraken Futures public endpoints."""

    TICKERS_URL = "https://futures.kraken.com/derivatives/api/v3/tickers"
    CHARTS_URL_TEMPLATE = (
        "https://futures.kraken.com/api/charts/v1/trade/{symbol}/{resolution}"
    )

    # Kraken PF_* (Linear Multi-Collateral Derivatives) report fundingRate as a
    # relative hourly rate; the arena's apply_funding_payments() formula assumes
    # a per-8h fraction, so multiply by 8 to convert.
    # Reference: Kraken docs — "funding received by maintaining 1 contract
    # unit short position for 1 hour."
    FUNDING_RATE_HOURLY_TO_8H = Decimal("8")

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    @property
    def name(self) -> str:
        return "kraken"

    async def start(self) -> None:
        self._client = httpx.AsyncClient(timeout=30.0)

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def get_data(self, symbols: list[str]) -> dict:
        """Fetch market data for symbols from Kraken /tickers endpoint."""
        tickers = await self._get_tickers(set(symbols))

        market: dict[str, dict] = {}
        for symbol in symbols:
            ticker = tickers.get(symbol)
            if ticker is None:
                continue
            market[symbol] = {
                "price": ticker["price"],
                "change_24h": ticker["change_24h"],
                "volume_24h": ticker["volume_24h"],
                "high_24h": ticker["high_24h"],
                "low_24h": ticker["low_24h"],
                "funding_rate": ticker["funding_rate"],
            }

        return {"market": market}

    async def _get_tickers(self, symbols: set[str]) -> dict[str, dict]:
        """Fetch all tickers in one call, return normalized dict keyed by symbol."""
        assert self._client is not None, "KrakenProvider.start() not called"

        try:
            response = await self._client.get(self.TICKERS_URL)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError:
            logger.warning("Failed to fetch Kraken tickers", exc_info=True)
            return {}

        result: dict[str, dict] = {}
        for item in data.get("tickers", []):
            symbol = item.get("symbol")
            if symbol not in symbols:
                continue
            try:
                result[symbol] = {
                    "price": Decimal(str(item["last"])),
                    "change_24h": float(item.get("change24h", 0.0)),
                    "volume_24h": Decimal(str(item.get("vol24h", "0"))),
                    "high_24h": Decimal(str(item["high24h"])),
                    "low_24h": Decimal(str(item["low24h"])),
                    "funding_rate": (
                        Decimal(str(item.get("fundingRate", "0")))
                        * self.FUNDING_RATE_HOURLY_TO_8H
                    ),
                }
            except (KeyError, ValueError, TypeError):
                logger.warning(
                    "Skipping malformed ticker for %s", symbol, exc_info=True
                )

        return result

    async def get_candles(
        self,
        symbol: str,
        interval: str = "1h",
        limit: int = 100,
    ) -> list[dict]:
        """Fetch OHLCV candles from Kraken Futures charts API."""
        assert self._client is not None, "KrakenProvider.start() not called"

        if interval not in _INTERVAL_SECONDS:
            raise ValueError(
                f"Interval {interval!r} not supported by Kraken charts API. "
                f"Supported: {sorted(_INTERVAL_SECONDS)}"
            )
        interval_seconds = _INTERVAL_SECONDS[interval]
        url = self.CHARTS_URL_TEMPLATE.format(symbol=symbol, resolution=interval)
        # Request only a little more than `limit` candles to cap response size;
        # Kraken's default window returns many hundreds that we'd discard.
        now = int(time.time())
        params = {"from": now - (limit + 2) * interval_seconds, "to": now}

        try:
            response = await self._client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError:
            logger.warning("Failed to fetch candles for %s", symbol, exc_info=True)
            return []

        raw_candles = data.get("candles", [])
        parsed: list[dict] = []
        interval_ms = interval_seconds * 1000
        for c in raw_candles[-limit:]:
            try:
                ts_ms = int(c["time"])  # Kraken already returns ms
                parsed.append({
                    "timestamp": ts_ms,
                    "open": Decimal(str(c["open"])),
                    "high": Decimal(str(c["high"])),
                    "low": Decimal(str(c["low"])),
                    "close": Decimal(str(c["close"])),
                    "volume": Decimal(str(c["volume"])),
                    "close_time": ts_ms + interval_ms,
                })
            except (KeyError, ValueError, TypeError):
                logger.warning(
                    "Skipping malformed candle for %s", symbol, exc_info=True
                )
        return parsed

    async def get_candles_multi(
        self,
        symbols: list[str],
        intervals: list[str] | None = None,
        limit: int = 100,
    ) -> dict[str, dict[str, list[dict]]]:
        """Fetch candles for multiple symbols and intervals concurrently."""
        if intervals is None:
            intervals = ["1h", "15m"]

        async def fetch_one(symbol: str, interval: str) -> tuple[str, str, list[dict]]:
            candles = await self.get_candles(symbol, interval, limit)
            return symbol, interval, candles

        tasks = [
            fetch_one(symbol, interval)
            for symbol in symbols
            for interval in intervals
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        out: dict[str, dict[str, list[dict]]] = {s: {} for s in symbols}
        for result in results:
            if isinstance(result, tuple):
                symbol, interval, candles = result
                out[symbol][interval] = candles
        return out
