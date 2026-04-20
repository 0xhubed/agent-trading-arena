"""Binance Futures market data provider.

DEACTIVATED — the active provider is KrakenProvider (see `kraken.py`).
Preserved for reference and potential multi-exchange support. To reactivate,
restore the imports in api/app.py, cli.py, core/runner.py, and api/routes.py,
and switch config symbols back to Binance USDT format.
"""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal

import httpx

from agent_arena.providers.base import DataProvider

logger = logging.getLogger(__name__)


class BinanceProvider(DataProvider):
    """
    Real-time market data from Binance Futures.
    This is the core provider - always active.
    """

    BASE_URL = "https://fapi.binance.com"

    def __init__(self):
        self._client: httpx.AsyncClient | None = None

    @property
    def name(self) -> str:
        return "binance"

    async def start(self) -> None:
        """Initialize HTTP client."""
        self._client = httpx.AsyncClient(timeout=30.0)

    async def stop(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def get_data(self, symbols: list[str]) -> dict:
        """Fetch market data for symbols."""
        if not self._client:
            self._client = httpx.AsyncClient(timeout=30.0)

        # Fetch all data concurrently
        prices, stats, funding = await asyncio.gather(
            self._get_prices(symbols),
            self._get_24h_stats(symbols),
            self._get_funding_rates(symbols),
        )

        market = {}
        for symbol in symbols:
            price = prices.get(symbol)
            stat = stats.get(symbol, {})
            fund_rate = funding.get(symbol)

            if price is not None:
                market[symbol] = {
                    "price": price,
                    "change_24h": stat.get("change_pct", 0.0),
                    "volume_24h": stat.get("volume", Decimal("0")),
                    "high_24h": stat.get("high"),
                    "low_24h": stat.get("low"),
                    "funding_rate": fund_rate,
                }

        return {"market": market}

    async def _get_prices(self, symbols: list[str]) -> dict[str, Decimal]:
        """Fetch current prices."""
        try:
            response = await self._client.get(f"{self.BASE_URL}/fapi/v1/ticker/price")
            response.raise_for_status()
            data = response.json()
            return {
                item["symbol"]: Decimal(item["price"])
                for item in data
                if item["symbol"] in symbols
            }
        except Exception:
            logger.warning("Failed to fetch prices", exc_info=True)
            return {}

    async def _get_24h_stats(self, symbols: list[str]) -> dict[str, dict]:
        """Fetch 24h statistics."""
        try:
            response = await self._client.get(f"{self.BASE_URL}/fapi/v1/ticker/24hr")
            response.raise_for_status()
            data = response.json()
            return {
                item["symbol"]: {
                    "change_pct": float(item["priceChangePercent"]),
                    "volume": Decimal(item["volume"]),
                    "high": Decimal(item["highPrice"]),
                    "low": Decimal(item["lowPrice"]),
                }
                for item in data
                if item["symbol"] in symbols
            }
        except Exception:
            logger.warning("Failed to fetch 24h stats", exc_info=True)
            return {}

    async def _get_funding_rates(self, symbols: list[str]) -> dict[str, Decimal]:
        """Fetch funding rates."""
        try:
            response = await self._client.get(f"{self.BASE_URL}/fapi/v1/premiumIndex")
            response.raise_for_status()
            data = response.json()
            return {
                item["symbol"]: Decimal(item["lastFundingRate"])
                for item in data
                if item["symbol"] in symbols
            }
        except Exception:
            logger.warning("Failed to fetch funding rates", exc_info=True)
            return {}

    async def get_candles(
        self,
        symbol: str,
        interval: str = "1h",
        limit: int = 100,
    ) -> list[dict]:
        """
        Fetch OHLCV candlestick data from Binance Futures.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            interval: Kline interval - 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
            limit: Number of candles (max 1500, default 100)

        Returns:
            List of candle dicts with open, high, low, close, volume, timestamp
        """
        if not self._client:
            self._client = httpx.AsyncClient(timeout=30.0)

        try:
            response = await self._client.get(
                f"{self.BASE_URL}/fapi/v1/klines",
                params={
                    "symbol": symbol,
                    "interval": interval,
                    "limit": min(limit, 1500),
                },
            )
            response.raise_for_status()
            data = response.json()

            candles = []
            for kline in data:
                candles.append({
                    "timestamp": kline[0],  # Open time in milliseconds
                    "open": Decimal(kline[1]),
                    "high": Decimal(kline[2]),
                    "low": Decimal(kline[3]),
                    "close": Decimal(kline[4]),
                    "volume": Decimal(kline[5]),
                    "close_time": kline[6],
                    "quote_volume": Decimal(kline[7]),
                    "trades": int(kline[8]),
                })
            return candles
        except Exception:
            logger.warning("Failed to fetch candles for %s", symbol, exc_info=True)
            return []

    async def get_candles_multi(
        self,
        symbols: list[str],
        intervals: list[str] | None = None,
        limit: int = 100,
    ) -> dict[str, dict[str, list[dict]]]:
        """
        Fetch candles for multiple symbols and intervals concurrently.

        Args:
            symbols: List of trading pairs
            intervals: List of intervals (default: ["1h", "15m"])
            limit: Number of candles per request

        Returns:
            Dict mapping symbol -> interval -> candles list
        """
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

        candles_data: dict[str, dict[str, list[dict]]] = {s: {} for s in symbols}
        for result in results:
            if isinstance(result, tuple):
                symbol, interval, candles = result
                candles_data[symbol][interval] = candles

        return candles_data
