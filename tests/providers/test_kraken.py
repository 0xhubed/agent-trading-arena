"""Tests for KrakenProvider."""

from __future__ import annotations

import json
import re
from decimal import Decimal
from pathlib import Path

import pytest

from agent_arena.providers.kraken import KrakenProvider

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "kraken"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def test_name_is_kraken():
    provider = KrakenProvider()
    assert provider.name == "kraken"


@pytest.mark.asyncio
async def test_get_data_parses_tickers(httpx_mock):
    fixture = _load_fixture("tickers.json")
    httpx_mock.add_response(
        url=KrakenProvider.TICKERS_URL,
        json=fixture,
    )

    provider = KrakenProvider()
    await provider.start()
    try:
        result = await provider.get_data(["PF_XBTUSD", "PF_ETHUSD"])
    finally:
        await provider.stop()

    assert "market" in result
    assert set(result["market"].keys()) == {"PF_XBTUSD", "PF_ETHUSD"}

    btc = result["market"]["PF_XBTUSD"]
    assert isinstance(btc["price"], Decimal)
    assert btc["price"] > 0
    assert "change_24h" in btc
    assert isinstance(btc["volume_24h"], Decimal)
    assert isinstance(btc["high_24h"], Decimal)
    assert isinstance(btc["low_24h"], Decimal)
    assert isinstance(btc["funding_rate"], Decimal)


@pytest.mark.asyncio
async def test_funding_rate_normalization_yields_expected_payment(httpx_mock):
    """Given a Kraken-style raw funding rate, verify the normalized rate
    produces the correct $ flow when passed through arena.apply_funding_payments()
    formula: payment = notional * rate_per_8h * (tick_seconds / 28800).

    Target: $10k long notional × 0.0001 per-8h fraction × full 8h window = $1.00.
    The Kraken raw rate below corresponds to that per-8h target based on
    the conversion constant in KrakenProvider.FUNDING_RATE_HOURLY_TO_8H.
    """
    # Choose raw rate so that raw * FUNDING_RATE_HOURLY_TO_8H = 0.0001
    target_per_8h = Decimal("0.0001")
    raw_rate = target_per_8h / KrakenProvider.FUNDING_RATE_HOURLY_TO_8H

    synthetic = {
        "result": "success",
        "tickers": [
            {
                "symbol": "PF_XBTUSD",
                "last": "50000",
                "vol24h": "1",
                "fundingRate": str(raw_rate),
                "change24h": "0",
                "high24h": "51000",
                "low24h": "49000",
            }
        ],
    }
    httpx_mock.add_response(url=KrakenProvider.TICKERS_URL, json=synthetic)

    provider = KrakenProvider()
    await provider.start()
    try:
        result = await provider.get_data(["PF_XBTUSD"])
    finally:
        await provider.stop()

    rate = result["market"]["PF_XBTUSD"]["funding_rate"]

    # Apply the same formula the arena uses to compute the actual $ payment
    # on a $10k long over one full 8h funding window.
    notional = Decimal("10000")
    tick_seconds = Decimal("28800")
    window_seconds = Decimal("28800")
    payment = notional * rate * (tick_seconds / window_seconds)
    assert payment == Decimal("1.0000"), f"expected $1.00, got ${payment}"


@pytest.mark.asyncio
async def test_get_candles_parses_charts_response(httpx_mock):
    fixture = _load_fixture("charts.json")
    httpx_mock.add_response(
        url=re.compile(r".*/api/charts/v1/trade/PF_XBTUSD/1h.*"),
        json=fixture,
    )

    provider = KrakenProvider()
    await provider.start()
    try:
        candles = await provider.get_candles("PF_XBTUSD", "1h", limit=24)
    finally:
        await provider.stop()

    assert len(candles) > 0
    first = candles[0]
    assert set(["timestamp", "open", "high", "low", "close", "volume"]).issubset(first.keys())
    assert isinstance(first["open"], Decimal)
    # timestamp should be in milliseconds (13-digit range for recent data)
    assert first["timestamp"] > 1_000_000_000_000
