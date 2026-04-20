"""Tests for the scenarios module."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pytest

from agent_arena.scenarios.models import Scenario
from agent_arena.scenarios.provider import ScenarioProvider
from agent_arena.scenarios.registry import ScenarioRegistry

# --- Fixtures ---


def _make_candles(
    symbol: str,
    interval: str,
    start_ms: int,
    count: int,
    interval_ms: int,
    base_price: float = 100000.0,
) -> dict[str, list[dict]]:
    """Generate fake candle data for testing."""
    key = f"{symbol}/{interval}"
    candles = []
    for i in range(count):
        ts = start_ms + i * interval_ms
        price = base_price + i * 10  # simple uptrend
        candles.append(
            {
                "timestamp": ts,
                "open": str(price),
                "high": str(price + 5),
                "low": str(price - 5),
                "close": str(price + 2),
                "volume": str(100.0 + i),
                "close_time": ts + interval_ms - 1,
                "quote_volume": str(1000000.0),
                "trades": 500 + i,
            }
        )
    return {key: candles}


def _write_scenario(
    base_dir: Path,
    scenario_id: str,
    candles: dict,
    **meta_overrides,
) -> Scenario:
    """Write a complete scenario to disk and return its Scenario object."""
    scenario_dir = base_dir / scenario_id
    scenario_dir.mkdir(parents=True, exist_ok=True)

    candles_json = json.dumps(candles, separators=(",", ":"))
    (scenario_dir / "candles.json").write_text(candles_json, encoding="utf-8")
    checksum = hashlib.sha256(candles_json.encode("utf-8")).hexdigest()

    defaults = dict(
        scenario_id=scenario_id,
        name="Test Scenario",
        description="A test scenario",
        symbols=["BTCUSDT"],
        interval="1h",
        candle_intervals=["1h"],
        start_date="2024-11-10",
        end_date="2024-11-11",
        total_ticks=24,
        checksum=checksum,
    )
    defaults.update(meta_overrides)
    scenario = Scenario(**defaults)

    (scenario_dir / "metadata.json").write_text(scenario.to_json(), encoding="utf-8")
    return scenario


# --- Scenario model tests ---


class TestScenarioModel:
    def test_roundtrip_dict(self):
        s = Scenario(
            scenario_id="test",
            name="Test",
            description="desc",
            symbols=["BTCUSDT"],
            interval="5m",
            candle_intervals=["5m", "1h"],
            start_date="2024-01-01",
            end_date="2024-01-02",
            total_ticks=288,
            checksum="abc123",
            created_at="2026-01-01T00:00:00+00:00",
        )
        d = s.to_dict()
        s2 = Scenario.from_dict(d)
        assert s == s2

    def test_roundtrip_json(self):
        s = Scenario(
            scenario_id="test",
            name="Test",
            description="desc",
            symbols=["BTCUSDT", "ETHUSDT"],
            interval="1h",
            candle_intervals=["1h"],
            start_date="2024-06-01",
            end_date="2024-06-02",
            total_ticks=24,
            checksum="deadbeef",
        )
        text = s.to_json()
        s2 = Scenario.from_json(text)
        assert s2.scenario_id == "test"
        assert s2.symbols == ["BTCUSDT", "ETHUSDT"]
        assert s2.total_ticks == 24


# --- Registry tests ---


class TestScenarioRegistry:
    def test_list_empty(self, tmp_path):
        reg = ScenarioRegistry(base_dir=tmp_path / "empty")
        assert reg.list_scenarios() == []

    def test_list_and_load(self, tmp_path):
        candles = _make_candles("BTCUSDT", "1h", 1731196800000, 24, 3600000)
        _write_scenario(tmp_path, "s1", candles, name="Scenario 1")
        _write_scenario(tmp_path, "s2", candles, name="Scenario 2")

        reg = ScenarioRegistry(base_dir=tmp_path)
        scenarios = reg.list_scenarios()
        assert len(scenarios) == 2
        ids = {s.scenario_id for s in scenarios}
        assert ids == {"s1", "s2"}

        loaded = reg.load_scenario("s1")
        assert loaded.name == "Scenario 1"

    def test_load_missing_raises(self, tmp_path):
        reg = ScenarioRegistry(base_dir=tmp_path)
        with pytest.raises(FileNotFoundError):
            reg.load_scenario("nonexistent")

    def test_verify_checksum_pass(self, tmp_path):
        candles = _make_candles("BTCUSDT", "1h", 1731196800000, 24, 3600000)
        _write_scenario(tmp_path, "valid", candles)

        reg = ScenarioRegistry(base_dir=tmp_path)
        assert reg.verify_checksum("valid") is True

    def test_verify_checksum_tampered(self, tmp_path):
        candles = _make_candles("BTCUSDT", "1h", 1731196800000, 24, 3600000)
        _write_scenario(tmp_path, "tampered", candles)

        # Tamper with candles.json
        candles_path = tmp_path / "tampered" / "candles.json"
        candles_path.write_text('{"tampered": true}', encoding="utf-8")

        reg = ScenarioRegistry(base_dir=tmp_path)
        assert reg.verify_checksum("tampered") is False

    def test_verify_all(self, tmp_path):
        candles = _make_candles("BTCUSDT", "1h", 1731196800000, 24, 3600000)
        _write_scenario(tmp_path, "good", candles)
        _write_scenario(tmp_path, "bad", candles)

        # Tamper with one
        (tmp_path / "bad" / "candles.json").write_text("{}", encoding="utf-8")

        reg = ScenarioRegistry(base_dir=tmp_path)
        results = reg.verify_all()
        assert results["good"] is True
        assert results["bad"] is False


# --- Provider tests ---


class TestScenarioProvider:
    @pytest.fixture
    def scenario_dir(self, tmp_path):
        """Create a test scenario with 24 hourly candles."""
        # 2024-11-10 00:00 UTC = 1731196800000
        start_ms = 1731196800000
        interval_ms = 3600000  # 1h

        candles = _make_candles(
            "BTCUSDT", "1h", start_ms, 24, interval_ms, base_price=90000.0
        )
        _write_scenario(
            tmp_path,
            "test_replay",
            candles,
            total_ticks=24,
        )
        return tmp_path

    @pytest.mark.asyncio
    async def test_start_loads_data(self, scenario_dir):
        p = ScenarioProvider("test_replay", base_dir=scenario_dir)
        await p.start()

        assert p.scenario is not None
        assert p.scenario.scenario_id == "test_replay"
        assert p.total_ticks == 24
        assert not p.is_finished

        await p.stop()

    @pytest.mark.asyncio
    async def test_advance_tick(self, scenario_dir):
        p = ScenarioProvider("test_replay", base_dir=scenario_dir)
        await p.start()

        # Advance a few ticks
        assert p.advance_tick() is True
        assert p.advance_tick() is True
        assert p._tick_count == 2

        await p.stop()

    @pytest.mark.asyncio
    async def test_get_data_returns_market(self, scenario_dir):
        p = ScenarioProvider("test_replay", base_dir=scenario_dir)
        await p.start()

        data = await p.get_data(["BTCUSDT"])
        assert "market" in data
        assert "BTCUSDT" in data["market"]

        btc = data["market"]["BTCUSDT"]
        assert "price" in btc
        assert isinstance(btc["price"], Decimal)
        assert btc["price"] > 0

        await p.stop()

    @pytest.mark.asyncio
    async def test_get_data_missing_symbol(self, scenario_dir):
        p = ScenarioProvider("test_replay", base_dir=scenario_dir)
        await p.start()

        data = await p.get_data(["XYZUSDT"])
        assert data["market"] == {}

        await p.stop()

    @pytest.mark.asyncio
    async def test_candle_windowing(self, scenario_dir):
        """Only candles up to current tick should be returned."""
        p = ScenarioProvider("test_replay", candle_limit=100, base_dir=scenario_dir)
        await p.start()

        # At start (tick 0), should get first candle
        candles = await p.get_candles("BTCUSDT", "1h", limit=100)
        initial_count = len(candles)
        assert initial_count >= 1

        # Advance 5 ticks
        for _ in range(5):
            p.advance_tick()

        candles_after = await p.get_candles("BTCUSDT", "1h", limit=100)
        assert len(candles_after) >= initial_count
        # All returned candles should have timestamps <= current time
        for c in candles_after:
            assert c["timestamp"] <= p.current_timestamp_ms

        await p.stop()

    @pytest.mark.asyncio
    async def test_candle_limit(self, scenario_dir):
        """Candle limit should be respected."""
        p = ScenarioProvider("test_replay", base_dir=scenario_dir)
        await p.start()

        # Advance to the end so all 24 candles are available
        for _ in range(23):
            p.advance_tick()

        candles = await p.get_candles("BTCUSDT", "1h", limit=5)
        assert len(candles) == 5

        await p.stop()

    @pytest.mark.asyncio
    async def test_get_candles_multi(self, scenario_dir):
        p = ScenarioProvider("test_replay", base_dir=scenario_dir)
        await p.start()

        result = await p.get_candles_multi(["BTCUSDT"], intervals=["1h"], limit=10)
        assert "BTCUSDT" in result
        assert "1h" in result["BTCUSDT"]
        assert len(result["BTCUSDT"]["1h"]) > 0

        await p.stop()

    @pytest.mark.asyncio
    async def test_replay_to_end(self, scenario_dir):
        """Replay should finish after total_ticks advances."""
        p = ScenarioProvider("test_replay", base_dir=scenario_dir)
        await p.start()

        ticks = 0
        while p.advance_tick():
            ticks += 1

        assert p.is_finished
        # advance_tick returns False on the final advance, so ticks = total - 1
        assert ticks == p.total_ticks - 1

        await p.stop()

    @pytest.mark.asyncio
    async def test_reset(self, scenario_dir):
        p = ScenarioProvider("test_replay", base_dir=scenario_dir)
        await p.start()

        for _ in range(5):
            p.advance_tick()
        assert p._tick_count == 5

        p.reset()
        assert p._tick_count == 0
        assert not p.is_finished

        await p.stop()

    @pytest.mark.asyncio
    async def test_start_missing_scenario_raises(self, tmp_path):
        p = ScenarioProvider("nonexistent", base_dir=tmp_path)
        with pytest.raises(FileNotFoundError):
            await p.start()

    @pytest.mark.asyncio
    async def test_prices_advance_with_ticks(self, scenario_dir):
        """Prices should change as ticks advance (our test data has uptrend)."""
        p = ScenarioProvider("test_replay", base_dir=scenario_dir)
        await p.start()

        data0 = await p.get_data(["BTCUSDT"])
        price0 = data0["market"]["BTCUSDT"]["price"]

        for _ in range(10):
            p.advance_tick()

        data10 = await p.get_data(["BTCUSDT"])
        price10 = data10["market"]["BTCUSDT"]["price"]

        # Our test data has an uptrend (+10 per candle)
        assert price10 > price0

        await p.stop()
