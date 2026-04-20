"""Tests for ReflexionService — reflection generation with mocked LLM."""

import pytest

from agent_arena.reflexion.models import TradeReflection


class TestTradeReflection:
    def test_to_dict(self):
        r = TradeReflection(
            agent_id="agent_1",
            trade_id="t_123",
            symbol="BTCUSDT",
            side="long",
            entry_price=50000.0,
            exit_price=51000.0,
            realized_pnl=100.0,
            market_regime="trending_up",
            entry_signal="RSI oversold bounce",
            outcome="win",
            what_went_right="Good timing on entry",
            what_went_wrong="",
            lesson="RSI works well in uptrends",
            confidence=0.8,
        )
        d = r.to_dict()
        assert d["agent_id"] == "agent_1"
        assert d["symbol"] == "BTCUSDT"
        assert d["outcome"] == "win"
        assert d["realized_pnl"] == 100.0
        assert d["lesson"] == "RSI works well in uptrends"

    def test_from_dict(self):
        data = {
            "agent_id": "agent_2",
            "trade_id": "t_456",
            "symbol": "ETHUSDT",
            "side": "short",
            "realized_pnl": -50.0,
            "outcome": "loss",
            "lesson": "Don't short in uptrend",
        }
        r = TradeReflection.from_dict(data)
        assert r.agent_id == "agent_2"
        assert r.outcome == "loss"
        assert r.lesson == "Don't short in uptrend"

    def test_defaults(self):
        r = TradeReflection(agent_id="test")
        assert r.metabolic_score == 1.0
        assert r.access_count == 0
        assert r.is_digested is False
        assert r.confidence == 0.5

    def test_round_trip(self):
        original = TradeReflection(
            agent_id="agent_1",
            trade_id="t_789",
            symbol="SOLUSDT",
            side="long",
            entry_price=145.0,
            exit_price=140.0,
            realized_pnl=-50.0,
            market_regime="volatile",
            entry_signal="Breakout",
            outcome="loss",
            what_went_right="Good entry signal",
            what_went_wrong="No stop loss",
            lesson="Always set stop loss",
            confidence=0.7,
            metabolic_score=0.8,
            access_count=3,
        )
        restored = TradeReflection.from_dict(original.to_dict())
        assert restored.agent_id == original.agent_id
        assert restored.lesson == original.lesson
        assert restored.metabolic_score == original.metabolic_score
        assert restored.access_count == original.access_count
