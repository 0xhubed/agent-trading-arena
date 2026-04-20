"""Tests for ExemplarBuilder — exemplar formatting and retrieval."""

import pytest

from agent_arena.reflexion.exemplars import ExemplarBuilder
from agent_arena.reflexion.models import TradeReflection


class TestFormatForPrompt:
    def test_empty_list(self):
        builder = ExemplarBuilder(storage=None)
        result = builder.format_for_prompt([])
        assert result == ""

    def test_single_win(self):
        exemplars = [
            TradeReflection(
                agent_id="test",
                symbol="BTCUSDT",
                side="long",
                realized_pnl=100.0,
                outcome="win",
                lesson="Trend following works in uptrends",
                what_went_right="Good timing",
            ),
        ]
        builder = ExemplarBuilder(storage=None)
        result = builder.format_for_prompt(exemplars)
        assert "BTCUSDT" in result
        assert "long" in result
        assert "+100.00" in result
        assert "Trend following" in result
        assert "Right:" in result

    def test_single_loss(self):
        exemplars = [
            TradeReflection(
                agent_id="test",
                symbol="ETHUSDT",
                side="short",
                realized_pnl=-50.0,
                outcome="loss",
                lesson="Don't short in uptrend",
                what_went_wrong="Ignored trend direction",
            ),
        ]
        builder = ExemplarBuilder(storage=None)
        result = builder.format_for_prompt(exemplars)
        assert "ETHUSDT" in result
        assert "-50.00" in result
        assert "Mistake:" in result

    def test_mixed_wins_losses(self):
        exemplars = [
            TradeReflection(
                agent_id="test",
                symbol="BTCUSDT",
                side="long",
                realized_pnl=200.0,
                outcome="win",
                lesson="Good entry",
            ),
            TradeReflection(
                agent_id="test",
                symbol="SOLUSDT",
                side="short",
                realized_pnl=-75.0,
                outcome="loss",
                lesson="Bad exit",
                what_went_wrong="Too early",
            ),
        ]
        builder = ExemplarBuilder(storage=None)
        result = builder.format_for_prompt(exemplars)
        assert "## Lessons from Recent Trades" in result
        assert "BTCUSDT" in result
        assert "SOLUSDT" in result

    def test_header_present(self):
        exemplars = [
            TradeReflection(
                agent_id="test",
                outcome="win",
                lesson="test",
                symbol="BTC",
                side="long",
                realized_pnl=10.0,
            ),
        ]
        builder = ExemplarBuilder(storage=None)
        result = builder.format_for_prompt(exemplars)
        assert result.startswith("## Lessons from Recent Trades")
