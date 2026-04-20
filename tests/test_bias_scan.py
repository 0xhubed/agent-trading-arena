"""Tests for behavioral bias calculators.

All tests use synthetic data — no DB required since calculators are pure functions.
"""

from __future__ import annotations

import pytest

from agent_arena.analysis.bias_models import BiasProfile, BiasScore
from agent_arena.analysis.bias_scan import (
    _match_open_close_pairs,
    _pearson_correlation,
    analyze_agent_biases,
    calculate_disposition_effect,
    calculate_loss_aversion,
    calculate_overconfidence,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_decision(
    agent_id: str = "agent_1",
    tick: int = 1,
    action: str = "open_long",
    symbol: str = "BTCUSDT",
    size: str = "0.1",
    confidence: float = 0.8,
    decision_id: int = None,
    trade_id: str = None,
) -> dict:
    return {
        "id": decision_id or tick,
        "agent_id": agent_id,
        "tick": tick,
        "timestamp": f"2025-01-01T00:{tick:02d}:00",
        "action": action,
        "symbol": symbol,
        "size": size,
        "leverage": 5,
        "confidence": confidence,
        "reasoning": "test",
        "metadata": {},
        "trade_id": trade_id,
    }


def _make_trade(
    trade_id: str = "t1",
    agent_id: str = "agent_1",
    symbol: str = "BTCUSDT",
    side: str = "long",
    realized_pnl: str = None,
    decision_id: int = None,
) -> dict:
    return {
        "id": trade_id,
        "agent_id": agent_id,
        "symbol": symbol,
        "side": side,
        "size": "0.1",
        "price": "50000",
        "leverage": 5,
        "fee": "2.0",
        "realized_pnl": realized_pnl,
        "timestamp": "2025-01-01T00:00:00",
        "decision_id": decision_id,
    }


def _make_pairs(
    n_winners: int,
    n_losers: int,
    winner_duration: int = 10,
    loser_duration: int = 10,
    winner_confidence: float = 0.8,
    loser_confidence: float = 0.8,
) -> list[dict]:
    """Generate synthetic matched pairs."""
    pairs = []
    for i in range(n_winners):
        pairs.append({
            "agent_id": "agent_1",
            "symbol": "BTCUSDT",
            "open_tick": i * 20,
            "close_tick": i * 20 + winner_duration,
            "hold_duration": winner_duration,
            "open_size": 0.1,
            "realized_pnl": 100.0,
            "open_confidence": winner_confidence,
        })
    for i in range(n_losers):
        pairs.append({
            "agent_id": "agent_1",
            "symbol": "ETHUSDT",
            "open_tick": (n_winners + i) * 20,
            "close_tick": (n_winners + i) * 20 + loser_duration,
            "hold_duration": loser_duration,
            "open_size": 0.1,
            "realized_pnl": -100.0,
            "open_confidence": loser_confidence,
        })
    return pairs


# ---------------------------------------------------------------------------
# Pair Matching Tests
# ---------------------------------------------------------------------------

class TestMatchOpenClosePairs:
    def test_simple_pair(self):
        decisions = [
            _make_decision(tick=1, action="open_long", decision_id=1),
            _make_decision(tick=5, action="close", decision_id=2),
        ]
        trades = [
            _make_trade("t_open", decision_id=1),  # Trade for the open
            _make_trade("t_close", decision_id=2, realized_pnl="150.0"),
        ]
        pairs = _match_open_close_pairs(decisions, trades)
        assert len(pairs) == 1
        assert pairs[0]["hold_duration"] == 4
        assert pairs[0]["realized_pnl"] == 150.0

    def test_multiple_symbols(self):
        decisions = [
            _make_decision(tick=1, action="open_long", symbol="BTCUSDT", decision_id=1),
            _make_decision(tick=2, action="open_short", symbol="ETHUSDT", decision_id=2),
            _make_decision(tick=5, action="close", symbol="BTCUSDT", decision_id=3),
            _make_decision(tick=6, action="close", symbol="ETHUSDT", decision_id=4),
        ]
        trades = [
            _make_trade("t_ob", symbol="BTCUSDT", decision_id=1),  # Open BTC
            _make_trade("t_oe", symbol="ETHUSDT", decision_id=2),  # Open ETH
            _make_trade("t1", symbol="BTCUSDT", decision_id=3, realized_pnl="50.0"),
            _make_trade("t2", symbol="ETHUSDT", decision_id=4, realized_pnl="-30.0"),
        ]
        pairs = _match_open_close_pairs(decisions, trades)
        assert len(pairs) == 2
        btc = next(p for p in pairs if p["symbol"] == "BTCUSDT")
        eth = next(p for p in pairs if p["symbol"] == "ETHUSDT")
        assert btc["realized_pnl"] == 50.0
        assert eth["realized_pnl"] == -30.0

    def test_unmatched_close_ignored(self):
        decisions = [
            _make_decision(tick=5, action="close", decision_id=1),
        ]
        pairs = _match_open_close_pairs(decisions, [])
        assert len(pairs) == 0

    def test_empty_data(self):
        assert _match_open_close_pairs([], []) == []

    def test_only_executed_opens_tracked(self):
        """Opens without a matching trade (rejected by arena) are ignored."""
        decisions = [
            # First open — has a trade, so it's tracked
            _make_decision(tick=1, action="open_long", decision_id=1, confidence=0.5),
            # Second open — no trade (arena rejected it, position already exists)
            _make_decision(tick=3, action="open_long", decision_id=2, confidence=0.9),
            _make_decision(tick=5, action="close", decision_id=3),
        ]
        trades = [
            _make_trade("t_open", decision_id=1),  # Trade for first open
            _make_trade("t_close", decision_id=3, realized_pnl="100.0"),
        ]
        pairs = _match_open_close_pairs(decisions, trades)
        assert len(pairs) == 1
        # Matched the first open (the only executed one), not the phantom second
        assert pairs[0]["open_tick"] == 1
        assert pairs[0]["open_confidence"] == 0.5

    def test_phantom_open_ignored(self):
        """An open decision with no trade should not appear in pairs."""
        decisions = [
            _make_decision(tick=1, action="open_long", decision_id=1),
            _make_decision(tick=5, action="close", decision_id=2),
        ]
        # No trade for decision_id=1, so the open is not tracked
        trades = [
            _make_trade("t_close", decision_id=2, realized_pnl="50.0"),
        ]
        pairs = _match_open_close_pairs(decisions, trades)
        assert len(pairs) == 0  # No executed open to match

    def test_trade_id_fallback(self):
        """When decision_id lookup fails for close, fall back to trade_id."""
        decisions = [
            _make_decision(tick=1, action="open_long", decision_id=1),
            _make_decision(tick=5, action="close", decision_id=2, trade_id="t_close"),
        ]
        trades = [
            _make_trade("t_open", decision_id=1),  # Trade for the open
            _make_trade("t_close", realized_pnl="200.0"),  # Close trade (no decision_id)
        ]
        pairs = _match_open_close_pairs(decisions, trades)
        assert len(pairs) == 1
        assert pairs[0]["realized_pnl"] == 200.0


# ---------------------------------------------------------------------------
# Disposition Effect Tests
# ---------------------------------------------------------------------------

class TestDispositionEffect:
    def test_equal_hold_times(self):
        pairs = _make_pairs(15, 15, winner_duration=10, loser_duration=10)
        result = calculate_disposition_effect(pairs)
        assert result.sufficient_data is True
        assert result.value == pytest.approx(0.5, abs=0.01)

    def test_losers_held_longer(self):
        pairs = _make_pairs(15, 15, winner_duration=10, loser_duration=20)
        result = calculate_disposition_effect(pairs)
        assert result.sufficient_data is True
        # 20 / (20 + 10) = 0.667
        assert result.value == pytest.approx(0.667, abs=0.01)

    def test_winners_held_longer(self):
        pairs = _make_pairs(15, 15, winner_duration=20, loser_duration=10)
        result = calculate_disposition_effect(pairs)
        assert result.sufficient_data is True
        # 10 / (10 + 20) = 0.333
        assert result.value == pytest.approx(0.333, abs=0.01)

    def test_insufficient_data(self):
        pairs = _make_pairs(5, 5)
        result = calculate_disposition_effect(pairs)
        assert result.sufficient_data is False
        assert result.value is None

    def test_no_losers(self):
        pairs = _make_pairs(20, 0)
        result = calculate_disposition_effect(pairs)
        assert result.sufficient_data is False

    def test_bias_type(self):
        pairs = _make_pairs(15, 15)
        result = calculate_disposition_effect(pairs)
        assert result.bias_type == "disposition_effect"


# ---------------------------------------------------------------------------
# Loss Aversion Tests
# ---------------------------------------------------------------------------

class TestLossAversion:
    def _build_sequential_data(
        self, n: int, size_after_win: str, size_after_loss: str
    ) -> tuple[list[dict], list[dict]]:
        """Build alternating close→open sequences for loss aversion testing."""
        decisions = []
        trades = []
        tick = 0

        for i in range(n):
            # Open
            tick += 1
            decisions.append(_make_decision(
                tick=tick, action="open_long", decision_id=tick, size="0.1",
            ))

            # Close as winner
            tick += 1
            decisions.append(_make_decision(
                tick=tick, action="close", decision_id=tick, symbol="BTCUSDT",
            ))
            trades.append(_make_trade(
                f"tw{i}", decision_id=tick, realized_pnl="100.0",
            ))

            # Open after win
            tick += 1
            decisions.append(_make_decision(
                tick=tick, action="open_long", decision_id=tick,
                size=size_after_win,
            ))

            # Close as loser
            tick += 1
            decisions.append(_make_decision(
                tick=tick, action="close", decision_id=tick, symbol="BTCUSDT",
            ))
            trades.append(_make_trade(
                f"tl{i}", decision_id=tick, realized_pnl="-100.0",
            ))

            # Open after loss
            tick += 1
            decisions.append(_make_decision(
                tick=tick, action="open_long", decision_id=tick,
                size=size_after_loss,
            ))

        return decisions, trades

    def test_equal_sizing(self):
        decisions, trades = self._build_sequential_data(12, "0.1", "0.1")
        result = calculate_loss_aversion(decisions, trades)
        assert result.sufficient_data is True
        assert result.value == pytest.approx(0.0, abs=0.01)

    def test_halved_after_loss(self):
        decisions, trades = self._build_sequential_data(12, "0.1", "0.05")
        result = calculate_loss_aversion(decisions, trades)
        assert result.sufficient_data is True
        # 1 - (0.05 / 0.1) = 0.5
        assert result.value == pytest.approx(0.5, abs=0.01)

    def test_insufficient_data(self):
        decisions, trades = self._build_sequential_data(3, "0.1", "0.1")
        result = calculate_loss_aversion(decisions, trades)
        assert result.sufficient_data is False
        assert result.value is None

    def test_bias_type(self):
        decisions, trades = self._build_sequential_data(12, "0.1", "0.1")
        result = calculate_loss_aversion(decisions, trades)
        assert result.bias_type == "loss_aversion"


# ---------------------------------------------------------------------------
# Overconfidence Tests
# ---------------------------------------------------------------------------

class TestOverconfidence:
    def test_perfect_calibration(self):
        """High confidence wins, low confidence losses → correlation ~1 → score ~0."""
        pairs = []
        for i in range(25):
            pairs.append({
                "agent_id": "a", "symbol": "BTC",
                "open_tick": i, "close_tick": i + 5, "hold_duration": 5,
                "open_size": 0.1,
                "realized_pnl": 100.0,
                "open_confidence": 0.9,
            })
        for i in range(25):
            pairs.append({
                "agent_id": "a", "symbol": "BTC",
                "open_tick": 50 + i, "close_tick": 55 + i, "hold_duration": 5,
                "open_size": 0.1,
                "realized_pnl": -100.0,
                "open_confidence": 0.2,
            })
        result = calculate_overconfidence(pairs)
        assert result.sufficient_data is True
        assert result.value < 0.15  # Should be near 0

    def test_anti_calibrated(self):
        """High confidence losses, low confidence wins → negative correlation → score ~1."""
        pairs = []
        for i in range(25):
            pairs.append({
                "agent_id": "a", "symbol": "BTC",
                "open_tick": i, "close_tick": i + 5, "hold_duration": 5,
                "open_size": 0.1,
                "realized_pnl": 100.0,
                "open_confidence": 0.2,
            })
        for i in range(25):
            pairs.append({
                "agent_id": "a", "symbol": "BTC",
                "open_tick": 50 + i, "close_tick": 55 + i, "hold_duration": 5,
                "open_size": 0.1,
                "realized_pnl": -100.0,
                "open_confidence": 0.9,
            })
        result = calculate_overconfidence(pairs)
        assert result.sufficient_data is True
        assert result.value > 0.85  # Should be near 1

    def test_random_decorrelated(self):
        """Same confidence for all — zero variance → score = 0.5."""
        pairs = _make_pairs(15, 15, winner_confidence=0.7, loser_confidence=0.7)
        result = calculate_overconfidence(pairs)
        assert result.sufficient_data is True
        assert result.value == pytest.approx(0.5, abs=0.01)

    def test_insufficient_data(self):
        pairs = _make_pairs(5, 5)
        result = calculate_overconfidence(pairs)
        assert result.sufficient_data is False
        assert result.value is None

    def test_bias_type(self):
        pairs = _make_pairs(15, 15)
        result = calculate_overconfidence(pairs)
        assert result.bias_type == "overconfidence"


# ---------------------------------------------------------------------------
# Pearson Correlation
# ---------------------------------------------------------------------------

class TestPearsonCorrelation:
    def test_perfect_positive(self):
        assert _pearson_correlation([1, 2, 3], [1, 2, 3]) == pytest.approx(1.0)

    def test_perfect_negative(self):
        assert _pearson_correlation([1, 2, 3], [3, 2, 1]) == pytest.approx(-1.0)

    def test_zero_variance(self):
        assert _pearson_correlation([1, 1, 1], [1, 2, 3]) is None

    def test_too_few(self):
        assert _pearson_correlation([1], [1]) is None


# ---------------------------------------------------------------------------
# BiasScore / BiasProfile model tests
# ---------------------------------------------------------------------------

class TestBiasModels:
    def test_bias_score_to_dict(self):
        score = BiasScore("disposition_effect", 0.65, 30, True, {"winners": 15})
        d = score.to_dict()
        assert d["bias_type"] == "disposition_effect"
        assert d["value"] == 0.65
        assert d["sufficient_data"] is True

    def test_bias_score_summary_insufficient(self):
        score = BiasScore("loss_aversion", None, 5, False, {})
        assert "INSUFFICIENT" in score.summary()

    def test_bias_profile_to_dict(self):
        from datetime import datetime, timezone
        profile = BiasProfile(
            agent_id="test",
            timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
            disposition_effect=BiasScore("disposition_effect", 0.5, 20, True),
            loss_aversion=BiasScore("loss_aversion", 0.3, 20, True),
            overconfidence=BiasScore("overconfidence", None, 5, False),
        )
        d = profile.to_dict()
        assert d["agent_id"] == "test"
        assert d["disposition_effect"]["value"] == 0.5
        assert d["overconfidence"]["value"] is None

    def test_bias_profile_scores_property(self):
        from datetime import datetime, timezone
        s1 = BiasScore("disposition_effect", 0.5, 20, True)
        s2 = BiasScore("loss_aversion", 0.3, 20, True)
        s3 = BiasScore("overconfidence", 0.4, 20, True)
        profile = BiasProfile("a", datetime.now(timezone.utc), s1, s2, s3)
        assert len(profile.scores) == 3


# ---------------------------------------------------------------------------
# Integration: analyze_agent_biases
# ---------------------------------------------------------------------------

class TestAnalyzeAgentBiases:
    def test_returns_valid_profile(self):
        """With enough data, returns a complete profile."""
        decisions = []
        trades = []
        tick = 0
        for i in range(25):
            tick += 1
            open_id = tick
            decisions.append(_make_decision(
                tick=tick, action="open_long", decision_id=open_id, size="0.1",
                confidence=0.8,
            ))
            trades.append(_make_trade(
                f"to{i}", decision_id=open_id,
            ))
            tick += 1
            pnl = "100.0" if i % 2 == 0 else "-100.0"
            decisions.append(_make_decision(
                tick=tick, action="close", decision_id=tick, symbol="BTCUSDT",
            ))
            trades.append(_make_trade(
                f"tc{i}", decision_id=tick, realized_pnl=pnl,
            ))

        profile = analyze_agent_biases("agent_1", decisions, trades)
        assert isinstance(profile, BiasProfile)
        assert profile.agent_id == "agent_1"
        assert profile.disposition_effect.bias_type == "disposition_effect"
        assert profile.loss_aversion.bias_type == "loss_aversion"
        assert profile.overconfidence.bias_type == "overconfidence"

    def test_empty_data(self):
        """With no data, all biases should have insufficient data."""
        profile = analyze_agent_biases("agent_1", [], [])
        assert not profile.disposition_effect.sufficient_data
        assert not profile.loss_aversion.sufficient_data
        assert not profile.overconfidence.sufficient_data


# ---------------------------------------------------------------------------
# Edge Cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_invalid_size_values(self):
        """Non-numeric size values should be handled gracefully."""
        decisions = [
            _make_decision(tick=1, action="open_long", decision_id=1, size="invalid"),
            _make_decision(tick=5, action="close", decision_id=2),
        ]
        trades = [
            _make_trade("t_open", decision_id=1),
            _make_trade("t_close", decision_id=2, realized_pnl="50.0"),
        ]
        pairs = _match_open_close_pairs(decisions, trades)
        assert len(pairs) == 1
        assert pairs[0]["open_size"] is None  # Gracefully set to None

    def test_invalid_pnl_values(self):
        """Non-numeric realized_pnl should default to 0."""
        decisions = [
            _make_decision(tick=1, action="open_long", decision_id=1),
            _make_decision(tick=5, action="close", decision_id=2),
        ]
        trades = [
            _make_trade("t_open", decision_id=1),
            _make_trade("t_close", decision_id=2, realized_pnl="not_a_number"),
        ]
        pairs = _match_open_close_pairs(decisions, trades)
        assert len(pairs) == 1
        assert pairs[0]["realized_pnl"] == 0.0

    def test_none_confidence(self):
        """Decisions without confidence should be handled in overconfidence calc."""
        pairs = [{
            "agent_id": "a", "symbol": "BTC",
            "open_tick": 1, "close_tick": 5, "hold_duration": 4,
            "open_size": 0.1, "realized_pnl": 100.0,
            "open_confidence": None,
        }]
        result = calculate_overconfidence(pairs)
        assert result.sufficient_data is False

    def test_zero_duration_pairs(self):
        """Hold duration of 0 should not cause division errors."""
        pairs = _make_pairs(15, 15, winner_duration=0, loser_duration=0)
        result = calculate_disposition_effect(pairs)
        assert result.sufficient_data is True
        assert result.value == 0.5  # 0 / (0 + 0) → default to 0.5
