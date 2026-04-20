"""Tests for the Contagion Tracker analysis module."""

from __future__ import annotations

import pytest

from agent_arena.analysis.contagion import (
    ContagionScore,
    ContagionSnapshot,
    analyze_contagion,
    calculate_position_diversity,
    calculate_reasoning_entropy,
)


# --- Helpers ---


def _make_decision(
    agent_id: str = "a1",
    tick: int = 1,
    action: str = "open_long",
    symbol: str = "BTCUSDT",
    reasoning: str = "I see bullish momentum building",
    confidence: float = 0.8,
) -> dict:
    return {
        "id": tick,
        "agent_id": agent_id,
        "tick": tick,
        "timestamp": f"2025-01-01T00:{tick:02d}:00",
        "action": action,
        "symbol": symbol,
        "size": "0.1",
        "leverage": 5,
        "confidence": confidence,
        "reasoning": reasoning,
        "metadata": {},
    }


def _group_by_agent(decisions: list[dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for d in decisions:
        groups.setdefault(d["agent_id"], []).append(d)
    return groups


# --- ContagionScore tests ---


class TestContagionScore:
    def test_to_dict_with_value(self):
        s = ContagionScore("position_diversity", 0.65, 50, True)
        d = s.to_dict()
        assert d["metric_type"] == "position_diversity"
        assert d["value"] == 0.65
        assert d["sufficient_data"] is True

    def test_to_dict_none_value(self):
        s = ContagionScore("position_diversity", None, 1, False)
        d = s.to_dict()
        assert d["value"] is None

    def test_summary_insufficient(self):
        s = ContagionScore("reasoning_entropy", None, 1, False)
        assert "INSUFFICIENT DATA" in s.summary()

    def test_summary_healthy(self):
        s = ContagionScore("position_diversity", 0.7, 50, True)
        assert "HEALTHY" in s.summary()


# --- Position Diversity tests ---


class TestPositionDiversity:
    def test_identical_actions_zero_diversity(self):
        """All agents do the same thing → diversity = 0."""
        decisions = _group_by_agent([
            _make_decision("a1", 1, "open_long", "BTCUSDT"),
            _make_decision("a2", 1, "open_long", "BTCUSDT"),
            _make_decision("a3", 1, "open_long", "BTCUSDT"),
        ])
        score = calculate_position_diversity(decisions)
        assert score.sufficient_data is True
        assert score.value == pytest.approx(0.0, abs=0.01)

    def test_all_different_high_diversity(self):
        """All agents do different things → diversity > 0."""
        decisions = _group_by_agent([
            _make_decision("a1", 1, "open_long", "BTCUSDT"),
            _make_decision("a2", 1, "open_short", "ETHUSDT"),
            _make_decision("a3", 1, "hold", ""),
        ])
        score = calculate_position_diversity(decisions)
        assert score.sufficient_data is True
        assert score.value == pytest.approx(1.0, abs=0.01)

    def test_partial_agreement(self):
        """2 of 3 agree → intermediate diversity."""
        decisions = _group_by_agent([
            _make_decision("a1", 1, "open_long", "BTCUSDT"),
            _make_decision("a2", 1, "open_long", "BTCUSDT"),
            _make_decision("a3", 1, "open_short", "BTCUSDT"),
        ])
        score = calculate_position_diversity(decisions)
        assert score.sufficient_data is True
        # 3 pairs: (a1,a2)=agree, (a1,a3)=disagree, (a2,a3)=disagree → 2/3
        assert score.value == pytest.approx(2.0 / 3.0, abs=0.01)

    def test_multiple_ticks_averaged(self):
        """Diversity averaged across ticks."""
        decisions = _group_by_agent([
            # Tick 1: all same
            _make_decision("a1", 1, "open_long", "BTCUSDT"),
            _make_decision("a2", 1, "open_long", "BTCUSDT"),
            # Tick 2: all different
            _make_decision("a1", 2, "open_long", "BTCUSDT"),
            _make_decision("a2", 2, "open_short", "ETHUSDT"),
        ])
        score = calculate_position_diversity(decisions)
        assert score.sufficient_data is True
        # Tick 1: 0.0, Tick 2: 1.0 → avg 0.5
        assert score.value == pytest.approx(0.5, abs=0.01)

    def test_insufficient_agents(self):
        decisions = _group_by_agent([
            _make_decision("a1", 1, "open_long", "BTCUSDT"),
        ])
        score = calculate_position_diversity(decisions, min_agents=2)
        assert score.sufficient_data is False
        assert score.value is None

    def test_empty_input(self):
        score = calculate_position_diversity({})
        assert score.sufficient_data is False

    def test_details_populated(self):
        decisions = _group_by_agent([
            _make_decision("a1", 1, "open_long", "BTCUSDT"),
            _make_decision("a2", 1, "hold", ""),
        ])
        score = calculate_position_diversity(decisions)
        assert "ticks_analyzed" in score.details
        assert "agents" in score.details


# --- Reasoning Entropy tests ---


class TestReasoningEntropy:
    def test_identical_reasoning_zero_entropy(self):
        """Same reasoning text → entropy near 0."""
        same_text = "BTC looks bullish, I will go long on BTCUSDT"
        decisions = _group_by_agent([
            _make_decision("a1", t, reasoning=same_text)
            for t in range(1, 11)
        ] + [
            _make_decision("a2", t, reasoning=same_text)
            for t in range(1, 11)
        ])
        score = calculate_reasoning_entropy(decisions, min_ticks=5)
        assert score.sufficient_data is True
        assert score.value is not None
        assert score.value < 0.1  # Very low entropy

    def test_different_reasoning_high_entropy(self):
        """Completely different reasoning text → high entropy."""
        decisions = _group_by_agent([
            _make_decision("a1", t, reasoning=f"agent one unique analysis number {t} alpha beta")
            for t in range(1, 11)
        ] + [
            _make_decision("a2", t, reasoning=f"second perspective with different vocabulary tick {t} gamma delta")
            for t in range(1, 11)
        ])
        score = calculate_reasoning_entropy(decisions, min_ticks=5)
        assert score.sufficient_data is True
        assert score.value is not None
        assert score.value > 0.5  # High entropy

    def test_empty_reasoning_skipped(self):
        """Empty reasoning text should be skipped."""
        decisions = _group_by_agent([
            _make_decision("a1", t, reasoning="some analysis")
            for t in range(1, 11)
        ] + [
            _make_decision("a2", t, reasoning="")
            for t in range(1, 11)
        ])
        score = calculate_reasoning_entropy(decisions, min_ticks=5)
        # Only 1 agent has reasoning per tick → insufficient
        assert score.sufficient_data is False

    def test_insufficient_agents(self):
        decisions = _group_by_agent([
            _make_decision("a1", t, reasoning="analysis") for t in range(1, 11)
        ])
        score = calculate_reasoning_entropy(decisions)
        assert score.sufficient_data is False

    def test_insufficient_ticks(self):
        decisions = _group_by_agent([
            _make_decision("a1", 1, reasoning="analysis one"),
            _make_decision("a2", 1, reasoning="analysis two"),
        ])
        score = calculate_reasoning_entropy(decisions, min_ticks=5)
        assert score.sufficient_data is False

    def test_details_populated(self):
        decisions = _group_by_agent([
            _make_decision("a1", t, reasoning=f"unique text alpha {t}")
            for t in range(1, 11)
        ] + [
            _make_decision("a2", t, reasoning=f"different text beta {t}")
            for t in range(1, 11)
        ])
        score = calculate_reasoning_entropy(decisions, min_ticks=5)
        assert "ticks_analyzed" in score.details
        assert "agents" in score.details


# --- ContagionSnapshot tests ---


class TestContagionSnapshot:
    def test_to_dict(self):
        snap = ContagionSnapshot(
            timestamp=None,  # will be set
            tick=10,
            position_diversity=ContagionScore("position_diversity", 0.6, 50, True),
            reasoning_entropy=ContagionScore("reasoning_entropy", 0.7, 50, True),
            agent_count=5,
        )
        # Override timestamp for deterministic test
        from datetime import datetime, timezone
        snap.timestamp = datetime(2025, 1, 1, tzinfo=timezone.utc)

        d = snap.to_dict()
        assert d["tick"] == 10
        assert d["agent_count"] == 5
        assert d["position_diversity"]["value"] == 0.6
        assert d["reasoning_entropy"]["value"] == 0.7

    def test_system_health_average(self):
        snap = ContagionSnapshot(
            timestamp=None,
            tick=1,
            position_diversity=ContagionScore("position_diversity", 0.4, 50, True),
            reasoning_entropy=ContagionScore("reasoning_entropy", 0.8, 50, True),
            agent_count=3,
        )
        assert snap.system_health == pytest.approx(0.6, abs=0.01)

    def test_system_health_partial(self):
        """Only one metric has data."""
        snap = ContagionSnapshot(
            timestamp=None,
            tick=1,
            position_diversity=ContagionScore("position_diversity", 0.5, 50, True),
            reasoning_entropy=ContagionScore("reasoning_entropy", None, 1, False),
            agent_count=3,
        )
        assert snap.system_health == pytest.approx(0.5, abs=0.01)

    def test_system_health_none(self):
        snap = ContagionSnapshot(
            timestamp=None,
            tick=1,
            position_diversity=ContagionScore("position_diversity", None, 0, False),
            reasoning_entropy=ContagionScore("reasoning_entropy", None, 0, False),
            agent_count=0,
        )
        assert snap.system_health is None
        assert snap.health_label == "UNKNOWN"

    def test_health_labels(self):
        for val, expected in [(0.7, "HEALTHY"), (0.4, "MODERATE"), (0.2, "WARNING")]:
            snap = ContagionSnapshot(
                timestamp=None, tick=1,
                position_diversity=ContagionScore("position_diversity", val, 50, True),
                reasoning_entropy=ContagionScore("reasoning_entropy", val, 50, True),
                agent_count=3,
            )
            assert snap.health_label == expected


# --- Orchestrator tests ---


class TestAnalyzeContagion:
    def test_full_analysis(self):
        decisions = _group_by_agent([
            _make_decision("a1", t, "open_long", "BTCUSDT", f"bullish analysis {t}")
            for t in range(1, 11)
        ] + [
            _make_decision("a2", t, "open_short", "ETHUSDT", f"bearish perspective {t}")
            for t in range(1, 11)
        ])
        snap = analyze_contagion(decisions, tick=10)
        assert snap.agent_count == 2
        assert snap.tick == 10
        assert snap.position_diversity.sufficient_data is True
        assert snap.reasoning_entropy.sufficient_data is True

    def test_empty_input(self):
        snap = analyze_contagion({})
        assert snap.agent_count == 0
        assert snap.position_diversity.sufficient_data is False
        assert snap.reasoning_entropy.sufficient_data is False
