"""Tests for CostTracker budget enforcement."""

import pytest

from agent_arena.experiment.cost_tracker import BudgetExceededError, CostTracker


def test_initial_state():
    tracker = CostTracker(budget_limit_usd=10.0)
    assert tracker.total_spent == 0.0
    assert tracker.remaining == 10.0


def test_record_and_total():
    tracker = CostTracker(budget_limit_usd=5.0)
    tracker.record(1.50, "gen 1")
    tracker.record(2.00, "gen 2")
    assert tracker.total_spent == pytest.approx(3.50)
    assert tracker.remaining == pytest.approx(1.50)


def test_check_budget_ok():
    tracker = CostTracker(budget_limit_usd=5.0)
    tracker.record(4.99, "gen 1")
    tracker.check_budget()  # Should not raise


def test_check_budget_exceeded():
    tracker = CostTracker(budget_limit_usd=5.0)
    tracker.record(5.01, "gen 1")
    with pytest.raises(BudgetExceededError) as exc_info:
        tracker.check_budget()
    assert exc_info.value.spent == pytest.approx(5.01)
    assert exc_info.value.limit == 5.0


def test_can_afford_generation():
    tracker = CostTracker(budget_limit_usd=1.0)
    # 16 agents, 100 ticks, $0.00015/tick = $0.24
    assert tracker.can_afford_generation(16, 100) is True
    tracker.record(0.80, "spent")
    # Now only $0.20 remaining, can't afford $0.24
    assert tracker.can_afford_generation(16, 100) is False


def test_estimate_generation_cost():
    tracker = CostTracker(budget_limit_usd=10.0)
    cost = tracker.estimate_generation_cost(16, 100, cost_per_tick=0.001)
    assert cost == pytest.approx(1.6)


def test_to_dict():
    tracker = CostTracker(budget_limit_usd=5.0)
    tracker.record(1.23, "test entry")
    d = tracker.to_dict()
    assert d["budget_limit_usd"] == 5.0
    assert d["total_spent_usd"] == pytest.approx(1.23, abs=0.001)
    assert d["num_entries"] == 1
    assert len(d["entries"]) == 1
    assert d["entries"][0]["description"] == "test entry"


def test_remaining_never_negative():
    tracker = CostTracker(budget_limit_usd=1.0)
    tracker.record(5.0, "over")
    assert tracker.remaining == 0.0
