"""Tests for ExperimentOrchestrator config validation and candidate identification."""

import pytest

from agent_arena.experiment.orchestrator import ExperimentConfig, ExperimentResult


class TestExperimentConfig:
    def test_default_config_valid(self):
        config = ExperimentConfig(backtest_start="2025-01-01", backtest_end="2025-01-07")
        assert config.validate() == []

    def test_missing_dates_and_scenarios(self):
        config = ExperimentConfig()
        errors = config.validate()
        assert any("backtest_start" in e or "scenario_ids" in e for e in errors)

    def test_population_too_small(self):
        config = ExperimentConfig(
            population_size=2,
            backtest_start="2025-01-01",
            backtest_end="2025-01-07",
        )
        errors = config.validate()
        assert any("population_size" in e for e in errors)

    def test_zero_budget(self):
        config = ExperimentConfig(
            budget_limit_usd=0,
            backtest_start="2025-01-01",
            backtest_end="2025-01-07",
        )
        errors = config.validate()
        assert any("budget" in e for e in errors)

    def test_elite_exceeds_population(self):
        config = ExperimentConfig(
            population_size=4,
            elite_count=5,
            backtest_start="2025-01-01",
            backtest_end="2025-01-07",
        )
        errors = config.validate()
        assert any("elite_count" in e for e in errors)

    def test_valid_with_scenarios(self):
        config = ExperimentConfig(scenario_ids=["btc_crash_2025"])
        assert config.validate() == []


class TestExperimentResult:
    def test_to_dict(self):
        result = ExperimentResult(
            experiment_id="exp_test123",
            status="completed",
            best_fitness=0.85,
            validation_fitness=0.72,
            overfit_warning=False,
            total_cost_usd=2.34,
            generations_completed=5,
            promotion_candidates=[{"genome": {}, "fitness": 0.85}],
        )
        d = result.to_dict()
        assert d["experiment_id"] == "exp_test123"
        assert d["status"] == "completed"
        assert d["best_fitness"] == pytest.approx(0.85)
        assert len(d["promotion_candidates"]) == 1

    def test_default_result(self):
        result = ExperimentResult()
        assert result.status == "pending"
        assert result.promotion_candidates == []
