"""Tests for FailureClusterer — cluster formation with synthetic data."""

import pytest

from agent_arena.reflexion.clustering import FailureCluster, FailureClusterer


class TestFailureCluster:
    def test_dataclass_defaults(self):
        cluster = FailureCluster(
            cluster_label="RSI Reversal Failures",
            regime="trending_up",
            failure_mode="Shorting against trend based on RSI alone",
        )
        assert cluster.cluster_label == "RSI Reversal Failures"
        assert cluster.sample_size == 0
        assert cluster.proposed_skill_validated is False
        assert cluster.reflection_ids == []


class TestFailureClustererGrouping:
    def test_group_by_regime(self):
        clusterer = FailureClusterer(storage=None, min_cluster_size=2)

        reflections = [
            {"market_regime": "trending_up", "symbol": "BTC"},
            {"market_regime": "trending_up", "symbol": "ETH"},
            {"market_regime": "ranging", "symbol": "SOL"},
            {"market_regime": "ranging", "symbol": "BNB"},
            {"market_regime": None, "symbol": "DOGE"},
        ]

        groups = clusterer._group_by_regime(reflections)
        assert "trending_up" in groups
        assert len(groups["trending_up"]) == 2
        assert "ranging" in groups
        assert len(groups["ranging"]) == 2
        assert "unknown" in groups  # None maps to "unknown"

    def test_empty_reflections(self):
        clusterer = FailureClusterer(storage=None)
        groups = clusterer._group_by_regime([])
        assert groups == {}
