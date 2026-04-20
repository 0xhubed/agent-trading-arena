"""Tests for MemoryScorer — decay curves, normalization, threshold classification."""

import pytest
from datetime import datetime, timedelta, timezone

from agent_arena.memory.scoring import (
    DIGEST_THRESHOLD,
    HALF_LIFE_DAYS,
    PRUNE_THRESHOLD,
    MemoryScorer,
)
from agent_arena.memory.models import ScoredMemory


class TestRecency:
    def test_just_created(self):
        now = datetime.now(timezone.utc)
        score = MemoryScorer._recency(now, now)
        assert score == pytest.approx(1.0, abs=0.01)

    def test_one_half_life(self):
        now = datetime.now(timezone.utc)
        created = now - timedelta(days=HALF_LIFE_DAYS)
        score = MemoryScorer._recency(created, now)
        assert score == pytest.approx(0.5, abs=0.01)

    def test_two_half_lives(self):
        now = datetime.now(timezone.utc)
        created = now - timedelta(days=HALF_LIFE_DAYS * 2)
        score = MemoryScorer._recency(created, now)
        assert score == pytest.approx(0.25, abs=0.01)

    def test_very_old(self):
        now = datetime.now(timezone.utc)
        created = now - timedelta(days=365)
        score = MemoryScorer._recency(created, now)
        assert score < 0.01

    def test_none_returns_zero(self):
        now = datetime.now(timezone.utc)
        score = MemoryScorer._recency(None, now)
        assert score == 0.0

    def test_future_returns_one(self):
        now = datetime.now(timezone.utc)
        created = now + timedelta(days=1)
        score = MemoryScorer._recency(created, now)
        assert score == 1.0


class TestImpact:
    def test_zero_pnl(self):
        assert MemoryScorer._impact(0.0, 100.0) == 0.0

    def test_max_pnl(self):
        assert MemoryScorer._impact(100.0, 100.0) == pytest.approx(1.0)

    def test_half_pnl(self):
        assert MemoryScorer._impact(50.0, 100.0) == pytest.approx(0.5)

    def test_negative_pnl_uses_abs(self):
        assert MemoryScorer._impact(-75.0, 100.0) == pytest.approx(0.75)

    def test_zero_max_pnl(self):
        assert MemoryScorer._impact(50.0, 0.0) == 0.0

    def test_over_max_capped(self):
        assert MemoryScorer._impact(200.0, 100.0) == 1.0


class TestFrequency:
    def test_zero_access(self):
        assert MemoryScorer._frequency(0) == 0.0

    def test_one_access(self):
        score = MemoryScorer._frequency(1)
        assert 0.0 < score < 0.5

    def test_twenty_accesses(self):
        score = MemoryScorer._frequency(20)
        assert score == pytest.approx(1.0, abs=0.01)

    def test_monotonic_increasing(self):
        prev = 0.0
        for count in range(1, 25):
            score = MemoryScorer._frequency(count)
            assert score >= prev
            prev = score

    def test_negative_access(self):
        assert MemoryScorer._frequency(-1) == 0.0


class TestThresholds:
    def test_digest_threshold(self):
        assert DIGEST_THRESHOLD == 0.3

    def test_prune_threshold(self):
        assert PRUNE_THRESHOLD == 0.1

    def test_prune_below_digest(self):
        assert PRUNE_THRESHOLD < DIGEST_THRESHOLD
