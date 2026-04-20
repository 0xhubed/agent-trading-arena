"""Tests for AbstractPrinciple model."""

import pytest

from agent_arena.memory.models import AbstractPrinciple


class TestAbstractPrinciple:
    def test_to_dict(self):
        p = AbstractPrinciple(
            agent_id="agent_1",
            principle="Low RSI alone is insufficient for long entries",
            source_type="trade_reflection",
            regime="trending_up",
            confidence=0.8,
            application_count=5,
            source_reflection_ids=[1, 2, 3],
        )
        d = p.to_dict()
        assert d["agent_id"] == "agent_1"
        assert d["principle"].startswith("Low RSI")
        assert d["regime"] == "trending_up"
        assert d["confidence"] == 0.8
        assert len(d["source_reflection_ids"]) == 3
        assert d["is_active"] is True

    def test_defaults(self):
        p = AbstractPrinciple(agent_id="test", principle="test rule")
        assert p.source_type == "trade_reflection"
        assert p.regime == ""
        assert p.confidence == 0.5
        assert p.application_count == 0
        assert p.is_active is True
        assert p.source_reflection_ids == []
        assert p.source_pattern_ids == []

    def test_inactive_principle(self):
        p = AbstractPrinciple(
            agent_id="test",
            principle="Deprecated rule",
            is_active=False,
        )
        assert p.is_active is False
        d = p.to_dict()
        assert d["is_active"] is False
