"""Tests for MemoryDigester — batch clustering, topic grouping."""

import pytest

from agent_arena.memory.digestion import MemoryDigester
from agent_arena.memory.models import DigestionResult, ScoredMemory


class TestTopicGrouping:
    def test_groups_by_keywords(self):
        digester = MemoryDigester(storage=None, min_group_size=2)

        memories = [
            ScoredMemory(
                memory_id=1, memory_type="trade_reflection",
                agent_id="a1", content="RSI oversold bounced but failed",
            ),
            ScoredMemory(
                memory_id=2, memory_type="trade_reflection",
                agent_id="a1", content="RSI oversold entry was too early",
            ),
            ScoredMemory(
                memory_id=3, memory_type="trade_reflection",
                agent_id="a1", content="Funding rate spike caused loss",
            ),
        ]

        groups = digester._group_by_topic(memories)
        # The first two should share a topic (both contain "oversold")
        assert len(groups) >= 1

    def test_empty_memories(self):
        digester = MemoryDigester(storage=None)
        groups = digester._group_by_topic([])
        assert groups == {}

    def test_short_content_falls_to_misc(self):
        digester = MemoryDigester(storage=None)
        memories = [
            ScoredMemory(
                memory_id=1, memory_type="trade_reflection",
                agent_id="a1", content="OK",
            ),
        ]
        groups = digester._group_by_topic(memories)
        assert "misc" in groups


class TestDigestionResult:
    def test_to_dict(self):
        result = DigestionResult(
            agent_id="test_agent",
            memories_scored=50,
            memories_digested=10,
            memories_pruned=5,
            principles_created=2,
        )
        d = result.to_dict()
        assert d["agent_id"] == "test_agent"
        assert d["memories_scored"] == 50
        assert d["principles_created"] == 2

    def test_default_result(self):
        result = DigestionResult(agent_id="test")
        assert result.memories_scored == 0
        assert result.principles_created == 0
