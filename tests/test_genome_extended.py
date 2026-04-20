"""Tests for extended AgentGenome (HarnessGenome) mutation, crossover, serialization."""

import pytest

from agent_arena.evolution.genome import (
    ALL_AGENTIC_TOOLS,
    ALLOWED_AGENT_CLASSES,
    CHARACTER_TEMPLATES,
    PARAM_BOUNDS,
    PROMPT_TEMPLATES,
    AgentGenome,
)


class TestRandomize:
    def test_creates_valid_genome(self):
        g = AgentGenome.randomize()
        assert g.genome_id.startswith("g_")
        assert g.model in PARAM_BOUNDS["model"]
        assert g.prompt_template in PROMPT_TEMPLATES
        assert 0.1 <= g.temperature <= 1.0
        assert 3 <= len(g.enabled_tools) <= len(ALL_AGENTIC_TOOLS)
        assert 1 <= g.max_iterations <= 5
        assert isinstance(g.use_skills, bool)
        assert 0.0 <= g.skill_weight <= 1.0
        assert 1 <= g.max_concurrent_positions <= 5

    def test_different_each_time(self):
        g1 = AgentGenome.randomize()
        g2 = AgentGenome.randomize()
        assert g1.genome_id != g2.genome_id


class TestMutation:
    def test_creates_new_id(self):
        parent = AgentGenome.randomize()
        child = parent.mutate(rate=1.0)
        assert child.genome_id != parent.genome_id
        assert parent.genome_id in child.parent_ids

    def test_high_rate_produces_mutations(self):
        parent = AgentGenome.randomize()
        child = parent.mutate(rate=1.0)
        assert len(child.mutations) > 0

    def test_zero_rate_no_mutations(self):
        parent = AgentGenome.randomize()
        child = parent.mutate(rate=0.0)
        assert len(child.mutations) == 0

    def test_boolean_toggle_mutation(self):
        parent = AgentGenome(use_skills=True, use_forum=False, use_journal=True)
        # Run many times to catch at least one flip
        any_flipped = False
        for _ in range(100):
            child = parent.mutate(rate=1.0)
            if child.use_skills != parent.use_skills:
                any_flipped = True
                break
        assert any_flipped

    def test_tools_subset_mutation(self):
        parent = AgentGenome(enabled_tools=list(ALL_AGENTIC_TOOLS))
        changed = False
        for _ in range(50):
            child = parent.mutate(rate=1.0)
            if set(child.enabled_tools) != set(parent.enabled_tools):
                changed = True
                break
        assert changed

    def test_continuous_params_stay_in_bounds(self):
        for _ in range(100):
            parent = AgentGenome.randomize()
            child = parent.mutate(rate=1.0)
            assert 0.1 <= child.temperature <= 1.0
            assert 0.3 <= child.confidence_threshold <= 0.9
            assert 0.0 <= child.skill_weight <= 1.0
            assert 0.0 <= child.forum_weight <= 1.0
            assert 0.0 <= child.funding_sensitivity <= 1.0
            assert 0.3 <= child.correlation_threshold <= 0.9

    def test_integer_params_stay_in_bounds(self):
        for _ in range(100):
            parent = AgentGenome.randomize()
            child = parent.mutate(rate=1.0)
            assert 512 <= child.max_tokens <= 4000
            assert 1 <= child.max_leverage <= 10
            assert 1 <= child.max_iterations <= 5
            assert 1 <= child.max_concurrent_positions <= 5


class TestCrossover:
    def test_creates_child_with_both_parents(self):
        p1 = AgentGenome.randomize()
        p2 = AgentGenome.randomize()
        child = p1.crossover(p2)
        assert p1.genome_id in child.parent_ids
        assert p2.genome_id in child.parent_ids

    def test_tools_are_merged(self):
        p1 = AgentGenome(enabled_tools=["technical", "risk"])
        p2 = AgentGenome(enabled_tools=["history", "search"])
        child = p1.crossover(p2)
        # Child should have at least 2 tools and draw from both parents' pools
        assert len(child.enabled_tools) >= 2

    def test_generation_increments(self):
        p1 = AgentGenome(generation=3)
        p2 = AgentGenome(generation=5)
        child = p1.crossover(p2)
        assert child.generation == 6


class TestSerialization:
    def test_round_trip(self):
        original = AgentGenome.randomize()
        data = original.to_dict()
        restored = AgentGenome.from_dict(data)
        assert restored.genome_id == original.genome_id
        assert restored.model == original.model
        assert restored.temperature == original.temperature
        assert restored.prompt_template == original.prompt_template
        assert restored.system_prefix == original.system_prefix
        assert set(restored.enabled_tools) == set(original.enabled_tools)
        assert restored.max_iterations == original.max_iterations
        assert restored.use_skills == original.use_skills
        assert restored.skill_weight == original.skill_weight
        assert restored.use_forum == original.use_forum
        assert restored.forum_weight == original.forum_weight
        assert restored.use_journal == original.use_journal
        assert restored.max_concurrent_positions == original.max_concurrent_positions
        assert restored.correlation_threshold == original.correlation_threshold
        assert restored.funding_sensitivity == original.funding_sensitivity

    def test_backwards_compat_old_genome(self):
        """Old 9-param genomes should deserialize with sane defaults."""
        old_data = {
            "genome_id": "g_old123",
            "model": "glm-5",
            "temperature": 0.7,
            "max_tokens": 1024,
            "character": "test",
            "confidence_threshold": 0.5,
            "position_size_pct": 0.15,
            "sl_pct": 0.02,
            "tp_pct": 0.04,
            "max_leverage": 5,
        }
        g = AgentGenome.from_dict(old_data)
        assert g.prompt_template == "default"
        assert g.system_prefix == ""
        assert g.enabled_tools == list(ALL_AGENTIC_TOOLS)
        assert g.max_iterations == 3
        assert g.use_skills is True
        assert g.use_forum is True
        assert g.use_journal is True
        assert g.max_concurrent_positions == 3


class TestToAgentConfig:
    def test_basic_llm_trader(self):
        g = AgentGenome(
            agent_class="agent_arena.agents.llm_trader.LLMTrader",
            model="gpt-oss-120b",
            use_skills=True,
        )
        cfg = g.to_agent_config("test_id", "Test Agent", "http://localhost:8001/v1", "API_KEY")
        assert cfg["id"] == "test_id"
        assert cfg["class"] == "agent_arena.agents.llm_trader.LLMTrader"
        assert cfg["config"]["model"] == "gpt-oss-120b"
        # LLMTrader shouldn't get skill/forum config
        assert "use_skills" not in cfg["config"]

    def test_skill_aware_trader(self):
        g = AgentGenome(
            agent_class="agent_arena.agents.skill_aware_llm.SkillAwareLLMTrader",
            use_skills=True,
            skill_weight=0.8,
        )
        cfg = g.to_agent_config("test_id", "Test", "", "KEY")
        assert cfg["config"]["use_skills"] is True
        assert cfg["config"]["skill_weight"] == 0.8

    def test_agentic_trader(self):
        g = AgentGenome(
            agent_class="agent_arena.agents.agentic_llm.AgenticLLMTrader",
            enabled_tools=["technical", "risk"],
            max_iterations=2,
        )
        cfg = g.to_agent_config("test_id", "Test", "", "KEY")
        assert cfg["config"]["enabled_tools"] == ["technical", "risk"]
        assert cfg["config"]["max_iterations"] == 2

    def test_system_prefix_prepended(self):
        g = AgentGenome(
            system_prefix="Focus on risk. ",
            character="Aggressive trader.",
        )
        cfg = g.to_agent_config("test", "Test", "", "KEY")
        assert cfg["config"]["character"] == "Focus on risk. Aggressive trader."
