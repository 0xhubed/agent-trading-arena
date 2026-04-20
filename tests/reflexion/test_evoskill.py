"""Tests for EvoSkillValidator — skill derivation and writing."""

import pytest
import tempfile
from pathlib import Path

from agent_arena.reflexion.clustering import FailureCluster
from agent_arena.reflexion.evoskill import EvoSkillValidator, SkillValidationResult


class TestSkillNameDerivation:
    def test_basic_name(self):
        validator = EvoSkillValidator(storage=None)
        cluster = FailureCluster(
            cluster_label="RSI Reversal Failures",
            regime="trending_up",
            failure_mode="test",
        )
        name = validator._derive_skill_name(cluster)
        assert name == "evolved-rsi-reversal-failures"

    def test_special_characters_removed(self):
        validator = EvoSkillValidator(storage=None)
        cluster = FailureCluster(
            cluster_label="High-Confidence (Bad) Trades!",
            regime="",
            failure_mode="test",
        )
        name = validator._derive_skill_name(cluster)
        assert "(" not in name
        assert "!" not in name
        assert name.startswith("evolved-")

    def test_empty_label(self):
        validator = EvoSkillValidator(storage=None)
        cluster = FailureCluster(cluster_label="", failure_mode="test")
        name = validator._derive_skill_name(cluster)
        assert name == "evolved-unknown"

    def test_long_label_truncated(self):
        validator = EvoSkillValidator(storage=None)
        cluster = FailureCluster(
            cluster_label="A" * 100,
            failure_mode="test",
        )
        name = validator._derive_skill_name(cluster)
        assert len(name) <= 58  # "evolved-" + 50 chars


class TestSkillWriting:
    def test_write_skill_creates_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            validator = EvoSkillValidator(
                storage=None,
                skills_dir=tmpdir,
            )
            cluster = FailureCluster(
                cluster_label="Test Pattern",
                regime="trending_up",
                failure_mode="Entering against the trend",
                sample_size=7,
                proposed_skill="Check trend direction before entering.",
            )
            validator._write_skill("evolved-test-pattern", cluster)

            skill_path = Path(tmpdir) / "evolved-test-pattern" / "SKILL.md"
            assert skill_path.exists()
            content = skill_path.read_text()
            assert "Test Pattern" in content
            assert "trending_up" in content
            assert "7 losing trades" in content
            assert "Check trend direction" in content


class TestSkillValidationResult:
    def test_defaults(self):
        result = SkillValidationResult(cluster_id=1, skill_name="test")
        assert result.promoted is False
        assert result.improvement_pct == 0.0
