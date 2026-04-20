"""Tests for agent_arena.codegen.findings — pure Python finding detection."""

from __future__ import annotations

import json

import pytest

from agent_arena.codegen.findings import (
    CODEGEN_HISTORY_PATH,
    extract_findings,
    load_codegen_history,
    save_codegen_history,
)


def _make_entry(
    date: str,
    agent_stats: dict | None = None,
    metrics_extra: dict | None = None,
    forum_summary: str = "",
) -> dict:
    """Build a minimal journal entry dict for testing."""
    metrics = {"agent_stats": agent_stats or {}}
    if metrics_extra:
        metrics.update(metrics_extra)
    return {
        "journal_date": date,
        "metrics": metrics,
        "forum_summary": forum_summary,
    }


# ---------------------------------------------------------------------------
# Overtrading
# ---------------------------------------------------------------------------

class TestOvertrading:
    def test_detected_when_recurring(self):
        entries = [
            _make_entry(f"2026-01-0{i}", {"a1": {"overtrading_score": 0.7}})
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "overtrading" in ids

    def test_not_detected_below_threshold(self):
        entries = [
            _make_entry("2026-01-01", {"a1": {"overtrading_score": 0.7}}),
            _make_entry("2026-01-02", {"a1": {"overtrading_score": 0.3}}),
            _make_entry("2026-01-03", {"a1": {"overtrading_score": 0.3}}),
            _make_entry("2026-01-04", {"a1": {"overtrading_score": 0.3}}),
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "overtrading" not in ids

    def test_agent_ids_populated(self):
        entries = [
            _make_entry(f"2026-01-0{i}", {"agent_x": {"overtrading_score": 0.8}})
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        ot = next(f for f in findings if f.finding_id == "overtrading")
        assert "agent_x" in ot.agent_ids


# ---------------------------------------------------------------------------
# High confidence bad PnL
# ---------------------------------------------------------------------------

class TestHighConfBadPnl:
    def test_detected(self):
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                {"b1": {"avg_confidence": 0.85, "pnl": -50.0}},
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "high_conf_bad_pnl" in ids

    def test_not_detected_with_positive_pnl(self):
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                {"b1": {"avg_confidence": 0.85, "pnl": 100.0}},
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "high_conf_bad_pnl" not in ids


# ---------------------------------------------------------------------------
# R:R inversion
# ---------------------------------------------------------------------------

class TestRRInversion:
    def test_detected(self):
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                {"c1": {"win_rate": 0.6, "pnl": -30.0}},
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "rr_inversion" in ids


# ---------------------------------------------------------------------------
# Skill underperform
# ---------------------------------------------------------------------------

class TestSkillUnderperform:
    def test_detected(self):
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                metrics_extra={
                    "skill_aware_avg_pnl": -20.0,
                    "non_skill_avg_pnl": 10.0,
                },
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "skill_underperform" in ids

    def test_not_detected_when_skill_wins(self):
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                metrics_extra={
                    "skill_aware_avg_pnl": 50.0,
                    "non_skill_avg_pnl": 10.0,
                },
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "skill_underperform" not in ids


# ---------------------------------------------------------------------------
# Forum echo
# ---------------------------------------------------------------------------

class TestForumEcho:
    def test_detected_with_echo_chamber(self):
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                forum_summary="Signs of echo chamber in market channel.",
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "forum_echo" in ids

    def test_detected_with_groupthink(self):
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                forum_summary="Possible groupthink observed.",
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "forum_echo" in ids

    def test_not_detected_without_keywords(self):
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                forum_summary="Healthy debate observed.",
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "forum_echo" not in ids


# ---------------------------------------------------------------------------
# Recurrence ratio
# ---------------------------------------------------------------------------

class TestRecurrenceRatio:
    def test_50_pct_threshold(self):
        """With 4 entries and 50% ratio, need ≥2 hits."""
        entries = [
            _make_entry("2026-01-01", {"a1": {"overtrading_score": 0.8}}),
            _make_entry("2026-01-02", {"a1": {"overtrading_score": 0.8}}),
            _make_entry("2026-01-03", {"a1": {"overtrading_score": 0.1}}),
            _make_entry("2026-01-04", {"a1": {"overtrading_score": 0.1}}),
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "overtrading" in ids

    def test_barely_below_threshold(self):
        """With 4 entries and 50% ratio, 1 hit is not enough."""
        entries = [
            _make_entry("2026-01-01", {"a1": {"overtrading_score": 0.8}}),
            _make_entry("2026-01-02", {"a1": {"overtrading_score": 0.1}}),
            _make_entry("2026-01-03", {"a1": {"overtrading_score": 0.1}}),
            _make_entry("2026-01-04", {"a1": {"overtrading_score": 0.1}}),
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "overtrading" not in ids

    def test_severity_ordering(self):
        """Findings should be sorted by severity descending."""
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                {
                    "a1": {
                        "overtrading_score": 0.6,
                        "avg_confidence": 0.9,
                        "pnl": -100.0,
                    },
                },
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        severities = [f.severity for f in findings]
        assert severities == sorted(severities, reverse=True)


class TestEmptyInput:
    def test_empty_entries(self):
        assert extract_findings([]) == []

    def test_entries_with_no_metrics(self):
        entries = [_make_entry(f"2026-01-0{i}") for i in range(1, 6)]
        assert extract_findings(entries) == []


# ---------------------------------------------------------------------------
# Regime blindness
# ---------------------------------------------------------------------------

class TestRegimeBlindness:
    def test_detected_when_most_agents_lose_during_strong_move(self):
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                agent_stats={
                    "a1": {"pnl": -50.0},
                    "a2": {"pnl": -30.0},
                    "a3": {"pnl": 10.0},
                },
                metrics_extra={
                    "price_changes": {"BTCUSDT": 8.5},
                },
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "regime_blindness" in ids

    def test_not_detected_without_strong_move(self):
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                agent_stats={
                    "a1": {"pnl": -50.0},
                    "a2": {"pnl": -30.0},
                },
                metrics_extra={
                    "price_changes": {"BTCUSDT": 2.0},
                },
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "regime_blindness" not in ids

    def test_not_detected_when_agents_profit(self):
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                agent_stats={
                    "a1": {"pnl": 50.0},
                    "a2": {"pnl": 30.0},
                    "a3": {"pnl": -5.0},
                },
                metrics_extra={
                    "price_changes": {"BTCUSDT": -7.0},
                },
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "regime_blindness" not in ids

    def test_target_file(self):
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                agent_stats={"a1": {"pnl": -50.0}},
                metrics_extra={"price_changes": {"ETHUSDT": -10.0}},
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        rb = next(
            f for f in findings if f.finding_id == "regime_blindness"
        )
        assert "agent_arena/agents/llm_trader.py" in rb.target_files


# ---------------------------------------------------------------------------
# Codegen history persistence
# ---------------------------------------------------------------------------

class TestCodegenHistory:
    @pytest.fixture(autouse=True)
    def _isolate_history(self, tmp_path, monkeypatch):
        """Redirect history file to tmp_path for test isolation."""
        self.history_path = tmp_path / "codegen_history.json"
        monkeypatch.setattr(
            "agent_arena.codegen.findings.CODEGEN_HISTORY_PATH",
            self.history_path,
        )

    def test_save_and_load(self):
        save_codegen_history(
            finding_ids=["overtrading", "forum_echo"],
            files_changed=["configs/production.yaml"],
            summary="test",
        )
        assert self.history_path.exists()
        data = json.loads(self.history_path.read_text())
        assert len(data) == 2
        assert data[0]["finding_id"] == "overtrading"
        assert data[1]["finding_id"] == "forum_echo"

        history = load_codegen_history(lookback_days=30)
        assert len(history) == 2

    def test_load_empty_when_no_file(self):
        assert load_codegen_history() == []

    def test_load_filters_old_entries(self):
        old_data = [
            {
                "finding_id": "overtrading",
                "date": "2020-01-01",
                "files_changed": [],
                "summary": "old",
            },
        ]
        self.history_path.write_text(json.dumps(old_data))
        assert load_codegen_history(lookback_days=30) == []

    def test_append_preserves_existing(self):
        save_codegen_history(["first"], ["a.py"])
        save_codegen_history(["second"], ["b.py"])
        data = json.loads(self.history_path.read_text())
        assert len(data) == 2
        assert data[0]["finding_id"] == "first"
        assert data[1]["finding_id"] == "second"


# ---------------------------------------------------------------------------
# Stale fix detection
# ---------------------------------------------------------------------------

class TestStaleFix:
    @pytest.fixture(autouse=True)
    def _isolate_history(self, tmp_path, monkeypatch):
        """Redirect history file to tmp_path for test isolation."""
        self.history_path = tmp_path / "codegen_history.json"
        monkeypatch.setattr(
            "agent_arena.codegen.findings.CODEGEN_HISTORY_PATH",
            self.history_path,
        )

    def _write_history(self, finding_id: str, count: int):
        """Write mock codegen history with *count* entries for *finding_id*."""
        from datetime import datetime, timezone
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        data = [
            {
                "finding_id": finding_id,
                "date": date,
                "files_changed": ["configs/production.yaml"],
                "summary": f"fix attempt {i}",
            }
            for i in range(count)
        ]
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        self.history_path.write_text(json.dumps(data))

    def test_stale_fix_detected_after_3_attempts(self):
        """Overtrading fixed 3x but still recurring → stale_fix emitted."""
        self._write_history("overtrading", 3)
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                {"a1": {"overtrading_score": 0.7}},
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "stale_fix" in ids
        assert "overtrading" in ids  # original finding still present

        stale = next(f for f in findings if f.finding_id == "stale_fix")
        assert stale.stale_finding_id == "overtrading"
        assert stale.prior_fix_count == 3
        assert stale.target_files == ["_escalate"]

    def test_no_stale_fix_below_threshold(self):
        """Only 2 prior fixes → not stale yet."""
        self._write_history("overtrading", 2)
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                {"a1": {"overtrading_score": 0.7}},
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "stale_fix" not in ids

    def test_no_stale_fix_without_history(self):
        """No history file → no stale_fix."""
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                {"a1": {"overtrading_score": 0.7}},
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        ids = [f.finding_id for f in findings]
        assert "stale_fix" not in ids

    def test_stale_fix_severity_bumped(self):
        """Stale fix severity should be original + 0.2, capped at 1.0."""
        self._write_history("overtrading", 5)
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                {"a1": {"overtrading_score": 0.7}},
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        overtrading = next(
            f for f in findings if f.finding_id == "overtrading"
        )
        stale = next(f for f in findings if f.finding_id == "stale_fix")
        assert stale.severity == min(overtrading.severity + 0.2, 1.0)

    def test_stale_fix_sorted_by_severity(self):
        """Stale fix findings should sort correctly with others."""
        self._write_history("overtrading", 4)
        entries = [
            _make_entry(
                f"2026-01-0{i}",
                {"a1": {"overtrading_score": 0.7}},
            )
            for i in range(1, 6)
        ]
        findings = extract_findings(entries)
        severities = [f.severity for f in findings]
        assert severities == sorted(severities, reverse=True)
