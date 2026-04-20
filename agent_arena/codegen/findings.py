"""Programmatic finding detection from Observer Journal metrics.

Pure Python — no LLM needed.  Scans journal entries for recurring
problems and returns structured Finding objects that the codegen
agent can act on.
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

CODEGEN_HISTORY_PATH = Path("data/codegen_history.json")

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Finding:
    """A recurring problem detected across journal entries."""

    finding_id: str
    agent_ids: list[str] = field(default_factory=list)
    severity: float = 0.0  # 0-1
    evidence: list[str] = field(default_factory=list)
    target_files: list[str] = field(default_factory=list)
    entry_dates: list[str] = field(default_factory=list)
    # stale_fix fields (only set for stale_fix findings)
    stale_finding_id: str = ""  # the original finding_id that keeps recurring
    prior_fix_count: int = 0


# ---------------------------------------------------------------------------
# Per-entry detectors
# ---------------------------------------------------------------------------

def _detect_overtrading(entry: dict) -> list[tuple[str, str, float]]:
    """Detect agents with overtrading_score > 0.5."""
    hits: list[tuple[str, str, float]] = []
    metrics = entry.get("metrics") or {}
    agent_stats = metrics.get("agent_stats") or {}
    date = entry.get("journal_date", "?")
    for aid, stats in agent_stats.items():
        score = stats.get("overtrading_score", 0.0)
        if score > 0.5:
            hits.append((
                aid,
                f"[{date}] {aid}: overtrading_score={score:.2f}",
                score,
            ))
    return hits


def _detect_high_conf_bad_pnl(entry: dict) -> list[tuple[str, str, float]]:
    """Detect agents with avg_confidence > 0.7 AND pnl < 0."""
    hits: list[tuple[str, str, float]] = []
    metrics = entry.get("metrics") or {}
    agent_stats = metrics.get("agent_stats") or {}
    date = entry.get("journal_date", "?")
    for aid, stats in agent_stats.items():
        conf = stats.get("avg_confidence", 0.0)
        pnl = stats.get("pnl", 0.0)
        if conf > 0.7 and pnl < 0:
            hits.append((
                aid,
                f"[{date}] {aid}: conf={conf:.2f}, pnl={pnl:.2f}",
                conf,
            ))
    return hits


def _detect_rr_inversion(entry: dict) -> list[tuple[str, str, float]]:
    """Detect agents winning trades but losing money (bad R:R)."""
    hits: list[tuple[str, str, float]] = []
    metrics = entry.get("metrics") or {}
    agent_stats = metrics.get("agent_stats") or {}
    date = entry.get("journal_date", "?")
    for aid, stats in agent_stats.items():
        wr = stats.get("win_rate", 0.0)
        pnl = stats.get("pnl", 0.0)
        if wr > 0.4 and pnl < 0:
            hits.append((
                aid,
                f"[{date}] {aid}: win_rate={wr:.2f} but pnl={pnl:.2f}",
                wr,
            ))
    return hits


def _detect_skill_underperform(entry: dict) -> list[tuple[str, str, float]]:
    """Detect when skill-aware agents underperform non-skill agents."""
    hits: list[tuple[str, str, float]] = []
    metrics = entry.get("metrics") or {}
    sk = metrics.get("skill_aware_avg_pnl", 0.0)
    nsk = metrics.get("non_skill_avg_pnl", 0.0)
    date = entry.get("journal_date", "?")
    if sk < nsk and nsk != 0:
        hits.append((
            "_system",
            f"[{date}] skill_aware_avg_pnl={sk:.2f} < non_skill={nsk:.2f}",
            abs(sk - nsk),
        ))
    return hits


def _detect_forum_echo(entry: dict) -> list[tuple[str, str, float]]:
    """Detect echo-chamber / groupthink signals in forum summary."""
    hits: list[tuple[str, str, float]] = []
    text = (entry.get("forum_summary") or "").lower()
    date = entry.get("journal_date", "?")
    keywords = ["echo chamber", "groupthink", "herd", "uniform"]
    for kw in keywords:
        if kw in text:
            hits.append((
                "_system",
                f"[{date}] forum_summary mentions '{kw}'",
                0.7,
            ))
            break  # one hit per entry is enough
    return hits


# ---------------------------------------------------------------------------
# Codegen history persistence
# ---------------------------------------------------------------------------

def load_codegen_history(lookback_days: int = 30) -> list[dict]:
    """Load recent codegen history entries within *lookback_days*."""
    if not CODEGEN_HISTORY_PATH.exists():
        return []
    try:
        data = json.loads(CODEGEN_HISTORY_PATH.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to load codegen history: %s", exc)
        return []
    if not isinstance(data, list):
        return []
    # Simple date-string comparison works for ISO dates
    cutoff_date = (
        datetime.now(timezone.utc) - timedelta(days=lookback_days)
    ).strftime("%Y-%m-%d")
    return [e for e in data if e.get("date", "") >= cutoff_date]


def save_codegen_history(
    finding_ids: list[str],
    files_changed: list[str],
    summary: str = "",
) -> None:
    """Append entries to the codegen history file."""
    CODEGEN_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    existing: list[dict] = []
    if CODEGEN_HISTORY_PATH.exists():
        try:
            existing = json.loads(CODEGEN_HISTORY_PATH.read_text())
            if not isinstance(existing, list):
                existing = []
        except (json.JSONDecodeError, OSError):
            existing = []

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for fid in finding_ids:
        existing.append({
            "finding_id": fid,
            "date": date_str,
            "files_changed": files_changed,
            "summary": summary,
        })

    CODEGEN_HISTORY_PATH.write_text(
        json.dumps(existing, indent=2) + "\n"
    )


# ---------------------------------------------------------------------------
# Stale fix detector
# ---------------------------------------------------------------------------

STALE_FIX_THRESHOLD = 3  # acted on 3+ times in last 5 entries

def _detect_stale_fix(entry: dict) -> list[tuple[str, str, float]]:
    """Detect findings that have been 'fixed' repeatedly but still recur.

    This detector is special — it runs *after* the normal detectors to
    check whether any current findings were already acted on by codegen
    multiple times without resolution.  It is called by
    ``extract_findings()`` as a post-pass.

    For the per-entry pass required by the detector registry interface,
    this is a no-op.  The real logic lives in ``_check_stale_fixes()``.
    """
    return []  # real logic in _check_stale_fixes()


def _check_stale_fixes(
    current_findings: list["Finding"],
) -> list["Finding"]:
    """Post-pass: emit stale_fix findings for repeatedly-fixed problems."""
    history = load_codegen_history(lookback_days=30)
    if not history:
        return []

    # Count how many times each finding_id was acted on
    fix_counts: Counter[str] = Counter()
    for entry in history:
        fid = entry.get("finding_id", "")
        if fid:
            fix_counts[fid] += 1

    stale: list[Finding] = []
    for f in current_findings:
        count = fix_counts.get(f.finding_id, 0)
        if count >= STALE_FIX_THRESHOLD:
            stale.append(Finding(
                finding_id="stale_fix",
                agent_ids=f.agent_ids,
                severity=min(f.severity + 0.2, 1.0),  # bump severity
                evidence=[
                    f"'{f.finding_id}' was fixed {count} times "
                    f"but still recurs (severity {f.severity:.2f})",
                    *f.evidence[:5],
                ],
                target_files=["_escalate"],
                entry_dates=f.entry_dates,
                stale_finding_id=f.finding_id,
                prior_fix_count=count,
            ))
    return stale


# ---------------------------------------------------------------------------
# Regime blindness detector
# ---------------------------------------------------------------------------

def _detect_regime_blindness(entry: dict) -> list[tuple[str, str, float]]:
    """Detect agents losing money during strong directional moves.

    Fires when most agents have negative PnL while price moved >5%.
    """
    hits: list[tuple[str, str, float]] = []
    metrics = entry.get("metrics") or {}
    agent_stats = metrics.get("agent_stats") or {}
    price_changes = metrics.get("price_changes") or {}
    date = entry.get("journal_date", "?")

    # Check if any symbol had a strong move (>5%)
    strong_move = False
    move_details: list[str] = []
    for symbol, change in price_changes.items():
        if isinstance(change, (int, float)) and abs(change) > 5.0:
            strong_move = True
            move_details.append(f"{symbol}={change:+.1f}%")

    if not strong_move or not agent_stats:
        return hits

    # Count agents with negative PnL
    losers = 0
    total = 0
    for aid, stats in agent_stats.items():
        pnl = stats.get("pnl", 0.0)
        total += 1
        if pnl < 0:
            losers += 1

    if total > 0 and losers / total > 0.5:
        moves_str = ", ".join(move_details)
        hits.append((
            "_system",
            f"[{date}] {losers}/{total} agents lost money "
            f"during strong move ({moves_str})",
            min(losers / total, 1.0),
        ))
    return hits


# ---------------------------------------------------------------------------
# Detector registry
# ---------------------------------------------------------------------------

_DETECTORS: dict[str, dict] = {
    "overtrading": {
        "fn": _detect_overtrading,
        "targets": ["configs/production.yaml"],
    },
    "high_conf_bad_pnl": {
        "fn": _detect_high_conf_bad_pnl,
        "targets": ["configs/production.yaml"],
    },
    "rr_inversion": {
        "fn": _detect_rr_inversion,
        "targets": ["agent_arena/evolution/genome.py"],
    },
    "skill_underperform": {
        "fn": _detect_skill_underperform,
        "targets": ["agent_arena/agents/skill_aware_llm.py"],
    },
    "forum_echo": {
        "fn": _detect_forum_echo,
        "targets": ["agent_arena/agents/forum_aware_llm.py"],
    },
    "regime_blindness": {
        "fn": _detect_regime_blindness,
        "targets": ["agent_arena/agents/llm_trader.py"],
    },
    # stale_fix is handled as a post-pass in extract_findings(), not here.
    # It's registered so that external callers can see it in the registry.
    "stale_fix": {
        "fn": _detect_stale_fix,  # no-op per entry; real logic is post-pass
        "targets": ["_escalate"],
    },
}

# Minimum fraction of entries that must trigger for a finding to be
# considered "recurring".  E.g. 0.5 means ≥50% of entries.
RECURRENCE_RATIO = 0.5


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_findings(entries: list[dict]) -> list[Finding]:
    """Scan journal entries and return recurring findings.

    Only findings present in ≥ RECURRENCE_RATIO of the entries are
    returned.  Entries should already be ordered newest-first and limited
    to the desired lookback window (e.g. last 5).

    After the per-entry scan, a post-pass checks for **stale fixes** —
    findings that codegen has already attempted to fix multiple times
    without resolution.  These are emitted as ``stale_fix`` findings
    with ``target_files=["_escalate"]``.
    """
    findings: list[Finding] = []

    for finding_id, spec in _DETECTORS.items():
        # stale_fix is a post-pass, skip in per-entry loop
        if finding_id == "stale_fix":
            continue

        detect_fn = spec["fn"]
        all_evidence: list[str] = []
        agent_counter: Counter[str] = Counter()
        dates_hit: list[str] = []
        severity_sum = 0.0

        for entry in entries:
            hits = detect_fn(entry)
            if hits:
                date = entry.get("journal_date", "?")
                dates_hit.append(date)
                for aid, evidence_text, sev in hits:
                    agent_counter[aid] += 1
                    all_evidence.append(evidence_text)
                    severity_sum += sev

        min_hits = max(1, int(len(entries) * RECURRENCE_RATIO))
        if len(dates_hit) < min_hits:
            continue

        avg_severity = min(severity_sum / max(len(all_evidence), 1), 1.0)
        agent_ids = [
            aid for aid, _ in agent_counter.most_common()
            if aid != "_system"
        ]

        findings.append(Finding(
            finding_id=finding_id,
            agent_ids=agent_ids,
            severity=round(avg_severity, 3),
            evidence=all_evidence,
            target_files=spec["targets"],
            entry_dates=dates_hit,
        ))

    # Post-pass: detect stale fixes (findings fixed repeatedly but unresolved)
    stale_findings = _check_stale_fixes(findings)
    findings.extend(stale_findings)

    # Sort by severity descending
    findings.sort(key=lambda f: f.severity, reverse=True)
    return findings
