"""Data models for the Observer Journal."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any


@dataclass
class AgentDailyStats:
    """Per-agent metrics for a journal period."""

    agent_id: str
    agent_name: str
    trade_count: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    pnl: float = 0.0
    pnl_pct: float = 0.0
    avg_hold_ticks: float = 0.0
    overtrading_score: float = 0.0  # 0-1, higher = more overtrade risk
    hold_count: int = 0
    total_decisions: int = 0
    avg_confidence: float = 0.0
    dominant_action: str = "hold"
    symbols_traded: list[str] = field(default_factory=list)
    agent_type: str = ""  # e.g. "skill_aware", "forum_aware", "simple"
    unrealized_pnl: float = 0.0
    total_pnl: float = 0.0  # realized + unrealized
    open_position_count: int = 0
    open_positions_detail: list[dict] = field(default_factory=list)


@dataclass
class JournalMetrics:
    """Computed metrics for all journal sections."""

    # Per-agent
    agent_stats: dict[str, AgentDailyStats] = field(default_factory=dict)

    # Forum quality
    forum_post_count: int = 0
    forum_accuracy: dict[str, dict] = field(default_factory=dict)

    # Learning delta
    skill_aware_avg_pnl: float = 0.0
    non_skill_avg_pnl: float = 0.0
    forum_aware_avg_pnl: float = 0.0

    # Market summary
    price_changes: dict[str, float] = field(default_factory=dict)
    funding_rates: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to JSON-compatible dict."""
        return {
            "agent_stats": {
                k: {
                    "agent_id": v.agent_id,
                    "agent_name": v.agent_name,
                    "trade_count": v.trade_count,
                    "winning_trades": v.winning_trades,
                    "losing_trades": v.losing_trades,
                    "win_rate": v.win_rate,
                    "pnl": v.pnl,
                    "pnl_pct": v.pnl_pct,
                    "avg_hold_ticks": v.avg_hold_ticks,
                    "overtrading_score": v.overtrading_score,
                    "hold_count": v.hold_count,
                    "total_decisions": v.total_decisions,
                    "avg_confidence": v.avg_confidence,
                    "dominant_action": v.dominant_action,
                    "symbols_traded": v.symbols_traded,
                    "agent_type": v.agent_type,
                    "unrealized_pnl": v.unrealized_pnl,
                    "total_pnl": v.total_pnl,
                    "open_position_count": v.open_position_count,
                    "open_positions_detail": v.open_positions_detail,
                }
                for k, v in self.agent_stats.items()
            },
            "forum_post_count": self.forum_post_count,
            "forum_accuracy": self.forum_accuracy,
            "skill_aware_avg_pnl": self.skill_aware_avg_pnl,
            "non_skill_avg_pnl": self.non_skill_avg_pnl,
            "forum_aware_avg_pnl": self.forum_aware_avg_pnl,
            "price_changes": self.price_changes,
            "funding_rates": self.funding_rates,
        }


@dataclass
class JournalEntry:
    """A complete journal entry."""

    id: str  # UUID
    journal_date: date
    generated_at: datetime
    lookback_hours: int
    full_markdown: str  # Complete article
    market_summary: str = ""
    forum_summary: str = ""
    learning_summary: str = ""
    recommendations: str = ""
    agent_reports: dict[str, str] = field(default_factory=dict)  # {agent_id: report_text}
    metrics: dict[str, Any] = field(default_factory=dict)  # Raw computed metrics
    model: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialize for storage."""
        return {
            "id": self.id,
            "journal_date": self.journal_date.isoformat(),
            "generated_at": self.generated_at.isoformat(),
            "lookback_hours": self.lookback_hours,
            "full_markdown": self.full_markdown,
            "market_summary": self.market_summary,
            "forum_summary": self.forum_summary,
            "learning_summary": self.learning_summary,
            "recommendations": self.recommendations,
            "agent_reports": self.agent_reports,
            "metrics": self.metrics,
            "model": self.model,
        }
