"""Data models for the forum system."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from uuid import UUID


@dataclass
class ForumMessage:
    """A message posted to the forum."""

    id: UUID
    channel: str
    agent_id: str
    agent_name: str
    agent_type: str
    content: str
    reply_to: Optional[UUID]
    metadata: dict
    created_at: datetime

    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            "id": str(self.id),
            "channel": self.channel,
            "agent_id": self.agent_id,
            "agent_name": self.agent_name,
            "agent_type": self.agent_type,
            "content": self.content,
            "reply_to": str(self.reply_to) if self.reply_to else None,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class WitnessSummary:
    """Observer-generated summary of forum discussions."""

    id: int
    witness_type: str
    insight: str
    confidence: float
    symbols: list[str]
    timeframe: Optional[str]
    based_on: dict
    metadata: dict
    created_at: datetime
    valid_until: Optional[datetime] = None

    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            "id": self.id,
            "witness_type": self.witness_type,
            "insight": self.insight,
            "confidence": self.confidence,
            "symbols": self.symbols,
            "timeframe": self.timeframe,
            "based_on": self.based_on,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "valid_until": self.valid_until.isoformat() if self.valid_until else None,
        }


@dataclass
class ObserverForumRun:
    """Metadata for an Observer forum analysis run."""

    id: UUID
    timestamp: datetime
    window_start: datetime
    window_end: datetime
    messages_analyzed: int
    trades_analyzed: int
    witness_generated: int
    raw_analysis: Optional[str] = None
    summary: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            "id": str(self.id),
            "timestamp": self.timestamp.isoformat(),
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "messages_analyzed": self.messages_analyzed,
            "trades_analyzed": self.trades_analyzed,
            "witness_generated": self.witness_generated,
            "raw_analysis": self.raw_analysis,
            "summary": self.summary,
            "metadata": self.metadata,
        }
