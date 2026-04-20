"""Metabolic Memory data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class ScoredMemory:
    """A memory with its metabolic score components."""

    memory_id: int
    memory_type: str  # "trade_reflection" or "learned_pattern"
    agent_id: str
    content: str = ""

    # Score components (0.0 - 1.0)
    recency_score: float = 0.0      # Exponential decay, 7-day half-life
    impact_score: float = 0.0       # Normalized PnL
    frequency_score: float = 0.0    # Retrieval count (log-scaled)

    # Combined score
    metabolic_score: float = 0.0

    # Classification
    action: str = ""  # "keep", "digest", "prune"

    # Metadata
    created_at: Optional[datetime] = None
    last_accessed: Optional[datetime] = None
    access_count: int = 0
    pnl: float = 0.0

    def to_dict(self) -> dict:
        return {
            "memory_id": self.memory_id,
            "memory_type": self.memory_type,
            "agent_id": self.agent_id,
            "recency_score": round(self.recency_score, 4),
            "impact_score": round(self.impact_score, 4),
            "frequency_score": round(self.frequency_score, 4),
            "metabolic_score": round(self.metabolic_score, 4),
            "action": self.action,
            "access_count": self.access_count,
        }


@dataclass
class AbstractPrinciple:
    """A compressed principle extracted from multiple memories."""

    agent_id: str
    principle: str
    source_type: str = "trade_reflection"  # or "learned_pattern"
    regime: str = ""
    symbol: str = ""
    confidence: float = 0.5
    application_count: int = 0
    source_reflection_ids: list[int] = field(default_factory=list)
    source_pattern_ids: list[int] = field(default_factory=list)
    principle_embedding: Optional[list[float]] = None
    is_active: bool = True

    def to_dict(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "principle": self.principle,
            "source_type": self.source_type,
            "regime": self.regime,
            "symbol": self.symbol,
            "confidence": self.confidence,
            "application_count": self.application_count,
            "source_reflection_ids": self.source_reflection_ids,
            "source_pattern_ids": self.source_pattern_ids,
            "is_active": self.is_active,
        }


@dataclass
class DigestionResult:
    """Result of a memory digestion cycle."""

    agent_id: str
    memories_scored: int = 0
    memories_digested: int = 0
    memories_pruned: int = 0
    principles_created: int = 0
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "memories_scored": self.memories_scored,
            "memories_digested": self.memories_digested,
            "memories_pruned": self.memories_pruned,
            "principles_created": self.principles_created,
            "details": self.details,
        }
