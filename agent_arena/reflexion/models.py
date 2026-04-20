"""Reflexion data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class TradeReflection:
    """Structured reflection on a closed trade."""

    agent_id: str
    trade_id: str = ""
    decision_id: Optional[int] = None
    symbol: str = ""
    side: str = ""
    entry_price: float = 0.0
    exit_price: float = 0.0
    realized_pnl: float = 0.0

    # Structured reflection fields
    market_regime: str = ""            # e.g., "trending_up", "ranging", "volatile"
    entry_signal: str = ""             # What triggered the entry
    outcome: str = ""                  # "win", "loss", "breakeven"
    what_went_right: str = ""
    what_went_wrong: str = ""
    lesson: str = ""                   # Key takeaway

    # Embedding for similarity search
    lesson_embedding: Optional[list[float]] = None

    confidence: float = 0.5

    # Metabolic memory fields
    metabolic_score: float = 1.0
    last_accessed: Optional[datetime] = None
    access_count: int = 0
    is_digested: bool = False

    created_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "trade_id": self.trade_id,
            "decision_id": self.decision_id,
            "symbol": self.symbol,
            "side": self.side,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "realized_pnl": self.realized_pnl,
            "market_regime": self.market_regime,
            "entry_signal": self.entry_signal,
            "outcome": self.outcome,
            "what_went_right": self.what_went_right,
            "what_went_wrong": self.what_went_wrong,
            "lesson": self.lesson,
            "confidence": self.confidence,
            "metabolic_score": self.metabolic_score,
            "access_count": self.access_count,
            "is_digested": self.is_digested,
        }

    @classmethod
    def from_row(cls, row) -> TradeReflection:
        """Construct from a database row (asyncpg Record or dict-like)."""
        return cls(
            agent_id=row["agent_id"],
            trade_id=row.get("trade_id", ""),
            decision_id=row.get("decision_id"),
            symbol=row.get("symbol", ""),
            side=row.get("side", ""),
            entry_price=float(row.get("entry_price", 0)),
            exit_price=float(row.get("exit_price", 0)),
            realized_pnl=float(row.get("realized_pnl", 0)),
            market_regime=row.get("market_regime", ""),
            entry_signal=row.get("entry_signal", ""),
            outcome=row.get("outcome", ""),
            what_went_right=row.get("what_went_right", ""),
            what_went_wrong=row.get("what_went_wrong", ""),
            lesson=row.get("lesson", ""),
            confidence=row.get("confidence", 0.5),
            metabolic_score=row.get("metabolic_score", 1.0),
            access_count=row.get("access_count", 0),
            is_digested=row.get("is_digested", False),
            created_at=row.get("created_at"),
        )

    @classmethod
    def from_dict(cls, data: dict) -> TradeReflection:
        return cls(
            agent_id=data.get("agent_id", ""),
            trade_id=data.get("trade_id", ""),
            decision_id=data.get("decision_id"),
            symbol=data.get("symbol", ""),
            side=data.get("side", ""),
            entry_price=data.get("entry_price", 0.0),
            exit_price=data.get("exit_price", 0.0),
            realized_pnl=data.get("realized_pnl", 0.0),
            market_regime=data.get("market_regime", ""),
            entry_signal=data.get("entry_signal", ""),
            outcome=data.get("outcome", ""),
            what_went_right=data.get("what_went_right", ""),
            what_went_wrong=data.get("what_went_wrong", ""),
            lesson=data.get("lesson", ""),
            confidence=data.get("confidence", 0.5),
            metabolic_score=data.get("metabolic_score", 1.0),
            access_count=data.get("access_count", 0),
            is_digested=data.get("is_digested", False),
        )
