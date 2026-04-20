"""Data models for behavioral bias analysis."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class BiasScore:
    """Score for a single behavioral bias metric."""

    bias_type: str  # "disposition_effect", "loss_aversion", "overconfidence"
    value: Optional[float]  # 0-1 normalized score, None if insufficient data
    sample_size: int
    sufficient_data: bool
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "bias_type": self.bias_type,
            "value": round(self.value, 4) if self.value is not None else None,
            "sample_size": self.sample_size,
            "sufficient_data": self.sufficient_data,
            "details": self.details,
        }

    def summary(self) -> str:
        """Human-readable summary."""
        if not self.sufficient_data:
            return f"{self.bias_type}: INSUFFICIENT DATA ({self.sample_size} samples)"
        rating = _rating(self.value)
        return (
            f"{self.bias_type}: {self.value:.2f} ({rating}) "
            f"[{self.sample_size} samples]"
        )


@dataclass
class BiasProfile:
    """Complete bias profile for one agent."""

    agent_id: str
    timestamp: datetime
    disposition_effect: BiasScore
    loss_aversion: BiasScore
    overconfidence: BiasScore

    def to_dict(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "timestamp": self.timestamp.isoformat(),
            "disposition_effect": self.disposition_effect.to_dict(),
            "loss_aversion": self.loss_aversion.to_dict(),
            "overconfidence": self.overconfidence.to_dict(),
        }

    @property
    def scores(self) -> list[BiasScore]:
        return [self.disposition_effect, self.loss_aversion, self.overconfidence]


def _rating(value: Optional[float]) -> str:
    """Convert a 0-1 bias score to a human-readable rating."""
    if value is None:
        return "N/A"
    if value < 0.3:
        return "LOW"
    if value < 0.6:
        return "MODERATE"
    return "HIGH"
