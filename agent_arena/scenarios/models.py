"""Scenario data model."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone


@dataclass
class Scenario:
    """A deterministic, replayable market scenario."""

    scenario_id: str
    name: str
    description: str
    symbols: list[str]
    interval: str  # tick interval (e.g. "5m")
    candle_intervals: list[str]  # intervals stored (e.g. ["5m", "1h"])
    start_date: str  # YYYY-MM-DD
    end_date: str  # YYYY-MM-DD
    total_ticks: int
    checksum: str  # SHA256 of candles.json
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: dict) -> Scenario:
        return cls(**data)

    @classmethod
    def from_json(cls, text: str) -> Scenario:
        return cls.from_dict(json.loads(text))
