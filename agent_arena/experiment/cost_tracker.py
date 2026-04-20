"""Cost tracking and budget enforcement for experiments."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


class BudgetExceededError(Exception):
    """Raised when an experiment exceeds its budget limit."""

    def __init__(self, spent: float, limit: float):
        self.spent = spent
        self.limit = limit
        super().__init__(
            f"Budget exceeded: ${spent:.2f} spent of ${limit:.2f} limit"
        )


@dataclass
class CostEntry:
    """A single cost entry."""

    description: str
    amount_usd: float
    timestamp: float = field(default_factory=time.time)


class CostTracker:
    """Tracks API costs during an experiment run and enforces budget limits.

    Usage:
        tracker = CostTracker(budget_limit_usd=5.0)
        tracker.record(0.02, "Generation 1: 16 agents x 100 ticks")
        tracker.check_budget()  # raises BudgetExceededError if over limit
    """

    def __init__(self, budget_limit_usd: float = 5.0):
        self.budget_limit_usd = budget_limit_usd
        self.entries: list[CostEntry] = []
        self._total_spent: float = 0.0

    @property
    def total_spent(self) -> float:
        """Total amount spent so far."""
        return self._total_spent

    @property
    def remaining(self) -> float:
        """Budget remaining."""
        return max(0.0, self.budget_limit_usd - self.total_spent)

    def record(self, amount_usd: float, description: str = "") -> None:
        """Record a cost entry."""
        self.entries.append(CostEntry(description=description, amount_usd=amount_usd))
        self._total_spent += amount_usd
        logger.debug(
            "Cost recorded: $%.4f (%s) — total: $%.4f / $%.2f",
            amount_usd, description, self.total_spent, self.budget_limit_usd,
        )

    def check_budget(self) -> None:
        """Raise BudgetExceededError if budget is exceeded."""
        if self.total_spent > self.budget_limit_usd:
            raise BudgetExceededError(self.total_spent, self.budget_limit_usd)

    def estimate_generation_cost(
        self,
        population_size: int,
        total_ticks: int,
        cost_per_tick: float = 0.00015,
    ) -> float:
        """Estimate cost for one generation.

        Default cost_per_tick assumes Together AI GPT-OSS-120B pricing:
        ~2500 input tokens at $0.15/1M + ~250 output tokens at $0.60/1M ≈ $0.000525/tick
        Conservative: $0.00015/tick for cheaper models.
        """
        return population_size * total_ticks * cost_per_tick

    def can_afford_generation(
        self,
        population_size: int,
        total_ticks: int,
        cost_per_tick: float = 0.00015,
    ) -> bool:
        """Check if budget allows another generation."""
        estimated = self.estimate_generation_cost(
            population_size, total_ticks, cost_per_tick
        )
        return (self.total_spent + estimated) <= self.budget_limit_usd

    def to_dict(self) -> dict:
        """Serialize tracker state."""
        return {
            "budget_limit_usd": self.budget_limit_usd,
            "total_spent_usd": round(self.total_spent, 4),
            "remaining_usd": round(self.remaining, 4),
            "num_entries": len(self.entries),
            "entries": [
                {
                    "description": e.description,
                    "amount_usd": round(e.amount_usd, 4),
                    "timestamp": e.timestamp,
                }
                for e in self.entries
            ],
        }
