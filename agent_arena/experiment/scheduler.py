"""Experiment Scheduler — decides whether to run an overnight experiment."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)


class ExperimentScheduler:
    """Integrates with the daily analysis loop to decide whether to launch
    an overnight experiment cycle.

    Decision factors:
    - Has enough data been collected since last experiment?
    - Is the budget within limits for the month?
    - Are there recent journal findings suggesting improvement opportunities?
    - Was the last experiment recent enough to skip?
    """

    def __init__(
        self,
        storage: Any,
        min_ticks_since_last: int = 96,  # ~24h at 15min intervals
        min_days_between_runs: int = 1,
        monthly_budget_usd: float = 50.0,
    ):
        self.storage = storage
        self.min_ticks_since_last = min_ticks_since_last
        self.min_days_between_runs = min_days_between_runs
        self.monthly_budget_usd = monthly_budget_usd

    async def should_run_tonight(self) -> tuple[bool, str]:
        """Decide whether to run an experiment tonight.

        Returns (should_run, reason) tuple.
        """
        # Check if we have recent experiment runs
        last_run = await self._get_last_experiment()
        if last_run:
            days_since = (datetime.now(timezone.utc) - last_run["created_at"]).days
            if days_since < self.min_days_between_runs:
                return False, f"Last experiment was {days_since} days ago (min: {self.min_days_between_runs})"

        # Check monthly budget
        monthly_spent = await self._get_monthly_spend()
        if monthly_spent >= self.monthly_budget_usd:
            return False, f"Monthly budget exhausted: ${monthly_spent:.2f} / ${self.monthly_budget_usd:.2f}"

        # Check if there are journal findings suggesting opportunities
        has_findings = await self._has_recent_findings()
        if not has_findings:
            return False, "No recent journal findings to act on"

        remaining = self.monthly_budget_usd - monthly_spent
        return True, f"Ready to run (${remaining:.2f} budget remaining)"

    async def _get_last_experiment(self) -> Optional[dict]:
        """Get the most recent experiment run."""
        if not hasattr(self.storage, "pool"):
            return None

        try:
            async with self.storage.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT id, name, status, created_at, total_cost_usd
                    FROM experiment_runs
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                )
                if row:
                    return dict(row)
        except Exception:
            logger.debug("No experiment_runs table yet")
        return None

    async def _get_monthly_spend(self) -> float:
        """Get total experiment cost this month."""
        if not hasattr(self.storage, "pool"):
            return 0.0

        try:
            async with self.storage.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT COALESCE(SUM(total_cost_usd), 0) as total
                    FROM experiment_runs
                    WHERE created_at >= date_trunc('month', NOW())
                    """
                )
                return float(row["total"]) if row else 0.0
        except Exception:
            return 0.0

    async def _has_recent_findings(self) -> bool:
        """Check if recent journal entries have actionable findings."""
        if not hasattr(self.storage, "pool"):
            return True  # Default to yes for SQLite

        try:
            async with self.storage.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT COUNT(*) as cnt
                    FROM observer_journal
                    WHERE generated_at >= NOW() - INTERVAL '3 days'
                    AND (recommendations IS NOT NULL AND recommendations != '')
                    """
                )
                return (row["cnt"] if row else 0) > 0
        except Exception:
            return True  # Default to yes if table doesn't exist yet
