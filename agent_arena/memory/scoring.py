"""Memory Scorer — scores memories by recency, impact, and frequency."""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Any

from agent_arena.memory.models import ScoredMemory

logger = logging.getLogger(__name__)

# Thresholds for memory actions
DIGEST_THRESHOLD = 0.3   # Below this → candidate for digestion
PRUNE_THRESHOLD = 0.1    # Below this → candidate for pruning
HALF_LIFE_DAYS = 7.0     # Exponential decay half-life


class MemoryScorer:
    """Scores memories using recency + impact + frequency.

    Score components:
    - Recency: exponential decay with 7-day half-life
    - Impact: normalized PnL (absolute value relative to max)
    - Frequency: log-scaled retrieval count

    Combined: weighted average → metabolic_score.
    Actions: keep (>= 0.3), digest (0.1 - 0.3), prune (< 0.1).
    """

    def __init__(
        self,
        storage: Any,
        recency_weight: float = 0.4,
        impact_weight: float = 0.35,
        frequency_weight: float = 0.25,
    ):
        self.storage = storage
        self.recency_weight = recency_weight
        self.impact_weight = impact_weight
        self.frequency_weight = frequency_weight

    async def score_memories(self, agent_id: str) -> list[ScoredMemory]:
        """Score all non-digested memories for an agent.

        Returns list of ScoredMemory objects with metabolic_score and action.
        """
        memories = await self._load_memories(agent_id)
        if not memories:
            return []

        # Find max PnL for normalization
        max_pnl = max(
            (abs(m.pnl) for m in memories if m.pnl != 0),
            default=1.0,
        )

        now = datetime.now(timezone.utc)

        for m in memories:
            m.recency_score = self._recency(m.created_at, now)
            m.impact_score = self._impact(m.pnl, max_pnl)
            m.frequency_score = self._frequency(m.access_count)

            m.metabolic_score = (
                self.recency_weight * m.recency_score
                + self.impact_weight * m.impact_score
                + self.frequency_weight * m.frequency_score
            )

            # Classify
            if m.metabolic_score < PRUNE_THRESHOLD:
                m.action = "prune"
            elif m.metabolic_score < DIGEST_THRESHOLD:
                m.action = "digest"
            else:
                m.action = "keep"

        return memories

    @staticmethod
    def _recency(created_at: datetime | None, now: datetime) -> float:
        """Exponential decay with 7-day half-life. Returns 0.0 - 1.0."""
        if not created_at:
            return 0.0

        # Ensure timezone-aware
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)

        age_days = (now - created_at).total_seconds() / 86400.0
        if age_days < 0:
            return 1.0

        decay = math.exp(-math.log(2) * age_days / HALF_LIFE_DAYS)
        return decay

    @staticmethod
    def _impact(pnl: float, max_pnl: float) -> float:
        """Normalized absolute PnL. Returns 0.0 - 1.0."""
        if max_pnl == 0:
            return 0.0
        return min(1.0, abs(pnl) / max_pnl)

    @staticmethod
    def _frequency(access_count: int) -> float:
        """Log-scaled retrieval count. Returns 0.0 - 1.0."""
        if access_count <= 0:
            return 0.0
        # log(1 + count) / log(1 + 20) gives ~1.0 at 20 accesses
        return min(1.0, math.log1p(access_count) / math.log1p(20))

    async def _load_memories(self, agent_id: str) -> list[ScoredMemory]:
        """Load non-digested trade reflections for scoring."""
        if not hasattr(self.storage, "pool"):
            return []

        try:
            async with self.storage.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, agent_id, realized_pnl, lesson,
                           created_at, last_accessed, access_count
                    FROM trade_reflections
                    WHERE agent_id = $1 AND is_digested = FALSE
                    ORDER BY created_at DESC
                    """,
                    agent_id,
                )
                return [
                    ScoredMemory(
                        memory_id=r["id"],
                        memory_type="trade_reflection",
                        agent_id=r["agent_id"],
                        content=r.get("lesson", ""),
                        pnl=float(r.get("realized_pnl", 0)),
                        created_at=r.get("created_at"),
                        last_accessed=r.get("last_accessed"),
                        access_count=r.get("access_count", 0),
                    )
                    for r in rows
                ]
        except Exception:
            logger.exception("Failed to load memories for %s", agent_id)
            return []

    async def update_scores_in_db(self, memories: list[ScoredMemory]) -> None:
        """Write metabolic scores back to the database (batch update)."""
        if not memories or not hasattr(self.storage, "pool"):
            return

        try:
            ids = [m.memory_id for m in memories]
            scores = [m.metabolic_score for m in memories]
            async with self.storage.pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE trade_reflections t
                    SET metabolic_score = v.score
                    FROM unnest($1::int[], $2::real[]) AS v(id, score)
                    WHERE t.id = v.id
                    """,
                    ids,
                    scores,
                )
        except Exception:
            logger.exception("Failed to update memory scores in DB")
