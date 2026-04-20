"""PostgreSQL storage for Observer Agent's self-correcting memory.

Tables are created by PostgresStorage._create_tables() in postgres.py.
This module provides CRUD operations and confidence evolution logic.

Tables:
  observer_runs    — metadata per analysis run (timestamp, stats, raw output)
  observer_memory  — individual pattern lifecycle tracking across runs
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional


class ObserverMemoryStorage:
    """CRUD operations for observer_memory and observer_runs tables."""

    def __init__(self, pool: Any):
        """
        Args:
            pool: asyncpg connection pool from PostgresStorage.
        """
        self.pool = pool

    # ------------------------------------------------------------------
    # Runs
    # ------------------------------------------------------------------

    async def create_run(
        self,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
        decisions_analyzed: int = 0,
        trades_analyzed: int = 0,
        agents_observed: int = 0,
    ) -> str:
        """Create a new observer run and return its UUID."""
        run_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO observer_runs
                    (id, timestamp, window_start, window_end,
                     decisions_analyzed, trades_analyzed, agents_observed)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                uuid.UUID(run_id),
                now,
                window_start,
                window_end,
                decisions_analyzed,
                trades_analyzed,
                agents_observed,
            )
        return run_id

    async def update_run(
        self,
        run_id: str,
        *,
        decisions_analyzed: Optional[int] = None,
        trades_analyzed: Optional[int] = None,
        agents_observed: Optional[int] = None,
        raw_analysis: Optional[str] = None,
        summary: Optional[dict] = None,
        skills_updated: Optional[list[str]] = None,
        patterns_confirmed: int = 0,
        patterns_contradicted: int = 0,
        patterns_new: int = 0,
        patterns_deprecated: int = 0,
        metadata: Optional[dict] = None,
    ) -> None:
        """Update a run record after analysis completes."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE observer_runs SET
                    decisions_analyzed = COALESCE($2, decisions_analyzed),
                    trades_analyzed = COALESCE($3, trades_analyzed),
                    agents_observed = COALESCE($4, agents_observed),
                    raw_analysis = COALESCE($5, raw_analysis),
                    summary = COALESCE($6, summary),
                    skills_updated = COALESCE($7, skills_updated),
                    patterns_confirmed = $8,
                    patterns_contradicted = $9,
                    patterns_new = $10,
                    patterns_deprecated = $11,
                    metadata = COALESCE($12, metadata)
                WHERE id = $1
                """,
                uuid.UUID(run_id),
                decisions_analyzed,
                trades_analyzed,
                agents_observed,
                raw_analysis,
                json.dumps(summary) if summary else None,
                skills_updated,
                patterns_confirmed,
                patterns_contradicted,
                patterns_new,
                patterns_deprecated,
                json.dumps(metadata) if metadata else None,
            )

    # ------------------------------------------------------------------
    # Memory — reading
    # ------------------------------------------------------------------

    async def get_latest_patterns(self) -> list[dict]:
        """
        Get the latest state of every pattern (from the most recent run
        that mentioned it). Returns only active/confirmed/weakened patterns.
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (pattern_id)
                    pattern_id, skill_name, pattern_type, description,
                    status, confidence, sample_size,
                    times_confirmed, times_contradicted,
                    first_seen, last_confirmed, last_contradicted,
                    reasoning, supporting_evidence, contradiction_evidence,
                    metadata
                FROM observer_memory
                WHERE status IN ('active', 'confirmed', 'weakened')
                ORDER BY pattern_id, run_timestamp DESC
            """)
            return [dict(r) for r in rows]

    async def get_all_patterns_latest(self) -> list[dict]:
        """Get the latest state of ALL patterns including deprecated."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (pattern_id)
                    pattern_id, skill_name, pattern_type, description,
                    status, confidence, sample_size,
                    times_confirmed, times_contradicted,
                    first_seen, last_confirmed, last_contradicted,
                    reasoning, metadata
                FROM observer_memory
                ORDER BY pattern_id, run_timestamp DESC
            """)
            return [dict(r) for r in rows]

    async def get_pattern_history(self, pattern_id: str) -> list[dict]:
        """Get the full history of a pattern across all runs."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM observer_memory
                WHERE pattern_id = $1
                ORDER BY run_timestamp ASC
                """,
                pattern_id,
            )
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Memory — writing
    # ------------------------------------------------------------------

    _UPSERT_SQL = """
        INSERT INTO observer_memory (
            run_id, run_timestamp,
            observation_window_start, observation_window_end,
            pattern_id, skill_name, pattern_type, description,
            status, confidence, sample_size,
            times_confirmed, times_contradicted,
            first_seen, last_confirmed, last_contradicted,
            reasoning, supporting_evidence, contradiction_evidence,
            metadata
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13, $14, $15, $16,
            $17, $18, $19, $20
        )
        ON CONFLICT (run_id, pattern_id) DO UPDATE SET
            status = EXCLUDED.status,
            confidence = EXCLUDED.confidence,
            times_confirmed = EXCLUDED.times_confirmed,
            times_contradicted = EXCLUDED.times_contradicted,
            last_confirmed = EXCLUDED.last_confirmed,
            last_contradicted = EXCLUDED.last_contradicted,
            reasoning = EXCLUDED.reasoning,
            supporting_evidence = EXCLUDED.supporting_evidence,
            contradiction_evidence = EXCLUDED.contradiction_evidence,
            metadata = EXCLUDED.metadata
    """

    def _pattern_to_args(self, p: dict) -> tuple:
        """Convert a pattern dict to positional args for the upsert SQL."""
        now = datetime.now(timezone.utc)
        return (
            uuid.UUID(p["run_id"]),
            p["run_timestamp"],
            p.get("window_start"),
            p.get("window_end"),
            p["pattern_id"],
            p["skill_name"],
            p.get("pattern_type", "unknown"),
            p["description"],
            p["status"],
            p["confidence"],
            p.get("sample_size", 0),
            p.get("times_confirmed", 0),
            p.get("times_contradicted", 0),
            p.get("first_seen") or now,
            p.get("last_confirmed"),
            p.get("last_contradicted"),
            p.get("reasoning"),
            json.dumps(p.get("supporting_evidence") or {}),
            json.dumps(p.get("contradiction_evidence") or {}),
            json.dumps(p.get("metadata") or {}),
        )

    async def upsert_pattern(
        self,
        run_id: str,
        run_timestamp: datetime,
        window_start: Optional[datetime],
        window_end: Optional[datetime],
        pattern_id: str,
        skill_name: str,
        pattern_type: str,
        description: str,
        status: str,
        confidence: float,
        sample_size: int = 0,
        times_confirmed: int = 0,
        times_contradicted: int = 0,
        first_seen: Optional[datetime] = None,
        last_confirmed: Optional[datetime] = None,
        last_contradicted: Optional[datetime] = None,
        reasoning: Optional[str] = None,
        supporting_evidence: Optional[dict] = None,
        contradiction_evidence: Optional[dict] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        """Insert or update a single pattern entry for this run."""
        p = {
            "run_id": run_id,
            "run_timestamp": run_timestamp,
            "window_start": window_start,
            "window_end": window_end,
            "pattern_id": pattern_id,
            "skill_name": skill_name,
            "pattern_type": pattern_type,
            "description": description,
            "status": status,
            "confidence": confidence,
            "sample_size": sample_size,
            "times_confirmed": times_confirmed,
            "times_contradicted": times_contradicted,
            "first_seen": first_seen,
            "last_confirmed": last_confirmed,
            "last_contradicted": last_contradicted,
            "reasoning": reasoning,
            "supporting_evidence": supporting_evidence,
            "contradiction_evidence": contradiction_evidence,
            "metadata": metadata,
        }
        async with self.pool.acquire() as conn:
            await conn.execute(self._UPSERT_SQL, *self._pattern_to_args(p))

    async def batch_upsert_patterns(self, patterns: list[dict]) -> None:
        """Upsert multiple patterns in a single transaction."""
        if not patterns:
            return
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for p in patterns:
                    await conn.execute(self._UPSERT_SQL, *self._pattern_to_args(p))

    # ------------------------------------------------------------------
    # Confidence evolution — pure logic (no I/O)
    # ------------------------------------------------------------------

    @staticmethod
    def apply_confirm(confidence: float, times_confirmed: int) -> tuple[float, int, str]:
        """
        Apply confirmation update to a pattern.

        Returns:
            (new_confidence, new_times_confirmed, new_status)
        """
        new_conf = min(0.95, confidence + 0.05 * (1 - confidence))
        new_count = times_confirmed + 1
        status = "confirmed" if new_count >= 3 else "active"
        return new_conf, new_count, status

    @staticmethod
    def apply_contradict(
        confidence: float, times_contradicted: int
    ) -> tuple[float, int, str]:
        """
        Apply contradiction update to a pattern.

        Returns:
            (new_confidence, new_times_contradicted, new_status)
        """
        new_conf = max(0.05, confidence - 0.10)
        new_count = times_contradicted + 1
        if new_conf < 0.2:
            status = "deprecated"
        elif new_conf < 0.4:
            status = "weakened"
        else:
            status = "active"
        return new_conf, new_count, status

    @staticmethod
    def apply_no_data(confidence: float) -> float:
        """Apply slow decay for patterns not seen in current window."""
        return confidence * 0.98

    @staticmethod
    def compute_verdict_update(
        existing: dict,
        verdict: str,
        evidence: str,
        now: datetime,
    ) -> dict:
        """
        Pure function: compute new pattern state from a verdict.

        Separates confidence evolution logic from I/O so it can be
        tested independently and used with batch_upsert_patterns.

        Args:
            existing: Current pattern state dict (from get_latest_patterns)
            verdict: "confirmed", "contradicted", or "no_data"
            evidence: Evidence string from LLM
            now: Current timestamp

        Returns:
            Dict with updated fields: status, confidence, times_confirmed,
            times_contradicted, last_confirmed, last_contradicted,
            reasoning, supporting_evidence, contradiction_evidence
        """
        conf = float(existing.get("confidence", 0.5))
        t_confirmed = int(existing.get("times_confirmed", 0))
        t_contradicted = int(existing.get("times_contradicted", 0))

        if verdict == "confirmed":
            conf, t_confirmed, status = ObserverMemoryStorage.apply_confirm(
                conf, t_confirmed,
            )
            return {
                "status": status,
                "confidence": conf,
                "times_confirmed": t_confirmed,
                "times_contradicted": t_contradicted,
                "last_confirmed": now,
                "last_contradicted": existing.get("last_contradicted"),
                "reasoning": evidence,
                "supporting_evidence": {"latest_evidence": evidence},
                "contradiction_evidence": existing.get("contradiction_evidence") or {},
            }

        if verdict == "contradicted":
            conf, t_contradicted, status = ObserverMemoryStorage.apply_contradict(
                conf, t_contradicted,
            )
            return {
                "status": status,
                "confidence": conf,
                "times_confirmed": t_confirmed,
                "times_contradicted": t_contradicted,
                "last_confirmed": existing.get("last_confirmed"),
                "last_contradicted": now,
                "reasoning": evidence,
                "supporting_evidence": existing.get("supporting_evidence") or {},
                "contradiction_evidence": {"latest_evidence": evidence},
            }

        # no_data — slow decay with proper status transitions (#1 fix)
        conf = ObserverMemoryStorage.apply_no_data(conf)
        status = existing.get("status", "active")
        if conf < 0.2:
            status = "deprecated"
        elif conf < 0.4:
            status = "weakened"
        return {
            "status": status,
            "confidence": conf,
            "times_confirmed": t_confirmed,
            "times_contradicted": t_contradicted,
            "last_confirmed": existing.get("last_confirmed"),
            "last_contradicted": existing.get("last_contradicted"),
            "reasoning": None,
            "supporting_evidence": existing.get("supporting_evidence") or {},
            "contradiction_evidence": existing.get("contradiction_evidence") or {},
        }

    async def get_run_count(self) -> int:
        """Get the total number of observer runs."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM observer_runs")
            return row["cnt"] if row else 0
