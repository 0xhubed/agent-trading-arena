"""Archive Service - Long-term storage for competition analysis.

This service handles archiving competition data to PostgreSQL for long-term
analysis. It's designed to run alongside the main SQLite storage without
blocking the competition runner.

Features:
- Daily performance snapshots per agent
- Decision archival with full context
- Skill version tracking
- Embedding generation for similarity search (optional)
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

try:
    from openai import AsyncOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    AsyncOpenAI = None


@dataclass
class DailyStats:
    """Accumulated stats for a single day."""

    agent_id: str
    date: date
    starting_equity: float
    ending_equity: float = 0.0
    trade_count: int = 0
    win_count: int = 0
    loss_count: int = 0
    total_volume: float = 0.0
    total_fees: float = 0.0
    total_funding: float = 0.0
    max_equity: float = 0.0
    min_equity: float = float("inf")
    confidences: list[float] = field(default_factory=list)
    regimes: dict[str, int] = field(default_factory=dict)
    symbols: dict[str, int] = field(default_factory=dict)

    @property
    def daily_pnl(self) -> float:
        return self.ending_equity - self.starting_equity

    @property
    def daily_pnl_pct(self) -> float:
        if self.starting_equity > 0:
            return (self.daily_pnl / self.starting_equity) * 100
        return 0.0

    @property
    def max_drawdown_pct(self) -> float:
        if self.max_equity > 0:
            return ((self.max_equity - self.min_equity) / self.max_equity) * 100
        return 0.0

    @property
    def avg_confidence(self) -> Optional[float]:
        if self.confidences:
            return sum(self.confidences) / len(self.confidences)
        return None

    def to_dict(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "date": self.date,
            "starting_equity": self.starting_equity,
            "ending_equity": self.ending_equity,
            "daily_pnl": self.daily_pnl,
            "daily_pnl_pct": self.daily_pnl_pct,
            "trade_count": self.trade_count,
            "win_count": self.win_count,
            "loss_count": self.loss_count,
            "total_volume": self.total_volume,
            "total_fees": self.total_fees,
            "total_funding": self.total_funding,
            "max_drawdown_pct": self.max_drawdown_pct,
            "avg_confidence": self.avg_confidence,
            "regime_distribution": self.regimes,
            "symbol_distribution": self.symbols,
        }


class ArchiveService:
    """
    Service for archiving competition data to PostgreSQL.

    Usage:
        archive = ArchiveService(postgres_storage)
        await archive.initialize()

        # During competition
        archive.track_decision(agent_id, decision, context)
        archive.track_trade(agent_id, trade)
        archive.update_equity(agent_id, equity)

        # End of day
        await archive.flush_daily_snapshots()

        # End of competition
        await archive.finalize_session()
    """

    def __init__(
        self,
        storage: Any,
        skills_dir: str | Path = ".claude/skills",
        generate_embeddings: bool = False,
        embedding_model: str = "text-embedding-3-small",
    ):
        self.storage = storage
        self.skills_dir = Path(skills_dir)
        self.generate_embeddings = generate_embeddings and OPENAI_AVAILABLE
        self.embedding_model = embedding_model

        # Session tracking
        self.session_id: Optional[str] = None
        self.session_started: Optional[datetime] = None

        # Daily stats accumulator (per agent)
        self._daily_stats: dict[str, DailyStats] = {}
        self._current_date: Optional[date] = None

        # Pending decisions waiting for outcome
        self._pending_decisions: dict[int, dict] = {}  # archive_id -> decision info

        # OpenAI client for embeddings
        self._openai: Optional[AsyncOpenAI] = None
        if self.generate_embeddings:
            self._openai = AsyncOpenAI()

    async def initialize(self, session_id: str, name: str, config: dict) -> None:
        """Initialize a new archive session."""
        self.session_id = session_id
        self.session_started = datetime.utcnow()
        self._current_date = self.session_started.date()

        # Start session in database
        await self.storage.start_competition_session(
            session_id=session_id,
            name=name,
            config=config,
        )

        # Archive current skill versions
        await self._archive_skill_versions()

    async def _archive_skill_versions(self) -> dict[str, str]:
        """Archive current skill versions and return version hashes."""
        versions = {}
        if not self.skills_dir.exists():
            return versions

        for skill_dir in self.skills_dir.iterdir():
            if not skill_dir.is_dir():
                continue

            skill_file = skill_dir / "SKILL.md"
            meta_file = skill_dir / ".skill_meta.json"

            if not skill_file.exists():
                continue

            content = skill_file.read_text()
            version_hash = hashlib.sha256(content.encode()).hexdigest()[:16]

            metadata = {}
            if meta_file.exists():
                try:
                    metadata = json.loads(meta_file.read_text())
                except (json.JSONDecodeError, IOError):
                    pass

            await self.storage.save_skill_version(
                skill_name=skill_dir.name,
                version_hash=version_hash,
                content=content,
                metadata=metadata,
            )

            versions[skill_dir.name] = version_hash

        return versions

    def init_agent_daily_stats(self, agent_id: str, starting_equity: float) -> None:
        """Initialize daily stats for an agent."""
        today = datetime.utcnow().date()

        # Check if we need to flush previous day
        if self._current_date and today != self._current_date:
            # Will flush on next opportunity (async)
            pass

        self._current_date = today

        if agent_id not in self._daily_stats:
            self._daily_stats[agent_id] = DailyStats(
                agent_id=agent_id,
                date=today,
                starting_equity=starting_equity,
                ending_equity=starting_equity,
                max_equity=starting_equity,
                min_equity=starting_equity,
            )

    def update_equity(self, agent_id: str, equity: float) -> None:
        """Update equity tracking for an agent."""
        if agent_id not in self._daily_stats:
            self.init_agent_daily_stats(agent_id, equity)
            return

        stats = self._daily_stats[agent_id]
        stats.ending_equity = equity
        stats.max_equity = max(stats.max_equity, equity)
        stats.min_equity = min(stats.min_equity, equity)

    def track_decision(
        self,
        agent_id: str,
        decision: dict,
        context: dict,
    ) -> None:
        """Track a decision (will be archived async later)."""
        if agent_id not in self._daily_stats:
            return

        stats = self._daily_stats[agent_id]

        # Track confidence
        if decision.get("confidence"):
            stats.confidences.append(decision["confidence"])

        # Track regime
        regime = context.get("regime", "unknown")
        stats.regimes[regime] = stats.regimes.get(regime, 0) + 1

        # Track symbol
        if decision.get("symbol"):
            symbol = decision["symbol"]
            stats.symbols[symbol] = stats.symbols.get(symbol, 0) + 1

    def track_trade(self, agent_id: str, trade: dict) -> None:
        """Track a trade."""
        if agent_id not in self._daily_stats:
            return

        stats = self._daily_stats[agent_id]
        stats.trade_count += 1

        # Track win/loss
        pnl = float(trade.get("realized_pnl") or 0)
        if pnl > 0:
            stats.win_count += 1
        elif pnl < 0:
            stats.loss_count += 1

        # Track volume
        size = float(trade.get("size") or 0)
        price = float(trade.get("price") or 0)
        stats.total_volume += size * price

        # Track fees
        fee = float(trade.get("fee") or 0)
        stats.total_fees += fee

    def track_funding(self, agent_id: str, amount: float) -> None:
        """Track funding payment."""
        if agent_id not in self._daily_stats:
            return

        self._daily_stats[agent_id].total_funding += amount

    async def archive_decision_with_context(
        self,
        decision: dict,
        context: dict,
    ) -> Optional[int]:
        """Archive a decision with full context to PostgreSQL."""
        try:
            archive_id = await self.storage.archive_decision(decision, context)

            # Store for outcome tracking
            self._pending_decisions[archive_id] = {
                "agent_id": decision["agent_id"],
                "symbol": decision.get("symbol"),
                "action": decision["action"],
                "tick": decision["tick"],
            }

            # Generate embedding if enabled
            if self.generate_embeddings and self._openai:
                embedding = await self._generate_context_embedding(decision, context)
                if embedding:
                    await self.storage.save_archive_embedding(archive_id, embedding)

            return archive_id
        except Exception as e:
            print(f"Failed to archive decision: {e}")
            return None

    async def _generate_context_embedding(
        self,
        decision: dict,
        context: dict,
    ) -> Optional[list[float]]:
        """Generate embedding for decision context."""
        if not self._openai:
            return None

        try:
            # Build text representation
            text_parts = [
                f"Action: {decision['action']}",
                f"Symbol: {decision.get('symbol', 'N/A')}",
                f"Confidence: {decision.get('confidence', 'N/A')}",
                f"Reasoning: {decision.get('reasoning', '')[:500]}",
                f"Regime: {context.get('regime', 'unknown')}",
            ]

            if context.get("market_prices"):
                prices = context["market_prices"]
                text_parts.append(f"Market: {json.dumps(prices)[:200]}")

            text = " | ".join(text_parts)

            response = await self._openai.embeddings.create(
                model=self.embedding_model,
                input=text,
            )

            return response.data[0].embedding
        except Exception as e:
            print(f"Failed to generate embedding: {e}")
            return None

    async def update_decision_outcome(
        self,
        archive_id: int,
        outcome: dict,
    ) -> None:
        """Update outcome for an archived decision."""
        try:
            await self.storage.update_decision_outcome(archive_id, outcome)
            # Remove from pending
            self._pending_decisions.pop(archive_id, None)
        except Exception as e:
            print(f"Failed to update decision outcome: {e}")

    async def flush_daily_snapshots(self) -> list[int]:
        """Flush accumulated daily stats to PostgreSQL."""
        saved_ids = []

        for agent_id, stats in self._daily_stats.items():
            try:
                snapshot_id = await self.storage.save_daily_snapshot(stats.to_dict())
                saved_ids.append(snapshot_id)
            except Exception as e:
                print(f"Failed to save daily snapshot for {agent_id}: {e}")

        return saved_ids

    async def end_of_day(self, next_day_equities: dict[str, float]) -> None:
        """Handle end of day: flush snapshots and reset for next day."""
        # Flush current day
        await self.flush_daily_snapshots()

        # Get current date before reset
        today = datetime.utcnow().date()

        # Reset for next day
        self._daily_stats.clear()
        self._current_date = today

        # Initialize with new day's starting equity
        for agent_id, equity in next_day_equities.items():
            self.init_agent_daily_stats(agent_id, equity)

    async def finalize_session(
        self,
        total_ticks: int,
        final_leaderboard: list[dict],
    ) -> None:
        """Finalize the archive session."""
        if not self.session_id:
            return

        # Flush any remaining daily stats
        await self.flush_daily_snapshots()

        # Get skill versions
        skill_versions = await self._archive_skill_versions()

        # End session
        await self.storage.end_competition_session(
            session_id=self.session_id,
            total_ticks=total_ticks,
            final_leaderboard=final_leaderboard,
            skill_versions=skill_versions,
        )

    async def get_historical_performance(
        self,
        agent_id: str,
        days: int = 30,
    ) -> dict:
        """Get historical performance for an agent."""
        return await self.storage.get_agent_performance_over_time(agent_id, days)

    async def find_similar_past_decisions(
        self,
        decision: dict,
        context: dict,
        limit: int = 10,
        min_outcome_pnl: Optional[float] = None,
    ) -> list[dict]:
        """Find similar past decisions using embeddings."""
        if not self.generate_embeddings or not self._openai:
            return []

        embedding = await self._generate_context_embedding(decision, context)
        if not embedding:
            return []

        return await self.storage.find_similar_archived_decisions(
            embedding=embedding,
            limit=limit,
            min_outcome_pnl=min_outcome_pnl,
            regime=context.get("regime"),
        )
