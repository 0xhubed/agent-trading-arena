"""Forum observer scheduler - runs forum analysis every N hours."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from agent_arena.agents.observer_agent import ObserverAgent

logger = logging.getLogger(__name__)


class ForumObserverScheduler:
    """Schedules periodic forum analysis by the Observer.

    Runs analyze_forum_window() every N hours to:
    - Correlate forum discussions with trading outcomes
    - Generate witness summaries
    - Store results in database for forum-aware traders
    """

    def __init__(
        self,
        observer: ObserverAgent,
        interval_hours: int = 3,
        symbols: Optional[list[str]] = None,
    ):
        """Initialize scheduler.

        Args:
            observer: ObserverAgent instance
            interval_hours: Hours between runs (default 3)
            symbols: Symbols to analyze (None = all)
        """
        self.observer = observer
        self.interval_hours = interval_hours
        self.symbols = symbols
        self.running = False
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Start the scheduler loop."""
        if self.running:
            logger.warning("Forum observer scheduler already running")
            return

        self.running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info(
            f"Forum observer scheduler started (every {self.interval_hours}h)"
        )

    async def stop(self) -> None:
        """Stop the scheduler loop."""
        async with self._lock:
            if not self.running:
                return

            self.running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass

        logger.info("Forum observer scheduler stopped")

    async def _run_loop(self) -> None:
        """Main scheduler loop."""
        while self.running:
            try:
                # Run forum analysis
                await self._run_analysis()

                # Sleep until next run
                sleep_seconds = self.interval_hours * 3600
                logger.info(
                    f"Next forum analysis in {self.interval_hours} hours "
                    f"({(datetime.now(timezone.utc) + timedelta(seconds=sleep_seconds)).isoformat()})"
                )
                await asyncio.sleep(sleep_seconds)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in forum observer loop: {e}", exc_info=True)
                # Wait 5 minutes before retrying on error
                await asyncio.sleep(300)

    async def _run_analysis(self) -> None:
        """Run a single forum analysis cycle."""
        try:
            logger.info("Starting forum analysis cycle...")

            result = await self.observer.analyze_forum_window(
                hours=self.interval_hours,
                symbols=self.symbols,
            )

            logger.info(
                f"Forum analysis complete: "
                f"{result['messages_analyzed']} messages, "
                f"{result['trades_analyzed']} trades, "
                f"{result['witness_generated']} witness summaries generated"
            )

        except Exception as e:
            logger.error(f"Forum analysis failed: {e}", exc_info=True)

    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self.running


async def run_forum_observer_standalone(
    storage,
    interval_hours: int = 3,
    symbols: Optional[list[str]] = None,
    model: str = "claude-opus-4-6",
) -> None:
    """Run forum observer as a standalone process.

    Usage:
        python -m agent_arena.forum.observer_scheduler

    Args:
        storage: PostgresStorage instance
        interval_hours: Hours between analyses
        symbols: Symbols to track (None = all)
        model: LLM model for analysis
    """
    from agent_arena.agents.observer_agent import ObserverAgent

    # Create observer
    observer = ObserverAgent(
        storage=storage,
        model=model,
        skills_dir=".claude/skills",
    )

    # Create and start scheduler
    scheduler = ForumObserverScheduler(
        observer=observer,
        interval_hours=interval_hours,
        symbols=symbols,
    )

    try:
        await scheduler.start()

        # Run indefinitely
        while True:
            await asyncio.sleep(60)

    except KeyboardInterrupt:
        logger.info("Shutting down forum observer...")
        await scheduler.stop()


if __name__ == "__main__":
    import os
    from agent_arena.storage.postgres import PostgresStorage

    # Get database connection from environment
    db_url = os.getenv(
        "POSTGRES_URL",
        "postgresql://localhost:5432/arena"
    )

    async def main():
        storage = PostgresStorage(db_url)

        try:
            await storage.initialize()
            await run_forum_observer_standalone(storage)
        finally:
            await storage.close()

    # Run
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    asyncio.run(main())
