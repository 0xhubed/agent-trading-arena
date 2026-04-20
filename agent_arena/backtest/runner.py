"""Backtest runner - orchestrates historical simulation."""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Optional

from agent_arena.backtest.results import (
    AgentResult,
    BacktestResult,
    ComparisonResult,
    EquityPoint,
    TradeRecord,
)
from agent_arena.core.agent import BaseAgent
from agent_arena.core.arena import TradingArena
from agent_arena.core.config import CompetitionConfig
from agent_arena.core.models import Decision
from agent_arena.providers.historical import HistoricalProvider
from agent_arena.storage.candles import CandleStorage

from agent_arena.utils.time import utc_iso  # noqa: E402

logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# Cost estimation constants (Together AI pricing)
COST_PER_INPUT_TOKEN = {
    "gpt-oss-20b": 0.0000002,  # $0.20 per 1M tokens
    "gpt-oss-120b": 0.0000008,  # $0.80 per 1M tokens
    "default": 0.0000005,  # $0.50 per 1M tokens (conservative)
}
COST_PER_OUTPUT_TOKEN = {
    "gpt-oss-20b": 0.00000004,  # $0.04 per 1M tokens
    "gpt-oss-120b": 0.00000016,  # $0.16 per 1M tokens
    "default": 0.0000001,  # $0.10 per 1M tokens
}
TOKENS_PER_TICK_INPUT = 2500  # Estimated input tokens per tick
TOKENS_PER_TICK_OUTPUT = 250  # Estimated output tokens per tick


class BacktestRunner:
    """
    Run agents on historical data.

    Features:
    - Parallel agent execution
    - Progress tracking
    - Result persistence
    - Cost estimation
    """

    def __init__(
        self,
        config: CompetitionConfig,
        agents: list[BaseAgent],
        storage: Any,
        start_date: str,
        end_date: str,
        tick_interval: str = "1h",
        event_emitter: Optional[Callable[..., Any]] = None,
    ):
        """
        Initialize backtest runner.

        Args:
            config: Competition configuration
            agents: List of agents to run
            storage: Storage instance for candle data
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            tick_interval: Interval between ticks
            event_emitter: Optional callback for progress events
        """
        self.config = config
        self.agents = {a.agent_id: a for a in agents}
        self.storage = storage
        self.start_date = start_date
        self.end_date = end_date
        self.tick_interval = tick_interval
        self.emit = event_emitter or (lambda *args, **kwargs: None)

        # Components initialized during run
        self._provider: Optional[HistoricalProvider] = None
        self._arena: Optional[TradingArena] = None
        self._candle_storage: Optional[Any] = None  # CandleStorage or PostgresStorage

        # Per-agent recent decision history (in-memory ring buffer)
        self._decision_history: dict[str, list[dict]] = {a.agent_id: [] for a in agents}
        self._decision_history_limit = 10

        # State
        self._result: Optional[BacktestResult] = None
        self._running = False
        self._cancelled = False

    def estimate_cost(self) -> dict:
        """
        Estimate API cost before running backtest.

        Returns:
            Dict with cost breakdown by agent
        """
        # Create temporary provider to get tick count
        from agent_arena.providers.historical import INTERVAL_MS, parse_date, date_to_ms

        start_ms = date_to_ms(parse_date(self.start_date))
        end_ms = date_to_ms(parse_date(self.end_date))
        interval_ms = INTERVAL_MS.get(self.tick_interval, 3600000)
        total_ticks = (end_ms - start_ms) // interval_ms

        # Count LLM agents (non-baseline)
        llm_agents = []
        baseline_agents = []

        for agent_id, agent in self.agents.items():
            agent_class = agent.__class__.__name__
            # Check if agent uses LLM (has API costs)
            is_llm = any(
                name in agent_class.lower()
                for name in ["claude", "gpt", "together", "ollama", "llm"]
            )

            if is_llm:
                llm_agents.append({
                    "agent_id": agent_id,
                    "name": agent.name,
                    "class": agent_class,
                    "model": agent.config.get("model", "unknown"),
                })
            else:
                baseline_agents.append({
                    "agent_id": agent_id,
                    "name": agent.name,
                    "class": agent_class,
                })

        # Calculate costs for LLM agents
        agent_costs = []
        total_cost = 0.0

        for agent_info in llm_agents:
            model = agent_info["model"]

            # Get per-token costs
            input_cost = COST_PER_INPUT_TOKEN.get(
                model, COST_PER_INPUT_TOKEN["default"]
            )
            output_cost = COST_PER_OUTPUT_TOKEN.get(
                model, COST_PER_OUTPUT_TOKEN["default"]
            )

            # Calculate total cost
            input_tokens = total_ticks * TOKENS_PER_TICK_INPUT
            output_tokens = total_ticks * TOKENS_PER_TICK_OUTPUT

            cost = (input_tokens * input_cost) + (output_tokens * output_cost)
            total_cost += cost

            agent_costs.append({
                **agent_info,
                "estimated_cost": round(cost, 4),
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            })

        return {
            "total_ticks": total_ticks,
            "tick_interval": self.tick_interval,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "llm_agents": agent_costs,
            "baseline_agents": baseline_agents,
            "total_estimated_cost": round(total_cost, 2),
        }

    async def run(
        self,
        name: str = "Backtest",
        save_results: bool = True,
        run_id: Optional[str] = None,
    ) -> BacktestResult:
        """
        Run the backtest.

        Args:
            name: Name for this backtest run
            save_results: Whether to persist results to database
            run_id: Optional run ID (generated if not provided)

        Returns:
            BacktestResult with all metrics and data
        """
        if run_id is None:
            run_id = f"bt_{uuid.uuid4().hex[:12]}"

        # Initialize result
        self._result = BacktestResult(
            run_id=run_id,
            name=name,
            start_date=self.start_date,
            end_date=self.end_date,
            tick_interval=self.tick_interval,
            config={
                "symbols": self.config.symbols,
                "agents": list(self.agents.keys()),
            },
        )

        # Estimate cost
        cost_estimate = self.estimate_cost()
        self._result.estimated_cost = cost_estimate["total_estimated_cost"]
        self._result.total_ticks = cost_estimate["total_ticks"]

        # Create backtest run record
        # Support both SQLite (via CandleStorage wrapper) and PostgreSQL (direct)
        if save_results:
            if hasattr(self.storage, "_connection"):
                # SQLite storage - use CandleStorage wrapper
                self._candle_storage = CandleStorage(self.storage._connection)
            elif hasattr(self.storage, "pool"):
                # PostgreSQL storage - use directly (has same interface)
                self._candle_storage = self.storage

            if self._candle_storage:
                await self._candle_storage.create_backtest_run(
                    run_id=run_id,
                    name=name,
                    config=self._result.config,
                    start_date=self.start_date,
                    end_date=self.end_date,
                    tick_interval=self.tick_interval,
                    estimated_cost=self._result.estimated_cost,
                )

        self._running = True
        self._result.started_at = utc_now()
        self._result.status = "running"

        try:
            # Initialize components
            await self._initialize()

            # Run the simulation
            await self._run_simulation()

            # Calculate final metrics
            self._calculate_metrics()

            # Save results
            if save_results and self._candle_storage:
                await self._save_results()

            self._result.status = "completed"
            self._result.completed_at = utc_now()
            self._result.duration_seconds = (
                self._result.completed_at - self._result.started_at
            ).total_seconds()

        except Exception as e:
            logger.exception(f"Backtest failed: {e}")
            self._result.status = "failed"
            self._result.error_message = str(e)

            if save_results and self._candle_storage:
                await self._candle_storage.update_backtest_run(
                    run_id,
                    status="failed",
                    error_message=str(e),
                )

        finally:
            await self._cleanup()
            self._running = False

        return self._result

    async def _initialize(self) -> None:
        """Initialize all components for the backtest."""
        # Create historical provider
        self._provider = HistoricalProvider(
            storage=self.storage,
            start_date=self.start_date,
            end_date=self.end_date,
            symbols=self.config.symbols,
            tick_interval=self.tick_interval,
            candle_intervals=self.config.candles.intervals,
            candle_limit=self.config.candles.limit,
        )
        await self._provider.start()

        # Create arena
        self._arena = TradingArena(
            symbols=self.config.symbols,
            fees=self.config.fees,
            constraints=self.config.constraints,
            tick_interval_seconds=self._get_interval_seconds(),
        )

        # Initialize agents
        for agent in self.agents.values():
            self._arena.register_agent(agent.agent_id)

            # Initialize agent result tracking
            portfolio = self._arena.get_portfolio(agent.agent_id)
            self._result.agent_results[agent.agent_id] = AgentResult(
                agent_id=agent.agent_id,
                agent_name=agent.name,
                initial_capital=portfolio.initial_capital if portfolio else Decimal("10000"),
            )

            await agent.on_start()

        self.emit("backtest_started", {
            "run_id": self._result.run_id,
            "name": self._result.name,
            "total_ticks": self._result.total_ticks,
        })

    def _get_interval_seconds(self) -> int:
        """Convert tick interval string to seconds."""
        from agent_arena.providers.historical import INTERVAL_MS

        interval_ms = INTERVAL_MS.get(self.tick_interval, 3600000)
        return interval_ms // 1000

    async def _run_simulation(self) -> None:
        """Run the main simulation loop."""
        tick = 0

        while not self._provider.is_finished and not self._cancelled:
            tick += 1

            # Get current timestamp
            current_time = self._provider.current_timestamp
            timestamp_iso = utc_iso(current_time)

            # Build context
            context = await self._build_context(tick)

            # Update arena prices
            if "market" in context:
                prices = {
                    symbol: Decimal(str(data["price"]))
                    for symbol, data in context["market"].items()
                }
                self._arena.update_prices(prices)

            # Record equity snapshot
            self._arena.record_equity_snapshot(tick, current_time)

            # Process arena mechanics (funding, SL/TP, liquidations)
            await self._process_arena_mechanics(context, tick, timestamp_iso)

            # Get decisions from all agents
            decisions = await self._get_all_decisions(context)

            # Execute decisions
            for agent_id, decision in decisions.items():
                if decision:
                    trade = None
                    if decision.action != "hold":
                        trade = self._arena.execute(agent_id, decision)

                        # Record trade
                        if trade:
                            agent_result = self._result.agent_results[agent_id]
                            agent_result.trades.append(TradeRecord(
                                id=trade.id,
                                tick=tick,
                                timestamp=timestamp_iso,
                                symbol=trade.symbol,
                                side=trade.side.value,
                                action="open" if "open" in decision.action else "close",
                                size=trade.size,
                                price=trade.price,
                                leverage=trade.leverage,
                                fee=trade.fee,
                                realized_pnl=trade.realized_pnl,
                            ))

                    # Record in per-agent decision history buffer
                    history = self._decision_history.setdefault(agent_id, [])
                    history.append({
                        "tick": tick,
                        "action": decision.action,
                        "symbol": decision.symbol,
                        "confidence": decision.confidence,
                        "reasoning": decision.reasoning,
                        "trade_pnl": (
                            float(trade.realized_pnl)
                            if trade and trade.realized_pnl else None
                        ),
                    })
                    if len(history) > self._decision_history_limit:
                        history.pop(0)

            # Record equity for all agents
            for agent_id, agent_result in self._result.agent_results.items():
                portfolio = self._arena.get_portfolio(agent_id)
                if portfolio:
                    agent_result.equity_curve.append(EquityPoint(
                        tick=tick,
                        timestamp=timestamp_iso,
                        equity=portfolio.equity,
                        pnl=portfolio.total_pnl,
                        pnl_pct=float(portfolio.total_pnl / portfolio.initial_capital * 100),
                    ))

            # Update progress
            self._result.completed_ticks = tick

            # Emit progress event
            if tick % 10 == 0 or self._provider.is_finished:
                self.emit("backtest_progress", {
                    "run_id": self._result.run_id,
                    "tick": tick,
                    "total_ticks": self._result.total_ticks,
                    "progress_pct": self._provider.progress,
                    "timestamp": timestamp_iso,
                })

                # Update database
                if self._candle_storage:
                    await self._candle_storage.update_backtest_run(
                        self._result.run_id,
                        current_tick=tick,
                    )

            # Advance to next tick
            self._provider.advance_tick()

    async def _build_context(self, tick: int) -> dict:
        """Build context for agents."""
        context = {
            "tick": tick,
            "timestamp": utc_iso(self._provider.current_timestamp),
        }

        # Get market data from historical provider
        market_data = await self._provider.get_data(self.config.symbols)
        context.update(market_data)

        # Get candles
        if self.config.candles.enabled:
            candles = await self._provider.get_candles_multi(
                self.config.symbols,
                self.config.candles.intervals,
                self.config.candles.limit,
            )
            context["candles"] = candles

        return context

    async def _process_arena_mechanics(
        self,
        context: dict,
        tick: int,
        timestamp_iso: str,
    ) -> None:
        """Process funding, SL/TP, and liquidations."""
        if "market" not in context:
            return

        # Extract funding rates
        funding_rates = {}
        for symbol, data in context["market"].items():
            rate = data.get("funding_rate")
            if rate is not None:
                funding_rates[symbol] = (
                    Decimal(str(rate)) if not isinstance(rate, Decimal) else rate
                )

        # Apply funding payments
        if funding_rates:
            funding_events = self._arena.apply_funding_payments(
                funding_rates, self._get_interval_seconds()
            )

            # Track funding in agent results
            for event in funding_events:
                agent_id = event["agent_id"]
                amount = Decimal(str(event["amount"]))
                if agent_id in self._result.agent_results:
                    if event["direction"] == "paid":
                        self._result.agent_results[agent_id].total_funding_paid += abs(amount)
                    else:
                        self._result.agent_results[agent_id].total_funding_received += abs(amount)

        # Check SL/TP triggers
        self._arena.check_stop_loss_take_profit()

        # Check liquidations
        self._arena.check_liquidations()

    async def _get_all_decisions(
        self,
        base_context: dict,
    ) -> dict[str, Optional[Decision]]:
        """Get decisions from all agents concurrently."""

        async def get_decision(agent: BaseAgent) -> tuple[str, Optional[Decision]]:
            portfolio = self._arena.get_portfolio(agent.agent_id)
            if not portfolio:
                return agent.agent_id, None

            context = {
                **base_context,
                "portfolio": portfolio.to_context(),
                "recent_decisions": list(self._decision_history.get(agent.agent_id, [])),
            }

            try:
                decision = await asyncio.wait_for(
                    agent.decide(context),
                    timeout=self.config.agent_timeout_seconds,
                )
                return agent.agent_id, decision
            except asyncio.TimeoutError:
                return agent.agent_id, Decision(
                    action="hold",
                    reasoning="Timeout",
                    metadata={"error": "timeout"},
                )
            except Exception as e:
                logger.warning(f"Agent {agent.agent_id} error: {e}")
                return agent.agent_id, Decision(
                    action="hold",
                    reasoning=f"Error: {str(e)}",
                    metadata={"error": str(e)},
                )

        tasks = [get_decision(agent) for agent in self.agents.values()]
        results = await asyncio.gather(*tasks)
        return dict(results)

    def _calculate_metrics(self) -> None:
        """Calculate final metrics for all agents."""
        for agent_id, agent_result in self._result.agent_results.items():
            portfolio = self._arena.get_portfolio(agent_id)
            if not portfolio:
                continue

            # Final equity and P&L
            agent_result.final_equity = portfolio.equity
            agent_result.total_pnl = portfolio.total_pnl
            agent_result.total_pnl_pct = float(
                portfolio.total_pnl / portfolio.initial_capital * 100
            )

            # Get analytics from arena
            analytics = self._arena.get_analytics(agent_id)
            if analytics:
                agent_result.total_trades = analytics.total_trades
                agent_result.winning_trades = analytics.winning_trades
                agent_result.losing_trades = analytics.losing_trades
                agent_result.win_rate = analytics.win_rate
                agent_result.sharpe_ratio = analytics.sharpe_ratio
                agent_result.profit_factor = analytics.profit_factor
                agent_result.max_drawdown_pct = analytics.max_drawdown
                agent_result.max_drawdown_amount = analytics.max_drawdown_amount
                agent_result.total_fees = analytics.total_fees_paid

                if analytics.total_trades > 0:
                    agent_result.avg_trade_pnl = float(
                        analytics.total_pnl / analytics.total_trades
                    )
                agent_result.largest_win = analytics.largest_win
                agent_result.largest_loss = analytics.largest_loss

    async def _save_results(self) -> None:
        """Save results to database."""
        if not self._candle_storage:
            return

        # Update run status
        await self._candle_storage.update_backtest_run(
            self._result.run_id,
            status="completed",
            completed_at=utc_iso(self._result.completed_at),
            total_ticks=self._result.completed_ticks,
        )

        # Save agent results
        for agent_id, agent_result in self._result.agent_results.items():
            result_id = f"res_{uuid.uuid4().hex[:12]}"

            metrics = {
                "total_pnl": float(agent_result.total_pnl),
                "total_pnl_pct": agent_result.total_pnl_pct,
                "sharpe_ratio": agent_result.sharpe_ratio,
                "win_rate": agent_result.win_rate,
                "max_drawdown_pct": agent_result.max_drawdown_pct,
                "total_trades": agent_result.total_trades,
                "winning_trades": agent_result.winning_trades,
                "losing_trades": agent_result.losing_trades,
                "profit_factor": agent_result.profit_factor,
                "avg_trade_pnl": agent_result.avg_trade_pnl,
                "largest_win": float(agent_result.largest_win) if agent_result.largest_win else None,
                "largest_loss": float(agent_result.largest_loss) if agent_result.largest_loss else None,
                "total_fees": float(agent_result.total_fees),
            }

            equity_curve = [p.to_dict() for p in agent_result.equity_curve]
            trades = [t.to_dict() for t in agent_result.trades]

            await self._candle_storage.save_backtest_result(
                result_id=result_id,
                run_id=self._result.run_id,
                agent_id=agent_id,
                agent_name=agent_result.agent_name,
                metrics=metrics,
                equity_curve=equity_curve,
                trades=trades,
            )

    async def _cleanup(self) -> None:
        """Cleanup resources."""
        if self._provider:
            await self._provider.stop()

        for agent in self.agents.values():
            await agent.on_stop()

        self.emit("backtest_completed", {
            "run_id": self._result.run_id,
            "status": self._result.status,
            "duration_seconds": self._result.duration_seconds,
        })

    def cancel(self) -> None:
        """Cancel a running backtest."""
        self._cancelled = True

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def result(self) -> Optional[BacktestResult]:
        return self._result
