"""Competition runner - orchestrates the main loop."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Optional

from agent_arena.core.agent import BaseAgent
from agent_arena.core.arena import TradingArena
from agent_arena.core.config import CompetitionConfig
from agent_arena.core.models import Decision, Trade
from agent_arena.core.regime import classify_regime, get_regime_characteristics
from agent_arena.forum.runner import DiscussionAgentRunner
from agent_arena.providers.base import DataProvider
from agent_arena.providers.kraken import KrakenProvider
from agent_arena.utils.time import utc_iso, utc_now_iso

logger = logging.getLogger(__name__)


class CompetitionRunner:
    """
    Orchestrates the competition loop.

    Each tick:
    1. Fetch market data
    2. Build context for each agent
    3. Get decisions (concurrently)
    4. Execute trades
    5. Emit events for dashboard
    6. Store results
    """

    def __init__(
        self,
        config: CompetitionConfig,
        agents: list[BaseAgent],
        providers: list[DataProvider],
        arena: TradingArena,
        storage: Any = None,
        event_emitter: Optional[Callable[..., Any]] = None,
        archive: Any = None,  # Optional ArchiveService for long-term storage
    ):
        self.config = config
        self.agents = {a.agent_id: a for a in agents}
        self.providers = providers
        self.arena = arena
        self.storage = storage
        self.emit = event_emitter or (lambda *args, **kwargs: None)
        self.archive = archive  # PostgreSQL archival service

        self.tick = 0
        self.running = False
        self.started_at: Optional[datetime] = None
        self._last_archive_date = None  # Track for end-of-day archival

        # Per-agent recent decision history (in-memory ring buffer)
        self._decision_history: dict[str, list[dict]] = {a.agent_id: [] for a in agents}
        self._decision_history_limit = 10  # Keep last N decisions per agent

        # Trade frequency gate: rolling window of (tick, action) per agent
        window_size = config.constraints.trade_window_ticks
        self._tick_window_trades: dict[str, deque[int]] = {
            a.agent_id: deque(maxlen=window_size) for a in agents
        }

        # Forum discussion agents (M3)
        self.discussion_runner: Optional[DiscussionAgentRunner] = None

    async def start(self) -> None:
        """Start the competition."""
        self.running = True
        self.started_at = datetime.now(timezone.utc)
        resuming = self.tick > 0  # tick was set by app.py resume
        if not resuming:
            self.tick = 0
        self._last_archive_date = self.started_at.date()

        # Initialize agents — skip register if portfolio was restored
        for agent in self.agents.values():
            if not self.arena.get_portfolio(agent.agent_id):
                self.arena.register_agent(agent.agent_id)
            # Inject storage for agentic agents that need it
            if hasattr(agent, "set_storage") and self.storage:
                agent.set_storage(self.storage)
            await agent.on_start()

        # Initialize archive service if provided
        if self.archive:
            import uuid
            session_id = f"{self.config.name}-{uuid.uuid4().hex[:8]}"
            await self.archive.initialize(
                session_id=session_id,
                name=self.config.name,
                config={
                    "symbols": self.config.symbols,
                    "interval_seconds": self.config.interval_seconds,
                    "duration_seconds": self.config.duration_seconds,
                },
            )
            # Initialize agent stats with starting equity
            for agent_id in self.agents:
                portfolio = self.arena.get_portfolio(agent_id)
                if portfolio:
                    self.archive.init_agent_daily_stats(
                        agent_id, float(portfolio.equity)
                    )

        # Start providers
        for provider in self.providers:
            await provider.start()

        # Initialize forum discussion agents if configured (M3)
        discussion_agents_config = self.config.raw_config.get("discussion_agents", [])
        if discussion_agents_config and self.storage:
            try:
                self.discussion_runner = DiscussionAgentRunner(
                    storage=self.storage,
                    config={"discussion_agents": discussion_agents_config},
                )
                await self.discussion_runner.initialize()
                logger.info(f"Initialized {self.discussion_runner.get_agent_count()} discussion agents")
            except Exception as e:
                logger.error(f"Failed to initialize discussion agents: {e}", exc_info=True)
                self.discussion_runner = None

        self.emit(
            "competition_started",
            {
                "name": self.config.name,
                "agents": list(self.agents.keys()),
                "symbols": self.config.symbols,
            },
        )

        # Main loop
        try:
            while self.running:
                logger.info(f"Starting tick {self.tick + 1}")
                await self._run_tick()
                logger.info(f"Completed tick {self.tick}")

                # Check duration limit
                if self.config.duration_seconds:
                    elapsed = (datetime.now(timezone.utc) - self.started_at).total_seconds()
                    if elapsed >= self.config.duration_seconds:
                        logger.info("Duration limit reached, stopping")
                        break

                # Wait for next tick
                await asyncio.sleep(self.config.interval_seconds)
        except Exception as e:
            logger.exception(f"Exception in competition loop: {e}")
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the competition."""
        self.running = False

        for agent in self.agents.values():
            await agent.on_stop()

        for provider in self.providers:
            await provider.stop()

        # Finalize archive if provided
        if self.archive:
            await self.archive.finalize_session(
                total_ticks=self.tick,
                final_leaderboard=self.arena.get_leaderboard(),
            )

        self.emit(
            "competition_stopped",
            {
                "ticks": self.tick,
                "leaderboard": self.arena.get_leaderboard(),
            },
        )

    async def run_single_tick(self) -> dict:
        """Run a single tick and return results. Useful for testing."""
        return await self._run_tick()

    async def _run_tick(self) -> dict:
        """Execute one tick of the competition."""
        self.tick += 1
        tick_start = datetime.now(timezone.utc)

        # 1. Gather data from all providers
        context = await self._build_context()

        # 1b. Run forum discussion agents (M3)
        if self.discussion_runner:
            try:
                await self.discussion_runner.on_tick(context)
            except Exception as e:
                logger.error(f"Error running discussion agents: {e}", exc_info=True)

        # 2. Update arena prices
        if "market" in context:
            prices = {
                symbol: Decimal(str(data["price"]))
                for symbol, data in context["market"].items()
            }
            self.arena.update_prices(prices)

        # 3. Record equity snapshot for analytics (after price update)
        self.arena.record_equity_snapshot(self.tick, tick_start)

        # 4. Check and execute pending limit orders
        pending_order_events = []
        sl_tp_events = []
        funding_events = []
        liquidation_events = []

        if "market" in context:
            # Check pending limit orders first (they execute at limit price)
            pending_order_events = self.arena.check_pending_orders()
            if pending_order_events:
                self.emit("pending_orders", {
                    "tick": self.tick,
                    "timestamp": utc_iso(tick_start),
                    "orders": pending_order_events,
                })

            # Extract funding rates from market data
            funding_rates = {}
            for symbol, data in context["market"].items():
                rate = data.get("funding_rate")
                if rate is not None:
                    funding_rates[symbol] = (
                        Decimal(str(rate)) if not isinstance(rate, Decimal) else rate
                    )

            # Apply funding payments
            if funding_rates:
                funding_events = self.arena.apply_funding_payments(
                    funding_rates, self.config.interval_seconds
                )

            # Check stop-loss/take-profit triggers (before liquidation)
            sl_tp_events = self.arena.check_stop_loss_take_profit()
            if sl_tp_events:
                self.emit("sl_tp_triggered", {
                    "tick": self.tick,
                    "timestamp": utc_iso(tick_start),
                    "triggers": sl_tp_events,
                })
                if self.storage:
                    for event in sl_tp_events:
                        await self.storage.save_sl_tp_trigger(
                            self.tick, utc_iso(tick_start), event
                        )

            # Check for liquidations
            liquidation_events = self.arena.check_liquidations()

            # Emit and store funding events
            if funding_events:
                self.emit("funding", {
                    "tick": self.tick,
                    "timestamp": utc_iso(tick_start),
                    "payments": funding_events,
                })
                if self.storage:
                    for payment in funding_events:
                        await self.storage.save_funding_payment(
                            self.tick, utc_iso(tick_start), payment
                        )

            # Emit and store liquidation events
            if liquidation_events:
                self.emit("liquidation", {
                    "tick": self.tick,
                    "timestamp": utc_iso(tick_start),
                    "liquidations": liquidation_events,
                })
                if self.storage:
                    for liq in liquidation_events:
                        await self.storage.save_liquidation(
                            self.tick, utc_iso(tick_start), liq
                        )

        # 5. Get decisions from all agents (concurrently)
        decisions = await self._get_all_decisions(context)

        # 5b. Trade frequency gate — override to HOLD if agent exceeded limit
        # Closes are exempt: they reduce risk (profit-taking, stop-loss) and
        # should never be blocked by a frequency cap.
        _EXEMPT_ACTIONS = {"hold", "close", "set_stop_loss", "set_take_profit", "cancel_order"}
        max_trades = self.config.constraints.max_trades_per_window
        window_ticks = self.config.constraints.trade_window_ticks
        for agent_id, decision in decisions.items():
            if decision and decision.action not in _EXEMPT_ACTIONS:
                window = self._tick_window_trades.get(agent_id)
                if window is not None:
                    # Evict trades outside the rolling window
                    while window and window[0] <= self.tick - window_ticks:
                        window.popleft()
                if window is not None and len(window) >= max_trades:
                    logger.info(
                        "Frequency cap: %s capped at tick %d "
                        "(%d trades in last %d ticks, limit %d) — overriding %s to HOLD",
                        agent_id, self.tick, len(window),
                        self.config.constraints.trade_window_ticks,
                        max_trades, decision.action,
                    )
                    original_action = decision.action
                    decision.action = "hold"
                    decision.reasoning = (
                        f"[FREQUENCY CAP] Original: {original_action}. "
                        f"Overridden — {len(window)} trades in last "
                        f"{self.config.constraints.trade_window_ticks} ticks "
                        f"(limit: {max_trades}). {decision.reasoning}"
                    )
                    if decision.metadata is None:
                        decision.metadata = {}
                    decision.metadata["frequency_capped"] = True
                    decision.metadata["original_action"] = original_action
                    self.emit("frequency_cap", {
                        "agent_id": agent_id,
                        "tick": self.tick,
                        "original_action": original_action,
                        "trades_in_window": len(window),
                        "window_ticks": self.config.constraints.trade_window_ticks,
                        "limit": max_trades,
                    })

        # 5c. Record non-hold decisions in rolling window (after gate)
        # Only count position-opening actions; closes don't consume cap.
        for agent_id, decision in decisions.items():
            if decision and decision.action not in _EXEMPT_ACTIONS:
                window = self._tick_window_trades.get(agent_id)
                if window is not None:
                    window.append(self.tick)

        # 6. Execute decisions and collect results
        results = {}
        for agent_id, decision in decisions.items():
            trade = None
            if decision:
                trade = self.arena.execute(agent_id, decision)
                if self.storage:
                    await self._store_decision(agent_id, decision, trade)

                # Record in per-agent decision history buffer
                history = self._decision_history.setdefault(agent_id, [])
                history.append({
                    "tick": self.tick,
                    "action": decision.action,
                    "symbol": decision.symbol,
                    "confidence": decision.confidence,
                    "reasoning": decision.reasoning,
                    "trade_pnl": (
                        float(trade.realized_pnl) if trade and trade.realized_pnl else None
                    ),
                })
                if len(history) > self._decision_history_limit:
                    history.pop(0)

                self.emit(
                    "decision",
                    {
                        "agent_id": agent_id,
                        "decision": {
                            "action": decision.action,
                            "symbol": decision.symbol,
                            "size": str(decision.size) if decision.size else None,
                            "leverage": decision.leverage,
                            "confidence": decision.confidence,
                            "reasoning": decision.reasoning,
                        },
                        "trade": self._trade_to_dict(trade) if trade else None,
                    },
                )
            results[agent_id] = {"decision": decision, "trade": trade}

            # Archive tracking
            if self.archive:
                # Track decision with context
                if decision:
                    portfolio = self.arena.get_portfolio(agent_id)
                    archive_context = {
                        "market_prices": {
                            s: {"price": float(d["price"])}
                            for s, d in context.get("market", {}).items()
                        },
                        "portfolio_equity": float(portfolio.equity) if portfolio else 0,
                        "portfolio_positions": {
                            s: {"side": p.side, "size": float(p.size)}
                            for s, p in (portfolio.positions.items() if portfolio else {})
                        },
                        "available_margin": float(portfolio.available_margin) if portfolio else 0,
                    }
                    self.archive.track_decision(
                        agent_id,
                        {
                            "action": decision.action,
                            "symbol": decision.symbol,
                            "size": float(decision.size) if decision.size else None,
                            "confidence": decision.confidence,
                            "reasoning": decision.reasoning,
                            "tick": self.tick,
                            "timestamp": utc_iso(tick_start),
                        },
                        archive_context,
                    )

                # Track trade
                if trade:
                    self.archive.track_trade(agent_id, self._trade_to_dict(trade))

                # Update equity
                portfolio = self.arena.get_portfolio(agent_id)
                if portfolio:
                    self.archive.update_equity(agent_id, float(portfolio.equity))

        # Check for end-of-day archival
        if self.archive:
            current_date = tick_start.date()
            if self._last_archive_date and current_date != self._last_archive_date:
                # Day changed - flush daily snapshots
                equities = {}
                for agent_id in self.agents:
                    portfolio = self.arena.get_portfolio(agent_id)
                    if portfolio:
                        equities[agent_id] = float(portfolio.equity)
                await self.archive.end_of_day(equities)
                self._last_archive_date = current_date

            # Track funding payments
            for payment in funding_events:
                self.archive.track_funding(
                    payment["agent_id"],
                    float(payment["amount"]),
                )

        # 7. Emit tick update
        leaderboard = self.arena.get_extended_leaderboard()
        tick_data = {
            "tick": self.tick,
            "timestamp": utc_iso(tick_start),
            "leaderboard": leaderboard,
            "market": {
                symbol: {
                    "price": float(data["price"]),
                    "change_24h": data["change_24h"],
                }
                for symbol, data in context.get("market", {}).items()
            },
            "decisions": {
                agent_id: {
                    "action": r["decision"].action if r["decision"] else "error",
                    "reasoning": r["decision"].reasoning if r["decision"] else "",
                    "confidence": r["decision"].confidence if r["decision"] else 0,
                }
                for agent_id, r in results.items()
            },
            "pending_orders": pending_order_events,
            "sl_tp_triggers": sl_tp_events,
            "funding_payments": funding_events,
            "liquidations": liquidation_events,
        }
        self.emit("tick", tick_data)

        # 8. Save snapshot for historical charts
        if self.storage:
            await self.storage.save_snapshot(
                tick=self.tick,
                timestamp=utc_iso(tick_start),
                leaderboard=leaderboard,
                market_data=tick_data.get("market"),
            )

            # 9. Save arena state for resume capability (PostgreSQL only)
            if hasattr(self.storage, "save_arena_state"):
                try:
                    await self.storage.save_arena_state(
                        competition_name=self.config.name,
                        tick=self.tick,
                        timestamp=tick_start,
                        current_prices=self.arena.current_prices,
                        arena=self.arena,
                    )
                except Exception as e:
                    logger.warning(f"Failed to save arena state: {e}")

        return tick_data

    async def _build_context(self) -> dict:
        """Gather data from all providers."""
        context = {
            "tick": self.tick,
            "timestamp": utc_now_iso(),
        }

        # Fetch from all providers concurrently
        tasks = [provider.get_data(self.config.symbols) for provider in self.providers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Merge results into context
        for i, result in enumerate(results):
            if isinstance(result, dict):
                context.update(result)
            elif isinstance(result, Exception):
                provider_name = self.providers[i].name if i < len(self.providers) else "unknown"
                logger.warning(f"Provider '{provider_name}' failed: {result}")

        # Fetch candles if enabled
        if self.config.candles.enabled:
            candles = await self._fetch_candles()
            if candles:
                context["candles"] = candles

                # Compute market regime from primary symbol candles
                primary_symbol = self.config.symbols[0] if self.config.symbols else None
                if primary_symbol and primary_symbol in candles:
                    # Prefer 1h candles, fall back to 4h, then 15m
                    symbol_candles = candles[primary_symbol]
                    candle_list = (
                        symbol_candles.get("1h")
                        or symbol_candles.get("4h")
                        or symbol_candles.get("15m")
                        or []
                    )
                    if candle_list:
                        regime = classify_regime(candle_list)
                        guidance = get_regime_characteristics(regime)
                        context["regime"] = regime
                        context["regime_guidance"] = guidance

        return context

    async def _fetch_candles(self) -> dict[str, dict[str, list[dict]]]:
        """Fetch historical candles from Kraken provider."""
        kraken_provider = None
        for provider in self.providers:
            if isinstance(provider, KrakenProvider):
                kraken_provider = provider
                break

        if not kraken_provider:
            return {}

        return await kraken_provider.get_candles_multi(
            symbols=self.config.symbols,
            intervals=self.config.candles.intervals,
            limit=self.config.candles.limit,
        )

    async def _get_all_decisions(self, base_context: dict) -> dict[str, Optional[Decision]]:
        """Get decisions from all agents, staggered: fast agents first, then agentic.

        Staggering reduces GPU contention by letting simple/rule-based agents
        finish before agentic agents start their multi-step tool loops.
        """
        from agent_arena.agentic.base import AgenticTrader

        async def get_decision(agent: BaseAgent) -> tuple[str, Optional[Decision]]:
            # Add agent-specific portfolio to context
            portfolio = self.arena.get_portfolio(agent.agent_id)
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
                    reasoning="Timeout: Agent took too long to respond",
                    metadata={"error": "timeout"},
                )
            except Exception as e:
                return agent.agent_id, Decision(
                    action="hold",
                    reasoning=f"Error: {str(e)}",
                    metadata={"error": str(e)},
                )

        # Split agents into fast (simple/rule-based) and slow (agentic)
        fast_agents = []
        slow_agents = []
        for agent in self.agents.values():
            if isinstance(agent, AgenticTrader):
                slow_agents.append(agent)
            else:
                fast_agents.append(agent)

        # Run fast agents first (concurrent), then agentic agents (concurrent)
        all_results = {}

        if fast_agents:
            fast_tasks = [get_decision(a) for a in fast_agents]
            fast_results = await asyncio.gather(*fast_tasks)
            all_results.update(dict(fast_results))

        if slow_agents:
            slow_tasks = [get_decision(a) for a in slow_agents]
            slow_results = await asyncio.gather(*slow_tasks)
            all_results.update(dict(slow_results))

        return all_results

    async def _store_decision(
        self,
        agent_id: str,
        decision: Decision,
        trade: Optional[Trade],
    ) -> None:
        """Persist decision and trade."""
        if not self.storage:
            return

        timestamp = utc_now_iso()

        decision_id = await self.storage.save_decision(
            {
                "agent_id": agent_id,
                "tick": self.tick,
                "timestamp": timestamp,
                "action": decision.action,
                "symbol": decision.symbol,
                "size": str(decision.size) if decision.size else None,
                "leverage": decision.leverage,
                "confidence": decision.confidence,
                "reasoning": decision.reasoning,
                "metadata": decision.metadata,
                "trade_id": trade.id if trade else None,
            }
        )

        # Also save the trade if one was executed
        if trade:
            await self.storage.save_trade(
                {
                    "id": trade.id,
                    "agent_id": agent_id,
                    "symbol": trade.symbol,
                    "side": trade.side.value,
                    "size": str(trade.size),
                    "price": str(trade.price),
                    "leverage": trade.leverage,
                    "fee": str(trade.fee),
                    "realized_pnl": str(trade.realized_pnl) if trade.realized_pnl else None,
                    "timestamp": timestamp,
                    "decision_id": decision_id,
                }
            )

    def _trade_to_dict(self, trade: Trade) -> dict:
        """Convert trade to dict."""
        return {
            "id": trade.id,
            "symbol": trade.symbol,
            "side": trade.side.value,
            "size": str(trade.size),
            "price": str(trade.price),
            "leverage": trade.leverage,
            "fee": str(trade.fee),
            "realized_pnl": str(trade.realized_pnl) if trade.realized_pnl else None,
        }
