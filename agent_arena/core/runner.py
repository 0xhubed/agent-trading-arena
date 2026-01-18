"""Competition runner - orchestrates the main loop."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Optional

from agent_arena.core.agent import BaseAgent
from agent_arena.core.arena import TradingArena
from agent_arena.core.config import CompetitionConfig
from agent_arena.core.models import Decision, Trade
from agent_arena.providers.base import DataProvider
from agent_arena.providers.binance import BinanceProvider

logger = logging.getLogger(__name__)


def utc_iso(dt: datetime) -> str:
    """Format datetime as ISO string with Z suffix for JavaScript."""
    return dt.isoformat().replace("+00:00", "Z")


def utc_now_iso() -> str:
    """Return current UTC time as ISO string with Z suffix for JavaScript."""
    return utc_iso(datetime.now(timezone.utc))


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

    async def start(self) -> None:
        """Start the competition."""
        self.running = True
        self.started_at = datetime.now(timezone.utc)
        self.tick = 0
        self._last_archive_date = self.started_at.date()

        # Initialize agents
        for agent in self.agents.values():
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
                    elapsed = (datetime.utcnow() - self.started_at).total_seconds()
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

        # 6. Execute decisions and collect results
        results = {}
        for agent_id, decision in decisions.items():
            trade = None
            if decision:
                trade = self.arena.execute(agent_id, decision)
                if self.storage:
                    await self._store_decision(agent_id, decision, trade)
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
        leaderboard = self.arena.get_leaderboard()
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

        return context

    async def _fetch_candles(self) -> dict[str, dict[str, list[dict]]]:
        """Fetch historical candles from Binance provider."""
        # Find the Binance provider
        binance_provider = None
        for provider in self.providers:
            if isinstance(provider, BinanceProvider):
                binance_provider = provider
                break

        if not binance_provider:
            return {}

        return await binance_provider.get_candles_multi(
            symbols=self.config.symbols,
            intervals=self.config.candles.intervals,
            limit=self.config.candles.limit,
        )

    async def _get_all_decisions(self, base_context: dict) -> dict[str, Optional[Decision]]:
        """Get decisions from all agents concurrently."""

        async def get_decision(agent: BaseAgent) -> tuple[str, Optional[Decision]]:
            # Add agent-specific portfolio to context
            portfolio = self.arena.get_portfolio(agent.agent_id)
            if not portfolio:
                return agent.agent_id, None

            context = {
                **base_context,
                "portfolio": portfolio.to_context(),
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

        tasks = [get_decision(agent) for agent in self.agents.values()]
        results = await asyncio.gather(*tasks)

        return dict(results)

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
