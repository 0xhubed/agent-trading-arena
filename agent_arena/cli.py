"""Agent Arena CLI - Rich terminal interface."""

from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path
from typing import Optional

import click
import yaml
from dotenv import load_dotenv
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from agent_arena.core.arena import TradingArena
from agent_arena.core.config import CompetitionConfig
from agent_arena.core.config_parser import (
    parse_candle_config,
    parse_constraints_config,
    parse_fees_config,
)
from agent_arena.core.loader import load_agent
from agent_arena.core.runner import CompetitionRunner
from agent_arena.providers.kraken import KrakenProvider
from agent_arena.storage import ArchiveService, get_storage

console = Console()
logger = logging.getLogger(__name__)


def create_dashboard(tick_data: dict, agents_info: dict) -> Layout:
    """Create the dashboard layout."""
    layout = Layout()

    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
        Layout(name="footer", size=3),
    )

    layout["body"].split_row(
        Layout(name="left", ratio=1),
        Layout(name="right", ratio=2),
    )

    # Header
    tick = tick_data.get("tick", 0)
    timestamp = tick_data.get("timestamp", "")
    header_text = Text()
    header_text.append("AGENT ARENA", style="bold magenta")
    header_text.append(" | ", style="dim")
    header_text.append(f"Tick {tick}", style="cyan")
    header_text.append(" | ", style="dim")
    header_text.append(timestamp[:19] if timestamp else "", style="dim")
    header_text.append(" | ", style="dim")
    header_text.append("LIVE", style="bold green")

    layout["header"].update(Panel(header_text, style="bold"))

    # Leaderboard
    leaderboard = tick_data.get("leaderboard", [])
    lb_table = Table(title="Leaderboard", show_header=True, header_style="bold cyan")
    lb_table.add_column("#", style="dim", width=3)
    lb_table.add_column("Agent", style="bold")
    lb_table.add_column("Equity", justify="right")
    lb_table.add_column("P&L", justify="right")
    lb_table.add_column("Trades", justify="right", width=6)

    for i, entry in enumerate(leaderboard, 1):
        agent_id = entry["agent_id"]
        name = agents_info.get(agent_id, {}).get("name", agent_id)
        equity = entry["equity"]
        pnl = entry["pnl"]
        pnl_pct = entry["pnl_percent"]
        trades = entry["trades"]

        pnl_style = "green" if pnl >= 0 else "red"

        lb_table.add_row(
            str(i),
            name,
            f"${equity:,.2f}",
            Text(f"${pnl:+,.2f} ({pnl_pct:+.2f}%)", style=pnl_style),
            str(trades),
        )

    layout["left"].update(Panel(lb_table, title="Rankings"))

    # Market + Decisions
    right_layout = Layout()
    right_layout.split_column(
        Layout(name="market", size=6),
        Layout(name="decisions"),
    )

    # Market data
    market = tick_data.get("market", {})
    market_table = Table(show_header=True, header_style="bold yellow")
    market_table.add_column("Symbol")
    market_table.add_column("Price", justify="right")
    market_table.add_column("24h", justify="right")

    for symbol, data in market.items():
        price = data.get("price", 0)
        change = data.get("change_24h", 0)
        change_style = "green" if change >= 0 else "red"

        market_table.add_row(
            symbol,
            f"${price:,.2f}",
            Text(f"{change:+.2f}%", style=change_style),
        )

    right_layout["market"].update(Panel(market_table, title="Market"))

    # Decisions
    decisions = tick_data.get("decisions", {})
    decision_panels = []

    for agent_id, decision in decisions.items():
        name = agents_info.get(agent_id, {}).get("name", agent_id)
        action = decision.get("action", "hold")
        reasoning = decision.get("reasoning", "")
        confidence = decision.get("confidence", 0)

        action_style = {
            "hold": "dim",
            "open_long": "bold green",
            "open_short": "bold red",
            "close": "yellow",
        }.get(action, "white")

        text = Text()
        text.append(f"{name}\n", style="bold")
        text.append(f"{action.upper()}", style=action_style)
        text.append(f" (conf: {confidence:.0%})\n", style="dim")
        text.append(reasoning[:100] + ("..." if len(reasoning) > 100 else ""), style="italic")

        decision_panels.append(Panel(text, border_style="blue"))

    if decision_panels:
        decisions_layout = Layout()
        decisions_layout.split_column(*[Layout(p, size=6) for p in decision_panels[:4]])
        right_layout["decisions"].update(Panel(decisions_layout, title="Latest Decisions"))
    else:
        right_layout["decisions"].update(Panel("Waiting for decisions...", title="Latest Decisions"))

    layout["right"].update(right_layout)

    # Footer
    footer_text = Text()
    footer_text.append("Press ", style="dim")
    footer_text.append("Ctrl+C", style="bold")
    footer_text.append(" to stop", style="dim")

    layout["footer"].update(Panel(footer_text, style="dim"))

    return layout


class CLIEventHandler:
    """Handle events from the competition runner for CLI display."""

    def __init__(self, agents_info: dict):
        self.agents_info = agents_info
        self.current_tick_data = {}
        self.live: Optional[Live] = None

    def __call__(self, event_type: str, data: dict):
        """Handle competition events."""
        if event_type == "tick":
            self.current_tick_data = data
            if self.live:
                self.live.update(create_dashboard(data, self.agents_info))

        elif event_type == "competition_started":
            console.print(f"\n[bold green]Competition started:[/] {data['name']}")
            console.print(f"[dim]Agents: {', '.join(data['agents'])}[/]")
            console.print(f"[dim]Symbols: {', '.join(data['symbols'])}[/]\n")

        elif event_type == "competition_stopped":
            console.print("\n[bold yellow]Competition stopped[/]")
            console.print(f"[dim]Total ticks: {data['ticks']}[/]")

        elif event_type == "decision":
            agent_id = data["agent_id"]
            decision = data["decision"]
            name = self.agents_info.get(agent_id, {}).get("name", agent_id)

            if decision["action"] != "hold":
                action_style = "green" if "long" in decision["action"] else "red"
                console.print(
                    f"[bold]{name}[/]: [{action_style}]{decision['action'].upper()}[/] "
                    f"{decision.get('symbol', '')} - {decision.get('reasoning', '')[:60]}..."
                )


async def run_competition(config_path: str):
    """Run a competition from config file."""
    # Load config
    with open(config_path) as f:
        config_data = yaml.safe_load(f)

    # Parse fee, constraint, and candle configs
    fees_config = parse_fees_config(config_data)
    constraints_config = parse_constraints_config(config_data)
    candle_config = parse_candle_config(config_data)

    # Create competition config
    competition_config = CompetitionConfig(
        name=config_data.get("name", "Agent Arena"),
        symbols=config_data.get("symbols", ["PF_XBTUSD", "PF_ETHUSD"]),
        interval_seconds=config_data.get("interval_seconds", 60),
        duration_seconds=config_data.get("duration_seconds"),
        agent_timeout_seconds=config_data.get("agent_timeout_seconds", 60.0),
        fees=fees_config,
        constraints=constraints_config,
        candles=candle_config,
    )

    # Load agents
    agents = []
    agents_info = {}
    for agent_config in config_data.get("agents", []):
        agent = load_agent(agent_config)
        agents.append(agent)
        agents_info[agent.agent_id] = {
            "name": agent.name,
            "config": agent.config,
        }

    if not agents:
        console.print("[bold red]Error:[/] No agents configured")
        return

    # Create components with fee and constraint configs
    arena = TradingArena(
        competition_config.symbols,
        fees=fees_config,
        constraints=constraints_config,
        tick_interval_seconds=competition_config.interval_seconds,
    )

    # Use storage based on DATABASE_BACKEND env var (postgres or sqlite)
    storage = get_storage()
    await storage.initialize()

    # Create archive service for long-term PostgreSQL storage
    import os
    archive = None
    if os.getenv("DATABASE_BACKEND") == "postgres":
        archive = ArchiveService(storage, generate_embeddings=True)
        console.print("[dim]Archive service enabled for long-term storage[/]")

    providers = [KrakenProvider()]

    # Create event handler
    event_handler = CLIEventHandler(agents_info)

    # Create runner
    runner = CompetitionRunner(
        config=competition_config,
        agents=agents,
        providers=providers,
        arena=arena,
        storage=storage,
        event_emitter=event_handler,
        archive=archive,
    )

    # Handle Ctrl+C
    def signal_handler(sig, frame):
        console.print("\n[yellow]Stopping competition...[/]")
        runner.running = False

    signal.signal(signal.SIGINT, signal_handler)

    # Run with live display
    with Live(
        Panel("Starting Agent Arena...", title="Initializing"),
        console=console,
        refresh_per_second=1,
    ) as live:
        event_handler.live = live
        await runner.start()

    await storage.close()

    # Print final results
    console.print("\n[bold]Final Leaderboard:[/]")
    for i, entry in enumerate(arena.get_leaderboard(), 1):
        agent_id = entry["agent_id"]
        name = agents_info.get(agent_id, {}).get("name", agent_id)
        pnl_style = "green" if entry["pnl"] >= 0 else "red"
        console.print(
            f"  {i}. [bold]{name}[/]: "
            f"[{pnl_style}]${entry['equity']:,.2f} ({entry['pnl_percent']:+.2f}%)[/]"
        )


async def run_demo():
    """Run a quick demo with a single tick."""
    from agent_arena.agents.claude_trader import ClaudeTrader

    console.print("[bold]Agent Arena Demo[/]\n")

    # Create components
    symbols = ["PF_XBTUSD", "PF_ETHUSD"]
    arena = TradingArena(symbols)
    provider = KrakenProvider()

    # Create a single agent
    agent = ClaudeTrader(
        agent_id="demo_claude",
        name="Demo Claude",
        config={"model": "claude-sonnet-4-20250514"},
    )

    # Create runner
    config = CompetitionConfig(
        name="Demo",
        symbols=symbols,
        interval_seconds=60,
    )

    def print_event(event_type: str, data: dict):
        if event_type == "tick":
            console.print(f"\n[bold cyan]Tick {data['tick']}[/]")

            # Market
            console.print("\n[yellow]Market:[/]")
            for symbol, mdata in data.get("market", {}).items():
                change_style = "green" if mdata["change_24h"] >= 0 else "red"
                console.print(
                    f"  {symbol}: ${mdata['price']:,.2f} "
                    f"[{change_style}]({mdata['change_24h']:+.2f}%)[/]"
                )

            # Decisions
            console.print("\n[yellow]Decisions:[/]")
            for agent_id, decision in data.get("decisions", {}).items():
                action_style = {
                    "hold": "dim",
                    "open_long": "green",
                    "open_short": "red",
                    "close": "yellow",
                }.get(decision["action"], "white")
                console.print(
                    f"  [{action_style}]{decision['action'].upper()}[/] "
                    f"(conf: {decision['confidence']:.0%})"
                )
                console.print(f"    [italic]{decision['reasoning']}[/]")

            # Leaderboard
            console.print("\n[yellow]Leaderboard:[/]")
            for entry in data.get("leaderboard", []):
                pnl_style = "green" if entry["pnl"] >= 0 else "red"
                console.print(
                    f"  {entry['agent_id']}: "
                    f"[{pnl_style}]${entry['equity']:,.2f} ({entry['pnl_percent']:+.2f}%)[/]"
                )

    runner = CompetitionRunner(
        config=config,
        agents=[agent],
        providers=[provider],
        arena=arena,
        event_emitter=print_event,
    )

    # Initialize
    arena.register_agent(agent.agent_id)
    await agent.on_start()
    await provider.start()

    console.print("[dim]Fetching market data and running agent...[/]\n")

    # Run single tick
    await runner.run_single_tick()

    await provider.stop()
    await agent.on_stop()

    console.print("\n[bold green]Demo complete![/]")


@click.group()
def cli():
    """Agent Arena - AI Agents vs. The Market"""
    pass


@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
def run(config_path: str):
    """Run a competition from a YAML config file."""
    asyncio.run(run_competition(config_path))


@cli.command()
def demo():
    """Run a quick demo with a single tick."""
    asyncio.run(run_demo())


@cli.command("data-status")
def data_status():
    """Show available historical data status."""
    asyncio.run(_data_status())


async def _data_status():
    """Check historical data availability."""
    storage = get_storage()
    await storage.initialize()

    console.print("\n[bold]Historical Data Status[/]\n")

    if hasattr(storage, "get_data_status"):
        # PostgreSQL backend
        status = await storage.get_data_status()
    else:
        # SQLite backend with CandleStorage wrapper
        from agent_arena.storage.candles import CandleStorage
        candle_storage = CandleStorage(storage._connection)
        status = await candle_storage.get_data_status()

    if not status:
        console.print("[yellow]No historical data available.[/]")
        console.print("Use [bold]agent-arena fetch-data[/] to download data from Kraken Futures.")
        await storage.close()
        return

    # Create table
    table = Table(title="Available Data", show_header=True, header_style="bold cyan")
    table.add_column("Symbol", style="bold")
    table.add_column("Interval")
    table.add_column("From", style="dim")
    table.add_column("To", style="dim")
    table.add_column("Candles", justify="right")
    table.add_column("Gaps", justify="right")

    total_candles = 0
    for item in status:
        total_candles += item["count"]
        gap_style = "red" if item["gaps"] > 0 else "green"
        table.add_row(
            item["symbol"],
            item["interval"],
            item["earliest"][:10] if item["earliest"] else "N/A",
            item["latest"][:10] if item["latest"] else "N/A",
            f"{item['count']:,}",
            Text(str(item["gaps"]), style=gap_style),
        )

    console.print(table)
    console.print(f"\n[dim]Total candles: {total_candles:,}[/]")

    await storage.close()


@cli.command("fetch-data")
@click.option("--symbols", "-s", multiple=True, default=["PF_XBTUSD", "PF_ETHUSD"],
              help="Symbols to fetch (can specify multiple)")
@click.option("--intervals", "-i", multiple=True, default=["1h"],
              help="Intervals to fetch (can specify multiple)")
@click.option("--start", "-S", required=True, help="Start date (YYYY-MM-DD)")
@click.option("--end", "-E", default=None, help="End date (YYYY-MM-DD, defaults to today)")
def fetch_data(symbols: tuple, intervals: tuple, start: str, end: str):
    """Fetch historical data from Kraken Futures."""
    if end is None:
        from datetime import datetime
        end = datetime.now().strftime("%Y-%m-%d")

    asyncio.run(_fetch_data(list(symbols), list(intervals), start, end))


async def _fetch_data(symbols: list, intervals: list, start_date: str, end_date: str):
    """Fetch historical data from Kraken Futures API."""
    from agent_arena.data.fetch_historical import fetch_and_store_historical

    storage = get_storage()
    await storage.initialize()

    console.print("\n[bold]Fetching Historical Data[/]")
    console.print(f"  Symbols: {', '.join(symbols)}")
    console.print(f"  Intervals: {', '.join(intervals)}")
    console.print(f"  Date range: {start_date} to {end_date}\n")

    try:
        result = await fetch_and_store_historical(
            storage=storage,
            symbols=symbols,
            intervals=intervals,
            start_date=start_date,
            end_date=end_date,
            progress_callback=lambda symbol, interval, count, total: console.print(
                f"  [dim]{symbol}/{interval}: {count} candles[/]"
            ),
        )
        console.print(f"\n[green]Fetched {result['total_candles']:,} candles[/]")
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/]")

    await storage.close()


# --- Backtest commands ---

@cli.group(name="backtest")
def _backtest_group():
    """Backtesting commands."""
    pass


@_backtest_group.command("estimate")
@click.option("--start", "-S", required=True, help="Start date (YYYY-MM-DD)")
@click.option("--end", "-E", required=True, help="End date (YYYY-MM-DD)")
@click.option("--symbols", "-s", multiple=True, default=["PF_XBTUSD", "PF_ETHUSD"],
              help="Symbols to trade")
@click.option("--interval", "-i", default="1h", help="Tick interval")
@click.option("--baselines/--no-baselines", default=True, help="Include baseline agents")
@click.option("--agent", "-a", multiple=True, help="Additional agent class paths")
def backtest_estimate(start: str, end: str, symbols: tuple, interval: str,
                      baselines: bool, agent: tuple):
    """Estimate backtest cost before running."""
    asyncio.run(_backtest_estimate(start, end, list(symbols), interval, baselines, list(agent)))


async def _backtest_estimate(start_date: str, end_date: str, symbols: list,
                              tick_interval: str, run_baselines: bool, agent_classes: list):
    """Estimate backtest cost."""
    from agent_arena.providers.historical import INTERVAL_MS, date_to_ms, parse_date

    storage = get_storage()
    await storage.initialize()

    # Calculate total ticks
    start_ms = date_to_ms(parse_date(start_date))
    end_ms = date_to_ms(parse_date(end_date))
    interval_ms = INTERVAL_MS.get(tick_interval, 3600000)
    total_ticks = (end_ms - start_ms) // interval_ms

    # Duration in hours
    duration_hours = (end_ms - start_ms) / (1000 * 60 * 60)

    # Count agents
    num_baseline = 5 if run_baselines else 0
    num_llm = len(agent_classes)
    total_agents = num_baseline + num_llm

    # Cost estimate (LLM agents only)
    # Assume ~2500 input tokens and ~250 output tokens per tick
    # At ~$0.50/1M input and $0.10/1M output (conservative)
    tokens_per_tick = 2500 + 250
    cost_per_tick = (2500 * 0.0000005) + (250 * 0.0000001)  # per LLM agent
    estimated_cost = num_llm * total_ticks * cost_per_tick

    console.print("\n[bold]Backtest Cost Estimate[/]\n")

    table = Table(show_header=False)
    table.add_column("Metric", style="dim")
    table.add_column("Value", style="bold")

    table.add_row("Date Range", f"{start_date} to {end_date}")
    table.add_row("Symbols", ", ".join(symbols))
    table.add_row("Tick Interval", tick_interval)
    table.add_row("Total Ticks", f"{total_ticks:,}")
    table.add_row("Duration", f"{duration_hours:.1f} hours of data")
    table.add_row("", "")
    table.add_row("Baseline Agents", f"{num_baseline}" if run_baselines else "No")
    table.add_row("LLM Agents", str(num_llm))
    table.add_row("Est. API Calls", f"{num_llm * total_ticks:,}")
    table.add_row("Est. Cost", f"${estimated_cost:.2f}")

    console.print(table)

    if agent_classes:
        console.print("\n[dim]Per-agent breakdown:[/]")
        per_agent_cost = total_ticks * cost_per_tick
        for class_path in agent_classes:
            agent_id = class_path.split(".")[-1].lower()
            console.print(f"  {agent_id}: ~${per_agent_cost:.2f}")

    await storage.close()


@_backtest_group.command("run")
@click.option("--start", "-S", required=True, help="Start date (YYYY-MM-DD)")
@click.option("--end", "-E", required=True, help="End date (YYYY-MM-DD)")
@click.option("--symbols", "-s", multiple=True, default=["PF_XBTUSD", "PF_ETHUSD"],
              help="Symbols to trade")
@click.option("--interval", "-i", default="1h", help="Tick interval")
@click.option("--baselines/--no-baselines", default=True, help="Include baseline agents")
@click.option("--agent", "-a", multiple=True, help="Additional agent class paths")
@click.option("--output", "-o", default=None, help="Output file for results (JSON)")
def backtest_run(start: str, end: str, symbols: tuple, interval: str,
                 baselines: bool, agent: tuple, output: str):
    """Run a backtest on historical data."""
    asyncio.run(_backtest_run(start, end, list(symbols), interval, baselines, list(agent), output))


async def _backtest_run(start_date: str, end_date: str, symbols: list,
                         tick_interval: str, run_baselines: bool, agent_classes: list,
                         output_file: str):
    """Run a backtest."""
    import importlib
    import json
    from decimal import Decimal

    from agent_arena.backtest.runner import BacktestRunner
    from agent_arena.core.config import (
        CandleConfig,
        CompetitionConfig,
    )

    storage = get_storage()
    await storage.initialize()

    # Build agents list
    agents = []

    # Add baseline agents if requested
    if run_baselines:
        from agent_arena.agents.baselines import (
            BuyAndHoldAgent,
            MeanReversionAgent,
            MomentumAgent,
            RandomAgent,
            SMAAgent,
        )

        baselines = [
            RandomAgent("random_baseline", "Random Trader", {"trade_frequency": 0.2}),
            SMAAgent("sma_baseline", "SMA Crossover", {"sma_period": 50}),
            MomentumAgent("momentum_baseline", "Momentum", {"rebalance_ticks": 24}),
            BuyAndHoldAgent("buyhold_baseline", "Buy & Hold", {}),
            MeanReversionAgent("meanrev_baseline", "Mean Reversion", {}),
        ]
        agents.extend(baselines)

    # Add custom agents
    for class_path in agent_classes:
        module_path, class_name = class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        agent_class = getattr(module, class_name)
        agent_id = class_name.lower()
        agent = agent_class(agent_id=agent_id, name=class_name, config={})
        agents.append(agent)

    if not agents:
        console.print("[bold red]Error:[/] No agents configured. Use --baselines or --agent options.")
        await storage.close()
        return

    console.print("\n[bold]Running Backtest[/]")
    console.print(f"  Date range: {start_date} to {end_date}")
    console.print(f"  Symbols: {', '.join(symbols)}")
    console.print(f"  Tick interval: {tick_interval}")
    console.print(f"  Agents: {len(agents)}\n")

    # Create competition config
    config = CompetitionConfig(
        name="CLI Backtest",
        symbols=symbols,
        interval_seconds=3600,  # Will be overridden by tick_interval
        candles=CandleConfig(enabled=True, intervals=["1h", "4h"], limit=100),
    )

    # Create and run backtest
    runner = BacktestRunner(
        config=config,
        agents=agents,
        storage=storage,
        start_date=start_date,
        end_date=end_date,
        tick_interval=tick_interval,
    )

    def print_progress(event_type: str, data: dict):
        if event_type == "backtest_progress":
            pct = data.get("progress_pct", 0)
            tick = data.get("tick", 0)
            total = data.get("total_ticks", 0)
            console.print(f"  [dim]Progress: {tick}/{total} ({pct:.1f}%)[/]", end="\r")

    runner.emit = print_progress

    result = await runner.run(name="CLI Backtest")

    console.print("\n\n[bold green]Backtest Complete![/]\n")

    # Display results
    table = Table(title="Results", show_header=True, header_style="bold cyan")
    table.add_column("#", style="dim", width=3)
    table.add_column("Agent", style="bold")
    table.add_column("Final Equity", justify="right")
    table.add_column("Return", justify="right")
    table.add_column("Win Rate", justify="right")
    table.add_column("Sharpe", justify="right")
    table.add_column("Max DD", justify="right")
    table.add_column("Trades", justify="right")

    # Sort by final equity
    sorted_agents = sorted(result.agents, key=lambda a: a.final_equity, reverse=True)

    for i, agent in enumerate(sorted_agents, 1):
        return_style = "green" if agent.total_return >= 0 else "red"
        table.add_row(
            str(i),
            agent.agent_name,
            f"${float(agent.final_equity):,.2f}",
            Text(f"{agent.total_return * 100:+.2f}%", style=return_style),
            f"{agent.win_rate * 100:.1f}%",
            f"{agent.sharpe_ratio:.2f}" if agent.sharpe_ratio else "N/A",
            f"-{agent.max_drawdown_pct * 100:.1f}%",
            str(agent.total_trades),
        )

    console.print(table)

    # Show statistical comparisons if available
    if result.comparisons:
        console.print("\n[bold]Statistical Comparisons[/]\n")
        for comp in result.comparisons:
            sig_text = "[green]SIGNIFICANT[/]" if comp.is_significant else "[dim]not significant[/]"
            out_style = "green" if comp.outperformance > 0 else "red"
            console.print(
                f"  {comp.agent_id} vs {comp.baseline_id}: "
                f"[{out_style}]{comp.outperformance * 100:+.2f}%[/] "
                f"(p={comp.p_value:.4f if comp.p_value else 'N/A'}) {sig_text}"
            )

    # Save to file if requested
    if output_file:
        def decimal_handler(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        result_dict = result.to_dict()
        with open(output_file, "w") as f:
            json.dump(result_dict, f, indent=2, default=decimal_handler)
        console.print(f"\n[dim]Results saved to {output_file}[/]")

    await storage.close()


# --- Evolution commands ---

@cli.group(name="evolve")
def _evolve_group():
    """Evolution engine commands."""
    pass


@_evolve_group.command("run")
@click.option("--population", "-p", default=20, help="Population size per generation")
@click.option("--generations", "-g", default=10, help="Number of generations")
@click.option("--start", "-S", required=True, help="Backtest start date (YYYY-MM-DD)")
@click.option("--end", "-E", required=True, help="Backtest end date (YYYY-MM-DD)")
@click.option("--interval", "-i", default="4h", help="Tick interval")
@click.option("--symbols", "-s", multiple=True, default=["PF_XBTUSD", "PF_ETHUSD", "PF_SOLUSD"],
              help="Symbols to trade")
@click.option("--agent-class", "-a",
              default="agent_arena.agents.llm_trader.LLMTrader",
              help="Agent class to evolve")
@click.option("--name", "-n", default=None, help="Name for this evolution run")
@click.option("--elite", default=3, help="Number of elite genomes preserved per generation")
@click.option("--mutation-rate", default=0.15, help="Per-gene mutation probability")
@click.option("--base-url", default="http://192.168.0.42:8001/v1", help="LLM API base URL")
@click.option("--api-key-env", default="LOCAL_API_KEY", help="Env var name for API key")
def evolve_run(population, generations, start, end, interval, symbols,
               agent_class, name, elite, mutation_rate, base_url, api_key_env):
    """Run a parameter evolution experiment."""
    asyncio.run(_evolve_run(
        population, generations, start, end, interval, list(symbols),
        agent_class, name, elite, mutation_rate, base_url, api_key_env,
    ))


async def _evolve_run(population, generations, start, end, interval, symbols,
                       agent_class, name, elite, mutation_rate, base_url, api_key_env):
    """Execute an evolution run."""
    from agent_arena.evolution.engine import EvolutionEngine

    storage = get_storage()
    await storage.initialize()

    run_name = name or f"Evolution {start} to {end}"

    console.print("\n[bold]Parameter Evolution[/]")
    console.print(f"  Population: {population}")
    console.print(f"  Generations: {generations}")
    console.print(f"  Date range: {start} to {end}")
    console.print(f"  Symbols: {', '.join(symbols)}")
    console.print(f"  Interval: {interval}")
    console.print(f"  Agent class: {agent_class}")
    console.print(f"  Elite count: {elite}")
    console.print(f"  Mutation rate: {mutation_rate}")
    console.print(f"  Base URL: {base_url}\n")

    def emit(event_type, data):
        if event_type == "evolution_generation":
            gen = data["generation"]
            best = data["best_fitness"]
            avg = data["avg_fitness"]
            console.print(
                f"  Gen {gen:3d} | "
                f"Best: [bold]{best:.4f}[/] | "
                f"Avg: {avg:.4f} | "
                f"Genome: {data['best_genome_id'][:8]}"
            )

    async def on_evolution_complete(run_id: str):
        """Callback triggered when evolution completes successfully."""
        from agent_arena.agents.observer_agent import ObserverAgent
        logger.info("Running Observer analysis for evolution run: %s", run_id)
        observer = ObserverAgent(
            storage=storage,
            skills_dir=".claude/skills",
        )
        observer_result = await observer.analyze_evolution_run(run_id)
        logger.info("Observer analysis result: %s", observer_result.get("status"))

    engine = EvolutionEngine(
        population_size=population,
        generations=generations,
        elite_count=elite,
        mutation_rate=mutation_rate,
        backtest_start=start,
        backtest_end=end,
        tick_interval=interval,
        symbols=symbols,
        storage=storage,
        agent_class=agent_class,
        base_url=base_url,
        api_key_env=api_key_env,
        event_emitter=emit,
        on_completion_callback=on_evolution_complete,
    )

    result = await engine.run(name=run_name)

    console.print("\n[bold green]Evolution Complete![/]")
    console.print(f"  Run ID: {result['run_id']}")
    console.print(f"  Generations: {result['generations_completed']}")
    console.print(f"  Best fitness: [bold]{result['best_fitness']:.4f}[/]")

    if result.get("validation"):
        val = result["validation"]
        console.print(f"  Train fitness: {val['train_fitness']:.4f}")
        console.print(f"  Validation fitness: {val['val_fitness']:.4f}")

    if result.get("overfit_warning"):
        console.print(
            "[bold yellow]  WARNING: Possible overfitting "
            "detected (>30% fitness drop on validation)[/]"
        )

    if result.get("best_genome"):
        genome = result["best_genome"]
        console.print("\n[bold]Best Genome:[/]")
        console.print(f"  Model: {genome['model']}")
        console.print(f"  Temperature: {genome['temperature']}")
        console.print(f"  Confidence threshold: {genome['confidence_threshold']}")
        console.print(f"  Position size: {genome['position_size_pct']}")
        console.print(f"  Stop-loss: {genome['sl_pct']}")
        console.print(f"  Take-profit: {genome['tp_pct']}")
        console.print(f"  Max leverage: {genome['max_leverage']}")

    await storage.close()


@_evolve_group.command("status")
@click.argument("run_id")
def evolve_status(run_id):
    """Show status of an evolution run."""
    asyncio.run(_evolve_status(run_id))


async def _evolve_status(run_id):
    """Display evolution run status."""
    from agent_arena.evolution.storage import EvolutionStorage

    storage = get_storage()
    await storage.initialize()

    evo_storage = EvolutionStorage(storage)
    summary = await evo_storage.get_run_summary(run_id)

    if not summary:
        console.print(f"[red]Run not found: {run_id}[/]")
        await storage.close()
        return

    console.print(f"\n[bold]Evolution Run: {summary['name']}[/]")
    console.print(f"  ID: {summary['run_id']}")
    console.print(f"  Status: {summary['status']}")
    console.print(f"  Population: {summary['population_size']}")
    console.print(f"  Generations: {summary['current_generation']}/{summary['max_generations']}")
    console.print(f"  Best fitness: {summary['best_fitness']}")
    console.print(f"  Date range: {summary['backtest_start']} to {summary['backtest_end']}")

    if summary.get("generations"):
        console.print("\n[bold]Fitness by Generation:[/]")
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Gen", width=5)
        table.add_column("Best", justify="right")
        table.add_column("Avg", justify="right")
        table.add_column("Worst", justify="right")
        table.add_column("Pop", justify="right", width=5)

        for g in summary["generations"]:
            table.add_row(
                str(g["generation"]),
                f"{g['best_fitness']:.4f}" if g["best_fitness"] else "N/A",
                f"{g['avg_fitness']:.4f}" if g["avg_fitness"] else "N/A",
                f"{g['worst_fitness']:.4f}" if g["worst_fitness"] else "N/A",
                str(g["pop_size"]),
            )
        console.print(table)

    await storage.close()


@_evolve_group.command("best")
@click.argument("run_id")
@click.option("--export", "-o", default=None, help="Export as YAML config file")
def evolve_best(run_id, export):
    """Show or export the best genome from a run."""
    asyncio.run(_evolve_best(run_id, export))


async def _evolve_best(run_id, export_path):
    """Display or export the best genome."""
    from agent_arena.evolution.genome import AgentGenome
    from agent_arena.evolution.storage import EvolutionStorage

    storage = get_storage()
    await storage.initialize()

    evo_storage = EvolutionStorage(storage)
    best = await evo_storage.get_best_genome(run_id)

    if not best:
        console.print(f"[red]No genomes found for run: {run_id}[/]")
        await storage.close()
        return

    genome = AgentGenome.from_dict(best["genome"])

    console.print(f"\n[bold]Best Genome from {run_id}[/]")
    console.print(f"  Genome ID: {best['genome_id']}")
    console.print(f"  Generation: {best['generation']}")
    console.print(f"  Fitness: {best['fitness']:.4f}")
    console.print(f"  Model: {genome.model}")
    console.print(f"  Temperature: {genome.temperature}")
    console.print(f"  Max tokens: {genome.max_tokens}")
    console.print(f"  Confidence threshold: {genome.confidence_threshold}")
    console.print(f"  Position size: {genome.position_size_pct}")
    console.print(f"  Stop-loss: {genome.sl_pct}")
    console.print(f"  Take-profit: {genome.tp_pct}")
    console.print(f"  Max leverage: {genome.max_leverage}")

    if best.get("metrics"):
        console.print("\n[bold]Metrics:[/]")
        for key, val in best["metrics"].items():
            console.print(f"  {key}: {val}")

    if export_path:
        # Build YAML config from best genome
        agent_config = genome.to_agent_config(
            agent_id="evolved_agent",
            agent_name="Evolved Trader",
            base_url="http://192.168.0.42:8001/v1",
            api_key_env="LOCAL_API_KEY",
        )

        run_summary = await evo_storage.get_run_summary(run_id)
        symbols = run_summary["symbols"] if run_summary else ["PF_XBTUSD", "PF_ETHUSD"]

        config = {
            "name": f"Evolved Agent ({run_id})",
            "symbols": symbols,
            "interval_seconds": 120,
            "agent_timeout_seconds": 180,
            "candles": {"enabled": True, "intervals": ["1h", "4h"], "limit": 100},
            "agents": [agent_config],
        }

        with open(export_path, "w") as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)

        console.print(f"\n[green]Exported to {export_path}[/]")

    await storage.close()


@_evolve_group.command("history")
@click.option("--limit", "-l", default=20, help="Number of runs to show")
def evolve_history(limit):
    """List all evolution runs."""
    asyncio.run(_evolve_history(limit))


async def _evolve_history(limit):
    """List evolution runs."""
    from agent_arena.evolution.storage import EvolutionStorage

    storage = get_storage()
    await storage.initialize()

    evo_storage = EvolutionStorage(storage)
    runs = await evo_storage.list_runs(limit=limit)

    if not runs:
        console.print("[dim]No evolution runs found.[/]")
        await storage.close()
        return

    table = Table(title="Evolution Runs", show_header=True, header_style="bold cyan")
    table.add_column("Run ID", style="dim")
    table.add_column("Name")
    table.add_column("Status")
    table.add_column("Pop", justify="right", width=5)
    table.add_column("Gen", justify="right", width=8)
    table.add_column("Best Fit", justify="right")
    table.add_column("Created", style="dim")

    for r in runs:
        status_style = {"completed": "green", "running": "yellow", "cancelled": "red"}.get(
            r["status"], "dim"
        )
        table.add_row(
            r["run_id"],
            r.get("name", ""),
            Text(r["status"], style=status_style),
            str(r["population_size"]),
            f"{r['current_generation']}/{r['max_generations']}",
            f"{r['best_fitness']:.4f}" if r["best_fitness"] else "N/A",
            r["created_at"][:19] if r["created_at"] else "",
        )

    console.print(table)
    await storage.close()


@_evolve_group.command("analyze")
@click.argument("run_id")
def evolve_analyze(run_id):
    """Analyze evolution run and generate evolved-parameters skill."""
    asyncio.run(_evolve_analyze(run_id))


async def _evolve_analyze(run_id):
    """Run Observer analysis on an evolution run."""
    from agent_arena.agents.observer_agent import ObserverAgent

    storage = get_storage()
    await storage.initialize()

    console.print(f"\n[bold]Analyzing Evolution Run: {run_id}[/]")
    console.print("[dim]Running Observer analysis...[/]\n")

    observer = ObserverAgent(
        storage=storage,
        skills_dir=".claude/skills",
    )

    try:
        result = await observer.analyze_evolution_run(run_id)

        if result.get("status") == "success":
            console.print("[bold green]Analysis Complete![/]\n")
            console.print(f"  Run ID: {result['run_id']}")
            console.print(f"  Genomes analyzed: {result['genomes_analyzed']}")
            console.print(f"  Parameter insights: {result['patterns_found']}")
            console.print(f"  Skill written: {result['skill_written']}\n")

            if result.get("summary"):
                summary = result["summary"]
                console.print("[bold]Summary:[/]")
                console.print(f"  Best fitness: {summary.get('best_fitness', 'N/A')}")
                console.print(f"  Parameter insights: {summary.get('parameter_insights', 0)}")
                console.print(f"  Character archetypes: {summary.get('character_archetypes', 0)}")

            console.print("\n[dim]Skill saved to: .claude/skills/evolved-parameters/SKILL.md[/]")
        else:
            console.print(f"[red]Analysis failed: {result.get('message', 'Unknown error')}[/]")

    except Exception as e:
        console.print(f"[red]Error during analysis: {e}[/]")
        import traceback
        traceback.print_exc()

    await storage.close()


# --- Experiment commands ---

@cli.group(name="experiment")
def _experiment_group():
    """Self-evolving experiment commands."""
    pass


@_experiment_group.command("run")
@click.option("--config", "-c", "config_path", default="configs/experiment.yaml",
              help="Experiment config file")
@click.option("--budget", "-b", type=float, default=None, help="Budget override (USD)")
@click.option("--generations", "-g", type=int, default=None, help="Generations override")
@click.option("--population", "-p", type=int, default=None, help="Population size override")
def experiment_run(config_path, budget, generations, population):
    """Run an experiment cycle."""
    asyncio.run(_experiment_run(config_path, budget, generations, population))


async def _experiment_run(config_path: str, budget: float | None,
                           generations: int | None, population: int | None):
    """Execute an experiment."""
    from agent_arena.experiment.orchestrator import ExperimentConfig, ExperimentOrchestrator

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    exp_cfg = raw.get("experiment", {})
    bt_cfg = raw.get("backtest", {})
    inf_cfg = raw.get("inference", {})

    config = ExperimentConfig(
        name=raw.get("name", "CLI Experiment"),
        population_size=population or exp_cfg.get("population_size", 16),
        generations=generations or exp_cfg.get("generations", 5),
        budget_limit_usd=budget or exp_cfg.get("budget_limit_usd", 5.0),
        elite_count=exp_cfg.get("elite_count", 3),
        mutation_rate=exp_cfg.get("mutation_rate", 0.15),
        fitness_weights=exp_cfg.get("fitness_weights", {}),
        validation_threshold=exp_cfg.get("validation_threshold", 0.7),
        promotion_count=exp_cfg.get("promotion_count", 3),
        backtest_start=bt_cfg.get("start", ""),
        backtest_end=bt_cfg.get("end", ""),
        tick_interval=bt_cfg.get("tick_interval", "4h"),
        symbols=bt_cfg.get("symbols", ["PF_XBTUSD", "PF_ETHUSD", "PF_SOLUSD"]),
        base_url=inf_cfg.get("base_url", ""),
        api_key_env=inf_cfg.get("api_key_env", "TOGETHER_API_KEY"),
    )

    errors = config.validate()
    if errors:
        console.print(f"[red]Config errors: {'; '.join(errors)}[/]")
        return

    storage = get_storage()
    await storage.initialize()

    console.print("\n[bold]Self-Evolving Experiment[/]")
    console.print(f"  Population: {config.population_size}")
    console.print(f"  Generations: {config.generations}")
    console.print(f"  Budget: ${config.budget_limit_usd:.2f}")
    console.print(f"  Date range: {config.backtest_start} to {config.backtest_end}")
    console.print(f"  Symbols: {', '.join(config.symbols)}\n")

    def emit(event_type, data):
        if event_type == "evolution_generation":
            gen = data.get("generation", "?")
            best = data.get("best_fitness", 0)
            avg = data.get("avg_fitness", 0)
            console.print(
                f"  Gen {gen:3} | Best: [bold]{best:.4f}[/] | Avg: {avg:.4f}"
            )

    orchestrator = ExperimentOrchestrator(
        config=config,
        storage=storage,
        event_emitter=emit,
    )

    result = await orchestrator.run()

    console.print(f"\n[bold]Result: {result.status}[/]")
    console.print(f"  Best fitness: {result.best_fitness:.4f}")
    console.print(f"  Validation fitness: {result.validation_fitness:.4f}")
    console.print(f"  Generations: {result.generations_completed}")
    console.print(f"  Cost: ${result.total_cost_usd:.2f}")
    console.print(f"  Promotion candidates: {len(result.promotion_candidates)}")

    if result.overfit_warning:
        console.print("[bold yellow]  WARNING: Possible overfitting detected[/]")

    if result.error:
        console.print(f"[red]  Error: {result.error}[/]")

    await storage.close()


@_experiment_group.command("status")
@click.option("--limit", "-l", default=10, help="Number of runs to show")
def experiment_status(limit):
    """List recent experiment runs."""
    asyncio.run(_experiment_status(limit))


async def _experiment_status(limit: int):
    """List experiment runs."""
    storage = get_storage()
    await storage.initialize()

    if not hasattr(storage, "pool"):
        console.print("[yellow]Experiments require PostgreSQL backend.[/]")
        await storage.close()
        return

    try:
        async with storage.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, name, status, best_fitness, total_cost_usd,
                       generations_completed, created_at
                FROM experiment_runs
                ORDER BY created_at DESC
                LIMIT $1
                """,
                limit,
            )

        if not rows:
            console.print("[dim]No experiment runs found.[/]")
        else:
            table = Table(title="Experiment Runs", show_header=True, header_style="bold cyan")
            table.add_column("ID", style="dim")
            table.add_column("Name")
            table.add_column("Status")
            table.add_column("Fitness", justify="right")
            table.add_column("Cost", justify="right")
            table.add_column("Gens", justify="right")
            table.add_column("Created", style="dim")

            for r in rows:
                status_style = {
                    "completed": "green", "running": "yellow",
                    "failed": "red", "budget_exceeded": "red",
                }.get(r["status"], "dim")
                table.add_row(
                    r["id"],
                    r["name"] or "",
                    Text(r["status"], style=status_style),
                    f"{r['best_fitness']:.4f}" if r["best_fitness"] else "N/A",
                    f"${float(r['total_cost_usd']):.2f}" if r["total_cost_usd"] else "$0.00",
                    str(r["generations_completed"]),
                    r["created_at"].strftime("%Y-%m-%d %H:%M") if r["created_at"] else "",
                )
            console.print(table)
    except Exception as e:
        console.print(f"[red]Error: {e}[/]")

    await storage.close()


@_experiment_group.command("promotions")
@click.option("--status", "-s", default=None, help="Filter by status (pending/approved/rejected)")
def experiment_promotions(status):
    """List promotion queue."""
    asyncio.run(_experiment_promotions(status))


async def _experiment_promotions(status_filter: str | None):
    """List promotion candidates."""
    storage = get_storage()
    await storage.initialize()

    if not hasattr(storage, "pool"):
        console.print("[yellow]Experiments require PostgreSQL backend.[/]")
        await storage.close()
        return

    try:
        async with storage.pool.acquire() as conn:
            if status_filter:
                rows = await conn.fetch(
                    "SELECT * FROM promotion_queue WHERE status = $1 ORDER BY fitness DESC",
                    status_filter,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM promotion_queue ORDER BY created_at DESC LIMIT 50"
                )

        if not rows:
            console.print("[dim]No promotions found.[/]")
        else:
            table = Table(title="Promotion Queue", show_header=True, header_style="bold cyan")
            table.add_column("ID", justify="right")
            table.add_column("Experiment", style="dim")
            table.add_column("Fitness", justify="right")
            table.add_column("Status")
            table.add_column("Created", style="dim")

            for r in rows:
                status_style = {
                    "pending": "yellow", "approved": "green",
                    "rejected": "red", "deployed": "bold green",
                }.get(r["status"], "dim")
                table.add_row(
                    str(r["id"]),
                    r["experiment_id"] or "",
                    f"{r['fitness']:.4f}" if r["fitness"] else "N/A",
                    Text(r["status"], style=status_style),
                    r["created_at"].strftime("%Y-%m-%d %H:%M") if r["created_at"] else "",
                )
            console.print(table)
    except Exception as e:
        console.print(f"[red]Error: {e}[/]")

    await storage.close()


@_experiment_group.command("approve")
@click.argument("promotion_id", type=int)
@click.option("--notes", "-n", default="", help="Approval notes")
def experiment_approve(promotion_id, notes):
    """Approve a promotion candidate."""
    asyncio.run(_experiment_approve(promotion_id, notes))


async def _experiment_approve(promotion_id: int, notes: str):
    """Approve a promotion."""
    from datetime import datetime, timezone

    storage = get_storage()
    await storage.initialize()

    if not hasattr(storage, "pool"):
        console.print("[yellow]Experiments require PostgreSQL backend.[/]")
        await storage.close()
        return

    try:
        async with storage.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM promotion_queue WHERE id = $1", promotion_id
            )
            if not row:
                console.print(f"[red]Promotion {promotion_id} not found.[/]")
            else:
                await conn.execute(
                    """
                    UPDATE promotion_queue
                    SET status = 'approved', notes = $1, reviewed_at = $2
                    WHERE id = $3
                    """,
                    notes, datetime.now(timezone.utc), promotion_id,
                )
                console.print(f"[green]Promotion {promotion_id} approved.[/]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/]")

    await storage.close()


# --- Memory commands ---

@cli.group(name="memory")
def _memory_group():
    """Metabolic memory commands."""
    pass


@_memory_group.command("health")
@click.option("--agent", "-a", "agent_id", default=None, help="Agent ID")
def memory_health(agent_id):
    """Show memory health metrics."""
    asyncio.run(_memory_health(agent_id))


async def _memory_health(agent_id: str | None):
    storage = get_storage()
    await storage.initialize()

    if not hasattr(storage, "pool"):
        console.print("[yellow]Memory requires PostgreSQL backend.[/]")
        await storage.close()
        return

    try:
        async with storage.pool.acquire() as conn:
            where = "WHERE agent_id = $1" if agent_id else ""
            params = [agent_id] if agent_id else []

            total = await conn.fetchrow(
                f"SELECT COUNT(*) as cnt FROM trade_reflections {where}", *params
            )
            digested = await conn.fetchrow(
                f"SELECT COUNT(*) as cnt FROM trade_reflections {where} {'AND' if where else 'WHERE'} is_digested = TRUE",
                *params,
            )
            principles = await conn.fetchrow(
                f"SELECT COUNT(*) as cnt FROM abstract_principles {where} {'AND' if where else 'WHERE'} is_active = TRUE",
                *params,
            )

            table = Table(title="Memory Health", show_header=False)
            table.add_column("Metric", style="dim")
            table.add_column("Value", style="bold")
            table.add_row("Total reflections", str(total["cnt"]))
            table.add_row("Digested", str(digested["cnt"]))
            table.add_row("Active principles", str(principles["cnt"]))
            if agent_id:
                table.add_row("Agent", agent_id)
            console.print(table)
    except Exception as e:
        console.print(f"[red]Error: {e}[/]")

    await storage.close()


@_memory_group.command("digest")
@click.option("--agent", "-a", "agent_id", required=True, help="Agent ID")
@click.option("--dry-run", is_flag=True, help="Show scores without digesting")
def memory_digest(agent_id, dry_run):
    """Run memory digestion for an agent."""
    asyncio.run(_memory_digest(agent_id, dry_run))


async def _memory_digest(agent_id: str, dry_run: bool):
    storage = get_storage()
    await storage.initialize()

    if dry_run:
        from agent_arena.memory.scoring import MemoryScorer

        scorer = MemoryScorer(storage)
        scored = await scorer.score_memories(agent_id)

        if not scored:
            console.print("[dim]No memories to score.[/]")
        else:
            table = Table(title=f"Memory Scores: {agent_id}", show_header=True,
                          header_style="bold cyan")
            table.add_column("ID", justify="right")
            table.add_column("Score", justify="right")
            table.add_column("Action")
            table.add_column("Recency", justify="right")
            table.add_column("Impact", justify="right")
            table.add_column("Frequency", justify="right")

            for m in scored:
                action_style = {"keep": "green", "digest": "yellow", "prune": "red"}.get(m.action, "dim")
                table.add_row(
                    str(m.memory_id),
                    f"{m.metabolic_score:.3f}",
                    Text(m.action, style=action_style),
                    f"{m.recency_score:.3f}",
                    f"{m.impact_score:.3f}",
                    f"{m.frequency_score:.3f}",
                )
            console.print(table)

            to_digest = sum(1 for m in scored if m.action == "digest")
            to_prune = sum(1 for m in scored if m.action == "prune")
            console.print(f"\n[dim]Would digest {to_digest}, prune {to_prune}[/]")
    else:
        from agent_arena.memory.digestion import MemoryDigester

        digester = MemoryDigester(storage=storage)
        result = await digester.run_digestion_cycle(agent_id)

        console.print(f"\n[bold]Digestion Result: {agent_id}[/]")
        console.print(f"  Scored: {result.memories_scored}")
        console.print(f"  Digested: {result.memories_digested}")
        console.print(f"  Pruned: {result.memories_pruned}")
        console.print(f"  Principles created: {result.principles_created}")

    await storage.close()


@_memory_group.command("principles")
@click.option("--agent", "-a", "agent_id", default=None, help="Agent ID")
@click.option("--limit", "-l", default=20, help="Max results")
def memory_principles(agent_id, limit):
    """List active trading principles."""
    asyncio.run(_memory_principles(agent_id, limit))


async def _memory_principles(agent_id: str | None, limit: int):
    storage = get_storage()
    await storage.initialize()

    if not hasattr(storage, "pool"):
        console.print("[yellow]Memory requires PostgreSQL backend.[/]")
        await storage.close()
        return

    try:
        async with storage.pool.acquire() as conn:
            if agent_id:
                rows = await conn.fetch(
                    """
                    SELECT * FROM abstract_principles
                    WHERE agent_id = $1 AND is_active = TRUE
                    ORDER BY confidence DESC
                    LIMIT $2
                    """,
                    agent_id, limit,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM abstract_principles WHERE is_active = TRUE ORDER BY confidence DESC LIMIT $1",
                    limit,
                )

            if not rows:
                console.print("[dim]No active principles found.[/]")
            else:
                for r in rows:
                    regime = f" [{r['regime']}]" if r.get('regime') and r['regime'] != 'all' else ""
                    console.print(
                        f"  [bold]{r['principle']}[/]{regime} "
                        f"(conf={r['confidence']:.0%}, applied={r['application_count']}x) "
                        f"[dim]{r['agent_id']}[/]"
                    )
    except Exception as e:
        console.print(f"[red]Error: {e}[/]")

    await storage.close()


# --- Reflexion commands ---

@cli.group(name="reflexion")
def _reflexion_group():
    """Reflexion system commands."""
    pass


@_reflexion_group.command("recent")
@click.option("--agent", "-a", "agent_id", default=None, help="Agent ID")
@click.option("--outcome", "-o", default=None, help="Filter by outcome (win/loss)")
@click.option("--limit", "-l", default=10, help="Max results")
def reflexion_recent(agent_id, outcome, limit):
    """List recent trade reflections."""
    asyncio.run(_reflexion_recent(agent_id, outcome, limit))


async def _reflexion_recent(agent_id: str | None, outcome: str | None, limit: int):
    storage = get_storage()
    await storage.initialize()

    if not hasattr(storage, "pool"):
        console.print("[yellow]Reflexion requires PostgreSQL backend.[/]")
        await storage.close()
        return

    try:
        async with storage.pool.acquire() as conn:
            conditions = []
            params = []
            idx = 1

            if agent_id:
                conditions.append(f"agent_id = ${idx}")
                params.append(agent_id)
                idx += 1
            if outcome:
                conditions.append(f"outcome = ${idx}")
                params.append(outcome)
                idx += 1

            params.append(limit)
            where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            rows = await conn.fetch(
                f"""
                SELECT agent_id, symbol, side, realized_pnl, outcome, lesson, created_at
                FROM trade_reflections
                {where}
                ORDER BY created_at DESC
                LIMIT ${idx}
                """,
                *params,
            )

            if not rows:
                console.print("[dim]No reflections found.[/]")
            else:
                for r in rows:
                    emoji = "[green]+[/]" if r["outcome"] == "win" else "[red]-[/]"
                    pnl = float(r["realized_pnl"]) if r["realized_pnl"] else 0
                    console.print(
                        f"  {emoji} {r['agent_id']}: {r['symbol']} {r['side']} "
                        f"${pnl:+.2f} — {r.get('lesson', '')}"
                    )
    except Exception as e:
        console.print(f"[red]Error: {e}[/]")

    await storage.close()


@_reflexion_group.command("clusters")
@click.option("--limit", "-l", default=10, help="Max results")
def reflexion_clusters(limit):
    """List failure clusters."""
    asyncio.run(_reflexion_clusters(limit))


async def _reflexion_clusters(limit: int):
    storage = get_storage()
    await storage.initialize()

    if not hasattr(storage, "pool"):
        console.print("[yellow]Reflexion requires PostgreSQL backend.[/]")
        await storage.close()
        return

    try:
        async with storage.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM failure_clusters ORDER BY created_at DESC LIMIT $1",
                limit,
            )
            if not rows:
                console.print("[dim]No failure clusters found.[/]")
            else:
                for r in rows:
                    regime = f" [{r['regime']}]" if r.get('regime') else ""
                    console.print(
                        f"  [bold]{r['cluster_label']}[/]{regime} "
                        f"({r['sample_size']} trades)"
                    )
                    if r.get("proposed_skill"):
                        console.print(f"    Rule: {r['proposed_skill']}")
    except Exception as e:
        console.print(f"[red]Error: {e}[/]")

    await storage.close()


@_reflexion_group.command("proposals")
@click.option("--status", "-s", default=None, help="Filter by status")
def reflexion_proposals(status):
    """List skill proposals."""
    asyncio.run(_reflexion_proposals(status))


async def _reflexion_proposals(status_filter: str | None):
    storage = get_storage()
    await storage.initialize()

    if not hasattr(storage, "pool"):
        console.print("[yellow]Reflexion requires PostgreSQL backend.[/]")
        await storage.close()
        return

    try:
        async with storage.pool.acquire() as conn:
            if status_filter:
                rows = await conn.fetch(
                    "SELECT * FROM skill_proposals WHERE status = $1 ORDER BY created_at DESC",
                    status_filter,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM skill_proposals ORDER BY created_at DESC LIMIT 20"
                )

            if not rows:
                console.print("[dim]No skill proposals found.[/]")
            else:
                table = Table(title="Skill Proposals", show_header=True, header_style="bold cyan")
                table.add_column("Name")
                table.add_column("Status")
                table.add_column("Improvement", justify="right")
                table.add_column("Created", style="dim")

                for r in rows:
                    status_style = {"promoted": "green", "proposed": "yellow"}.get(r["status"], "dim")
                    table.add_row(
                        r["skill_name"],
                        Text(r["status"], style=status_style),
                        f"{r['improvement_pct']:.1f}%" if r.get("improvement_pct") else "N/A",
                        r["created_at"].strftime("%Y-%m-%d") if r.get("created_at") else "",
                    )
                console.print(table)
    except Exception as e:
        console.print(f"[red]Error: {e}[/]")

    await storage.close()


# --- DEACTIVATED: Bias commands (implementation preserved in analysis/) ---
# To reactivate: uncomment @cli.group() and @_bias_group.command() decorators below

# @cli.group(name="bias")
def _bias_group():
    """Behavioral bias analysis commands."""
    pass


# @_bias_group.command("analyze")
@click.option("--agent", "-a", "agent_id", default=None, help="Analyze a single agent")
@click.option("--save/--no-save", default=True, help="Save results to database")
def bias_analyze(agent_id, save):
    """Analyze behavioral biases for agents."""
    asyncio.run(_bias_analyze(agent_id, save))


async def _bias_analyze(agent_id: Optional[str], save: bool):
    """Run bias analysis."""
    from agent_arena.analysis.bias_scan import analyze_agent_biases

    storage = get_storage()
    await storage.initialize()

    # Determine which agents to analyze
    if agent_id:
        agent_ids = [agent_id]
    else:
        agent_ids = await storage.get_all_agent_ids()

    if not agent_ids:
        console.print("[yellow]No agents found in database.[/]")
        await storage.close()
        return

    console.print(f"\n[bold]Bias Analysis[/] ({len(agent_ids)} agent{'s' if len(agent_ids) != 1 else ''})\n")

    for aid in agent_ids:
        decisions = await storage.get_all_decisions(aid)
        trades = await storage.get_all_trades(aid)

        if not decisions:
            console.print(f"[dim]{aid}: no decisions found, skipping[/]")
            continue

        profile = analyze_agent_biases(aid, decisions, trades)

        # Display results
        table = Table(
            title=f"{aid}",
            show_header=True,
            header_style="bold cyan",
        )
        table.add_column("Bias Type", style="bold")
        table.add_column("Score", justify="right")
        table.add_column("Rating", justify="center")
        table.add_column("Sample Size", justify="right")
        table.add_column("Key Detail")

        for score in profile.scores:
            if not score.sufficient_data:
                table.add_row(
                    score.bias_type.replace("_", " ").title(),
                    "N/A",
                    Text("INSUFFICIENT", style="dim"),
                    str(score.sample_size),
                    _format_detail(score),
                )
            else:
                rating, style = _bias_rating_style(score.value)
                table.add_row(
                    score.bias_type.replace("_", " ").title(),
                    f"{score.value:.3f}",
                    Text(rating, style=style),
                    str(score.sample_size),
                    _format_detail(score),
                )

        console.print(table)
        console.print()

        # Save to DB
        if save:
            await storage.save_bias_profile(profile.to_dict())

    if save:
        console.print("[dim]Results saved to database.[/]")

    await storage.close()


def _bias_rating_style(value: Optional[float]) -> tuple[str, str]:
    """Return (rating_text, rich_style) for a bias score."""
    if value is None:
        return "N/A", "dim"
    if value < 0.3:
        return "LOW", "green"
    if value < 0.6:
        return "MODERATE", "yellow"
    return "HIGH", "red"


def _format_detail(score) -> str:
    """Extract the most useful detail from a BiasScore."""
    d = score.details
    if score.bias_type == "disposition_effect":
        if score.sufficient_data:
            return f"win avg {d.get('avg_winner_duration', '?')}t, loss avg {d.get('avg_loser_duration', '?')}t"
        return f"{d.get('winners', 0)}W / {d.get('losers', 0)}L (need {d.get('min_required', 10)}+)"
    if score.bias_type == "loss_aversion":
        if score.sufficient_data:
            return f"after win: {d.get('avg_size_after_win', '?')}, after loss: {d.get('avg_size_after_loss', '?')}"
        return f"{d.get('post_win_opens', 0)} post-win / {d.get('post_loss_opens', 0)} post-loss"
    if score.bias_type == "overconfidence":
        if score.sufficient_data:
            return f"corr={d.get('correlation', '?')}, win_rate={d.get('win_rate', '?')}"
        return f"{d.get('pairs_with_confidence', 0)} pairs (need {d.get('min_required', 20)}+)"
    return ""


@cli.group()
def contagion():
    """Contagion Tracker commands — system health analysis."""
    pass


@contagion.command("analyze")
@click.option("--save/--no-save", default=True, help="Save results to database")
def contagion_analyze(save):
    """Analyze position diversity and reasoning entropy."""
    asyncio.run(_contagion_analyze(save))


async def _contagion_analyze(save: bool):
    """Run contagion analysis."""
    from agent_arena.analysis.contagion import analyze_contagion

    storage = get_storage()
    await storage.initialize()

    agent_ids = await storage.get_all_agent_ids()

    if not agent_ids:
        console.print("[yellow]No agents found in database.[/]")
        await storage.close()
        return

    # Build decisions_by_agent
    decisions_by_agent = {}
    for aid in agent_ids:
        decisions = await storage.get_all_decisions(aid)
        if decisions:
            decisions_by_agent[aid] = decisions

    if not decisions_by_agent:
        console.print("[yellow]No decisions found.[/]")
        await storage.close()
        return

    console.print(
        f"\n[bold]Contagion Analysis[/] "
        f"({len(decisions_by_agent)} agents)\n"
    )

    snap = analyze_contagion(decisions_by_agent)

    # Display results
    table = Table(
        title="System Health",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")
    table.add_column("Rating", justify="center")
    table.add_column("Samples", justify="right")
    table.add_column("Details")

    for score in snap.scores:
        if not score.sufficient_data:
            table.add_row(
                score.metric_type.replace("_", " ").title(),
                "N/A",
                Text("INSUFFICIENT", style="dim"),
                str(score.sample_size),
                score.details.get("reason", ""),
            )
        else:
            label = _health_rating_style(score.value)
            table.add_row(
                score.metric_type.replace("_", " ").title(),
                f"{score.value:.3f}",
                Text(label[0], style=label[1]),
                str(score.sample_size),
                f"{score.details.get('ticks_analyzed', '?')} ticks, "
                f"{score.details.get('agents', '?')} agents",
            )

    console.print(table)

    # Overall health
    health = snap.system_health
    if health is not None:
        label, style = _health_rating_style(health)
        console.print(
            f"\n  Overall Health: "
            f"[{style}]{health:.2f} ({label})[/]"
        )
    console.print()

    # Save to DB
    if save and hasattr(storage, "save_contagion_snapshot"):
        await storage.save_contagion_snapshot(snap.to_dict())
        console.print("[dim]Results saved to database.[/]")

    await storage.close()


@contagion.command("history")
@click.option("--limit", default=20, help="Number of snapshots to show")
@click.option("--metric", default=None, help="Filter by metric type")
def contagion_history(limit, metric):
    """Show contagion metric history."""
    asyncio.run(_contagion_history(limit, metric))


async def _contagion_history(limit: int, metric: Optional[str]):
    """Display contagion history."""
    storage = get_storage()
    await storage.initialize()

    if not hasattr(storage, "get_contagion_snapshots"):
        console.print("[yellow]Contagion storage not available.[/]")
        await storage.close()
        return

    snapshots = await storage.get_contagion_snapshots(
        metric_type=metric, limit=limit,
    )

    if not snapshots:
        console.print("[yellow]No contagion data found.[/]")
        await storage.close()
        return

    table = Table(
        title="Contagion History",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Time", style="dim")
    table.add_column("Tick", justify="right")
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")
    table.add_column("Rating", justify="center")
    table.add_column("Agents", justify="right")

    for s in snapshots:
        value = s.get("value")
        label, style = _health_rating_style(value)
        table.add_row(
            str(s.get("created_at", ""))[:19],
            str(s.get("tick", "")),
            s.get("metric_type", "").replace("_", " ").title(),
            f"{value:.3f}" if value is not None else "N/A",
            Text(label, style=style),
            str(s.get("agent_count", "")),
        )

    console.print(table)
    await storage.close()


def _health_rating_style(value: Optional[float]) -> tuple[str, str]:
    """Return (rating_text, rich_style) for a contagion score."""
    if value is None:
        return "N/A", "dim"
    if value >= 0.6:
        return "HEALTHY", "green"
    if value >= 0.3:
        return "MODERATE", "yellow"
    return "WARNING", "red"


@cli.group()
def scenario():
    """Scenario management commands."""
    pass


@scenario.command("curate")
@click.option("--id", "scenario_id", required=True, help="Unique scenario identifier")
@click.option("--name", required=True, help="Human-readable name")
@click.option("--description", default="", help="Description of the scenario")
@click.option("--start", "start_date", required=True, help="Start date (YYYY-MM-DD)")
@click.option("--end", "end_date", required=True, help="End date (YYYY-MM-DD)")
@click.option("--symbols", required=True, multiple=True, help="Trading pairs (e.g. PF_XBTUSD)")
@click.option("--interval", default="5m", help="Tick interval (default: 5m)")
@click.option("--candle-intervals", multiple=True, default=None, help="Additional candle intervals")
def scenario_curate(
    scenario_id, name, description, start_date, end_date,
    symbols, interval, candle_intervals,
):
    """Curate a new scenario from Kraken Futures historical data."""
    asyncio.run(_scenario_curate(
        scenario_id, name, description, start_date, end_date,
        list(symbols), interval,
        list(candle_intervals) if candle_intervals else None,
    ))


async def _scenario_curate(
    scenario_id: str,
    name: str,
    description: str,
    start_date: str,
    end_date: str,
    symbols: list[str],
    interval: str,
    candle_intervals: list[str] | None,
):
    from agent_arena.scenarios.curator import ScenarioCurator

    curator = ScenarioCurator()
    console.print(f"\n[bold]Curating scenario:[/] {scenario_id}")
    console.print(f"  Symbols: {', '.join(symbols)}")
    console.print(f"  Period: {start_date} → {end_date}")
    console.print(f"  Interval: {interval}")
    console.print()

    def on_progress(symbol, intv, fetched, total):
        console.print(f"  [dim]{symbol}/{intv}: {fetched}/{total} candles[/]", end="\r")

    scenario = await curator.curate(
        scenario_id=scenario_id,
        name=name,
        description=description,
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        interval=interval,
        candle_intervals=candle_intervals,
        progress_callback=on_progress,
    )

    console.print(f"\n[green]Scenario saved:[/] data/scenarios/{scenario_id}/")
    console.print(f"  Ticks: {scenario.total_ticks}")
    console.print(f"  Checksum: {scenario.checksum[:16]}...")


@scenario.command("list")
def scenario_list():
    """List all saved scenarios."""
    from agent_arena.scenarios.registry import ScenarioRegistry

    reg = ScenarioRegistry()
    scenarios = reg.list_scenarios()

    if not scenarios:
        console.print("[yellow]No scenarios found in data/scenarios/[/]")
        return

    table = Table(title="Saved Scenarios", show_header=True, header_style="bold cyan")
    table.add_column("ID", style="bold")
    table.add_column("Name")
    table.add_column("Symbols")
    table.add_column("Period")
    table.add_column("Interval")
    table.add_column("Ticks", justify="right")
    table.add_column("Created")

    for s in scenarios:
        table.add_row(
            s.scenario_id,
            s.name,
            ", ".join(s.symbols),
            f"{s.start_date} → {s.end_date}",
            s.interval,
            str(s.total_ticks),
            s.created_at[:10],
        )

    console.print(table)


@scenario.command("verify")
def scenario_verify():
    """Verify checksum integrity of all scenarios."""
    from agent_arena.scenarios.registry import ScenarioRegistry

    reg = ScenarioRegistry()
    results = reg.verify_all()

    if not results:
        console.print("[yellow]No scenarios found.[/]")
        return

    for sid, passed in results.items():
        if passed:
            console.print(f"  [green]✓[/] {sid}")
        else:
            console.print(f"  [red]✗[/] {sid} — checksum mismatch!")

    total = len(results)
    passed_count = sum(1 for v in results.values() if v)
    console.print(f"\n{passed_count}/{total} scenarios verified.")


# ---------------------------------------------------------------------------
# Codegen
# ---------------------------------------------------------------------------


@cli.command("codegen")
@click.option(
    "--config", "-c", default="configs/production.yaml",
    help="Competition config (used to identify agents).",
)
@click.option(
    "--lookback-days", default=5, type=int,
    help="Number of journal entries to scan.",
)
@click.option(
    "--dry-run", is_flag=True, default=False,
    help="Show findings without making changes.",
)
@click.option(
    "--max-changes", default=3, type=int,
    help="Maximum findings to act on.",
)
@click.option(
    "--no-pr", is_flag=True, default=False,
    help="Skip PR creation (commit to branch only).",
)
def codegen(config, lookback_days, dry_run, max_changes, no_pr):
    """Generate code fixes from Observer Journal findings."""
    asyncio.run(_codegen(config, lookback_days, dry_run, max_changes, no_pr))


async def _codegen(
    config_path: str,
    lookback_days: int,
    dry_run: bool,
    max_changes: int,
    no_pr: bool,
):
    """Load journals, extract findings, run codegen agent, open PR."""
    import subprocess
    from datetime import datetime, timezone

    from agent_arena.codegen.agent import (
        MODEL as CODEGEN_MODEL,
        CodegenAgent,
    )
    from agent_arena.codegen.findings import (
        extract_findings,
        save_codegen_history,
    )

    storage = get_storage()
    await storage.initialize()

    # --- Load journal entries ---
    entries = await storage.get_journal_entries(limit=lookback_days)
    await storage.close()

    if not entries:
        console.print("[yellow]No journal entries found.[/]")
        return

    console.print(
        f"\n[bold]Journal Codegen[/]  "
        f"({len(entries)} entries, lookback={lookback_days})\n"
    )

    # --- Extract findings ---
    findings = extract_findings(entries)

    if not findings:
        console.print("[green]No recurring findings detected. Nothing to do.[/]")
        return

    # --- Display findings table ---
    table = Table(
        title="Recurring Findings",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("#", justify="right", style="dim")
    table.add_column("Finding", style="bold")
    table.add_column("Severity", justify="right")
    table.add_column("Agents")
    table.add_column("Entries", justify="right")
    table.add_column("Targets")

    for i, f in enumerate(findings, 1):
        table.add_row(
            str(i),
            f.finding_id,
            f"{f.severity:.2f}",
            ", ".join(f.agent_ids[:3]) or "system",
            str(len(f.entry_dates)),
            ", ".join(f.target_files),
        )

    console.print(table)
    console.print()

    if dry_run:
        console.print("[dim]--dry-run: stopping before code changes.[/]")
        return

    # --- Create an isolated git worktree so the main tree is untouched ---
    # Place worktree in /tmp to avoid triggering uvicorn's StatReload
    # watcher (which monitors all .py files under the project root).
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    branch = f"codegen/{date_str}"
    worktree_dir = Path(f"/tmp/agent-arena-codegen-{date_str}").resolve()

    # Clean up stale worktree from a previous run
    if worktree_dir.exists():
        subprocess.run(
            ["git", "worktree", "remove", "--force", str(worktree_dir)],
            capture_output=True, text=True,
        )

    try:
        subprocess.run(
            [
                "git", "worktree", "add",
                str(worktree_dir), "-b", branch,
            ],
            check=True, capture_output=True, text=True,
        )
        console.print(
            f"[green]Created worktree:[/] {worktree_dir} "
            f"(branch {branch})"
        )
    except subprocess.CalledProcessError as exc:
        if "already exists" in exc.stderr:
            # Branch exists — create worktree on existing branch
            subprocess.run(
                [
                    "git", "worktree", "add",
                    str(worktree_dir), branch,
                ],
                check=True, capture_output=True, text=True,
            )
            console.print(
                f"[yellow]Created worktree on existing branch:[/] "
                f"{branch}"
            )
        else:
            console.print(f"[red]Git worktree error:[/] {exc.stderr}")
            return

    try:
        # --- Use HTTPS for push so SSH key is not required ---
        # The gh CLI credential helper handles auth via its token.
        ssh_url = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            capture_output=True, text=True,
            cwd=str(worktree_dir),
        ).stdout.strip()
        if ssh_url.startswith("git@github.com:"):
            https_url = ssh_url.replace(
                "git@github.com:", "https://github.com/",
            )
            if not https_url.endswith(".git"):
                https_url += ".git"
            subprocess.run(
                [
                    "git", "remote", "set-url", "origin",
                    https_url,
                ],
                cwd=str(worktree_dir),
                capture_output=True, text=True,
            )

        # --- Separate stale_fix findings (escalate) from normal ones ---
        normal_findings = [
            f for f in findings if f.finding_id != "stale_fix"
        ]
        stale_findings = [
            f for f in findings if f.finding_id == "stale_fix"
        ]

        # --- Run codegen agent for normal findings ---
        agent = CodegenAgent(project_root=str(worktree_dir))
        all_changes = []
        acted = 0

        for f in normal_findings[:max_changes]:
            console.print(f"\n[bold cyan]Processing:[/] {f.finding_id}")
            for ev in f.evidence[:3]:
                console.print(f"  [dim]{ev}[/]")

            result = agent.run(f)

            if result.error:
                console.print(f"  [red]Error:[/] {result.error}")
                continue

            if not result.changes:
                console.print("  [yellow]No changes made.[/]")
                if result.summary:
                    console.print(
                        f"  [dim]{result.summary[:200]}[/]"
                    )
                continue

            for ch in result.changes:
                console.print(
                    f"  [green]✓[/] {ch.file_path}: "
                    f"{ch.description}"
                )
                all_changes.append(ch)

            if result.summary:
                console.print(
                    Panel(
                        result.summary[:500],
                        title=f"Summary: {f.finding_id}",
                        border_style="dim",
                    )
                )
            acted += 1

        # --- Handle stale_fix findings: escalate via GitHub issue ---
        for sf in stale_findings:
            console.print(
                f"\n[bold yellow]Escalating:[/] "
                f"{sf.stale_finding_id} "
                f"(fixed {sf.prior_fix_count}x, still recurring)"
            )
            for ev in sf.evidence[:3]:
                console.print(f"  [dim]{ev}[/]")

            escalation = agent.escalate(sf)

            console.print(
                Panel(
                    escalation.suggested_approach[:800],
                    title=(
                        f"Escalation: {escalation.stale_finding_id}"
                    ),
                    border_style="yellow",
                )
            )

            if not no_pr:
                # Create GitHub issue for human review
                issue_title = (
                    f"[codegen-escalation] "
                    f"{escalation.stale_finding_id} — "
                    f"prompt fixes ineffective after "
                    f"{escalation.prior_fix_count} attempts"
                )
                evidence_lines = "\n".join(
                    f"- {ev}" for ev in escalation.evidence[:10]
                )
                issue_body = (
                    f"## Codegen Escalation\n\n"
                    f"The codegen agent has attempted to fix "
                    f"**{escalation.stale_finding_id}** "
                    f"{escalation.prior_fix_count} times via "
                    f"prompt/threshold tweaks, but the finding "
                    f"still recurs.\n\n"
                    f"### Evidence\n\n{evidence_lines}\n\n"
                    f"### LLM Diagnosis\n\n"
                    f"{escalation.suggested_approach}\n\n"
                    f"---\n"
                    f"\U0001f916 Generated by `agent-arena codegen`"
                )
                try:
                    issue_result = subprocess.run(
                        [
                            "gh", "issue", "create",
                            "--title", issue_title,
                            "--body", issue_body,
                            "--label",
                            "codegen-escalation,needs-human",
                        ],
                        capture_output=True, text=True,
                        cwd=wt,
                    )
                    if issue_result.returncode == 0:
                        console.print(
                            f"  [bold green]Issue created:[/] "
                            f"{issue_result.stdout.strip()}"
                        )
                    else:
                        console.print(
                            f"  [yellow]Issue creation failed:[/] "
                            f"{issue_result.stderr.strip()}"
                        )
                except FileNotFoundError:
                    console.print(
                        "  [yellow]gh CLI not found — "
                        "skipping issue creation.[/]"
                    )

        if not all_changes:
            console.print(
                "\n[yellow]No files were changed. "
                "Cleaning up worktree.[/]"
            )
            return

        # --- Git add + commit (inside worktree) ---
        changed_files = list({ch.file_path for ch in all_changes})
        wt = str(worktree_dir)
        subprocess.run(
            ["git", "add"] + changed_files,
            check=True, cwd=wt,
        )

        finding_ids = [f.finding_id for f in normal_findings[:acted]]
        first_date = (
            entries[-1].get("journal_date", "?") if entries else "?"
        )
        last_date = (
            entries[0].get("journal_date", "?") if entries else "?"
        )
        commit_msg = (
            f"codegen: address {', '.join(finding_ids)} "
            f"from journal analysis\n\n"
            f"Findings from {len(entries)} journal entries "
            f"({first_date} to {last_date}).\n\n"
            f"Files changed: {', '.join(changed_files)}\n\n"
            f"Co-Authored-By: {CODEGEN_MODEL} "
            f"<noreply@anthropic.com>"
        )

        subprocess.run(
            ["git", "commit", "-m", commit_msg],
            check=True, capture_output=True, text=True, cwd=wt,
        )
        console.print(
            f"\n[green]Committed {len(changed_files)} file(s) "
            f"in worktree.[/]"
        )

        # --- Record codegen history for stale-fix tracking ---
        save_codegen_history(
            finding_ids=finding_ids,
            files_changed=changed_files,
            summary=commit_msg.split("\n")[0],
        )

        # --- Open PR ---
        if no_pr:
            console.print(
                f"[dim]--no-pr: skipping PR creation. "
                f"Branch: {branch}[/]"
            )
            return

        pr_body = (
            "## Summary\n\n"
            f"Automated code fixes from Observer Journal "
            f"analysis ({len(entries)} entries).\n\n"
            "### Findings addressed\n\n"
        )
        for f in normal_findings[:acted]:
            pr_body += (
                f"- **{f.finding_id}** "
                f"(severity {f.severity:.2f}): "
                f"{len(f.evidence)} evidence items across "
                f"{len(f.entry_dates)} entries\n"
            )
        pr_body += (
            "\n### Files changed\n\n"
            + "\n".join(f"- `{fp}`" for fp in changed_files)
            + "\n\n---\n"
            "\U0001f916 Generated with "
            "[agent-arena codegen]"
            "(https://github.com/anthropics/claude-code)"
        )

        try:
            subprocess.run(
                ["git", "push", "-u", "origin", branch],
                check=True, capture_output=True, text=True,
                cwd=wt,
            )
            pr_result = subprocess.run(
                [
                    "gh", "pr", "create",
                    "--title",
                    f"codegen: {', '.join(finding_ids)}",
                    "--body", pr_body,
                ],
                check=True, capture_output=True, text=True,
                cwd=wt,
            )
            console.print(
                f"\n[bold green]PR created:[/] "
                f"{pr_result.stdout.strip()}"
            )
        except FileNotFoundError:
            console.print(
                "[yellow]gh CLI not found — skipping PR. "
                f"Push branch '{branch}' manually.[/]"
            )
        except subprocess.CalledProcessError as exc:
            console.print(
                f"[red]PR creation failed:[/] {exc.stderr}"
            )
    finally:
        # Always clean up the worktree
        if worktree_dir.exists():
            subprocess.run(
                [
                    "git", "worktree", "remove",
                    "--force", str(worktree_dir),
                ],
                capture_output=True, text=True,
            )
            console.print("[dim]Cleaned up worktree.[/]")


@cli.command()
def init():
    """Initialize a new competition config file."""
    default_config = """# Agent Arena Competition Config
name: "My Competition"

symbols:
  - PF_XBTUSD
  - PF_ETHUSD

interval_seconds: 60  # 1 minute between ticks
duration_seconds: 3600  # Run for 1 hour (null for indefinite)

# Fee configuration (optional - defaults shown)
fees:
  taker_fee: 0.0004      # 0.04% for market orders
  maker_fee: 0.0002      # 0.02% for limit orders
  liquidation_fee: 0.005  # 0.5% liquidation penalty

# Trading constraints (optional - defaults shown)
constraints:
  max_leverage: 10
  max_position_pct: 0.25  # Max 25% of equity per position
  starting_capital: 10000

# Historical candles configuration (optional - defaults shown)
candles:
  enabled: true         # Whether to fetch and include candles in context
  intervals:            # Timeframes to fetch
    - 1h
    - 15m
  limit: 100            # Number of candles per interval

agents:
  - id: claude_analyst
    name: "The Analyst"
    class: agent_arena.agents.claude_trader.ClaudeTrader
    config:
      model: claude-sonnet-4-20250514
      character: "Cautious and analytical. Waits for high-conviction setups."
"""

    config_path = Path("configs/competition.yaml")
    config_path.parent.mkdir(parents=True, exist_ok=True)

    if config_path.exists():
        if not click.confirm(f"{config_path} already exists. Overwrite?"):
            return

    config_path.write_text(default_config)
    console.print(f"[green]Created config at {config_path}[/]")
    console.print(f"\nRun with: [bold]agent-arena run {config_path}[/]")


def main():
    """Entry point."""
    load_dotenv()
    cli()


if __name__ == "__main__":
    main()
