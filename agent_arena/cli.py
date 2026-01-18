"""Agent Arena CLI - Rich terminal interface."""

from __future__ import annotations

import asyncio
import signal
import sys
from pathlib import Path
from typing import Optional

import click
from dotenv import load_dotenv
import yaml
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from agent_arena.core.arena import TradingArena
from agent_arena.core.config import CandleConfig, CompetitionConfig, ConstraintsConfig, FeeConfig
from agent_arena.core.runner import CompetitionRunner
from agent_arena.providers.binance import BinanceProvider
from agent_arena.storage import get_storage, ArchiveService

console = Console()


def load_agent(agent_config: dict):
    """Dynamically load an agent from config."""
    class_path = agent_config["class"]
    module_path, class_name = class_path.rsplit(".", 1)

    import importlib

    module = importlib.import_module(module_path)
    agent_class = getattr(module, class_name)

    return agent_class(
        agent_id=agent_config["id"],
        name=agent_config["name"],
        config=agent_config.get("config", {}),
    )


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


def parse_fees_config(config_data: dict) -> FeeConfig:
    """Parse fee configuration from YAML."""
    fees_data = config_data.get("fees", {})
    from decimal import Decimal
    return FeeConfig(
        taker_fee=Decimal(str(fees_data.get("taker_fee", "0.0004"))),
        maker_fee=Decimal(str(fees_data.get("maker_fee", "0.0002"))),
        liquidation_fee=Decimal(str(fees_data.get("liquidation_fee", "0.005"))),
    )


def parse_constraints_config(config_data: dict) -> ConstraintsConfig:
    """Parse constraints configuration from YAML."""
    constraints_data = config_data.get("constraints", {})
    from decimal import Decimal
    return ConstraintsConfig(
        max_leverage=constraints_data.get("max_leverage", 10),
        max_position_pct=Decimal(str(constraints_data.get("max_position_pct", "0.25"))),
        starting_capital=Decimal(str(constraints_data.get("starting_capital", "10000"))),
    )


def parse_candle_config(config_data: dict) -> CandleConfig:
    """Parse candle configuration from YAML."""
    candles_data = config_data.get("candles", {})
    return CandleConfig(
        enabled=candles_data.get("enabled", True),
        intervals=candles_data.get("intervals", ["1h", "15m"]),
        limit=candles_data.get("limit", 100),
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
        symbols=config_data.get("symbols", ["BTCUSDT", "ETHUSDT"]),
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

    providers = [BinanceProvider()]

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
    symbols = ["BTCUSDT", "ETHUSDT"]
    arena = TradingArena(symbols)
    provider = BinanceProvider()

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


@cli.command()
def init():
    """Initialize a new competition config file."""
    default_config = """# Agent Arena Competition Config
name: "My Competition"

symbols:
  - BTCUSDT
  - ETHUSDT

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
