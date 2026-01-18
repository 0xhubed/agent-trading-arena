"""FastAPI application for Agent Arena dashboard."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import os
from typing import Any, Optional

import yaml
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Load environment variables before importing modules that use them
load_dotenv()

from agent_arena.api.routes import router, set_dependencies  # noqa: E402
from agent_arena.api.websocket import create_event_emitter, manager  # noqa: E402
from agent_arena.core.arena import TradingArena  # noqa: E402
from agent_arena.core.config import (  # noqa: E402
    CandleConfig,
    CompetitionConfig,
    ConstraintsConfig,
    FeeConfig,
)
from agent_arena.core.runner import CompetitionRunner  # noqa: E402
from agent_arena.providers.binance import BinanceProvider  # noqa: E402
from agent_arena.storage import get_storage, ArchiveService  # noqa: E402

# Global state
_runner: Optional[CompetitionRunner] = None
_storage: Optional[Any] = None  # SQLiteStorage or PostgresStorage
_archive: Optional[ArchiveService] = None
_arena: Optional[TradingArena] = None
_competition_task: Optional[asyncio.Task] = None


def load_agent_class(class_path: str) -> type:
    """Dynamically load an agent class from string path."""
    module_path, class_name = class_path.rsplit(".", 1)
    module = __import__(module_path, fromlist=[class_name])
    return getattr(module, class_name)


def parse_fees_config(config_data: dict) -> FeeConfig:
    """Parse fee configuration from YAML."""
    from decimal import Decimal
    fees_data = config_data.get("fees", {})
    return FeeConfig(
        taker_fee=Decimal(str(fees_data.get("taker_fee", "0.0004"))),
        maker_fee=Decimal(str(fees_data.get("maker_fee", "0.0002"))),
        liquidation_fee=Decimal(str(fees_data.get("liquidation_fee", "0.005"))),
    )


def parse_constraints_config(config_data: dict) -> ConstraintsConfig:
    """Parse constraints configuration from YAML."""
    from decimal import Decimal
    constraints_data = config_data.get("constraints", {})
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


async def start_competition(config_path: str) -> None:
    """Start a competition from config file."""
    global _runner, _storage, _arena, _competition_task

    # Load config
    with open(config_path) as f:
        raw_config = yaml.safe_load(f)

    # Parse fee, constraint, and candle configs
    fees_config = parse_fees_config(raw_config)
    constraints_config = parse_constraints_config(raw_config)
    candle_config = parse_candle_config(raw_config)

    config = CompetitionConfig(
        name=raw_config.get("name", "Agent Arena"),
        symbols=raw_config.get("symbols", ["BTCUSDT", "ETHUSDT"]),
        interval_seconds=raw_config.get("interval_seconds", 60),
        duration_seconds=raw_config.get("duration_seconds"),
        agent_timeout_seconds=raw_config.get("agent_timeout_seconds", 60.0),
        fees=fees_config,
        constraints=constraints_config,
        candles=candle_config,
    )

    # Initialize storage (respects DATABASE_BACKEND env var)
    _storage = get_storage()
    await _storage.initialize()

    # Create archive service for long-term storage if using postgres
    global _archive
    _archive = None
    if os.getenv("DATABASE_BACKEND") == "postgres":
        _archive = ArchiveService(_storage, generate_embeddings=True)

    # Initialize arena with fee and constraint configs
    _arena = TradingArena(
        symbols=config.symbols,
        fees=fees_config,
        constraints=constraints_config,
        tick_interval_seconds=config.interval_seconds,
    )

    # Load agents
    agents = []
    for agent_config in raw_config.get("agents", []):
        agent_class = load_agent_class(agent_config["class"])
        agent = agent_class(
            agent_id=agent_config["id"],
            name=agent_config["name"],
            config=agent_config.get("config", {}),
        )
        agents.append(agent)

    # Initialize providers
    providers = [BinanceProvider()]

    # Create runner with WebSocket event emitter
    event_emitter = create_event_emitter()
    _runner = CompetitionRunner(
        config=config,
        agents=agents,
        providers=providers,
        arena=_arena,
        storage=_storage,
        event_emitter=event_emitter,
        archive=_archive,
    )

    # Set dependencies for routes
    set_dependencies(_storage, _arena, _runner)

    # Start competition in background
    _competition_task = asyncio.create_task(_runner.start())


async def resume_competition(config_path: str) -> dict:
    """Resume a competition from saved state."""
    global _runner, _storage, _arena, _competition_task, _archive

    # Load config
    with open(config_path) as f:
        raw_config = yaml.safe_load(f)

    competition_name = raw_config.get("name", "Agent Arena")

    # Initialize storage
    _storage = get_storage()
    await _storage.initialize()

    # Check if we have saved state
    if not hasattr(_storage, "has_saved_state"):
        return {"error": "Resume not supported with SQLite backend. Use PostgreSQL."}

    has_state = await _storage.has_saved_state(competition_name)
    if not has_state:
        return {"error": f"No saved state found for '{competition_name}'"}

    # Load saved state
    saved_state = await _storage.load_arena_state(competition_name)
    if not saved_state:
        return {"error": "Failed to load saved state"}

    # Parse configs
    fees_config = parse_fees_config(raw_config)
    constraints_config = parse_constraints_config(raw_config)
    candle_config = parse_candle_config(raw_config)

    config = CompetitionConfig(
        name=competition_name,
        symbols=raw_config.get("symbols", ["BTCUSDT", "ETHUSDT"]),
        interval_seconds=raw_config.get("interval_seconds", 60),
        duration_seconds=raw_config.get("duration_seconds"),
        agent_timeout_seconds=raw_config.get("agent_timeout_seconds", 60.0),
        fees=fees_config,
        constraints=constraints_config,
        candles=candle_config,
    )

    # Create archive service if using postgres
    _archive = None
    if os.getenv("DATABASE_BACKEND") == "postgres":
        _archive = ArchiveService(_storage, generate_embeddings=True)

    # Initialize arena
    _arena = TradingArena(
        symbols=config.symbols,
        fees=fees_config,
        constraints=constraints_config,
        tick_interval_seconds=config.interval_seconds,
    )

    # Restore current prices
    _arena.current_prices = saved_state["current_prices"]

    # Load agents and restore their portfolio state
    agents = []
    restored_agents = []
    for agent_config in raw_config.get("agents", []):
        agent_id = agent_config["id"]
        agent_class = load_agent_class(agent_config["class"])
        agent = agent_class(
            agent_id=agent_id,
            name=agent_config["name"],
            config=agent_config.get("config", {}),
        )
        agents.append(agent)

        # Restore portfolio state if available
        if agent_id in saved_state["portfolios"]:
            portfolio_state = saved_state["portfolios"][agent_id]
            _arena.restore_portfolio_state(
                agent_id,
                portfolio_state,
                saved_state["current_prices"],
            )
            restored_agents.append(agent_id)
        else:
            # New agent, register fresh
            _arena.register_agent(agent_id)

    # Initialize providers
    providers = [BinanceProvider()]

    # Create runner with restored tick
    event_emitter = create_event_emitter()
    _runner = CompetitionRunner(
        config=config,
        agents=agents,
        providers=providers,
        arena=_arena,
        storage=_storage,
        event_emitter=event_emitter,
        archive=_archive,
    )

    # Set the starting tick
    _runner.tick = saved_state["last_tick"]

    # Set dependencies for routes
    set_dependencies(_storage, _arena, _runner)

    # Start competition in background
    _competition_task = asyncio.create_task(_runner.start())

    return {
        "status": "resumed",
        "competition_name": competition_name,
        "restored_tick": saved_state["last_tick"],
        "restored_agents": restored_agents,
        "last_timestamp": str(saved_state["last_timestamp"]),
    }


async def stop_competition() -> None:
    """Stop the running competition."""
    global _runner, _storage, _competition_task

    if _runner:
        await _runner.stop()

    if _competition_task:
        _competition_task.cancel()
        try:
            await _competition_task
        except asyncio.CancelledError:
            pass

    if _storage:
        await _storage.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global _storage

    # Startup - initialize storage so historical data is available
    # Uses DATABASE_BACKEND env var (postgres or sqlite)
    _storage = get_storage()
    await _storage.initialize()

    # Make storage available to routes for historical queries
    from agent_arena.api.routes import set_dependencies
    set_dependencies(_storage, None, None)

    yield

    # Shutdown
    await stop_competition()


def create_app(config_path: Optional[str] = None) -> FastAPI:
    """Create the FastAPI application."""
    app = FastAPI(
        title="Agent Arena",
        description="AI Agents vs. The Market",
        version="0.1.0",
        lifespan=lifespan,
    )

    # CORS for frontend
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # REST routes
    app.include_router(router, prefix="/api")

    # WebSocket endpoint
    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        await manager.connect(websocket)
        try:
            # Send initial state
            if _arena and _runner:
                await websocket.send_json({
                    "type": "init",
                    "data": {
                        "status": "running" if _runner.running else "stopped",
                        "tick": _runner.tick,
                        "leaderboard": _arena.get_leaderboard(),
                        "market": {
                            s: float(p) for s, p in _arena.current_prices.items()
                        },
                        "agents": [
                            {
                                "id": aid,
                                "name": a.name,
                                "model": getattr(a, "model", "unknown"),
                            }
                            for aid, a in _runner.agents.items()
                        ],
                    },
                })

            # Keep connection alive
            while True:
                try:
                    # Wait for any message (ping/pong)
                    await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                except asyncio.TimeoutError:
                    # Send ping to keep alive
                    await websocket.send_json({"type": "ping"})
        except WebSocketDisconnect:
            pass
        finally:
            await manager.disconnect(websocket)

    # Startup endpoint to begin competition
    @app.post("/api/start")
    async def start_competition_endpoint(config_path: str = "configs/development.yaml"):
        """Start a competition."""
        if _runner and _runner.running:
            return {"error": "Competition already running"}
        await start_competition(config_path)
        return {"status": "started"}

    @app.post("/api/resume")
    async def resume_competition_endpoint(config_path: str = "configs/lean_diverse.yaml"):
        """Resume a competition from saved state.

        Restores:
        - Agent portfolios (equity, margin, realized P&L)
        - Open positions (with entry prices, SL/TP)
        - Pending orders
        - Tick counter
        - Funding paid/received
        """
        if _runner and _runner.running:
            return {"error": "Competition already running"}
        result = await resume_competition(config_path)
        return result

    @app.get("/api/can-resume")
    async def can_resume_endpoint(config_path: str = "configs/lean_diverse.yaml"):
        """Check if a competition can be resumed."""
        try:
            with open(config_path) as f:
                raw_config = yaml.safe_load(f)
            competition_name = raw_config.get("name", "Agent Arena")

            storage = get_storage()
            await storage.initialize()

            if not hasattr(storage, "has_saved_state"):
                await storage.close()
                return {"can_resume": False, "reason": "SQLite backend doesn't support resume"}

            has_state = await storage.has_saved_state(competition_name)

            if has_state:
                saved_state = await storage.load_arena_state(competition_name)
                await storage.close()
                return {
                    "can_resume": True,
                    "competition_name": competition_name,
                    "last_tick": saved_state["last_tick"],
                    "last_timestamp": str(saved_state["last_timestamp"]),
                    "agents": list(saved_state["portfolios"].keys()),
                }
            else:
                await storage.close()
                return {"can_resume": False, "reason": f"No saved state for '{competition_name}'"}
        except Exception as e:
            return {"can_resume": False, "reason": str(e)}

    @app.post("/api/stop")
    async def stop_competition_endpoint():
        """Stop the competition."""
        await stop_competition()
        return {"status": "stopped"}

    @app.post("/api/reset")
    async def reset_competition_endpoint():
        """Reset competition by deleting the database and clearing all state."""
        global _storage, _arena, _runner

        # Broadcast reset event to all connected clients FIRST
        await manager.broadcast("reset", {})

        # Close existing storage connection
        if _storage:
            await _storage.close()
            _storage = None

        # Clear arena and runner references
        _arena = None
        _runner = None

        # Delete SQLite database file if using sqlite backend
        if os.getenv("DATABASE_BACKEND", "sqlite") == "sqlite":
            db_path = Path(__file__).parent.parent.parent / "data" / "arena.db"
            if db_path.exists():
                import os as os_module
                os_module.remove(db_path)

        # Reinitialize fresh storage
        _storage = get_storage()
        await _storage.initialize()

        # For PostgreSQL, truncate all tables
        if os.getenv("DATABASE_BACKEND") == "postgres" and hasattr(_storage, "reset_all"):
            await _storage.reset_all()

        # Update route dependencies with fresh storage (no arena/runner yet)
        set_dependencies(_storage, None, None)

        return {"status": "reset"}

    # Store config path for later
    app.state.config_path = config_path

    # Serve static frontend files (for production)
    frontend_dist = Path(__file__).parent.parent.parent / "frontend" / "dist"
    if frontend_dist.exists():
        # Serve static assets
        app.mount("/assets", StaticFiles(directory=frontend_dist / "assets"), name="assets")

        # Serve index.html for all other routes (SPA fallback)
        from fastapi.responses import FileResponse

        @app.get("/{full_path:path}")
        async def serve_spa(full_path: str):
            """Serve the SPA for any non-API route."""
            # Check if it's a static file first
            file_path = frontend_dist / full_path
            if file_path.is_file():
                return FileResponse(file_path)
            # Otherwise, serve index.html for SPA routing
            return FileResponse(frontend_dist / "index.html")

    return app


# Default app instance
app = create_app()
