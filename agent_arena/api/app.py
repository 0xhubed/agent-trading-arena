"""FastAPI application for Agent Arena dashboard."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import os
from typing import Any, Optional

import yaml
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Load environment variables before importing modules that use them
load_dotenv()

from agent_arena.api.routes import router, set_dependencies, require_admin_access  # noqa: E402
from agent_arena.api.routes_backtest import router as backtest_router, set_storage as set_backtest_storage  # noqa: E402
from agent_arena.api.routes_evolution import (  # noqa: E402
    router as evolution_router,
    set_storage as set_evolution_storage,
)
from agent_arena.api.routes_experiment import (  # noqa: E402
    router as experiment_router,
    set_storage as set_experiment_storage,
)
from agent_arena.api.routes_memory import (  # noqa: E402
    router as memory_router,
    set_storage as set_memory_storage,
)
from agent_arena.api.routes_reflexion import (  # noqa: E402
    router as reflexion_router,
    set_storage as set_reflexion_storage,
)
from agent_arena.api.routes_forum import router as forum_router, set_storage as set_forum_storage  # noqa: E402
from agent_arena.api.routes_journal import (  # noqa: E402
    router as journal_router,
    set_storage as set_journal_storage,
)
# from agent_arena.api.routes_lab import router as lab_router, set_storage as set_lab_storage  # noqa: E402
from agent_arena.api.websocket import create_event_emitter, manager  # noqa: E402
from agent_arena.core.arena import TradingArena  # noqa: E402
from agent_arena.core.config import CompetitionConfig  # noqa: E402
from agent_arena.core.config_parser import (  # noqa: E402
    parse_candle_config,
    parse_constraints_config,
    parse_fees_config,
)
from agent_arena.core.runner import CompetitionRunner  # noqa: E402
from agent_arena.providers.kraken import KrakenProvider  # noqa: E402
from agent_arena.storage import get_storage, ArchiveService  # noqa: E402

# Global state
_runner: Optional[CompetitionRunner] = None
_storage: Optional[Any] = None  # SQLiteStorage or PostgresStorage
_archive: Optional[ArchiveService] = None
_arena: Optional[TradingArena] = None
_competition_task: Optional[asyncio.Task] = None


from agent_arena.core.loader import ALLOWED_AGENT_PREFIXES  # noqa: E402


def load_agent_class(class_path: str) -> type:
    """Dynamically load an agent class from string path."""
    # Validate class path against allowlist
    if not any(class_path.startswith(prefix) for prefix in ALLOWED_AGENT_PREFIXES):
        raise ValueError(
            f"Agent class '{class_path}' is not in the allowed module list. "
            "Only classes under agent_arena.agents.* are permitted."
        )

    module_path, class_name = class_path.rsplit(".", 1)
    module = __import__(module_path, fromlist=[class_name])
    return getattr(module, class_name)


def _validate_config_path(config_path: str) -> Path:
    """Validate config path is within allowed directory."""
    resolved = Path(config_path).resolve()
    allowed_dir = Path("configs").resolve()
    if not str(resolved).startswith(str(allowed_dir)):
        raise HTTPException(
            status_code=400,
            detail="Config path must be within the configs/ directory",
        )
    if not resolved.exists():
        raise HTTPException(status_code=404, detail=f"Config file not found: {config_path}")
    return resolved


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
        symbols=raw_config.get("symbols", ["PF_XBTUSD", "PF_ETHUSD"]),
        interval_seconds=raw_config.get("interval_seconds", 60),
        duration_seconds=raw_config.get("duration_seconds"),
        agent_timeout_seconds=raw_config.get("agent_timeout_seconds", 60.0),
        fees=fees_config,
        constraints=constraints_config,
        candles=candle_config,
        raw_config=raw_config,  # M3: For forum discussion agents
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
    providers = [KrakenProvider()]

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
        symbols=raw_config.get("symbols", ["PF_XBTUSD", "PF_ETHUSD"]),
        interval_seconds=raw_config.get("interval_seconds", 60),
        duration_seconds=raw_config.get("duration_seconds"),
        agent_timeout_seconds=raw_config.get("agent_timeout_seconds", 60.0),
        fees=fees_config,
        constraints=constraints_config,
        candles=candle_config,
        raw_config=raw_config,  # M3: For forum discussion agents
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
    providers = [KrakenProvider()]

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

    # Don't close storage here - it's needed for historical queries
    # Storage is closed in lifespan shutdown instead


_daily_analysis_task: Optional[asyncio.Task] = None

# Hour (UTC) at which the daily Observer analysis runs automatically.
DAILY_ANALYSIS_HOUR_UTC = 20  # 8 PM UTC


async def _daily_analysis_loop() -> None:
    """Sleep until DAILY_ANALYSIS_HOUR_UTC, run Observer analysis, repeat."""
    import logging
    from datetime import datetime, timedelta, timezone

    log = logging.getLogger("agent_arena.daily_scheduler")

    while True:
        # Calculate seconds until next run
        now = datetime.now(timezone.utc)
        target = now.replace(
            hour=DAILY_ANALYSIS_HOUR_UTC,
            minute=0, second=0, microsecond=0,
        )
        if target <= now:
            target += timedelta(days=1)
        wait_seconds = (target - now).total_seconds()

        log.info(
            "Next daily analysis at %s UTC (in %.1f hours)",
            target.isoformat(), wait_seconds / 3600,
        )
        await asyncio.sleep(wait_seconds)

        # Phase 1: Observer daily analysis (existing: skills, forum witness, journal)
        log.info("Starting scheduled daily analysis...")
        try:
            from agent_arena.agents.observer_agent import ObserverAgent

            observer = ObserverAgent(
                storage=_storage,
                skills_dir=".claude/skills",
            )
            result = await observer.run_daily_analysis(
                lookback_hours=24,
            )
            log.info(
                "Daily analysis complete: %s",
                result.get("status", "unknown"),
            )
        except Exception:
            log.exception("Daily analysis failed (will retry tomorrow)")

        # Phase 2: Reflexion — generate reflections for closed trades
        log.info("Starting reflexion phase...")
        try:
            from agent_arena.reflexion.service import ReflexionService

            reflexion = ReflexionService(storage=_storage)
            if hasattr(_storage, "pool"):
                async with _storage.pool.acquire() as conn:
                    agent_rows = await conn.fetch(
                        """
                        SELECT DISTINCT agent_id FROM trades
                        WHERE timestamp >= NOW() - INTERVAL '48 hours'
                        AND realized_pnl IS NOT NULL AND realized_pnl != 0
                        """
                    )
                agent_ids = [r["agent_id"] for r in agent_rows]
                for aid in agent_ids:
                    reflections = await reflexion.reflect_on_closed_trades(aid)
                    if reflections:
                        log.info("Generated %d reflections for %s", len(reflections), aid)
        except Exception:
            log.exception("Reflexion phase failed")

        # Phase 3: Memory digestion — score → digest → prune → principles
        log.info("Starting memory digestion phase...")
        try:
            from agent_arena.memory.digestion import MemoryDigester

            digester = MemoryDigester(storage=_storage)
            if hasattr(_storage, "pool"):
                async with _storage.pool.acquire() as conn:
                    agent_rows = await conn.fetch(
                        "SELECT DISTINCT agent_id FROM trade_reflections WHERE is_digested = FALSE"
                    )
                for r in agent_rows:
                    dig_result = await digester.run_digestion_cycle(r["agent_id"])
                    if dig_result.principles_created > 0:
                        log.info(
                            "Digestion for %s: %d principles created",
                            r["agent_id"], dig_result.principles_created,
                        )
        except Exception:
            log.exception("Memory digestion phase failed")

        # Phase 4: Failure clustering + EvoSkill
        log.info("Starting failure clustering phase...")
        try:
            from agent_arena.reflexion.clustering import FailureClusterer
            from agent_arena.reflexion.evoskill import EvoSkillValidator

            clusterer = FailureClusterer(storage=_storage)
            clusters = await clusterer.cluster_failures(lookback_days=14)
            if clusters:
                validator = EvoSkillValidator(storage=_storage)
                results = await validator.validate_and_promote(clusters)
                promoted = [r for r in results if r.promoted]
                if promoted:
                    log.info("Promoted %d new skills from failure clusters", len(promoted))
        except Exception:
            log.exception("Failure clustering phase failed")

        # Phase 5: Experiment loop (budget-gated)
        log.info("Checking whether to run overnight experiment...")
        try:
            from agent_arena.experiment.scheduler import ExperimentScheduler

            scheduler = ExperimentScheduler(storage=_storage)
            should_run, reason = await scheduler.should_run_tonight()
            log.info("Experiment scheduler: %s (%s)", should_run, reason)

            if should_run:
                import yaml as _yaml
                from agent_arena.experiment.orchestrator import (
                    ExperimentConfig,
                    ExperimentOrchestrator,
                )

                # Load experiment config
                exp_config_path = Path("configs/experiment.yaml")
                if exp_config_path.exists():
                    with open(exp_config_path) as f:
                        raw = _yaml.safe_load(f)
                    exp_cfg = raw.get("experiment", {})
                    bt_cfg = raw.get("backtest", {})
                    inf_cfg = raw.get("inference", {})

                    config = ExperimentConfig(
                        name=f"Overnight {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
                        population_size=exp_cfg.get("population_size", 16),
                        generations=exp_cfg.get("generations", 5),
                        budget_limit_usd=exp_cfg.get("budget_limit_usd", 5.0),
                        backtest_start=bt_cfg.get("start", ""),
                        backtest_end=bt_cfg.get("end", ""),
                        tick_interval=bt_cfg.get("tick_interval", "4h"),
                        symbols=bt_cfg.get("symbols", ["PF_XBTUSD", "PF_ETHUSD", "PF_SOLUSD"]),
                        base_url=inf_cfg.get("base_url", ""),
                        api_key_env=inf_cfg.get("api_key_env", "TOGETHER_API_KEY"),
                    )

                    orchestrator = ExperimentOrchestrator(
                        config=config,
                        storage=_storage,
                    )
                    exp_result = await orchestrator.run()
                    log.info(
                        "Experiment complete: status=%s, fitness=%.4f, cost=$%.2f",
                        exp_result.status, exp_result.best_fitness, exp_result.total_cost_usd,
                    )

                    # Promotions are queued as "pending" — they require
                    # human approval via API/CLI before deployment.
                    if exp_result.promotion_candidates:
                        log.info(
                            "%d promotion candidates queued (approve via API or CLI)",
                            len(exp_result.promotion_candidates),
                        )
        except Exception:
            log.exception("Experiment phase failed")

        # Small delay to avoid double-trigger on clock edge
        await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global _storage, _daily_analysis_task

    # Startup - initialize storage so historical data is available
    # Uses DATABASE_BACKEND env var (postgres or sqlite)
    _storage = get_storage()
    await _storage.initialize()

    # Make storage available to routes for historical queries
    from agent_arena.api.routes import set_dependencies
    set_dependencies(_storage, None, None)

    set_backtest_storage(_storage)
    set_evolution_storage(_storage)
    set_experiment_storage(_storage)
    set_memory_storage(_storage)
    set_reflexion_storage(_storage)

    # Make storage available to forum routes
    set_forum_storage(_storage)

    # Make storage available to journal routes
    set_journal_storage(_storage)

    # DEACTIVATED: lab routes
    # set_lab_storage(_storage)

    # Start daily Observer analysis scheduler
    _daily_analysis_task = asyncio.create_task(_daily_analysis_loop())

    yield

    # Shutdown
    if _daily_analysis_task:
        _daily_analysis_task.cancel()
        try:
            await _daily_analysis_task
        except asyncio.CancelledError:
            pass
    await stop_competition()
    if _storage:
        await _storage.close()


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
        allow_origins=os.getenv("CORS_ORIGINS", "http://localhost:5173,http://localhost:3000").split(","),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # REST routes
    app.include_router(router, prefix="/api")
    app.include_router(backtest_router, prefix="/api")
    app.include_router(evolution_router, prefix="/api")
    app.include_router(experiment_router, prefix="/api")
    app.include_router(memory_router, prefix="/api")
    app.include_router(reflexion_router, prefix="/api")
    app.include_router(forum_router, prefix="/api")
    app.include_router(journal_router, prefix="/api")
    # app.include_router(lab_router, prefix="/api")

    # WebSocket endpoint
    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        accepted = await manager.connect(websocket)
        if not accepted:
            return
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
    async def start_competition_endpoint(
        config_path: str = "configs/production.yaml",
        _auth=Depends(require_admin_access),
    ):
        """Start a competition."""
        _validate_config_path(config_path)
        if _runner and _runner.running:
            return {"error": "Competition already running"}
        await start_competition(config_path)
        return {"status": "started"}

    @app.post("/api/resume")
    async def resume_competition_endpoint(
        config_path: str = "configs/production.yaml",
        _auth=Depends(require_admin_access),
    ):
        """Resume a competition from saved state.

        Restores:
        - Agent portfolios (equity, margin, realized P&L)
        - Open positions (with entry prices, SL/TP)
        - Pending orders
        - Tick counter
        - Funding paid/received
        """
        _validate_config_path(config_path)
        if _runner and _runner.running:
            return {"error": "Competition already running"}
        result = await resume_competition(config_path)
        return result

    @app.get("/api/can-resume")
    async def can_resume_endpoint(config_path: str = "configs/production.yaml"):
        """Check if a competition can be resumed."""
        _validate_config_path(config_path)
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
        except Exception:
            import logging
            logging.getLogger(__name__).error("can_resume check failed", exc_info=True)
            return {"can_resume": False, "reason": "Internal error checking resume state"}

    @app.post("/api/stop")
    async def stop_competition_endpoint(_auth=Depends(require_admin_access)):
        """Stop the competition."""
        await stop_competition()
        return {"status": "stopped"}

    @app.post("/api/reset")
    async def reset_competition_endpoint(_auth=Depends(require_admin_access)):
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
                os.remove(db_path)

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
            file_path = frontend_dist / full_path
            resolved = file_path.resolve()
            # Prevent path traversal outside frontend dist directory
            if not str(resolved).startswith(str(frontend_dist.resolve())):
                return FileResponse(frontend_dist / "index.html")
            if resolved.is_file():
                return FileResponse(resolved)
            # Otherwise, serve index.html for SPA routing
            return FileResponse(frontend_dist / "index.html")

    return app


# Default app instance
app = create_app()
