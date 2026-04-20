"""Backtest API routes for Agent Arena."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException
from pydantic import BaseModel, Field, field_validator

from agent_arena.api.routes import require_admin_access

VALID_INTERVALS = {"1m", "5m", "15m", "1h", "4h", "1d"}

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/backtest", tags=["backtest"])

# Module-level dependencies
_storage = None
_active_backtests: dict[str, Any] = {}  # run_id -> BacktestRunner
MAX_CONCURRENT_BACKTESTS = 3

# Allowed directory for config file access
_CONFIGS_DIR = Path(__file__).resolve().parents[2] / "configs"


def _validate_config_path(raw_path: str) -> Path:
    """Validate config_path is under the allowed configs/ directory."""
    resolved = Path(raw_path).resolve()
    if not resolved.is_relative_to(_CONFIGS_DIR):
        raise HTTPException(
            status_code=400,
            detail="config_path must be under the configs/ directory",
        )
    return resolved


def set_storage(storage: Any) -> None:
    """Set storage dependency."""
    global _storage
    _storage = storage


def _validate_date(v: str) -> str:
    """Validate YYYY-MM-DD format."""
    try:
        datetime.strptime(v, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Invalid date: {v}. Expected YYYY-MM-DD")
    return v


def _validate_interval(v: str) -> str:
    if v not in VALID_INTERVALS:
        raise ValueError(
            f"Invalid interval: {v}. Must be one of {VALID_INTERVALS}"
        )
    return v


class BacktestStartRequest(BaseModel):
    """Request to start a new backtest."""

    name: str = Field("Backtest", max_length=200)
    config_path: Optional[str] = None
    start_date: str
    end_date: str
    tick_interval: str = "4h"
    symbols: list[str] = Field(
        default=["PF_XBTUSD", "PF_ETHUSD", "PF_SOLUSD"],
        max_length=20,
    )
    agents: Optional[list[dict]] = Field(None, max_length=20)
    agent_configs: Optional[list[dict]] = Field(None, max_length=20)
    run_baselines: bool = False

    _check_start = field_validator("start_date")(_validate_date)
    _check_end = field_validator("end_date")(_validate_date)
    _check_interval = field_validator("tick_interval")(_validate_interval)

    @field_validator("end_date")
    @classmethod
    def end_after_start(cls, v: str, info) -> str:
        start = info.data.get("start_date")
        if start and v <= start:
            raise ValueError("end_date must be after start_date")
        return v


class BacktestEstimateRequest(BaseModel):
    """Request to estimate backtest cost."""

    config_path: Optional[str] = None
    start_date: str
    end_date: str
    tick_interval: str = "4h"
    agents: Optional[list[dict]] = Field(None, max_length=20)

    _check_start = field_validator("start_date")(_validate_date)
    _check_end = field_validator("end_date")(_validate_date)
    _check_interval = field_validator("tick_interval")(_validate_interval)


class FetchDataRequest(BaseModel):
    """Request to fetch historical data."""

    symbols: list[str] = Field(max_length=20)
    start_date: str
    end_date: str
    intervals: list[str] = ["1h", "4h"]


# =============================================================================
# Status Endpoints
# =============================================================================


@router.get("/access")
async def get_access_status(
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key"),
) -> dict:
    """
    Check if backtest execution is allowed.

    Returns readonly status so UI can hide/disable action buttons.
    If a valid admin key is provided, readonly=false.
    """
    from agent_arena.api.routes import _key_matches

    if _key_matches(x_admin_key):
        return {
            "readonly": False,
            "message": "Admin access granted.",
        }
    return {
        "readonly": True,
        "message": "Admin access required for backtest operations.",
    }


# =============================================================================
# Data Management Endpoints
# =============================================================================


@router.get("/data/status")
async def get_data_status() -> dict:
    """Get status of available historical data in database."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    try:
        # Check if storage has the method (PostgresStorage or has candle methods)
        if hasattr(_storage, "get_data_status"):
            status = await _storage.get_data_status()
        elif hasattr(_storage, "_connection"):
            from agent_arena.storage.candles import CandleStorage
            candle_storage = CandleStorage(_storage._connection)
            status = await candle_storage.get_data_status()
        else:
            return {
                "symbols": [],
                "total_candles": 0,
                "database_backend": "unknown",
            }

        # Convert nested dict to flat array for frontend
        # status is: {symbol: {interval: {start, end, count, start_date, end_date}}}
        symbols_list = []
        total_candles = 0

        for symbol, intervals in status.items():
            for interval, data in intervals.items():
                count = data.get("count", 0)
                total_candles += count
                symbols_list.append({
                    "symbol": symbol,
                    "interval": interval,
                    "earliest": data.get("start_date"),
                    "latest": data.get("end_date"),
                    "count": count,
                    "gaps": 0,  # TODO: Calculate gaps if needed
                })

        # Determine backend type
        backend = "sqlite"
        if hasattr(_storage, "__class__"):
            class_name = _storage.__class__.__name__.lower()
            if "postgres" in class_name:
                backend = "postgres"

        return {
            "symbols": symbols_list,
            "total_candles": total_candles,
            "database_backend": backend,
        }
    except Exception as e:
        logger.error("Failed to fetch historical data summary: %s", e)
        raise HTTPException(status_code=500, detail="Failed to fetch data summary")


@router.post("/data/fetch")
async def fetch_historical_data(
    request: FetchDataRequest,
    _admin: bool = Depends(require_admin_access),
) -> dict:
    """
    Fetch historical data from Kraken Futures and store in database.

    This runs in the background and returns immediately.
    """
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    from agent_arena.data.fetch_historical import estimate_data_size

    # Get estimate first
    estimate = estimate_data_size(
        request.symbols,
        request.start_date,
        request.end_date,
        request.intervals,
    )

    async def fetch_task():
        from agent_arena.data.fetch_historical import fetch_and_store_historical

        try:
            await fetch_and_store_historical(
                symbols=request.symbols,
                start_date=request.start_date,
                end_date=request.end_date,
                intervals=request.intervals,
                storage=_storage,
            )
            logger.info("Completed fetching historical data for %s", request.symbols)
        except Exception as e:
            logger.error("Error fetching historical data: %s", e)

    # Schedule async task — store ref to prevent GC, log failures
    task = asyncio.create_task(fetch_task())
    task.add_done_callback(
        lambda t: (
            logger.error("Data fetch task failed: %s", t.exception())
            if not t.cancelled() and t.exception()
            else None
        )
    )

    return {
        "status": "started",
        "message": "Data fetch started in background",
        "estimate": estimate,
    }


# =============================================================================
# Backtest Management Endpoints
# =============================================================================


@router.post("/estimate")
async def estimate_backtest_cost(
    request: BacktestEstimateRequest,
    _admin: bool = Depends(require_admin_access),
) -> dict:
    """Estimate API cost before running a backtest."""
    from agent_arena.providers.historical import INTERVAL_MS, date_to_ms, parse_date

    # Calculate number of ticks
    start_ms = date_to_ms(parse_date(request.start_date))
    end_ms = date_to_ms(parse_date(request.end_date))
    interval_ms = INTERVAL_MS.get(request.tick_interval, 3600000)
    total_ticks = (end_ms - start_ms) // interval_ms

    # Load agents from config or request
    agents_info = []
    if request.config_path:
        import yaml

        config_path = _validate_config_path(request.config_path)
        if config_path.exists():
            with open(config_path) as f:
                config_data = yaml.safe_load(f)
                agents_info = config_data.get("agents", [])
    elif request.agents:
        agents_info = request.agents

    # Estimate costs
    llm_agents = []
    baseline_agents = []

    for agent in agents_info:
        agent_class = agent.get("class", "")
        is_llm = any(
            name in agent_class.lower()
            for name in ["claude", "gpt", "together", "ollama", "llm"]
        )

        agent_entry = {
            "agent_id": agent.get("id"),
            "name": agent.get("name"),
            "class": agent_class,
        }

        if is_llm:
            # Cost estimation (Together AI pricing)
            tokens_per_tick = 2750  # input + output
            model = agent.get("config", {}).get("model", "unknown")
            cost_per_1m = 0.5  # $0.50 per 1M tokens (conservative)
            estimated_cost = (total_ticks * tokens_per_tick * cost_per_1m) / 1_000_000

            agent_entry["model"] = model
            agent_entry["estimated_cost"] = round(estimated_cost, 4)
            llm_agents.append(agent_entry)
        else:
            baseline_agents.append(agent_entry)

    total_cost = sum(a.get("estimated_cost", 0) for a in llm_agents)

    return {
        "start_date": request.start_date,
        "end_date": request.end_date,
        "tick_interval": request.tick_interval,
        "total_ticks": total_ticks,
        "llm_agents": llm_agents,
        "baseline_agents": baseline_agents,
        "total_estimated_cost": round(total_cost, 2),
    }


@router.post("/start")
async def start_backtest(
    request: BacktestStartRequest,
    background_tasks: BackgroundTasks,
    _admin: bool = Depends(require_admin_access),
) -> dict:
    """
    Start a new backtest.

    The backtest runs in the background. Use /backtest/{run_id}/status
    to check progress.
    """
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    if len(_active_backtests) >= MAX_CONCURRENT_BACKTESTS:
        raise HTTPException(
            status_code=429,
            detail=f"Max {MAX_CONCURRENT_BACKTESTS} concurrent "
            "backtests allowed",
        )

    import uuid
    from decimal import Decimal

    import yaml

    from agent_arena.backtest.runner import BacktestRunner
    from agent_arena.core.config import (
        CandleConfig,
        CompetitionConfig,
        ConstraintsConfig,
        FeeConfig,
    )
    from agent_arena.core.loader import load_agent

    run_id = f"bt_{uuid.uuid4().hex[:12]}"

    # Baseline agent definitions
    BASELINE_AGENTS = [
        {
            "id": "random_baseline",
            "name": "Random Trader",
            "class": "agent_arena.agents.baselines.RandomAgent",
            "config": {"trade_frequency": 0.2, "position_size": 0.1},
        },
        {
            "id": "sma_baseline",
            "name": "SMA Crossover",
            "class": "agent_arena.agents.baselines.SMAAgent",
            "config": {"sma_period": 50, "position_size": 0.15, "leverage": 3},
        },
        {
            "id": "momentum_baseline",
            "name": "Momentum",
            "class": "agent_arena.agents.baselines.MomentumAgent",
            "config": {"rebalance_ticks": 24, "position_size": 0.1, "long_only": True},
        },
        {
            "id": "buy_hold_baseline",
            "name": "Buy & Hold",
            "class": "agent_arena.agents.baselines.BuyAndHoldAgent",
            "config": {"position_size": 0.5, "leverage": 1},
        },
        {
            "id": "mean_reversion_baseline",
            "name": "Mean Reversion",
            "class": "agent_arena.agents.baselines.MeanReversionAgent",
            "config": {"rsi_oversold": 30, "rsi_overbought": 70, "position_size": 0.1},
        },
    ]

    # Load config
    agents = []
    config_data = {}

    if request.config_path:
        config_path = _validate_config_path(request.config_path)
        if not config_path.exists():
            raise HTTPException(
                status_code=404,
                detail="Config file not found",
            )

        with open(config_path) as f:
            config_data = yaml.safe_load(f)

        for agent_config in config_data.get("agents", []):
            try:
                agent = load_agent(agent_config)
                agents.append(agent)
            except Exception:
                logger.exception("Failed to load agent %s", agent_config.get("id"))
                raise HTTPException(
                    status_code=400,
                    detail=f"Failed to load agent '{agent_config.get('id')}'"
                )
    elif request.agents:
        for agent_config in request.agents:
            try:
                agent = load_agent(agent_config)
                agents.append(agent)
            except Exception:
                logger.exception("Failed to load agent %s", agent_config.get("id"))
                raise HTTPException(
                    status_code=400,
                    detail=f"Failed to load agent '{agent_config.get('id')}'"
                )

    # Handle agent_configs from frontend
    if request.agent_configs:
        for agent_config in request.agent_configs:
            # Convert frontend format to backend format
            converted = {
                "id": agent_config.get("agent_id"),
                "name": agent_config.get("name"),
                "class": agent_config.get("class_path"),
                "config": agent_config.get("config", {}),
            }
            # Skip if use_from_competition flag is set (not supported in backtest)
            if agent_config.get("use_from_competition"):
                continue
            try:
                agent = load_agent(converted)
                agents.append(agent)
            except Exception:
                logger.exception("Failed to load agent %s", converted.get("id"))
                raise HTTPException(
                    status_code=400,
                    detail=f"Failed to load agent '{converted.get('id')}'"
                )

    # Add baseline agents if requested
    if request.run_baselines:
        for baseline_config in BASELINE_AGENTS:
            try:
                agent = load_agent(baseline_config)
                agents.append(agent)
            except Exception as e:
                logger.warning("Failed to load baseline %s: %s", baseline_config.get('id'), e)

    if not agents:
        raise HTTPException(status_code=400, detail="No agents configured")

    # Build competition config
    fees_data = config_data.get("fees", {})
    constraints_data = config_data.get("constraints", {})
    candles_data = config_data.get("candles", {})

    config = CompetitionConfig(
        name=request.name,
        symbols=request.symbols,
        interval_seconds=_interval_to_seconds(request.tick_interval),
        agent_timeout_seconds=config_data.get("agent_timeout_seconds", 120),
        fees=FeeConfig(
            taker_fee=Decimal(str(fees_data.get("taker_fee", "0.0004"))),
            maker_fee=Decimal(str(fees_data.get("maker_fee", "0.0002"))),
            liquidation_fee=Decimal(str(fees_data.get("liquidation_fee", "0.005"))),
        ),
        constraints=ConstraintsConfig(
            max_leverage=constraints_data.get("max_leverage", 10),
            max_position_pct=Decimal(str(constraints_data.get("max_position_pct", "0.25"))),
            starting_capital=Decimal(str(constraints_data.get("starting_capital", "10000"))),
        ),
        candles=CandleConfig(
            enabled=candles_data.get("enabled", True),
            intervals=candles_data.get("intervals", [request.tick_interval]),
            limit=candles_data.get("limit", 100),
        ),
    )

    # Create runner
    runner = BacktestRunner(
        config=config,
        agents=agents,
        storage=_storage,
        start_date=request.start_date,
        end_date=request.end_date,
        tick_interval=request.tick_interval,
    )

    # Store runner reference
    _active_backtests[run_id] = runner

    # Get cost estimate
    estimate = runner.estimate_cost()

    # Run in background
    async def run_backtest():
        try:
            await runner.run(name=request.name, save_results=True, run_id=run_id)
        except Exception as e:
            import traceback
            logger.error("Backtest %s failed: %s", run_id, e)
            traceback.print_exc()

    background_tasks.add_task(run_backtest)

    return {
        "run_id": run_id,
        "status": "started",
        "name": request.name,
        "start_date": request.start_date,
        "end_date": request.end_date,
        "tick_interval": request.tick_interval,
        "total_ticks": estimate["total_ticks"],
        "estimated_cost": estimate["total_estimated_cost"],
        "agents": [a.agent_id for a in agents],
    }


@router.get("/{run_id}/status")
async def get_backtest_status(run_id: str) -> dict:
    """Get status of a backtest run."""
    # Check active backtests first
    if run_id in _active_backtests:
        runner = _active_backtests[run_id]
        result = runner.result

        if result:
            return {
                "run_id": run_id,
                "status": result.status,
                "name": result.name,
                "current_tick": result.completed_ticks,
                "total_ticks": result.total_ticks,
                "progress_pct": round(
                    (result.completed_ticks / result.total_ticks * 100)
                    if result.total_ticks > 0 else 0,
                    1
                ),
                "started_at": result.started_at.isoformat() if result.started_at else None,
                "completed_at": result.completed_at.isoformat() if result.completed_at else None,
                "error_message": result.error_message,
            }

    # Check database
    if _storage:
        if hasattr(_storage, "get_backtest_run"):
            run = await _storage.get_backtest_run(run_id)
        elif hasattr(_storage, "_connection"):
            from agent_arena.storage.candles import CandleStorage
            candle_storage = CandleStorage(_storage._connection)
            run = await candle_storage.get_backtest_run(run_id)
        else:
            run = None

        if run:
            return {
                "run_id": run_id,
                "status": run.get("status"),
                "name": run.get("name"),
                "current_tick": run.get("current_tick", 0),
                "total_ticks": run.get("total_ticks", 0),
                "progress_pct": round(
                    (run.get("current_tick", 0) / run.get("total_ticks", 1) * 100)
                    if run.get("total_ticks") else 0,
                    1
                ),
                "started_at": run.get("started_at"),
                "completed_at": run.get("completed_at"),
                "error_message": run.get("error_message"),
            }

    raise HTTPException(status_code=404, detail=f"Backtest run {run_id} not found")


@router.post("/{run_id}/stop")
async def stop_backtest(
    run_id: str,
    _admin: bool = Depends(require_admin_access),
) -> dict:
    """Stop a running backtest."""
    runner = _active_backtests.get(run_id)
    if not runner:
        raise HTTPException(
            status_code=404, detail="Active backtest not found",
        )
    runner.cancel()

    return {
        "run_id": run_id,
        "status": "cancelled",
    }


@router.get("/{run_id}/results")
async def get_backtest_results(run_id: str) -> dict:
    """Get results for a completed backtest."""
    # Check active backtests first
    if run_id in _active_backtests:
        runner = _active_backtests[run_id]
        result = runner.result

        if result and result.status == "completed":
            return result.to_dict()

    # Check database
    if _storage:
        if hasattr(_storage, "get_backtest_run"):
            run = await _storage.get_backtest_run(run_id)
            if run:
                results = await _storage.get_backtest_results(run_id)
                comparisons = await _storage.get_comparisons(run_id)

                return {
                    "run_id": run_id,
                    **run,
                    "agent_results": results,
                    "comparisons": comparisons,
                }
        elif hasattr(_storage, "_connection"):
            from agent_arena.storage.candles import CandleStorage
            candle_storage = CandleStorage(_storage._connection)

            run = await candle_storage.get_backtest_run(run_id)
            if run:
                results = await candle_storage.get_backtest_results(run_id)
                comparisons = await candle_storage.get_comparisons(run_id)

                return {
                    "run_id": run_id,
                    **run,
                    "agent_results": results,
                    "comparisons": comparisons,
                }

    raise HTTPException(status_code=404, detail=f"Backtest results for {run_id} not found")


@router.get("/{run_id}/leaderboard")
async def get_backtest_leaderboard(run_id: str) -> list[dict]:
    """Get leaderboard for a backtest run."""
    # Check active backtests first
    if run_id in _active_backtests:
        runner = _active_backtests[run_id]
        result = runner.result

        if result:
            return result.get_leaderboard()

    # Check database
    if _storage:
        if hasattr(_storage, "get_backtest_results"):
            results = await _storage.get_backtest_results(run_id)
        elif hasattr(_storage, "_connection"):
            from agent_arena.storage.candles import CandleStorage
            candle_storage = CandleStorage(_storage._connection)
            results = await candle_storage.get_backtest_results(run_id)
        else:
            results = []

        if results:
            return [
                {
                    "rank": i + 1,
                    "agent_id": r["agent_id"],
                    "agent_name": r["agent_name"],
                    "total_pnl": r.get("total_pnl", 0),
                    "total_pnl_pct": r.get("total_pnl_pct", 0),
                    "win_rate": r.get("win_rate"),
                    "sharpe_ratio": r.get("sharpe_ratio"),
                    "max_drawdown_pct": r.get("max_drawdown_pct"),
                    "total_trades": r.get("total_trades", 0),
                }
                for i, r in enumerate(results)
            ]

    raise HTTPException(status_code=404, detail=f"Backtest {run_id} not found")


@router.get("/{run_id}/equity-curves")
async def get_backtest_equity_curves(run_id: str) -> dict:
    """Get equity curves for all agents in a backtest."""
    if _storage:
        if hasattr(_storage, "get_backtest_results"):
            results = await _storage.get_backtest_results(run_id)
        elif hasattr(_storage, "_connection"):
            from agent_arena.storage.candles import CandleStorage
            candle_storage = CandleStorage(_storage._connection)
            results = await candle_storage.get_backtest_results(run_id)
        else:
            results = []

        if results:
            return {
                r["agent_id"]: r.get("equity_curve", [])
                for r in results
            }

    raise HTTPException(status_code=404, detail=f"Backtest {run_id} not found")


@router.get("/{run_id}/trades")
async def get_backtest_trades(
    run_id: str,
    agent_id: Optional[str] = None,
) -> list[dict]:
    """Get trades from a backtest run."""
    if _storage:
        if hasattr(_storage, "get_backtest_results"):
            results = await _storage.get_backtest_results(run_id)
        elif hasattr(_storage, "_connection"):
            from agent_arena.storage.candles import CandleStorage
            candle_storage = CandleStorage(_storage._connection)
            results = await candle_storage.get_backtest_results(run_id)
        else:
            results = []

        if results:
            all_trades = []
            for r in results:
                if agent_id and r["agent_id"] != agent_id:
                    continue

                trades = r.get("trades", [])
                for t in trades:
                    t["agent_id"] = r["agent_id"]
                    t["agent_name"] = r["agent_name"]
                all_trades.extend(trades)

            # Sort by timestamp
            all_trades.sort(key=lambda x: x.get("timestamp", ""))
            return all_trades

    raise HTTPException(status_code=404, detail=f"Backtest {run_id} not found")


@router.get("/runs")
async def list_backtest_runs(
    limit: int = 50,
    status: Optional[str] = None,
) -> dict:
    """List all backtest runs."""
    runs = []

    # First, add runs from memory (active/recent backtests)
    for run_id, runner in _active_backtests.items():
        result = runner.result
        if result:
            # Filter by status if requested
            if status and result.status != status:
                continue
            runs.append({
                "run_id": run_id,
                "name": result.name,
                "status": result.status,
                "start_date": result.start_date,
                "end_date": result.end_date,
                "symbols": result.config.get("symbols", []),
                "tick_interval": result.tick_interval,
                "total_ticks": result.total_ticks,
                "created_at": result.started_at.isoformat() if result.started_at else None,
                "completed_at": result.completed_at.isoformat() if result.completed_at else None,
                "duration_seconds": result.duration_seconds,
            })

    # Then add runs from database (if available)
    if _storage:
        if hasattr(_storage, "get_backtest_runs"):
            db_runs = await _storage.get_backtest_runs(limit=limit, status=status)
        elif hasattr(_storage, "_connection"):
            from agent_arena.storage.candles import CandleStorage
            candle_storage = CandleStorage(_storage._connection)
            db_runs = await candle_storage.get_backtest_runs(limit=limit, status=status)
        else:
            db_runs = []

        # Add DB runs that aren't already in memory
        memory_run_ids = {r["run_id"] for r in runs}
        for run in db_runs:
            if run.get("run_id") not in memory_run_ids:
                runs.append(run)

    # Sort by created_at descending
    runs.sort(key=lambda x: x.get("created_at") or "", reverse=True)

    return {"runs": runs[:limit]}


@router.delete("/{run_id}")
async def delete_backtest_run(
    run_id: str,
    _admin: bool = Depends(require_admin_access),
) -> dict:
    """Delete a backtest run and all associated data."""
    runner = _active_backtests.pop(run_id, None)
    if runner:
        runner.cancel()

    if _storage:
        if hasattr(_storage, "delete_backtest_run"):
            await _storage.delete_backtest_run(run_id)
        elif hasattr(_storage, "_connection"):
            from agent_arena.storage.candles import CandleStorage
            candle_storage = CandleStorage(_storage._connection)
            await candle_storage.delete_backtest_run(run_id)

    return {"deleted": run_id}


def _interval_to_seconds(interval: str) -> int:
    """Convert interval string to seconds."""
    intervals = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "2h": 7200,
        "4h": 14400,
        "6h": 21600,
        "8h": 28800,
        "12h": 43200,
        "1d": 86400,
    }
    return intervals.get(interval, 3600)
