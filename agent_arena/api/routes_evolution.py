"""Evolution API routes for Agent Arena."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, field_validator

from agent_arena.api.routes import require_admin_access

router = APIRouter(prefix="/evolution", tags=["evolution"])
logger = logging.getLogger(__name__)

# Module-level dependencies
_storage = None
_active_runs: dict[str, dict[str, Any]] = {}  # run_id -> {"engine": ..., "task": ...}
MAX_CONCURRENT_EVOLUTION_RUNS = 2


def set_storage(storage: Any) -> None:
    """Set storage dependency."""
    global _storage
    _storage = storage


async def _require_postgres() -> Any:
    """FastAPI dependency: require PostgreSQL storage."""
    if not _storage or not hasattr(_storage, "pool"):
        raise HTTPException(status_code=503, detail="PostgreSQL storage required")
    from agent_arena.evolution.storage import EvolutionStorage
    return EvolutionStorage(_storage)


VALID_INTERVALS = {"1m", "5m", "15m", "1h", "4h", "1d"}

ALLOWED_AGENT_CLASSES = {
    "agent_arena.agents.llm_trader.LLMTrader",
    "agent_arena.agents.claude_trader.ClaudeTrader",
    "agent_arena.agents.gpt_trader.GPTTrader",
    "agent_arena.agents.ollama_trader.OllamaTrader",
    "agent_arena.agents.agentic_llm.AgenticLLMTrader",
    "agent_arena.agents.agentic_claude.AgenticClaudeTrader",
    "agent_arena.agents.skill_aware_trader.SkillAwareTrader",
    "agent_arena.agents.forum_aware_trader.ForumAwareTradingAgent",
}


class EvolutionStartRequest(BaseModel):
    name: str = Field("Evolution Run", max_length=200)
    population_size: int = Field(ge=2, le=100, default=20)
    generations: int = Field(ge=1, le=100, default=10)
    elite_count: int = Field(ge=1, default=3)
    mutation_rate: float = Field(ge=0.0, le=1.0, default=0.15)
    backtest_start: str
    backtest_end: str
    tick_interval: str = "4h"
    symbols: list[str] = Field(
        default=["PF_XBTUSD", "PF_ETHUSD", "PF_SOLUSD"],
        max_length=20,
    )
    agent_class: str = "agent_arena.agents.llm_trader.LLMTrader"
    base_url: str = Field(
        default="http://100.104.221.46:4000/v1",
        max_length=500,
    )
    api_key_env: str = Field(
        default="LOCAL_API_KEY", pattern=r"^[A-Z_][A-Z0-9_]*$",
    )
    fitness_weights: Optional[dict] = None
    # Advanced GA features
    use_llm_operators: bool = False
    llm_operator_prob: float = Field(ge=0.0, le=1.0, default=0.3)
    use_novelty: bool = False
    use_islands: bool = False
    use_pareto: bool = False

    @field_validator("elite_count")
    @classmethod
    def elite_count_less_than_pop(cls, v: int, info: Any) -> int:
        pop = info.data.get("population_size", 20)
        if v >= pop:
            raise ValueError(
                "elite_count must be less than population_size"
            )
        return v

    @field_validator("backtest_start", "backtest_end")
    @classmethod
    def validate_date_format(cls, v: str) -> str:
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                f"Invalid date: {v}. Expected YYYY-MM-DD"
            )
        return v

    @field_validator("backtest_end")
    @classmethod
    def end_after_start(cls, v: str, info: Any) -> str:
        start_str = info.data.get("backtest_start")
        if start_str and v <= start_str:
            raise ValueError(
                "backtest_end must be after backtest_start"
            )
        return v

    @field_validator("tick_interval")
    @classmethod
    def validate_interval(cls, v: str) -> str:
        if v not in VALID_INTERVALS:
            raise ValueError(
                f"Invalid interval: {v}. "
                f"Must be one of {VALID_INTERVALS}"
            )
        return v

    @field_validator("agent_class")
    @classmethod
    def validate_agent_class(cls, v: str) -> str:
        if v not in ALLOWED_AGENT_CLASSES:
            raise ValueError(
                f"Agent class not allowed: {v}"
            )
        return v

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("base_url must be http:// or https://")
        return v


@router.post("/start")
async def start_evolution(
    request: EvolutionStartRequest,
    _admin: bool = Depends(require_admin_access),
):
    """Start an evolution run in the background."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    if len(_active_runs) >= MAX_CONCURRENT_EVOLUTION_RUNS:
        raise HTTPException(
            status_code=429,
            detail=f"Max {MAX_CONCURRENT_EVOLUTION_RUNS} concurrent "
            "evolution runs allowed",
        )

    from agent_arena.evolution.engine import EvolutionEngine

    engine = EvolutionEngine(
        population_size=request.population_size,
        generations=request.generations,
        elite_count=request.elite_count,
        mutation_rate=request.mutation_rate,
        backtest_start=request.backtest_start,
        backtest_end=request.backtest_end,
        tick_interval=request.tick_interval,
        symbols=request.symbols,
        storage=_storage,
        fitness_weights=request.fitness_weights,
        agent_class=request.agent_class,
        base_url=request.base_url,
        api_key_env=request.api_key_env,
        use_llm_operators=request.use_llm_operators,
        llm_operator_prob=request.llm_operator_prob,
        use_novelty=request.use_novelty,
        use_islands=request.use_islands,
        use_pareto=request.use_pareto,
    )

    # Start in background — store both engine and task to prevent GC
    task = asyncio.create_task(engine.run(name=request.name))
    run_id = engine.run_id
    _active_runs[run_id] = {"engine": engine, "task": task}

    async def _cleanup(t: asyncio.Task, rid: str) -> None:
        try:
            await t
        except Exception:
            logger.exception("Evolution task %s failed", rid)
        finally:
            _active_runs.pop(rid, None)

    cleanup_task = asyncio.create_task(_cleanup(task, run_id))
    _active_runs[run_id]["cleanup_task"] = cleanup_task

    return {
        "status": "started",
        "run_id": run_id,
        "population_size": request.population_size,
        "generations": request.generations,
    }


@router.get("/runs")
async def list_runs(
    evo_storage: Any = Depends(_require_postgres),
):
    """List all evolution runs."""
    runs = await evo_storage.list_runs()
    return {"runs": runs}


@router.get("/{run_id}")
async def get_run(
    run_id: str,
    evo_storage: Any = Depends(_require_postgres),
):
    """Get evolution run status and progress."""
    summary = await evo_storage.get_run_summary(run_id)
    if not summary:
        raise HTTPException(status_code=404, detail="Run not found")
    return summary


@router.get("/{run_id}/best")
async def get_best_genome(
    run_id: str,
    evo_storage: Any = Depends(_require_postgres),
):
    """Get the best genome from a run."""
    best = await evo_storage.get_best_genome(run_id)
    if not best:
        raise HTTPException(status_code=404, detail="No genomes found")
    return best


@router.get("/{run_id}/generations")
async def get_generations(
    run_id: str,
    generation: Optional[int] = None,
    evo_storage: Any = Depends(_require_postgres),
):
    """Get genomes from all or a specific generation."""
    if generation is not None:
        genomes = await evo_storage.get_generation(run_id, generation)
        return {"run_id": run_id, "generation": generation, "genomes": genomes}

    # Return per-generation stats from the summary
    summary = await evo_storage.get_run_summary(run_id)
    if not summary:
        raise HTTPException(status_code=404, detail="Run not found")
    return {"run_id": run_id, "generations": summary.get("generations", [])}


@router.post("/{run_id}/stop")
async def stop_evolution(
    run_id: str,
    _admin: bool = Depends(require_admin_access),
):
    """Cancel a running evolution."""
    run_info = _active_runs.get(run_id)
    if not run_info:
        raise HTTPException(status_code=404, detail="No active run with that ID")

    run_info["engine"].cancel()
    return {"status": "cancelling", "run_id": run_id}


@router.get("/{run_id}/lineage")
async def get_lineage(
    run_id: str,
    evo_storage: Any = Depends(_require_postgres),
):
    """Get all genomes with parent_ids for family tree visualization."""
    genomes = await evo_storage.get_all_genomes(run_id)
    if not genomes:
        raise HTTPException(status_code=404, detail="No genomes found")
    return {"run_id": run_id, "genomes": genomes}


@router.get("/{run_id}/diversity")
async def get_diversity(
    run_id: str,
    evo_storage: Any = Depends(_require_postgres),
):
    """Get per-generation diversity metrics."""
    diversity = await evo_storage.get_diversity_metrics(run_id)
    return {"run_id": run_id, "diversity": diversity}


@router.get("/{run_id}/export")
async def export_best_genome(
    run_id: str,
    evo_storage: Any = Depends(_require_postgres),
):
    """Export best genome as a YAML-compatible config dict."""
    from agent_arena.evolution.genome import AgentGenome

    best = await evo_storage.get_best_genome(run_id)
    if not best:
        raise HTTPException(status_code=404, detail="No genomes found")

    genome = AgentGenome.from_dict(best["genome"])
    summary = await evo_storage.get_run_summary(run_id)
    symbols = summary["symbols"] if summary else ["PF_XBTUSD", "PF_ETHUSD"]

    agent_config = genome.to_agent_config(
        agent_id="evolved_agent",
        agent_name="Evolved Trader",
        base_url="http://100.104.221.46:4000/v1",
        api_key_env="LOCAL_API_KEY",
    )

    return {
        "name": f"Evolved Agent ({run_id})",
        "symbols": symbols,
        "interval_seconds": 120,
        "agent_timeout_seconds": 180,
        "candles": {"enabled": True, "intervals": ["1h", "4h"], "limit": 100},
        "agents": [agent_config],
        "evolution_metadata": {
            "run_id": run_id,
            "genome_id": best["genome_id"],
            "generation": best["generation"],
            "fitness": best["fitness"],
            "metrics": best.get("metrics", {}),
        },
    }
