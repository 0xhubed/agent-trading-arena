"""Experiment API routes for Agent Arena."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from agent_arena.api.routes import require_admin_access

router = APIRouter(prefix="/experiment", tags=["experiment"])
logger = logging.getLogger(__name__)

# Module-level dependencies
_storage = None
_active_experiments: dict[str, dict[str, Any]] = {}
MAX_CONCURRENT_EXPERIMENTS = 1


def set_storage(storage: Any) -> None:
    """Set storage dependency."""
    global _storage
    _storage = storage


class ExperimentStartRequest(BaseModel):
    """Request body for starting an experiment."""

    name: str = "Experiment"
    population_size: int = Field(default=16, ge=4, le=100)
    generations: int = Field(default=5, ge=1, le=50)
    budget_limit_usd: float = Field(default=5.0, gt=0, le=100)
    backtest_start: str = ""
    backtest_end: str = ""
    tick_interval: str = "4h"
    symbols: list[str] = ["PF_XBTUSD", "PF_ETHUSD", "PF_SOLUSD"]
    agent_class: str = "agent_arena.agents.llm_trader.LLMTrader"
    base_url: str = ""
    api_key_env: str = "TOGETHER_API_KEY"
    elite_count: int = 3
    mutation_rate: float = 0.15
    validation_threshold: float = 0.7


class PromotionActionRequest(BaseModel):
    """Request body for approve/reject actions."""

    notes: str = ""


@router.post("/start")
async def start_experiment(
    request: ExperimentStartRequest,
    _auth=Depends(require_admin_access),
):
    """Start a new experiment run (admin-gated)."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    if len(_active_experiments) >= MAX_CONCURRENT_EXPERIMENTS:
        raise HTTPException(
            status_code=409,
            detail=f"Max concurrent experiments ({MAX_CONCURRENT_EXPERIMENTS}) reached",
        )

    from agent_arena.experiment.orchestrator import ExperimentConfig, ExperimentOrchestrator

    config = ExperimentConfig(
        name=request.name,
        population_size=request.population_size,
        generations=request.generations,
        budget_limit_usd=request.budget_limit_usd,
        backtest_start=request.backtest_start,
        backtest_end=request.backtest_end,
        tick_interval=request.tick_interval,
        symbols=request.symbols,
        agent_class=request.agent_class,
        base_url=request.base_url,
        api_key_env=request.api_key_env,
        elite_count=request.elite_count,
        mutation_rate=request.mutation_rate,
        validation_threshold=request.validation_threshold,
    )

    errors = config.validate()
    if errors:
        raise HTTPException(status_code=400, detail="; ".join(errors))

    orchestrator = ExperimentOrchestrator(
        config=config,
        storage=_storage,
    )

    async def _run():
        try:
            result = await orchestrator.run()
            # Queue promotion candidates
            if result.promotion_candidates:
                await _queue_promotions(result)
        except Exception:
            logger.exception("Experiment %s failed", orchestrator.experiment_id)
        finally:
            _active_experiments.pop(orchestrator.experiment_id, None)

    task = asyncio.create_task(_run())
    _active_experiments[orchestrator.experiment_id] = {
        "orchestrator": orchestrator,
        "task": task,
    }

    return {
        "experiment_id": orchestrator.experiment_id,
        "status": "started",
        "config": config.__dict__,
    }


@router.get("/runs")
async def list_experiments(limit: int = 20):
    """List experiment runs."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    if not hasattr(_storage, "pool"):
        return {"runs": [], "message": "Experiments require PostgreSQL"}

    try:
        async with _storage.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, name, status, best_fitness, validation_fitness,
                       overfit_warning, total_cost_usd, generations_completed,
                       error, created_at, updated_at
                FROM experiment_runs
                ORDER BY created_at DESC
                LIMIT $1
                """,
                limit,
            )
            return {
                "runs": [
                    {
                        "id": r["id"],
                        "name": r["name"],
                        "status": r["status"],
                        "best_fitness": float(r["best_fitness"]) if r["best_fitness"] else None,
                        "validation_fitness": float(r["validation_fitness"]) if r["validation_fitness"] else None,
                        "overfit_warning": r["overfit_warning"],
                        "total_cost_usd": float(r["total_cost_usd"]) if r["total_cost_usd"] else 0,
                        "generations_completed": r["generations_completed"],
                        "error": r["error"],
                        "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                    }
                    for r in rows
                ]
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/runs/{experiment_id}")
async def get_experiment(experiment_id: str):
    """Get experiment run detail."""
    if not _storage or not hasattr(_storage, "pool"):
        raise HTTPException(status_code=503, detail="Requires PostgreSQL")

    async with _storage.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM experiment_runs WHERE id = $1",
            experiment_id,
        )
        if not row:
            raise HTTPException(status_code=404, detail="Experiment not found")

        return dict(row)


@router.get("/promotions")
async def list_promotions(status: Optional[str] = None):
    """List promotion queue entries."""
    if not _storage or not hasattr(_storage, "pool"):
        raise HTTPException(status_code=503, detail="Requires PostgreSQL")

    try:
        async with _storage.pool.acquire() as conn:
            if status:
                rows = await conn.fetch(
                    """
                    SELECT * FROM promotion_queue
                    WHERE status = $1
                    ORDER BY fitness DESC
                    """,
                    status,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM promotion_queue ORDER BY created_at DESC LIMIT 50"
                )
            return {"promotions": [dict(r) for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/promotions/{promotion_id}/approve")
async def approve_promotion(
    promotion_id: int,
    request: PromotionActionRequest = PromotionActionRequest(),
    _auth=Depends(require_admin_access),
):
    """Approve a promotion candidate."""
    return await _update_promotion(promotion_id, "approved", request.notes)


@router.post("/promotions/{promotion_id}/reject")
async def reject_promotion(
    promotion_id: int,
    request: PromotionActionRequest = PromotionActionRequest(),
    _auth=Depends(require_admin_access),
):
    """Reject a promotion candidate."""
    return await _update_promotion(promotion_id, "rejected", request.notes)


async def _update_promotion(promotion_id: int, new_status: str, notes: str) -> dict:
    """Update promotion status."""
    if not _storage or not hasattr(_storage, "pool"):
        raise HTTPException(status_code=503, detail="Requires PostgreSQL")

    from datetime import datetime, timezone

    async with _storage.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM promotion_queue WHERE id = $1",
            promotion_id,
        )
        if not row:
            raise HTTPException(status_code=404, detail="Promotion not found")

        await conn.execute(
            """
            UPDATE promotion_queue
            SET status = $1, notes = $2, reviewed_at = $3
            WHERE id = $4
            """,
            new_status,
            notes,
            datetime.now(timezone.utc),
            promotion_id,
        )

        return {"id": promotion_id, "status": new_status}


async def _queue_promotions(result) -> None:
    """Insert promotion candidates into the queue."""
    if not _storage or not hasattr(_storage, "pool"):
        return

    import json

    try:
        async with _storage.pool.acquire() as conn:
            for candidate in result.promotion_candidates:
                await conn.execute(
                    """
                    INSERT INTO promotion_queue (
                        experiment_id, genome, fitness, validation_fitness, status
                    ) VALUES ($1, $2, $3, $4, 'pending')
                    """,
                    result.experiment_id,
                    json.dumps(candidate.get("genome", {})),
                    candidate.get("fitness", 0.0),
                    result.validation_fitness,
                )
    except Exception:
        logger.exception("Failed to queue promotions for %s", result.experiment_id)
