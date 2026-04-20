"""API routes for Lab tab — bias profiles, contagion metrics, and analysis."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel

from agent_arena.api.routes import require_admin_access

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/lab", tags=["lab"])

# Module-level storage reference (set by app.py)
_storage: Optional[Any] = None
_has_contagion: bool = False

# Prevent concurrent analysis runs
_analysis_lock = asyncio.Lock()

# Max recent decisions to load per agent for analysis
_MAX_DECISIONS_PER_AGENT = 5000


def set_storage(storage: Any) -> None:
    """Set the storage backend for lab routes."""
    global _storage, _has_contagion
    _storage = storage
    _has_contagion = hasattr(storage, "get_contagion_latest")


# --- Response models ---


class BiasProfilesResponse(BaseModel):
    profiles: list[dict]
    total: int


class BiasHistoryResponse(BaseModel):
    history: list[dict]
    total: int


class ContagionResponse(BaseModel):
    snapshots: list[dict]
    total: int


class ContagionLatestResponse(BaseModel):
    metrics: list[dict]
    system_health: Optional[float]
    health_label: str


class AnalysisResponse(BaseModel):
    status: str
    message: str


# --- Bias endpoints ---


@router.get("/bias/profiles", response_model=BiasProfilesResponse)
async def get_bias_profiles(
    agent_id: Optional[str] = Query(
        None, max_length=100, description="Filter by agent ID",
    ),
) -> BiasProfilesResponse:
    """Get latest bias profiles for all agents (or one agent)."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    try:
        profiles = await _storage.get_bias_profiles(agent_id=agent_id)
        return BiasProfilesResponse(profiles=profiles, total=len(profiles))
    except Exception:
        logger.exception("Failed to fetch bias profiles")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/bias/history/{agent_id}", response_model=BiasHistoryResponse)
async def get_bias_history(
    agent_id: str = Path(max_length=100),
    bias_type: Optional[str] = Query(
        None, max_length=50, description="Filter by bias type",
    ),
) -> BiasHistoryResponse:
    """Get historical bias scores for an agent."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    try:
        history = await _storage.get_bias_history(agent_id, bias_type=bias_type)
        return BiasHistoryResponse(history=history, total=len(history))
    except Exception:
        logger.exception("Failed to fetch bias history for %s", agent_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/bias/analyze", response_model=AnalysisResponse)
async def trigger_bias_analysis(
    agent_id: Optional[str] = Query(
        None, max_length=100, description="Analyze a single agent",
    ),
    _admin: bool = Depends(require_admin_access),
) -> AnalysisResponse:
    """Trigger bias analysis for agents. Requires admin access."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    if _analysis_lock.locked():
        return AnalysisResponse(
            status="in_progress",
            message="Analysis already running",
        )

    async with _analysis_lock:
        try:
            from agent_arena.analysis.bias_scan import analyze_agent_biases

            if agent_id:
                agent_ids = [agent_id]
            else:
                agent_ids = await _storage.get_all_agent_ids()

            if not agent_ids:
                return AnalysisResponse(
                    status="skipped", message="No agents found",
                )

            # Fetch decisions and trades concurrently
            async def _load_agent(aid: str):
                decisions = await _storage.get_all_decisions(
                    aid, limit=_MAX_DECISIONS_PER_AGENT,
                )
                trades = await _storage.get_all_trades(
                    aid, limit=_MAX_DECISIONS_PER_AGENT,
                )
                return aid, decisions, trades

            results = await asyncio.gather(
                *[_load_agent(aid) for aid in agent_ids]
            )

            count = 0
            for aid, decisions, trades in results:
                if not decisions:
                    continue
                profile = analyze_agent_biases(aid, decisions, trades)
                await _storage.save_bias_profile(profile.to_dict())
                count += 1

            return AnalysisResponse(
                status="completed",
                message=f"Analyzed {count} agent(s)",
            )
        except Exception:
            logger.exception("Bias analysis failed")
            raise HTTPException(
                status_code=500, detail="Internal server error",
            )


# --- Contagion endpoints ---


@router.get("/contagion/latest", response_model=ContagionLatestResponse)
async def get_contagion_latest() -> ContagionLatestResponse:
    """Get the most recent contagion metrics."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    try:
        if not _has_contagion:
            return ContagionLatestResponse(
                metrics=[], system_health=None, health_label="UNKNOWN",
            )

        from agent_arena.analysis.contagion import compute_system_health

        metrics = await _storage.get_contagion_latest()
        values = [
            m["value"] for m in metrics
            if m.get("sufficient_data") and m.get("value") is not None
        ]
        health, label = compute_system_health(values)

        return ContagionLatestResponse(
            metrics=metrics,
            system_health=health,
            health_label=label,
        )
    except Exception:
        logger.exception("Failed to fetch contagion latest")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/contagion/history", response_model=ContagionResponse)
async def get_contagion_history(
    metric_type: Optional[str] = Query(
        None, max_length=50, description="Filter by metric type",
    ),
    limit: int = Query(100, ge=1, le=500, description="Max records"),
) -> ContagionResponse:
    """Get contagion metric history."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    try:
        if not _has_contagion:
            return ContagionResponse(snapshots=[], total=0)

        snapshots = await _storage.get_contagion_snapshots(
            metric_type=metric_type, limit=limit,
        )
        return ContagionResponse(snapshots=snapshots, total=len(snapshots))
    except Exception:
        logger.exception("Failed to fetch contagion history")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/contagion/analyze", response_model=AnalysisResponse)
async def trigger_contagion_analysis(
    _admin: bool = Depends(require_admin_access),
) -> AnalysisResponse:
    """Trigger contagion analysis. Requires admin access."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    if _analysis_lock.locked():
        return AnalysisResponse(
            status="in_progress",
            message="Analysis already running",
        )

    async with _analysis_lock:
        try:
            from agent_arena.analysis.contagion import analyze_contagion

            agent_ids = await _storage.get_all_agent_ids()
            if not agent_ids:
                return AnalysisResponse(
                    status="skipped", message="No agents found",
                )

            # Fetch all agent decisions concurrently
            async def _load(aid: str):
                return aid, await _storage.get_all_decisions(
                    aid, limit=_MAX_DECISIONS_PER_AGENT,
                )

            results = await asyncio.gather(
                *[_load(aid) for aid in agent_ids]
            )

            decisions_by_agent = {
                aid: decs for aid, decs in results if decs
            }

            if not decisions_by_agent:
                return AnalysisResponse(
                    status="skipped", message="No decisions found",
                )

            snap = analyze_contagion(decisions_by_agent)

            if _has_contagion:
                await _storage.save_contagion_snapshot(snap.to_dict())

            return AnalysisResponse(
                status="completed",
                message=(
                    f"Analyzed {snap.agent_count} agents — "
                    f"health: {snap.health_label}"
                ),
            )
        except Exception:
            logger.exception("Contagion analysis failed")
            raise HTTPException(
                status_code=500, detail="Internal server error",
            )
