"""Reflexion API routes — reflections, failure clusters, skill proposals."""

from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException

from agent_arena.api.routes import require_admin_access

router = APIRouter(prefix="/reflexion", tags=["reflexion"])
logger = logging.getLogger(__name__)

_storage = None


def set_storage(storage: Any) -> None:
    global _storage
    _storage = storage


@router.get("/reflections")
async def list_reflections(
    agent_id: Optional[str] = None,
    outcome: Optional[str] = None,
    limit: int = 20,
):
    """List trade reflections."""
    if not _storage or not hasattr(_storage, "pool"):
        return {"reflections": []}

    try:
        async with _storage.pool.acquire() as conn:
            conditions = []
            params: list[Any] = []
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
                SELECT id, agent_id, trade_id, symbol, side,
                       entry_price, exit_price, realized_pnl,
                       market_regime, entry_signal, outcome,
                       what_went_right, what_went_wrong, lesson,
                       confidence, metabolic_score, created_at
                FROM trade_reflections
                {where}
                ORDER BY created_at DESC
                LIMIT ${idx}
                """,
                *params,
            )

            return {
                "reflections": [
                    {
                        "id": r["id"],
                        "agent_id": r["agent_id"],
                        "symbol": r.get("symbol", ""),
                        "side": r.get("side", ""),
                        "realized_pnl": float(r["realized_pnl"]) if r.get("realized_pnl") else 0,
                        "outcome": r.get("outcome", ""),
                        "lesson": r.get("lesson", ""),
                        "market_regime": r.get("market_regime", ""),
                        "confidence": r.get("confidence"),
                        "metabolic_score": r.get("metabolic_score"),
                        "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
                    }
                    for r in rows
                ]
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/clusters")
async def list_clusters(limit: int = 20):
    """List failure clusters."""
    if not _storage or not hasattr(_storage, "pool"):
        return {"clusters": []}

    try:
        async with _storage.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM failure_clusters
                ORDER BY created_at DESC
                LIMIT $1
                """,
                limit,
            )
            return {"clusters": [dict(r) for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/proposals")
async def list_proposals(status: Optional[str] = None, limit: int = 20):
    """List skill proposals."""
    if not _storage or not hasattr(_storage, "pool"):
        return {"proposals": []}

    try:
        async with _storage.pool.acquire() as conn:
            if status:
                rows = await conn.fetch(
                    """
                    SELECT * FROM skill_proposals
                    WHERE status = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                    """,
                    status, limit,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM skill_proposals ORDER BY created_at DESC LIMIT $1",
                    limit,
                )
            return {"proposals": [dict(r) for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reflect")
async def trigger_reflections(
    agent_id: str,
    lookback_hours: int = 24,
    _auth=Depends(require_admin_access),
):
    """Trigger trade reflections for an agent (admin-gated)."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    from agent_arena.reflexion.service import ReflexionService

    service = ReflexionService(storage=_storage)
    reflections = await service.reflect_on_closed_trades(agent_id, lookback_hours)

    return {
        "agent_id": agent_id,
        "reflections_generated": len(reflections),
    }


@router.post("/cluster")
async def trigger_clustering(
    lookback_days: int = 14,
    _auth=Depends(require_admin_access),
):
    """Trigger failure clustering (admin-gated)."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    from agent_arena.reflexion.clustering import FailureClusterer

    clusterer = FailureClusterer(storage=_storage)
    clusters = await clusterer.cluster_failures(lookback_days=lookback_days)

    return {
        "clusters_found": len(clusters),
        "clusters": [
            {
                "label": c.cluster_label,
                "regime": c.regime,
                "sample_size": c.sample_size,
                "proposed_skill": c.proposed_skill,
            }
            for c in clusters
        ],
    }
