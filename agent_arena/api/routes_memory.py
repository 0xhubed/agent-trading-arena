"""Memory API routes — health, principles, digestion history."""

from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException

from agent_arena.api.routes import require_admin_access

router = APIRouter(prefix="/memory", tags=["memory"])
logger = logging.getLogger(__name__)

_storage = None


def set_storage(storage: Any) -> None:
    global _storage
    _storage = storage


@router.get("/health")
async def memory_health(agent_id: Optional[str] = None):
    """Get memory health metrics."""
    if not _storage or not hasattr(_storage, "pool"):
        return {"status": "unavailable", "message": "Requires PostgreSQL"}

    try:
        async with _storage.pool.acquire() as conn:
            conditions = []
            params = []

            if agent_id:
                conditions.append("agent_id = $1")
                params.append(agent_id)

            where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

            # Count reflections
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

            return {
                "total_reflections": total["cnt"] if total else 0,
                "digested_reflections": digested["cnt"] if digested else 0,
                "active_principles": principles["cnt"] if principles else 0,
                "agent_id": agent_id,
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/principles")
async def list_principles(agent_id: Optional[str] = None, limit: int = 20):
    """List active abstract principles."""
    if not _storage or not hasattr(_storage, "pool"):
        return {"principles": []}

    try:
        async with _storage.pool.acquire() as conn:
            if agent_id:
                rows = await conn.fetch(
                    """
                    SELECT * FROM abstract_principles
                    WHERE agent_id = $1 AND is_active = TRUE
                    ORDER BY confidence DESC, application_count DESC
                    LIMIT $2
                    """,
                    agent_id, limit,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT * FROM abstract_principles
                    WHERE is_active = TRUE
                    ORDER BY confidence DESC
                    LIMIT $1
                    """,
                    limit,
                )
            return {
                "principles": [
                    {
                        "id": r["id"],
                        "agent_id": r["agent_id"],
                        "principle": r["principle"],
                        "regime": r.get("regime", ""),
                        "confidence": r["confidence"],
                        "application_count": r["application_count"],
                        "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
                    }
                    for r in rows
                ]
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/digestion")
async def digestion_history(agent_id: Optional[str] = None, limit: int = 10):
    """List digestion history."""
    if not _storage or not hasattr(_storage, "pool"):
        return {"history": []}

    try:
        async with _storage.pool.acquire() as conn:
            if agent_id:
                rows = await conn.fetch(
                    """
                    SELECT * FROM digestion_history
                    WHERE agent_id = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                    """,
                    agent_id, limit,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM digestion_history ORDER BY created_at DESC LIMIT $1",
                    limit,
                )
            return {"history": [dict(r) for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/digest")
async def trigger_digestion(
    agent_id: str,
    _auth=Depends(require_admin_access),
):
    """Trigger memory digestion for an agent (admin-gated)."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    from agent_arena.memory.digestion import MemoryDigester

    digester = MemoryDigester(storage=_storage)
    result = await digester.run_digestion_cycle(agent_id)

    return result.to_dict()
