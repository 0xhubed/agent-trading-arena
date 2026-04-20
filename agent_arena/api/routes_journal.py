"""API routes for Observer Journal — daily diagnostic editorials."""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from agent_arena.api.routes import require_admin_access

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/journal", tags=["journal"])

# Module-level storage reference (set by app.py)
_storage: Optional[Any] = None

# Prevent concurrent journal generation within a single process.
# NOTE: single-process only; use Postgres advisory lock for multi-worker.
_generate_lock = asyncio.Lock()


def set_storage(storage: Any) -> None:
    """Set the storage backend for journal routes."""
    global _storage
    _storage = storage


# --- Response models ---


class JournalListResponse(BaseModel):
    entries: list[dict]
    total: int


class JournalEntryResponse(BaseModel):
    entry: Optional[dict]


class GenerateResponse(BaseModel):
    status: str
    message: str
    entry_id: Optional[str] = None


# --- Endpoints ---


@router.get("/entries", response_model=JournalListResponse)
async def get_journal_entries(
    limit: int = Query(30, ge=1, le=100, description="Max entries"),
) -> JournalListResponse:
    """List journal entries (paginated, most recent first)."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    try:
        entries = await _storage.get_journal_entries(limit=limit)
        return JournalListResponse(entries=entries, total=len(entries))
    except Exception:
        logger.exception("Failed to fetch journal entries")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/entries/latest", response_model=JournalEntryResponse)
async def get_latest_journal_entry() -> JournalEntryResponse:
    """Get the most recent journal entry (full content)."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    try:
        entry = await _storage.get_latest_journal_entry()
        return JournalEntryResponse(entry=entry)
    except Exception:
        logger.exception("Failed to fetch latest journal entry")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/entries/{date}", response_model=JournalEntryResponse)
async def get_journal_entry_by_date(
    date: str,
) -> JournalEntryResponse:
    """Get journal entry by date (YYYY-MM-DD)."""
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        raise HTTPException(
            status_code=400,
            detail="Invalid date format, expected YYYY-MM-DD",
        )

    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    try:
        entry = await _storage.get_journal_entry_by_date(date)
        return JournalEntryResponse(entry=entry)
    except Exception:
        logger.exception("Failed to fetch journal entry for %s", date)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/generate", response_model=GenerateResponse)
async def generate_journal(
    lookback_hours: int = Query(24, ge=1, le=168, description="Hours of data to analyze"),
    _admin: bool = Depends(require_admin_access),
) -> GenerateResponse:
    """Trigger journal generation. Requires admin access."""
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    if _generate_lock.locked():
        return GenerateResponse(
            status="in_progress",
            message="Journal generation already running",
        )

    async with _generate_lock:
        try:
            from agent_arena.journal.service import JournalService

            service = JournalService(storage=_storage)
            entry = await service.generate_daily_journal(
                lookback_hours=lookback_hours,
            )

            # Trigger codegen in the background (non-blocking)
            asyncio.create_task(_trigger_codegen_background())

            return GenerateResponse(
                status="completed",
                message=f"Journal generated for {entry.journal_date}",
                entry_id=entry.id,
            )
        except Exception:
            logger.exception("Journal generation failed")
            raise HTTPException(
                status_code=500, detail="Journal generation failed",
            )


async def _trigger_codegen_background() -> None:
    """Run codegen as a background subprocess after journal generation."""
    import sys
    from pathlib import Path

    # Use the same Python that's running this process to invoke the CLI,
    # so we don't depend on PATH having the venv activated.
    cli_bin = Path(sys.executable).parent / "agent-arena"
    if not cli_bin.exists():
        logger.info("agent-arena CLI not found at %s, skipping codegen", cli_bin)
        return

    try:
        proc = await asyncio.create_subprocess_exec(
            str(cli_bin), "codegen",
            "--lookback-days", "5",
            "--max-changes", "3",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=300,
        )
        if proc.returncode == 0:
            logger.info("Codegen completed successfully")
        else:
            logger.warning(
                "Codegen exited %d: %s",
                proc.returncode, stderr.decode()[-300:],
            )
    except asyncio.TimeoutError:
        logger.warning("Codegen timed out after 300s")
        if proc:
            proc.kill()
    except Exception:
        logger.warning("Codegen trigger failed (non-fatal)", exc_info=True)
