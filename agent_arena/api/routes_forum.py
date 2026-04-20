"""API routes for forum features."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/forum", tags=["forum"])

# Module-level storage reference (set by app.py)
_storage: Optional[Any] = None


def set_storage(storage: Any) -> None:
    """Set the storage backend for forum routes."""
    global _storage
    _storage = storage


class ForumMessagesResponse(BaseModel):
    """Response model for forum messages."""

    messages: list[dict]
    total: int


class WitnessSummariesResponse(BaseModel):
    """Response model for witness summaries."""

    summaries: list[dict]
    total: int


@router.get("/messages", response_model=ForumMessagesResponse)
async def get_forum_messages(
    channel: Optional[str] = Query(None, description="Filter by channel"),
    limit: int = Query(50, ge=1, le=200, description="Maximum messages to return"),
    since_hours: Optional[int] = Query(
        None, ge=1, le=168, description="Only messages from last N hours"
    ),
) -> ForumMessagesResponse:
    """Get recent forum messages.

    Args:
        channel: Filter by channel (market, strategy, or None for all)
        limit: Maximum number of messages
        since_hours: Only return messages from last N hours

    Returns:
        ForumMessagesResponse with messages list
    """
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    try:
        # Build filters
        channels = [channel] if channel else None
        since = None
        if since_hours:
            since = datetime.now(timezone.utc) - timedelta(hours=since_hours)

        # Get messages from storage
        messages = await _storage.get_forum_messages(
            channels=channels,
            limit=limit,
            since=since,
        )

        # Convert to dict format
        message_dicts = [msg.to_dict() for msg in messages]

        return ForumMessagesResponse(
            messages=message_dicts,
            total=len(message_dicts),
        )

    except Exception:
        logger.exception("Error fetching forum messages")
        raise HTTPException(
            status_code=500,
            detail="Internal server error",
        )


@router.get("/witness", response_model=WitnessSummariesResponse)
async def get_witness_summaries(
    hours: int = Query(6, ge=1, le=48, description="Lookback window in hours"),
    min_confidence: float = Query(
        0.0, ge=0.0, le=1.0, description="Minimum confidence threshold"
    ),
    symbols: Optional[list[str]] = Query(None, description="Filter by symbols"),
) -> WitnessSummariesResponse:
    """Get recent witness summaries.

    Args:
        hours: Lookback window
        min_confidence: Minimum confidence filter
        symbols: Symbol filter

    Returns:
        WitnessSummariesResponse with summaries list
    """
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    try:
        since = datetime.now(timezone.utc) - timedelta(hours=hours)

        # Get witness summaries from storage
        summaries = await _storage.get_witness_summaries(
            since=since,
            symbols=symbols,
            min_confidence=min_confidence,
        )

        # Convert to dict format
        summary_dicts = [s.to_dict() for s in summaries]

        return WitnessSummariesResponse(
            summaries=summary_dicts,
            total=len(summary_dicts),
        )

    except Exception:
        logger.exception("Error fetching witness summaries")
        raise HTTPException(
            status_code=500,
            detail="Internal server error",
        )


@router.get("/stats")
async def get_forum_stats() -> dict:
    """Get forum activity statistics.

    Returns:
        Dict with message counts by channel and agent type
    """
    if not _storage:
        raise HTTPException(status_code=503, detail="Storage not initialized")

    try:
        # Get recent messages (last 24h)
        since = datetime.now(timezone.utc) - timedelta(hours=24)
        messages = await _storage.get_forum_messages(
            limit=1000,
            since=since,
        )

        # Calculate stats
        total_messages = len(messages)
        by_channel = {}
        by_agent_type = {}

        for msg in messages:
            # Count by channel
            channel = msg.channel
            by_channel[channel] = by_channel.get(channel, 0) + 1

            # Count by agent type
            agent_type = msg.agent_type
            by_agent_type[agent_type] = by_agent_type.get(agent_type, 0) + 1

        return {
            "total_messages_24h": total_messages,
            "by_channel": by_channel,
            "by_agent_type": by_agent_type,
        }

    except Exception:
        logger.exception("Error fetching forum stats")
        raise HTTPException(
            status_code=500,
            detail="Internal server error",
        )
