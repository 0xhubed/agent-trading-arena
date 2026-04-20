"""Forum service for agent discussions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Optional
from uuid import UUID

from agent_arena.forum.models import ForumMessage, WitnessSummary

if TYPE_CHECKING:
    from agent_arena.storage.postgres import PostgresStorage


class ForumService:
    """Service for managing forum messages and witness summaries."""

    def __init__(self, storage: PostgresStorage):
        """Initialize forum service.

        Args:
            storage: PostgreSQL storage backend
        """
        self.storage = storage

    async def post_message(
        self,
        channel: str,
        agent_id: str,
        agent_name: str,
        agent_type: str,
        content: str,
        reply_to: Optional[UUID] = None,
        metadata: Optional[dict] = None,
    ) -> UUID:
        """Post a message to the forum.

        Args:
            channel: Forum channel (e.g., 'market', 'strategy')
            agent_id: Unique agent identifier
            agent_name: Display name for the agent
            agent_type: Agent type ('discussion', 'trading')
            content: Message content (markdown supported)
            reply_to: Optional UUID of message being replied to
            metadata: Optional metadata dict

        Returns:
            UUID of the created message
        """
        if metadata is None:
            metadata = {}

        # Validate channel
        valid_channels = ["market", "strategy"]
        if channel not in valid_channels:
            raise ValueError(
                f"Invalid channel: {channel}. Must be one of {valid_channels}"
            )

        # Save to database
        message_id = await self.storage.save_forum_message(
            channel=channel,
            agent_id=agent_id,
            agent_name=agent_name,
            agent_type=agent_type,
            content=content,
            reply_to=reply_to,
            metadata=metadata,
        )

        return message_id

    async def get_recent_messages(
        self,
        channels: Optional[list[str]] = None,
        limit: int = 50,
        since: Optional[datetime] = None,
        symbols: Optional[list[str]] = None,
        agent_types: Optional[list[str]] = None,
    ) -> list[ForumMessage]:
        """Get recent forum messages.

        Args:
            channels: List of channels to filter by (None = all)
            limit: Maximum number of messages to return
            since: Only return messages after this timestamp
            symbols: Filter by symbols mentioned in metadata
            agent_types: Filter by agent types ('discussion', 'trading')

        Returns:
            List of ForumMessage objects, newest first
        """
        messages = await self.storage.get_forum_messages(
            channels=channels,
            limit=limit,
            since=since,
            symbols=symbols,
            agent_types=agent_types,
        )

        return messages

    async def get_message_by_id(self, message_id: UUID) -> Optional[ForumMessage]:
        """Get a specific message by ID.

        Args:
            message_id: Message UUID

        Returns:
            ForumMessage or None if not found
        """
        return await self.storage.get_forum_message_by_id(message_id)

    async def get_thread(self, message_id: UUID, limit: int = 20) -> list[ForumMessage]:
        """Get all replies to a message (thread).

        Args:
            message_id: Root message UUID
            limit: Maximum number of replies

        Returns:
            List of ForumMessage objects that reply to the given message
        """
        return await self.storage.get_forum_thread(message_id, limit)

    async def get_recent_witness_summaries(
        self,
        hours: int = 6,
        symbols: Optional[list[str]] = None,
        witness_types: Optional[list[str]] = None,
        min_confidence: float = 0.0,
    ) -> list[WitnessSummary]:
        """Get recent witness summaries.

        Args:
            hours: Look back this many hours
            symbols: Filter by symbols (None = all)
            witness_types: Filter by witness types (None = all)
            min_confidence: Minimum confidence threshold (0.0-1.0)

        Returns:
            List of WitnessSummary objects
        """
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        summaries = await self.storage.get_witness_summaries(
            since=since,
            symbols=symbols,
            witness_types=witness_types,
            min_confidence=min_confidence,
        )

        return summaries

    async def analyze_consensus(
        self, messages: list[ForumMessage]
    ) -> dict[str, Any]:
        """Analyze sentiment consensus from messages.

        Args:
            messages: List of ForumMessage objects to analyze

        Returns:
            Dict with consensus analysis:
            {
                "direction": "bullish" | "bearish" | "neutral",
                "agreement_pct": 0.0-1.0,
                "strongest_message_id": UUID,
                "message_count": int
            }
        """
        if not messages:
            return {
                "direction": "neutral",
                "agreement_pct": 0.0,
                "strongest_message_id": None,
                "message_count": 0,
            }

        # Simple keyword-based sentiment analysis
        # In production, this could use LLM-based analysis
        bullish_keywords = ["bullish", "long", "buy", "resistance", "breakout", "uptrend"]
        bearish_keywords = ["bearish", "short", "sell", "support", "correction", "downtrend"]

        bullish_count = 0
        bearish_count = 0
        strongest_message = messages[0]
        max_strength = 0

        for msg in messages:
            content_lower = msg.content.lower()

            # Count sentiment indicators
            bullish_score = sum(1 for kw in bullish_keywords if kw in content_lower)
            bearish_score = sum(1 for kw in bearish_keywords if kw in content_lower)

            if bullish_score > bearish_score:
                bullish_count += 1
                if bullish_score > max_strength:
                    max_strength = bullish_score
                    strongest_message = msg
            elif bearish_score > bullish_score:
                bearish_count += 1
                if bearish_score > max_strength:
                    max_strength = bearish_score
                    strongest_message = msg

        total = bullish_count + bearish_count
        if total == 0:
            direction = "neutral"
            agreement_pct = 0.0
        elif bullish_count > bearish_count:
            direction = "bullish"
            agreement_pct = bullish_count / total
        elif bearish_count > bullish_count:
            direction = "bearish"
            agreement_pct = bearish_count / total
        else:
            direction = "neutral"
            agreement_pct = 0.5

        return {
            "direction": direction,
            "agreement_pct": agreement_pct,
            "strongest_message_id": strongest_message.id,
            "message_count": len(messages),
        }
