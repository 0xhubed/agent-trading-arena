"""Tests for ForumService."""

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from agent_arena.forum.service import ForumService
from agent_arena.forum.models import ForumMessage, WitnessSummary


@pytest.fixture
def mock_storage():
    """Create a mock PostgresStorage."""
    storage = AsyncMock()
    return storage


@pytest.fixture
def forum_service(mock_storage):
    """Create a ForumService with mock storage."""
    return ForumService(mock_storage)


@pytest.mark.asyncio
async def test_post_message_success(forum_service, mock_storage):
    """Test posting a message to the forum."""
    message_id = uuid4()
    mock_storage.save_forum_message.return_value = message_id

    result = await forum_service.post_message(
        channel="market",
        agent_id="test_agent",
        agent_name="Test Agent",
        agent_type="discussion",
        content="Test message",
        metadata={"tick": 100},
    )

    assert result == message_id
    mock_storage.save_forum_message.assert_called_once()
    call_args = mock_storage.save_forum_message.call_args
    assert call_args.kwargs["channel"] == "market"
    assert call_args.kwargs["content"] == "Test message"
    assert call_args.kwargs["metadata"]["tick"] == 100


@pytest.mark.asyncio
async def test_post_message_invalid_channel(forum_service):
    """Test posting to an invalid channel raises error."""
    with pytest.raises(ValueError, match="Invalid channel"):
        await forum_service.post_message(
            channel="invalid_channel",
            agent_id="test_agent",
            agent_name="Test Agent",
            agent_type="discussion",
            content="Test message",
        )


@pytest.mark.asyncio
async def test_get_recent_messages(forum_service, mock_storage):
    """Test retrieving recent messages."""
    test_messages = [
        ForumMessage(
            id=uuid4(),
            channel="market",
            agent_id="agent1",
            agent_name="Agent 1",
            agent_type="discussion",
            content="Message 1",
            reply_to=None,
            metadata={},
            created_at=datetime.now(timezone.utc),
        ),
        ForumMessage(
            id=uuid4(),
            channel="strategy",
            agent_id="agent2",
            agent_name="Agent 2",
            agent_type="discussion",
            content="Message 2",
            reply_to=None,
            metadata={},
            created_at=datetime.now(timezone.utc),
        ),
    ]
    mock_storage.get_forum_messages.return_value = test_messages

    messages = await forum_service.get_recent_messages(
        channels=["market", "strategy"],
        limit=50,
    )

    assert len(messages) == 2
    assert messages[0].content == "Message 1"
    assert messages[1].content == "Message 2"
    mock_storage.get_forum_messages.assert_called_once()


@pytest.mark.asyncio
async def test_analyze_consensus_bullish(forum_service):
    """Test consensus analysis with bullish messages."""
    messages = [
        ForumMessage(
            id=uuid4(),
            channel="market",
            agent_id="agent1",
            agent_name="Agent 1",
            agent_type="discussion",
            content="Strong bullish breakout above resistance, buy signal",
            reply_to=None,
            metadata={},
            created_at=datetime.now(timezone.utc),
        ),
        ForumMessage(
            id=uuid4(),
            channel="market",
            agent_id="agent2",
            agent_name="Agent 2",
            agent_type="discussion",
            content="Uptrend continues, long position looks good",
            reply_to=None,
            metadata={},
            created_at=datetime.now(timezone.utc),
        ),
        ForumMessage(
            id=uuid4(),
            channel="market",
            agent_id="agent3",
            agent_name="Agent 3",
            agent_type="discussion",
            content="Bearish divergence forming, sell pressure",
            reply_to=None,
            metadata={},
            created_at=datetime.now(timezone.utc),
        ),
    ]

    consensus = await forum_service.analyze_consensus(messages)

    assert consensus["direction"] == "bullish"
    assert consensus["agreement_pct"] > 0.5
    assert consensus["message_count"] == 3
    assert consensus["strongest_message_id"] is not None


@pytest.mark.asyncio
async def test_analyze_consensus_empty(forum_service):
    """Test consensus analysis with no messages."""
    consensus = await forum_service.analyze_consensus([])

    assert consensus["direction"] == "neutral"
    assert consensus["agreement_pct"] == 0.0
    assert consensus["message_count"] == 0
    assert consensus["strongest_message_id"] is None


@pytest.mark.asyncio
async def test_get_message_by_id(forum_service, mock_storage):
    """Test retrieving a specific message by ID."""
    message_id = uuid4()
    test_message = ForumMessage(
        id=message_id,
        channel="market",
        agent_id="agent1",
        agent_name="Agent 1",
        agent_type="discussion",
        content="Test message",
        reply_to=None,
        metadata={},
        created_at=datetime.now(timezone.utc),
    )
    mock_storage.get_forum_message_by_id.return_value = test_message

    message = await forum_service.get_message_by_id(message_id)

    assert message == test_message
    mock_storage.get_forum_message_by_id.assert_called_once_with(message_id)


@pytest.mark.asyncio
async def test_get_recent_witness_summaries(forum_service, mock_storage):
    """Test retrieving recent witness summaries."""
    test_summaries = [
        WitnessSummary(
            id=1,
            witness_type="exit_timing",
            insight="Test insight",
            confidence=0.75,
            symbols=["BTCUSDT"],
            timeframe="2-3h",
            based_on={"trades_analyzed": 10},
            metadata={},
            created_at=datetime.now(timezone.utc),
        ),
    ]
    mock_storage.get_witness_summaries.return_value = test_summaries

    summaries = await forum_service.get_recent_witness_summaries(
        hours=6,
        symbols=["BTCUSDT"],
        min_confidence=0.7,
    )

    assert len(summaries) == 1
    assert summaries[0].witness_type == "exit_timing"
    assert summaries[0].confidence == 0.75
    mock_storage.get_witness_summaries.assert_called_once()
