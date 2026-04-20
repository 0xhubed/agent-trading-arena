"""Integration tests for discussion agents."""

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from agent_arena.forum.agents.market_analyst import MarketAnalystAgent
from agent_arena.forum.agents.contrarian import ContrarianAgent
from agent_arena.forum.models import ForumMessage


@pytest.fixture
def mock_forum():
    """Create a mock ForumService."""
    forum = AsyncMock()
    forum.post_message = AsyncMock(return_value=uuid4())
    forum.get_recent_messages = AsyncMock(return_value=[])
    forum.analyze_consensus = AsyncMock(
        return_value={
            "direction": "neutral",
            "agreement_pct": 0.0,
            "strongest_message_id": None,
            "message_count": 0,
        }
    )
    return forum


@pytest.fixture
def sample_context():
    """Create sample trading context."""
    return {
        "tick": 100,
        "timestamp": datetime.now(timezone.utc),
        "market": {
            "BTCUSDT": {
                "price": 48000.0,
                "change_24h": 2.5,  # 2.5% up
                "volume_24h": 1000000000,
                "funding_rate": 0.01,
            },
        },
        "candles": {
            "BTCUSDT": {
                "1h": [
                    {
                        "open": 47000,
                        "high": 48500,
                        "low": 46800,
                        "close": 48000,
                        "volume": 10000,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                    for _ in range(50)
                ],
            },
        },
    }


@pytest.mark.asyncio
async def test_market_analyst_posts_on_interval(mock_forum, sample_context):
    """Test MarketAnalyst posts every N ticks."""
    agent = MarketAnalystAgent(
        agent_id="test_analyst",
        config={"post_interval_ticks": 5},
        forum=mock_forum,
    )

    # Tick 0 - should not post (tick < interval)
    await agent.on_tick({**sample_context, "tick": 0})
    assert mock_forum.post_message.call_count == 0

    # Tick 5 - should post (tick >= interval)
    await agent.on_tick({**sample_context, "tick": 5})
    assert mock_forum.post_message.call_count == 1

    # Verify message content
    call_args = mock_forum.post_message.call_args
    assert call_args.kwargs["channel"] == "market"
    assert call_args.kwargs["agent_type"] == "discussion"
    assert "BTCUSDT" in call_args.kwargs["content"]


@pytest.mark.asyncio
async def test_market_analyst_posts_on_significant_move(mock_forum, sample_context):
    """Test MarketAnalyst posts on significant price movement."""
    agent = MarketAnalystAgent(
        agent_id="test_analyst",
        config={
            "post_interval_ticks": 10,
            "significant_move_threshold": 0.02,  # 2%
        },
        forum=mock_forum,
    )

    # First tick - establish baseline
    await agent.on_tick({**sample_context, "tick": 0})
    assert mock_forum.post_message.call_count == 0

    # Tick 3 - significant move (>2%)
    moved_context = {
        **sample_context,
        "tick": 3,
        "market": {
            "BTCUSDT": {
                **sample_context["market"]["BTCUSDT"],
                "price": 49000.0,  # ~2.1% move from 48000
            },
        },
    }
    await agent.on_tick(moved_context)
    assert mock_forum.post_message.call_count == 1  # Should override interval


@pytest.mark.asyncio
async def test_market_analyst_calculates_indicators(mock_forum, sample_context):
    """Test MarketAnalyst calculates and includes indicators."""
    agent = MarketAnalystAgent(
        agent_id="test_analyst",
        config={"post_interval_ticks": 1},
        forum=mock_forum,
    )

    await agent.on_tick(sample_context)

    call_args = mock_forum.post_message.call_args
    content = call_args.kwargs["content"]

    # Should include technical indicators
    assert "RSI" in content or "rsi" in content.lower()
    assert "SMA" in content or "sma" in content.lower()

    # Should include metadata
    metadata = call_args.kwargs["metadata"]
    assert "tick" in metadata
    assert "indicators" in metadata


@pytest.mark.asyncio
async def test_contrarian_challenges_consensus(mock_forum, sample_context):
    """Test Contrarian challenges strong consensus."""
    # Setup forum to return bullish consensus
    mock_forum.analyze_consensus = AsyncMock(
        return_value={
            "direction": "bullish",
            "agreement_pct": 0.75,  # 75% agreement
            "strongest_message_id": uuid4(),
            "message_count": 10,
        }
    )

    # Create bullish messages
    mock_messages = [
        ForumMessage(
            id=uuid4(),
            channel="market",
            agent_id="analyst1",
            agent_name="Analyst",
            agent_type="discussion",
            content="Bullish breakout, buy signal",
            reply_to=None,
            metadata={},
            created_at=datetime.now(timezone.utc),
        )
        for _ in range(10)
    ]
    mock_forum.get_recent_messages = AsyncMock(return_value=mock_messages)

    agent = ContrarianAgent(
        agent_id="test_contrarian",
        config={
            "consensus_threshold": 0.70,  # 70% threshold
            "check_interval_ticks": 3,
        },
        forum=mock_forum,
    )

    # Tick 0 - should not challenge (first check)
    await agent.on_tick({**sample_context, "tick": 0})

    # Tick 3 - should challenge consensus
    context_with_funding = {
        **sample_context,
        "tick": 3,
        "market": {
            "BTCUSDT": {
                **sample_context["market"]["BTCUSDT"],
                "funding_rate": 0.04,  # High funding = counter-evidence
            },
        },
    }
    await agent.on_tick(context_with_funding)

    # Should have posted challenge
    assert mock_forum.post_message.call_count >= 1

    call_args = mock_forum.post_message.call_args
    assert call_args.kwargs["channel"] == "strategy"
    assert call_args.kwargs["metadata"]["contrarian_trigger"] is True
    assert "greedy" in call_args.kwargs["content"].lower() or "caution" in call_args.kwargs["content"].lower()


@pytest.mark.asyncio
async def test_contrarian_skips_weak_consensus(mock_forum, sample_context):
    """Test Contrarian skips challenging weak consensus."""
    # Setup forum to return weak consensus
    mock_forum.analyze_consensus = AsyncMock(
        return_value={
            "direction": "bullish",
            "agreement_pct": 0.55,  # Only 55% agreement
            "strongest_message_id": uuid4(),
            "message_count": 10,
        }
    )

    mock_forum.get_recent_messages = AsyncMock(return_value=[])

    agent = ContrarianAgent(
        agent_id="test_contrarian",
        config={"consensus_threshold": 0.70},
        forum=mock_forum,
    )

    await agent.on_tick({**sample_context, "tick": 10})

    # Should NOT challenge (below threshold)
    assert mock_forum.post_message.call_count == 0


@pytest.mark.asyncio
async def test_contrarian_rate_limits_challenges(mock_forum, sample_context):
    """Test Contrarian doesn't spam challenges."""
    # Setup strong consensus
    mock_forum.analyze_consensus = AsyncMock(
        return_value={
            "direction": "bullish",
            "agreement_pct": 0.80,
            "strongest_message_id": uuid4(),
            "message_count": 10,
        }
    )

    mock_messages = [
        ForumMessage(
            id=uuid4(),
            channel="market",
            agent_id="analyst1",
            agent_name="Analyst",
            agent_type="discussion",
            content="Bullish",
            reply_to=None,
            metadata={},
            created_at=datetime.now(timezone.utc),
        )
    ]
    mock_forum.get_recent_messages = AsyncMock(return_value=mock_messages)

    agent = ContrarianAgent(
        agent_id="test_contrarian",
        config={
            "consensus_threshold": 0.70,
            "check_interval_ticks": 3,
        },
        forum=mock_forum,
    )

    # Tick 3 - first challenge
    await agent.on_tick({**sample_context, "tick": 3})
    first_count = mock_forum.post_message.call_count

    # Tick 4 - should not challenge (too soon)
    await agent.on_tick({**sample_context, "tick": 4})
    assert mock_forum.post_message.call_count == first_count  # No new challenge

    # Tick 10 - should challenge again (enough time passed)
    await agent.on_tick({**sample_context, "tick": 10})
    assert mock_forum.post_message.call_count > first_count  # New challenge
