"""Tests for ForumAwareTradingAgent."""

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from agent_arena.agents.forum_aware_trader import ForumAwareTradingAgent
from agent_arena.forum.models import WitnessSummary
from agent_arena.core.models import Decision


@pytest.fixture
def mock_forum():
    """Create a mock ForumService."""
    forum = AsyncMock()
    forum.get_recent_witness_summaries = AsyncMock(return_value=[])
    forum.post_message = AsyncMock(return_value=uuid4())
    return forum


@pytest.fixture
def sample_witness_summaries():
    """Create sample witness summaries."""
    return [
        WitnessSummary(
            id=1,
            witness_type="exit_timing",
            insight="Take profits at 1.5% when resistance identified in trending_up regime",
            confidence=0.75,
            symbols=["BTCUSDT"],
            timeframe="2-3h",
            based_on={"trades_analyzed": 18, "forum_posts": 5, "win_rate": 0.74},
            metadata={},
            created_at=datetime.now(timezone.utc),
        ),
        WitnessSummary(
            id=2,
            witness_type="risk_warning",
            insight="High funding rates (>0.03%) preceded corrections 65% of the time",
            confidence=0.68,
            symbols=["BTCUSDT", "ETHUSDT"],
            timeframe="4-6h",
            based_on={"trades_analyzed": 23, "forum_posts": 8, "win_rate": 0.65},
            metadata={},
            created_at=datetime.now(timezone.utc),
        ),
    ]


@pytest.fixture
def sample_context():
    """Create sample trading context."""
    return {
        "tick": 100,
        "timestamp": datetime.now(timezone.utc),
        "market": {
            "BTCUSDT": {
                "price": 48000.0,
                "change_24h": 2.5,
                "volume_24h": 1000000000,
                "funding_rate": 0.01,
            },
        },
        "portfolio": {
            "equity": 10000,
            "available_margin": 5000,
            "positions": [],
        },
        "candles": {
            "BTCUSDT": {
                "1h": [],
            },
        },
    }


def test_forum_aware_trader_initialization():
    """Test ForumAwareTradingAgent initialization."""
    agent = ForumAwareTradingAgent(
        agent_id="test_forum_aware",
        name="Test Forum Aware",
        config={
            "witness_lookback_hours": 6,
            "min_witness_confidence": 0.7,
            "post_to_forum": True,
        },
    )

    assert agent.agent_id == "test_forum_aware"
    assert agent.witness_lookback_hours == 6
    assert agent.min_witness_confidence == 0.7
    assert agent.post_to_forum is True


def test_format_witness_for_context_empty():
    """Test witness formatting with empty list."""
    agent = ForumAwareTradingAgent(
        agent_id="test_agent",
        name="Test",
        config={},
    )

    formatted = agent._format_witness_for_context([])

    assert "No recent forum insights" in formatted


def test_format_witness_for_context_with_summaries(sample_witness_summaries):
    """Test witness formatting with summaries."""
    agent = ForumAwareTradingAgent(
        agent_id="test_agent",
        name="Test",
        config={},
    )

    formatted = agent._format_witness_for_context(sample_witness_summaries)

    # Should include witness header
    assert "FORUM WITNESS" in formatted

    # Should include both witness summaries
    assert "exit_timing" in formatted.lower()
    assert "risk_warning" in formatted.lower()

    # Should include confidence scores
    assert "75%" in formatted
    assert "68%" in formatted

    # Should include insights
    assert "Take profits" in formatted
    assert "High funding rates" in formatted


def test_format_trade_rationale():
    """Test trade rationale formatting."""
    agent = ForumAwareTradingAgent(
        agent_id="test_agent",
        name="Test",
        config={},
    )

    decision = Decision(
        action="open_long",
        symbol="BTCUSDT",
        size=0.1,
        leverage=5,
        confidence=0.8,
        reasoning="Strong momentum and positive technical indicators suggest upward movement.",
    )

    context = {"tick": 100}

    formatted = agent._format_trade_rationale(
        decision, context, []
    )

    # Should include decision action and symbol
    assert "OPEN_LONG" in formatted
    assert "BTCUSDT" in formatted

    # Should include size and leverage
    assert "0.1000" in formatted
    assert "5x" in formatted

    # Should include confidence
    assert "80%" in formatted

    # Should include reasoning (truncated)
    assert "Strong momentum" in formatted


def test_format_trade_rationale_with_witness(sample_witness_summaries):
    """Test trade rationale includes witness influence note."""
    agent = ForumAwareTradingAgent(
        agent_id="test_agent",
        name="Test",
        config={},
    )

    decision = Decision(
        action="open_long",
        symbol="BTCUSDT",
        size=0.1,
        leverage=5,
        confidence=0.8,
        reasoning="Test",
    )

    formatted = agent._format_trade_rationale(
        decision, {}, sample_witness_summaries
    )

    # Should note witness influence
    assert "Influenced by 2 forum witness" in formatted


@pytest.mark.asyncio
async def test_load_witness_summaries_success(
    mock_forum, sample_context, sample_witness_summaries
):
    """Test loading witness summaries successfully."""
    agent = ForumAwareTradingAgent(
        agent_id="test_agent",
        name="Test",
        config={
            "witness_lookback_hours": 6,
            "min_witness_confidence": 0.6,
        },
    )

    # Mock forum service
    agent._forum = mock_forum
    mock_forum.get_recent_witness_summaries.return_value = sample_witness_summaries

    # Load witness
    summaries = await agent._load_witness_summaries(sample_context)

    assert len(summaries) == 2
    assert summaries[0].witness_type == "exit_timing"
    assert summaries[1].witness_type == "risk_warning"

    # Verify call parameters
    mock_forum.get_recent_witness_summaries.assert_called_once()
    call_kwargs = mock_forum.get_recent_witness_summaries.call_args.kwargs
    assert call_kwargs["hours"] == 6
    assert call_kwargs["symbols"] == ["BTCUSDT"]
    assert call_kwargs["min_confidence"] == 0.6


@pytest.mark.asyncio
async def test_load_witness_summaries_handles_error(sample_context):
    """Test witness loading gracefully handles errors."""
    agent = ForumAwareTradingAgent(
        agent_id="test_agent",
        name="Test",
        config={},
    )

    # Mock forum service that raises error
    mock_forum = AsyncMock()
    mock_forum.get_recent_witness_summaries.side_effect = Exception("DB error")
    agent._forum = mock_forum

    # Should return empty list, not raise
    summaries = await agent._load_witness_summaries(sample_context)

    assert summaries == []
