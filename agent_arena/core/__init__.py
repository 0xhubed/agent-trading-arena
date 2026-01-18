"""Core components for Agent Arena."""

from agent_arena.core.agent import BaseAgent
from agent_arena.core.arena import TradingArena
from agent_arena.core.config import CompetitionConfig
from agent_arena.core.models import Decision, Portfolio, Position, Side, Trade
from agent_arena.core.runner import CompetitionRunner

__all__ = [
    "BaseAgent",
    "CompetitionConfig",
    "CompetitionRunner",
    "Decision",
    "Portfolio",
    "Position",
    "Side",
    "Trade",
    "TradingArena",
]
