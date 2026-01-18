"""API module for Agent Arena dashboard."""

from agent_arena.api.app import app, create_app, start_competition, stop_competition
from agent_arena.api.websocket import ConnectionManager, manager

__all__ = [
    "app",
    "create_app",
    "start_competition",
    "stop_competition",
    "ConnectionManager",
    "manager",
]
