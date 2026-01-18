"""WebSocket hub for real-time updates."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages WebSocket connections and broadcasts events."""

    def __init__(self):
        self.active_connections: list[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        """Accept a new WebSocket connection."""
        await websocket.accept()
        async with self._lock:
            self.active_connections.append(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        """Remove a WebSocket connection."""
        async with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)

    async def broadcast(self, event_type: str, data: Any) -> None:
        """Broadcast an event to all connected clients."""
        if not self.active_connections:
            return

        message = json.dumps({
            "type": event_type,
            "data": data,
        })

        async with self._lock:
            dead_connections = []
            for connection in self.active_connections:
                try:
                    await connection.send_text(message)
                except Exception as e:
                    # Log non-connection errors for debugging
                    if "disconnect" not in str(e).lower():
                        logger.warning(f"WebSocket send error: {e}")
                    dead_connections.append(connection)

            for conn in dead_connections:
                self.active_connections.remove(conn)

    @property
    def connection_count(self) -> int:
        """Return number of active connections."""
        return len(self.active_connections)


# Global connection manager
manager = ConnectionManager()


def create_event_emitter():
    """Create an event emitter function for the competition runner."""
    def emit(event_type: str, data: Any) -> None:
        """Emit an event to all connected WebSocket clients."""
        asyncio.create_task(manager.broadcast(event_type, data))
    return emit
