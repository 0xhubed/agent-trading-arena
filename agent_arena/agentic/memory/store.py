"""SQLite-backed memory store for agentic traders."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional


class AgentMemoryStore:
    """
    Persistent memory storage for agents.

    Memory types:
    - observation: Market observations, tool results
    - reflection: Agent's analysis and reasoning
    - insight: Key learnings and patterns discovered
    - episode: Complete decision episodes (think -> act -> observe -> decide)

    Memories persist across sessions, allowing agents to learn over time.
    """

    def __init__(self, storage: Any, agent_id: str):
        """
        Initialize memory store.

        Args:
            storage: SQLiteStorage instance with initialized connection
            agent_id: Unique identifier for this agent
        """
        self._storage = storage
        self.agent_id = agent_id

    async def store_memory(
        self,
        memory_type: str,
        content: str,
        importance: float = 0.5,
        tick: Optional[int] = None,
        metadata: Optional[dict] = None,
    ) -> int:
        """
        Store a memory.

        Args:
            memory_type: Type of memory (observation, reflection, insight, episode)
            content: The memory content (text or JSON string)
            importance: Importance score 0.0-1.0 (higher = more important)
            tick: Associated tick number (optional)
            metadata: Additional metadata (optional)

        Returns:
            Memory ID
        """
        if not self._storage._connection:
            await self._storage.initialize()

        conn = self._storage._connection
        cursor = await conn.execute(
            """
            INSERT INTO agent_memories
            (agent_id, memory_type, content, importance, tick, timestamp, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.agent_id,
                memory_type,
                content,
                importance,
                tick,
                datetime.now(timezone.utc).isoformat(),
                json.dumps(metadata) if metadata else None,
            ),
        )
        await conn.commit()
        return cursor.lastrowid

    async def retrieve_memories(
        self,
        memory_type: Optional[str] = None,
        limit: int = 20,
        min_importance: float = 0.0,
    ) -> list[dict]:
        """
        Retrieve memories, optionally filtered by type and importance.

        Args:
            memory_type: Filter by memory type (optional)
            limit: Maximum number of memories to return
            min_importance: Minimum importance threshold

        Returns:
            List of memory dicts, most recent first
        """
        if not self._storage._connection:
            await self._storage.initialize()

        conn = self._storage._connection

        query = """
            SELECT id, agent_id, memory_type, content, importance, tick, timestamp, metadata
            FROM agent_memories
            WHERE agent_id = ? AND importance >= ?
        """
        params: list = [self.agent_id, min_importance]

        if memory_type:
            query += " AND memory_type = ?"
            params.append(memory_type)

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        cursor = await conn.execute(query, params)
        rows = await cursor.fetchall()
        columns = ["id", "agent_id", "memory_type", "content", "importance", "tick", "timestamp", "metadata"]

        memories = []
        for row in rows:
            m = dict(zip(columns, row))
            if m.get("metadata"):
                try:
                    m["metadata"] = json.loads(m["metadata"])
                except json.JSONDecodeError:
                    pass
            memories.append(m)

        return memories

    async def get_recent_context(self, ticks: int = 5, max_chars: int = 2000) -> str:
        """
        Get recent memories formatted as context for LLM.

        Args:
            ticks: Number of recent ticks to consider
            max_chars: Maximum characters to return

        Returns:
            Formatted string of recent memories
        """
        memories = await self.retrieve_memories(limit=ticks * 3)

        if not memories:
            return "No previous memories available."

        formatted = []
        total_chars = 0

        for m in memories[:15]:  # Max 15 memories
            content = m["content"]
            if len(content) > 200:
                content = content[:197] + "..."

            line = f"[{m['memory_type'].upper()}] {content}"
            if total_chars + len(line) > max_chars:
                break

            formatted.append(line)
            total_chars += len(line)

        return "\n".join(formatted)

    async def store_episode(
        self,
        tick: int,
        thoughts: list[str],
        tool_calls: list[dict],
        decision: dict,
        outcome: Optional[dict] = None,
    ) -> int:
        """
        Store a complete decision episode.

        Args:
            tick: Tick number when decision was made
            thoughts: List of reasoning steps
            tool_calls: List of tool calls made
            decision: Final decision dict
            outcome: Trade outcome (optional, can be updated later)

        Returns:
            Memory ID
        """
        episode = {
            "tick": tick,
            "thoughts": thoughts,
            "tool_calls": tool_calls,
            "decision": decision,
            "outcome": outcome,
        }

        # Calculate importance based on action
        importance = 0.5
        action = decision.get("action", "hold")
        if action != "hold":
            importance = 0.7
        if outcome and outcome.get("trade"):
            importance = 0.9

        return await self.store_memory(
            memory_type="episode",
            content=json.dumps(episode),
            importance=importance,
            tick=tick,
            metadata={"action": action, "symbol": decision.get("symbol")},
        )

    async def store_observation(
        self,
        content: str,
        tick: Optional[int] = None,
        importance: float = 0.3,
    ) -> int:
        """Store a market observation."""
        return await self.store_memory(
            memory_type="observation",
            content=content,
            importance=importance,
            tick=tick,
        )

    async def store_reflection(
        self,
        content: str,
        tick: Optional[int] = None,
        importance: float = 0.6,
    ) -> int:
        """Store a reflection or analysis."""
        return await self.store_memory(
            memory_type="reflection",
            content=content,
            importance=importance,
            tick=tick,
        )

    async def store_insight(
        self,
        content: str,
        tick: Optional[int] = None,
        importance: float = 0.8,
    ) -> int:
        """Store a key insight or learned pattern."""
        return await self.store_memory(
            memory_type="insight",
            content=content,
            importance=importance,
            tick=tick,
        )

    async def get_insights(self, limit: int = 10) -> list[str]:
        """Get high-importance insights."""
        memories = await self.retrieve_memories(
            memory_type="insight",
            limit=limit,
            min_importance=0.7,
        )
        return [m["content"] for m in memories]

    async def get_episode_count(self) -> int:
        """Get total number of episodes stored."""
        if not self._storage._connection:
            await self._storage.initialize()

        conn = self._storage._connection
        cursor = await conn.execute(
            "SELECT COUNT(*) FROM agent_memories WHERE agent_id = ? AND memory_type = 'episode'",
            (self.agent_id,),
        )
        row = await cursor.fetchone()
        return row[0] if row else 0

    async def clear_memories(self, memory_type: Optional[str] = None) -> int:
        """
        Clear memories (use with caution).

        Args:
            memory_type: If provided, only clear this type. Otherwise clear all.

        Returns:
            Number of memories deleted
        """
        if not self._storage._connection:
            await self._storage.initialize()

        conn = self._storage._connection

        if memory_type:
            cursor = await conn.execute(
                "DELETE FROM agent_memories WHERE agent_id = ? AND memory_type = ?",
                (self.agent_id, memory_type),
            )
        else:
            cursor = await conn.execute(
                "DELETE FROM agent_memories WHERE agent_id = ?",
                (self.agent_id,),
            )

        await conn.commit()
        return cursor.rowcount
