"""Discussion agent runner - manages forum agents alongside competition."""

from __future__ import annotations

import asyncio
import importlib
import logging
from typing import TYPE_CHECKING, Any, Optional

from agent_arena.forum.service import ForumService

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from agent_arena.storage.postgres import PostgresStorage


class DiscussionAgentRunner:
    """Manages discussion agents that post to the forum.

    Discussion agents run alongside trading agents but:
    - Don't manage portfolios
    - Don't make trading decisions
    - Focus on analysis and discussion
    """

    def __init__(
        self,
        storage: PostgresStorage,
        config: Optional[dict] = None,
    ):
        """Initialize discussion agent runner.

        Args:
            storage: PostgreSQL storage backend
            config: Runner configuration with:
                - discussion_agents: List of agent configs
        """
        self.storage = storage
        self.config = config or {}
        self.forum = ForumService(storage)
        self.agents = []
        self.running = False

    async def initialize(self) -> None:
        """Initialize discussion agents from config."""
        agent_configs = self.config.get("discussion_agents", [])

        for agent_config in agent_configs:
            agent = await self._create_agent(agent_config)
            if agent:
                self.agents.append(agent)

        if self.agents:
            logger.info("Initialized %d discussion agents", len(self.agents))

    async def _create_agent(self, config: dict) -> Optional[Any]:
        """Create a discussion agent from config.

        Args:
            config: Agent configuration dict with:
                - id: Agent ID
                - name: Display name
                - class: Python class path (e.g., agent_arena.forum.agents.MarketAnalystAgent)
                - config: Agent-specific config

        Returns:
            Agent instance or None if creation failed
        """
        try:
            agent_id = config["id"]
            agent_class_path = config["class"]

            # Import agent class
            module_path, class_name = agent_class_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            agent_class = getattr(module, class_name)

            # Create agent instance
            agent = agent_class(
                agent_id=agent_id,
                config=config.get("config", {}),
                forum=self.forum,
            )

            logger.info("Created %s (%s)", config.get("name", agent_id), class_name)
            return agent

        except Exception as e:
            logger.error("Failed to create agent %s: %s", config.get("id", "unknown"), e)
            return None

    async def on_tick(self, context: dict) -> None:
        """Run all discussion agents for current tick.

        Args:
            context: Trading context from competition runner
        """
        if not self.agents:
            return

        # Run agents concurrently (filter out None agents)
        tasks = [agent.on_tick(context) for agent in self.agents if agent is not None]

        if not tasks:
            return

        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error("Error running discussion agents: %s", e)

    async def on_stop(self) -> None:
        """Stop all discussion agents and clean up resources."""
        for agent in self.agents:
            if hasattr(agent, "on_stop"):
                try:
                    await agent.on_stop()
                except Exception as e:
                    logger.error("Error stopping agent %s: %s", getattr(agent, "name", "?"), e)

    def get_agent_count(self) -> int:
        """Get number of active discussion agents."""
        return len(self.agents)

    def get_agent_names(self) -> list[str]:
        """Get list of agent names."""
        return [
            getattr(agent, "name", f"Agent {i}") for i, agent in enumerate(self.agents)
        ]
