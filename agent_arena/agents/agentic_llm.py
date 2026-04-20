"""Agentic LLM trader using any OpenAI-compatible API endpoint."""

import os
from typing import Optional

from langchain_openai import ChatOpenAI

from agent_arena.agentic.base import AgenticTrader
from agent_arena.agentic.graph import create_trading_graph
from agent_arena.agents.model_registry import resolve_model


class AgenticLLMTrader(AgenticTrader):
    """
    Agentic trader using any OpenAI-compatible API endpoint.

    Supports models with strong tool calling via LangGraph ReAct-style
    reasoning. Works with local inference (LiteLLM, Ollama) and cloud
    providers via configurable base_url.

    Example config:
        agents:
          - id: agentic_gptoss
            name: "Agentic GPT-OSS"
            class: agent_arena.agents.agentic_llm.AgenticLLMTrader
            config:
              model: gpt-oss-120b
              base_url: https://api.together.xyz/v1
              max_iterations: 3
              character: "Data-driven trader analyzing multiple signals"
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        config = config or {}
        super().__init__(agent_id, name, config)

        # Resolve model shortcut (default to gpt-oss-20b - can run locally, good tools)
        model_input = config.get("model", "gpt-oss-20b")
        self.model = resolve_model(model_input)

        # OpenAI-compatible endpoint (local or cloud)
        self.base_url = config.get("base_url", "https://api.together.xyz/v1")

        # API key: configurable env var name for local inference compatibility
        self.api_key_env = config.get("api_key_env", "TOGETHER_API_KEY")
        self.max_tokens = config.get("max_tokens", 1024)

        # Default character
        if not self.character:
            self.character = config.get(
                "character",
                "An analytical agentic trader that systematically analyzes market conditions "
                "using technical indicators, risk calculations, and sentiment data. "
                "Makes data-driven decisions with proper position sizing.",
            )

    async def on_start(self) -> None:
        """Initialize LLM and graph."""
        api_key = os.environ.get(self.api_key_env, "")

        # Use LangChain's OpenAI wrapper
        self._llm = ChatOpenAI(
            model=self.model,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            api_key=api_key,
            base_url=self.base_url,
            timeout=120.0,
            max_retries=3,
        ).bind_tools(self.tools)

        # Create the trading graph
        self._graph = create_trading_graph(
            llm=self._llm,
            tools=self.tools,
            memory_store=self._memory_store,
        )
