"""Together AI based agentic trader implementation."""

from typing import Any, Optional

from langchain_openai import ChatOpenAI

from agent_arena.agentic.base import AgenticTrader
from agent_arena.agentic.graph import create_trading_graph

# Model shortcuts for Together AI (tool-calling capable models)
TOGETHER_MODELS = {
    # GPT-OSS models - strong tool calling
    "gpt-oss-20b": "openai/gpt-oss-20b",  # $0.10/$0.30 - can run locally, good tools
    "gpt-oss-120b": "openai/gpt-oss-120b",  # $0.15/$0.60 - larger, better tools
    # GLM models
    "glm-4.5-air": "THUDM/GLM-4.5-Air",  # Good tool calling alternative
    # Llama models
    "llama-4-scout": "meta-llama/Llama-4-Scout-17B-16E-Instruct",
    "llama-4-maverick": "meta-llama/Llama-4-Maverick-17B-128E-Instruct-FP8",
    "llama-3.3-70b": "meta-llama/Llama-3.3-70B-Instruct-Turbo",
    # Qwen models
    "qwen3-235b": "Qwen/Qwen3-235B-A22B-Instruct-2507-tput",
    "qwen-2.5-72b": "Qwen/Qwen2.5-72B-Instruct-Turbo",
    # DeepSeek models
    "deepseek-v3": "deepseek-ai/DeepSeek-V3",
    "deepseek-v3.1": "deepseek-ai/DeepSeek-V3.1",
    "deepseek-r1": "deepseek-ai/DeepSeek-R1",
}


class AgenticTogetherTrader(AgenticTrader):
    """
    Agentic trader using Together AI's open-source models.

    Supports models with strong tool calling:
    - GPT-OSS 20B (default - can run locally, good tools)
    - GPT-OSS 120B (larger, better tools)
    - GLM 4.5 Air (alternative with good tool calling)
    - Llama 4 Scout/Maverick (fast and efficient)

    Uses LangGraph for ReAct-style reasoning with tool use.

    Example config:
        agents:
          - id: agentic_gpt
            name: "Agentic GPT-OSS"
            class: agent_arena.agents.agentic_together.AgenticTogetherTrader
            config:
              model: gpt-oss-20b
              max_iterations: 3
              character: "Data-driven trader analyzing multiple signals"
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        super().__init__(agent_id, name, config)

        config = config or {}

        # Resolve model shortcut (default to gpt-oss-20b - can run locally, good tools)
        model_input = config.get("model", "gpt-oss-20b")
        self.model = TOGETHER_MODELS.get(model_input, model_input)

        # Together AI uses OpenAI-compatible API
        self.base_url = config.get("base_url", "https://api.together.xyz/v1")

        # Default character
        if not self.character:
            self.character = config.get(
                "character",
                "An analytical agentic trader that systematically analyzes market conditions "
                "using technical indicators, risk calculations, and sentiment data. "
                "Makes data-driven decisions with proper position sizing.",
            )

    async def on_start(self) -> None:
        """Initialize Together AI LLM and graph."""
        import os

        api_key = os.environ.get("TOGETHER_API_KEY", "")

        # Use LangChain's OpenAI wrapper with Together's API
        self._llm = ChatOpenAI(
            model=self.model,
            temperature=self.temperature,
            max_tokens=1024,
            api_key=api_key,
            base_url=self.base_url,
        ).bind_tools(self.tools)

        # Create the trading graph
        self._graph = create_trading_graph(
            llm=self._llm,
            tools=self.tools,
            memory_store=self._memory_store,
        )
