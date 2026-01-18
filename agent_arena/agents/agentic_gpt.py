"""GPT-based agentic trader implementation."""

from typing import Optional

from langchain_openai import ChatOpenAI

from agent_arena.agentic.base import AgenticTrader
from agent_arena.agentic.graph import create_trading_graph


class AgenticGPTTrader(AgenticTrader):
    """
    Fully agentic GPT-based trader.

    Combines GPT-5.1's strong reasoning with systematic tool-based analysis.
    Enforces trading discipline through mandatory tool usage.

    Uses LangGraph to orchestrate:
    - Trade Validation for discipline (required before trading)
    - Reflection for learning from mistakes
    - Technical Analysis for indicators (RSI, SMA, MACD, Bollinger)
    - Multi-Timeframe Analysis for trend confirmation
    - Portfolio Risk Analysis for position sizing
    - Trade History for learning from past decisions
    - Market Search for sentiment and Fear & Greed Index

    Maintains persistent memory across sessions for continuous learning.

    Example config:
        agents:
          - id: agentic_gpt
            name: "Agentic GPT Momentum"
            class: agent_arena.agents.agentic_gpt.AgenticGPTTrader
            config:
              model: gpt-5.1
              max_iterations: 3
              character: |
                Aggressive trend follower with discipline.
                RULES:
                1. ALWAYS call validate_trade before any open/close
                2. ALWAYS call technical_analysis for entry signals
                3. Ride trends - don't close winners early
                4. Scale into strength, cut losers quickly
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        super().__init__(agent_id, name, config)

        config = config or {}

        # Default to GPT-5.1 (best performer in testing)
        self.model = config.get("model", "gpt-5.1")

        # OpenAI API base URL
        self.base_url = config.get("base_url", "https://api.openai.com/v1")

        # Default character emphasizing momentum + discipline
        if not self.character:
            self.character = config.get(
                "character",
                "An aggressive momentum trader with strict discipline. "
                "Uses tools systematically to validate every trade. "
                "Rides winning trends and cuts losers quickly. "
                "Scales into strength when trends align across timeframes. "
                "ALWAYS validates trades before execution.",
            )

    async def on_start(self) -> None:
        """Initialize OpenAI LLM and graph."""
        import os

        api_key = os.environ.get("OPENAI_API_KEY", "")

        # Use LangChain's OpenAI wrapper
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
