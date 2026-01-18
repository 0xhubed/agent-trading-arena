"""Claude-based agentic trader implementation."""

from typing import Optional

from agent_arena.agentic.base import AgenticTrader


class AgenticClaudeTrader(AgenticTrader):
    """
    Fully agentic Claude-based trader.

    Uses LangGraph to orchestrate:
    - Technical Analysis for indicators (RSI, SMA, MACD, Bollinger)
    - Risk Calculator for position sizing and stop-loss levels
    - Trade History for learning from past decisions
    - Market Search for sentiment and Fear & Greed Index

    Maintains persistent memory across sessions for continuous learning.

    Example config:
        agents:
          - id: agentic_claude
            name: "Agentic Claude"
            class: agent_arena.agents.agentic_claude.AgenticClaudeTrader
            config:
              model: claude-sonnet-4-20250514
              max_iterations: 3
              character: "Methodical trader using tools for comprehensive analysis"
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        super().__init__(agent_id, name, config)

        # Set default character if not provided
        config = config or {}
        if not self.character:
            self.character = config.get(
                "character",
                "A methodical agentic trader that uses tools to gather information "
                "before making decisions. Balances technical analysis with risk management. "
                "Patient and disciplined, waiting for high-conviction setups.",
            )
