"""Base class for agentic traders using LangGraph."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from langchain_anthropic import ChatAnthropic

from agent_arena.agentic.graph import create_trading_graph
from agent_arena.agentic.memory.store import AgentMemoryStore
from agent_arena.agentic.state import AgentState
from agent_arena.agentic.tools.history import TradeHistoryTool
from agent_arena.agentic.tools.multi_tf import MultiTimeframeTool
from agent_arena.agentic.tools.portfolio_risk import PortfolioRiskTool
from agent_arena.agentic.tools.reflection import ReflectionTool
from agent_arena.agentic.tools.risk import RiskCalculatorTool
from agent_arena.agentic.tools.rules import TradeRulesTool
from agent_arena.agentic.tools.search import MarketSearchTool
from agent_arena.agentic.tools.technical import TechnicalAnalysisTool
from agent_arena.core.agent import BaseAgent
from agent_arena.core.models import Decision


class AgenticTrader(BaseAgent):
    """
    Base class for agentic traders using LangGraph.

    Implements ReAct-style reasoning:
    1. Think - Analyze situation, consider what information is needed
    2. Act - Call tools to gather information (TA, risk, history, sentiment)
    3. Observe - Process tool results
    4. Decide - Make trading decision

    Features:
    - Persistent memory across sessions (SQLite-backed)
    - Tool ecosystem for market analysis
    - Configurable reasoning iterations
    - Automatic fallback on errors

    Example config:
        config:
            model: claude-haiku-4-5-20251001
            max_iterations: 3
            character: "Methodical trader using tools for analysis"
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        super().__init__(agent_id, name, config)
        config = config or {}

        # Default to Haiku 4.5 for agentic (tools assist reasoning, so cheaper/faster model works)
        self.model = config.get("model", "claude-haiku-4-5-20251001")
        self.max_iterations = config.get("max_iterations", 3)
        self.character = config.get("character", "")
        self.temperature = config.get("temperature", 0.7)

        # Initialize tools - includes new discipline-enforcing tools
        self.tools = [
            # Analysis tools
            TechnicalAnalysisTool(),
            MultiTimeframeTool(),
            RiskCalculatorTool(),
            PortfolioRiskTool(),
            # Discipline tools
            TradeRulesTool(),
            ReflectionTool(),
            # History/sentiment tools
            TradeHistoryTool(),
            MarketSearchTool(),
        ]

        # Graph and memory will be created on first decide() or when storage is set
        self._graph: Optional[Any] = None
        self._memory_store: Optional[AgentMemoryStore] = None
        self._storage: Optional[Any] = None
        self._llm: Optional[Any] = None

    def set_storage(self, storage: Any) -> None:
        """
        Set storage for memory and history tools.

        Called by CompetitionRunner before first decide().
        """
        self._storage = storage
        self._memory_store = AgentMemoryStore(storage, self.agent_id)

        # Set storage on tools that need it
        for tool in self.tools:
            if hasattr(tool, "set_storage"):
                tool.set_storage(storage)

    async def on_start(self) -> None:
        """Initialize LLM and graph on competition start."""
        # Create LLM with tools bound
        self._llm = ChatAnthropic(
            model=self.model,
            temperature=self.temperature,
            max_tokens=1024,
        ).bind_tools(self.tools)

        # Create the trading graph
        self._graph = create_trading_graph(
            llm=self._llm,
            tools=self.tools,
            memory_store=self._memory_store,
        )

    async def decide(self, context: dict) -> Decision:
        """
        Make a trading decision using LangGraph.

        This runs the ReAct loop:
        think -> tool_call -> observe -> (repeat or decide)

        Args:
            context: Market data, portfolio, tick info

        Returns:
            Decision object with action, symbol, size, etc.
        """
        # Ensure graph is created
        if self._graph is None:
            await self.on_start()

        # Add agent_id to context for tools
        context["agent_id"] = self.agent_id

        # Retrieve relevant memories
        memories = []
        if self._memory_store:
            try:
                memories = await self._memory_store.retrieve_memories(limit=5)
            except Exception:
                pass  # Memory retrieval is optional

        # Build initial state
        initial_state: AgentState = {
            "context": context,
            "agent_id": self.agent_id,
            "messages": [],
            "tool_calls": [],
            "tool_results": [],
            "thoughts": [],
            "iteration": 0,
            "max_iterations": self.max_iterations,
            "should_continue": True,
            "decision": None,
            "memories_retrieved": memories,
        }

        try:
            # Run the graph
            start_time = datetime.now(timezone.utc)
            result = await self._graph.ainvoke(initial_state)
            latency = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

            # Extract decision
            decision_dict = result.get("decision", {})
            if not decision_dict:
                decision_dict = {"action": "hold", "reasoning": "No decision produced"}

            # Auto-calculate size when model chose to open but omitted size.
            # Agentic agents go through multi-step reasoning to reach an open
            # decision — silently dropping it due to missing size is wrong.
            action = decision_dict.get("action", "hold")
            size_val = decision_dict.get("size")
            if action in ("open_long", "open_short") and not size_val:
                symbol = decision_dict.get("symbol")
                market = context.get("market", {})
                portfolio = context.get("portfolio", {})
                price = float(market.get(symbol, {}).get("price", 0))
                equity = float(portfolio.get("equity", 10000))
                if price > 0 and equity > 0:
                    conf = decision_dict.get("confidence", 0.5)
                    # 10% of equity, scaled by confidence
                    notional = equity * 0.10 * max(conf, 0.3)
                    size_val = notional / price
                    decision_dict["size"] = size_val

            # Create Decision object
            confidence = min(
                decision_dict.get("confidence", 0.5), self.max_confidence
            )
            decision = Decision(
                action=decision_dict.get("action", "hold"),
                symbol=decision_dict.get("symbol"),
                size=Decimal(str(decision_dict["size"])) if decision_dict.get("size") else None,
                leverage=decision_dict.get("leverage", 1),
                confidence=confidence,
                reasoning=decision_dict.get("reasoning", ""),
                metadata={
                    "model": self.model,
                    "agent_type": "agentic",
                    "iterations": result.get("iteration", 0),
                    "tool_calls_count": len(result.get("tool_results", [])),
                    "thoughts": result.get("thoughts", []),
                    "latency_ms": latency,
                    "auto_sized": not bool(
                        result.get("decision", {}).get("size")
                    ),
                },
            )

            # Store episode in memory
            if self._memory_store:
                try:
                    await self._memory_store.store_episode(
                        tick=context.get("tick", 0),
                        thoughts=result.get("thoughts", []),
                        tool_calls=[{"result": r[:200]} for r in result.get("tool_results", [])],
                        decision=decision_dict,
                    )
                except Exception:
                    pass  # Memory storage is optional

            return decision

        except Exception as e:
            # Fallback on error
            return Decision(
                action="hold",
                reasoning=f"Agentic loop error: {str(e)}",
                confidence=0.0,
                metadata={
                    "error": str(e),
                    "agent_type": "agentic",
                    "model": self.model,
                },
            )

    async def on_stop(self) -> None:
        """Cleanup on competition end."""
        # Could summarize session here if needed
        pass
