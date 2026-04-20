"""Learning Trader (LLM) - RAG-based learning agent using any OpenAI-compatible endpoint.

LLM version of LearningTraderAgent. Works with local inference (LiteLLM, Ollama)
and cloud providers via configurable base_url.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Optional

from langchain_openai import ChatOpenAI

from agent_arena.agentic.base import AgenticTrader
from agent_arena.agentic.graph import create_trading_graph
from agent_arena.agentic.tools import (
    AgentPerformanceTool,
    PatternMatcherTool,
    SimilarSituationsTool,
)
from agent_arena.agents.model_registry import resolve_model
from agent_arena.core.context_builder import ContextBuilder
from agent_arena.core.embeddings import get_embedding_service
from agent_arena.core.models import Decision


class LearningTraderLLM(AgenticTrader):
    """
    Learning agent using any OpenAI-compatible API endpoint.

    Combines RAG, pattern matching, and meta-learning with configurable
    LLM backends. Uses the same learning infrastructure as
    LearningTraderAgent but with any OpenAI-compatible model.

    Features:
    1. RAG (Retrieval-Augmented Generation):
       - Finds similar historical market situations
       - Retrieves past decisions and their outcomes
       - Uses vector similarity search in PostgreSQL

    2. Pattern Recognition:
       - Matches current conditions against learned patterns
       - Patterns discovered from successful/failed trades

    3. Meta-Learning:
       - Analyzes which agents perform best in current conditions
       - Learns from strategies of top performers

    Example config:
        config:
            model: gpt-oss-20b          # or gpt-oss-120b for better quality
            base_url: https://api.together.xyz/v1  # or local endpoint
            rag_weight: 0.4
            pattern_weight: 0.3
            meta_weight: 0.3
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        config = config or {}
        super().__init__(agent_id, name, config)

        # Model configuration - default to gpt-oss-20b (best value)
        model_input = config.get("model", "gpt-oss-20b")
        self.model = resolve_model(model_input)

        # API configuration
        self.base_url = config.get("base_url", "https://api.together.xyz/v1")
        self.api_key_env = config.get("api_key_env", "TOGETHER_API_KEY")

        # Learning weights (must sum to 1.0)
        self.rag_weight = config.get("rag_weight", 0.4)
        self.pattern_weight = config.get("pattern_weight", 0.3)
        self.meta_weight = config.get("meta_weight", 0.3)

        # Initialize embedding service (still uses OpenAI for embeddings)
        use_mock = config.get("use_mock_embeddings", False)
        self._embedder = get_embedding_service(use_mock=use_mock)

        # Initialize context builder
        self._context_builder = ContextBuilder(
            primary_symbol=config.get("primary_symbol", "PF_XBTUSD")
        )

        # Learning-specific tools (added to inherited tools)
        self._similar_situations_tool = SimilarSituationsTool()
        self._pattern_matcher_tool = PatternMatcherTool()
        self._agent_performance_tool = AgentPerformanceTool()

        # Add learning tools to the tool list
        self.tools.extend([
            self._similar_situations_tool,
            self._pattern_matcher_tool,
            self._agent_performance_tool,
        ])

    def set_storage(self, storage: Any) -> None:
        """Set storage and configure learning tools."""
        super().set_storage(storage)

        # Configure learning tools with dependencies
        self._similar_situations_tool.set_storage(storage)
        self._similar_situations_tool.set_embedder(self._embedder)
        self._similar_situations_tool.set_context_builder(self._context_builder)

        self._pattern_matcher_tool.set_storage(storage)
        self._agent_performance_tool.set_storage(storage)

    async def on_start(self) -> None:
        """Initialize LLM and graph."""
        api_key = os.environ.get(self.api_key_env, "")

        # Use LangChain's OpenAI wrapper
        self._llm = ChatOpenAI(
            model=self.model,
            temperature=self.temperature,
            max_tokens=2048,
            api_key=api_key,
            base_url=self.base_url,
        ).bind_tools(self.tools)

        # Create the trading graph
        self._graph = create_trading_graph(
            llm=self._llm,
            tools=self.tools,
            memory_store=self._memory_store,
            system_prompt=self._build_learning_system_prompt(),
        )

    def _build_learning_system_prompt(self) -> str:
        """Build system prompt emphasizing learning capabilities."""
        return f"""You are a sophisticated trading agent with LEARNING capabilities.

Unlike basic traders, you can:
1. **Find Similar Situations**: Use `find_similar_situations` to retrieve historical
   market conditions similar to now. Learn what worked and what didn't.

2. **Match Patterns**: Use `check_patterns` to see if current conditions match any
   learned trading patterns with known success rates.

3. **Analyze Top Performers**: Use `analyze_agent_performance` to see which agents
   perform best in the current market regime and learn from their strategies.

LEARNING STRATEGY:
- Before making a decision, ALWAYS use at least one learning tool
- Weight your confidence based on historical evidence:
  * Similar situations with good outcomes → increase confidence
  * Matching high-confidence patterns → follow their recommendations
  * Top performers in this regime → consider their typical strategies
- If learning tools show conflicting signals, be cautious (hold or reduce size)
- Document what you learned in your reasoning

DECISION WEIGHTS:
- RAG (similar situations): {self.rag_weight:.0%}
- Pattern matching: {self.pattern_weight:.0%}
- Meta-learning: {self.meta_weight:.0%}

CHARACTER:
{self.character or "A systematic learner who combines historical evidence with current analysis."}

Remember: The best traders learn from history. Use your tools to understand what
has worked before making your decision."""

    async def decide(self, context: dict) -> Decision:
        """Make a learning-enhanced trading decision."""
        # Ensure graph is created
        if self._graph is None:
            await self.on_start()

        # Build enriched context
        enriched_context = self._context_builder.build_context(
            market_data=context.get("market", {}),
            candles=context.get("candles", {}),
            portfolio=context.get("portfolio", {}),
            tick=context.get("tick", 0),
            timestamp=context.get("timestamp", datetime.now(timezone.utc)),
        )

        # Add enriched data to context for tools
        context["enriched"] = enriched_context
        context["regime"] = enriched_context.get("regime", "unknown")
        context["indicators"] = enriched_context.get("indicators", {})
        context["volatility_percentile"] = enriched_context.get("volatility_percentile", 50)

        # Set context on learning tools
        self._similar_situations_tool.set_context(enriched_context)
        self._pattern_matcher_tool.set_context(enriched_context)
        self._agent_performance_tool.set_context(enriched_context)

        # Get decision from parent class (runs the LangGraph loop)
        decision = await super().decide(context)

        # Add learning metadata
        decision.metadata["learning_agent"] = True
        decision.metadata["provider"] = "llm"
        decision.metadata["regime"] = enriched_context.get("regime")
        decision.metadata["volatility_pct"] = enriched_context.get("volatility_percentile")
        decision.metadata["weights"] = {
            "rag": self.rag_weight,
            "pattern": self.pattern_weight,
            "meta": self.meta_weight,
        }

        return decision
