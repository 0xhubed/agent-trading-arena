"""Learning Trader Agent - combines RAG, reflection, and meta-learning."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from langchain_anthropic import ChatAnthropic

from agent_arena.agentic.base import AgenticTrader
from agent_arena.agentic.graph import create_trading_graph
from agent_arena.agentic.tools import (
    AgentPerformanceTool,
    PatternMatcherTool,
    SimilarSituationsTool,
)
from agent_arena.core.context_builder import ContextBuilder
from agent_arena.core.embeddings import get_embedding_service
from agent_arena.core.models import Decision


class LearningTraderAgent(AgenticTrader):
    """
    Agent that learns from historical decisions using:

    1. RAG (Retrieval-Augmented Generation):
       - Finds similar historical market situations
       - Retrieves past decisions and their outcomes
       - Uses vector similarity search in PostgreSQL

    2. Pattern Recognition:
       - Matches current conditions against learned patterns
       - Patterns discovered from successful/failed trades
       - Includes entry signals, exit signals, risk rules

    3. Meta-Learning:
       - Analyzes which agents perform best in current conditions
       - Learns from strategies of top performers
       - Adapts behavior based on market regime

    Example config:
        config:
            model: claude-sonnet-4-20250514
            rag_weight: 0.4        # Weight for similar situation insights
            pattern_weight: 0.3    # Weight for pattern matches
            meta_weight: 0.3       # Weight for meta-learning
            use_mock_embeddings: false  # Use real embeddings
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        super().__init__(agent_id, name, config)
        config = config or {}

        # Use Sonnet by default for learning agent (needs stronger reasoning)
        self.model = config.get("model", "claude-sonnet-4-20250514")

        # Learning weights (must sum to 1.0)
        self.rag_weight = config.get("rag_weight", 0.4)
        self.pattern_weight = config.get("pattern_weight", 0.3)
        self.meta_weight = config.get("meta_weight", 0.3)

        # Initialize embedding service
        use_mock = config.get("use_mock_embeddings", False)
        self._embedder = get_embedding_service(use_mock=use_mock)

        # Initialize context builder
        self._context_builder = ContextBuilder(
            primary_symbol=config.get("primary_symbol", "BTCUSDT")
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
        """Initialize with learning-enhanced prompt."""
        # Create LLM with tools bound
        self._llm = ChatAnthropic(
            model=self.model,
            temperature=self.temperature,
            max_tokens=2048,  # Larger for learning context
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
            timestamp=context.get("timestamp", datetime.utcnow()),
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
        decision.metadata["regime"] = enriched_context.get("regime")
        decision.metadata["volatility_pct"] = enriched_context.get("volatility_percentile")
        decision.metadata["weights"] = {
            "rag": self.rag_weight,
            "pattern": self.pattern_weight,
            "meta": self.meta_weight,
        }

        # Store enriched context for future learning
        if self._storage and decision.action not in ("hold",):
            try:
                # Context will be saved by runner, but we ensure it's enriched
                context["_enriched_for_storage"] = enriched_context
            except Exception:
                pass

        return decision

    async def gather_learning_insights(
        self,
        context: dict,
    ) -> dict:
        """
        Gather all learning insights before decision.

        This can be called separately to pre-fetch learning data.

        Args:
            context: Enriched market context.

        Returns:
            Dict with learning insights from all three sources.
        """
        insights = {
            "similar_situations": [],
            "matching_patterns": [],
            "top_agents": [],
        }

        try:
            # Find similar historical situations
            similar = await self._similar_situations_tool._arun(
                limit=5,
                min_outcome_score=-0.3,
            )
            insights["similar_situations_raw"] = similar
        except Exception as e:
            insights["similar_error"] = str(e)

        try:
            # Check for matching patterns
            patterns = await self._pattern_matcher_tool._arun(
                min_confidence=0.5,
            )
            insights["patterns_raw"] = patterns
        except Exception as e:
            insights["patterns_error"] = str(e)

        try:
            # Get top performer analysis
            regime = context.get("regime", "unknown")
            if regime != "unknown":
                agents = await self._agent_performance_tool._arun(
                    regime=regime,
                    min_trades=5,
                )
                insights["top_agents_raw"] = agents
        except Exception as e:
            insights["agents_error"] = str(e)

        return insights

    async def on_trade_closed(
        self,
        trade: dict,
        context: dict,
        outcome: Optional[dict] = None,
    ) -> None:
        """
        Called when a trade is closed. Opportunity for reflection learning.

        Args:
            trade: Trade details (symbol, pnl, etc.).
            context: Market context when trade closed.
            outcome: Outcome metrics if available.
        """
        if not self._storage:
            return

        try:
            pnl = trade.get("realized_pnl", 0)
            symbol = trade.get("symbol", "")
            regime = context.get("regime", "unknown")

            # Update regime performance
            await self._storage.update_regime_performance(
                agent_id=self.agent_id,
                regime=regime,
                symbol=symbol,
                trade_result={"pnl": pnl},
            )

            # Log learning event
            event_type = "trade_closed_profit" if pnl > 0 else "trade_closed_loss"
            await self._storage.save_learning_event(
                agent_id=self.agent_id,
                event_type=event_type,
                summary=f"Closed {symbol} trade with ${pnl:+.2f} in {regime} regime",
                details={
                    "symbol": symbol,
                    "pnl": float(pnl),
                    "regime": regime,
                    "outcome": outcome,
                },
            )

        except Exception:
            pass  # Non-critical

    async def discover_pattern(
        self,
        pattern_type: str,
        conditions: dict,
        description: str,
        supporting_decisions: list[int],
        success_rate: float,
    ) -> Optional[int]:
        """
        Save a newly discovered pattern.

        Args:
            pattern_type: Type of pattern (entry_signal, exit_signal, etc.).
            conditions: Dict of conditions that define the pattern.
            description: Human-readable description of the pattern.
            supporting_decisions: List of decision IDs that support this pattern.
            success_rate: Historical success rate (0-1).

        Returns:
            Pattern ID if saved, None on failure.
        """
        if not self._storage:
            return None

        try:
            confidence = min(0.95, 0.5 + (len(supporting_decisions) * 0.05))

            pattern_id = await self._storage.save_learned_pattern({
                "agent_id": self.agent_id,
                "pattern_type": pattern_type,
                "pattern_description": description,
                "conditions": conditions,
                "supporting_decisions": supporting_decisions,
                "success_rate": success_rate,
                "sample_size": len(supporting_decisions),
                "confidence": confidence,
            })

            # Log the discovery
            await self._storage.save_learning_event(
                agent_id=self.agent_id,
                event_type="pattern_discovered",
                summary=f"Discovered new {pattern_type}: {description[:50]}",
                details={
                    "pattern_id": pattern_id,
                    "conditions": conditions,
                    "success_rate": success_rate,
                    "sample_size": len(supporting_decisions),
                },
            )

            return pattern_id

        except Exception:
            return None
