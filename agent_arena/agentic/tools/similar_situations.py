"""Tool for finding similar historical situations using RAG."""

from __future__ import annotations

from typing import Any, Optional, Type

from pydantic import BaseModel, Field

from .base import TradingTool


class SimilarSituationsInput(BaseModel):
    """Input schema for similar situations tool."""

    limit: int = Field(
        default=5,
        description="Number of similar situations to return (1-10)"
    )
    min_outcome_score: Optional[float] = Field(
        default=None,
        description="Minimum outcome score filter (-1 to 1). Use 0.3 for profitable trades only."
    )
    regime: Optional[str] = Field(
        default=None,
        description="Filter by market regime (trending_up, trending_down, ranging, volatile)"
    )
    symbol: Optional[str] = Field(
        default=None,
        description="Filter by trading symbol (e.g., BTCUSDT)"
    )


class SimilarSituationsTool(TradingTool):
    """
    Find similar historical market situations and their outcomes.

    Uses vector similarity search to find past market conditions
    similar to the current state, returning what decisions were made
    and their outcomes. Essential for learning from experience.
    """

    name: str = "find_similar_situations"
    description: str = """Search for historical situations similar to current market conditions.
Returns past decisions made in similar conditions and their outcomes.
Use this to learn from historical patterns before making a decision.

Returns list of similar situations with:
- similarity: How similar to current (0-1)
- decision: What action was taken (action, symbol, confidence, reasoning)
- outcome: What happened (P&L, score, exit_reason)
- regime: Market regime at that time"""

    args_schema: Type[BaseModel] = SimilarSituationsInput

    # Dependencies injected at runtime
    _embedder: Any = None
    _context_builder: Any = None

    def set_embedder(self, embedder: Any) -> None:
        """Set the embedding service for vector search."""
        self._embedder = embedder

    def set_context_builder(self, context_builder: Any) -> None:
        """Set the context builder for generating embeddings."""
        self._context_builder = context_builder

    def _run(
        self,
        limit: int = 5,
        min_outcome_score: Optional[float] = None,
        regime: Optional[str] = None,
        symbol: Optional[str] = None,
    ) -> str:
        """Synchronous fallback - returns guidance to use async."""
        return (
            "Similar situations search requires async execution. "
            "Use the async version of this tool."
        )

    async def _arun(
        self,
        limit: int = 5,
        min_outcome_score: Optional[float] = None,
        regime: Optional[str] = None,
        symbol: Optional[str] = None,
    ) -> str:
        """Find similar situations to current context."""
        if not self._storage:
            return "Error: Storage not available for similarity search."

        if not self._embedder:
            return "Error: Embedding service not configured."

        if not self._context:
            return "Error: No current market context available."

        # Limit to reasonable range
        limit = max(1, min(10, limit))

        try:
            # Build enriched context if we have a context builder
            if self._context_builder:
                enriched = self._context_builder.build_context(
                    market_data=self._context.get("market", {}),
                    candles=self._context.get("candles", {}),
                    portfolio=self._context.get("portfolio", {}),
                    tick=self._context.get("tick", 0),
                    timestamp=self._context.get("timestamp"),
                )
            else:
                enriched = self._context

            # Generate embedding for current context
            embedding = await self._embedder.embed_context(enriched)

            # Search for similar contexts
            results = await self._storage.find_similar_contexts(
                embedding=embedding,
                limit=limit,
                min_outcome_score=min_outcome_score,
                regime=regime,
                symbol=symbol,
            )

            if not results:
                return "No similar historical situations found matching the criteria."

            # Format results for agent consumption
            output_parts = [
                f"Found {len(results)} similar historical situations:\n"
            ]

            for i, r in enumerate(results, 1):
                similarity = r.get("similarity", 0)
                action = r.get("action", "unknown")
                sym = r.get("symbol", "N/A")
                confidence = r.get("confidence", 0)
                reasoning = r.get("reasoning", "")
                pnl = r.get("realized_pnl")
                score = r.get("outcome_score")
                exit_reason = r.get("exit_reason", "unknown")
                ctx_regime = r.get("regime", "unknown")
                timestamp = r.get("timestamp", "")

                # Truncate reasoning if too long
                if reasoning and len(reasoning) > 150:
                    reasoning = reasoning[:150] + "..."

                was_profitable = pnl and pnl > 0

                output_parts.append(
                    f"\n{i}. [{similarity*100:.0f}% similar] {timestamp}\n"
                    f"   Regime: {ctx_regime}\n"
                    f"   Decision: {action} {sym} (confidence: {confidence:.0%})\n"
                    f"   Outcome: {'PROFIT' if was_profitable else 'LOSS'} "
                    f"${pnl:+.2f} (score: {score:.2f})\n"
                    f"   Exit: {exit_reason}\n"
                    f"   Reasoning: \"{reasoning}\""
                )

            # Add summary insights
            profitable_count = sum(1 for r in results if r.get("realized_pnl", 0) > 0)
            avg_score = (
                sum(r.get("outcome_score", 0) for r in results) / len(results)
                if results else 0
            )

            output_parts.append(
                f"\n\n📊 Summary: {profitable_count}/{len(results)} were profitable, "
                f"avg outcome score: {avg_score:.2f}"
            )

            # Add regime-specific insight
            current_regime = enriched.get("regime", "unknown")
            same_regime = [r for r in results if r.get("regime") == current_regime]
            if same_regime:
                regime_profitable = sum(
                    1 for r in same_regime if r.get("realized_pnl", 0) > 0
                )
                output_parts.append(
                    f"\n   In {current_regime} regime specifically: "
                    f"{regime_profitable}/{len(same_regime)} profitable"
                )

            return "\n".join(output_parts)

        except Exception as e:
            return f"Error searching for similar situations: {str(e)}"
