"""Embedding generation for RAG retrieval in learning agents."""

from __future__ import annotations

import os
from typing import Optional

try:
    import openai

    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    openai = None


class EmbeddingService:
    """
    Generate embeddings for context vectors.

    Uses OpenAI's text-embedding-3-small model by default.
    Embeddings are used for similarity search in PostgreSQL with pgvector.
    """

    def __init__(
        self,
        model: str = "text-embedding-3-small",
        api_key: Optional[str] = None,
        dimensions: int = 1536,
    ):
        """
        Initialize embedding service.

        Args:
            model: OpenAI embedding model name.
            api_key: OpenAI API key (defaults to OPENAI_API_KEY env var).
            dimensions: Embedding dimension (1536 for text-embedding-3-small).
        """
        if not OPENAI_AVAILABLE:
            raise ImportError(
                "OpenAI package required for embeddings. "
                "Install with: pip install agent-arena[embeddings]"
            )

        self.model = model
        self.dimensions = dimensions
        self.client = openai.AsyncOpenAI(
            api_key=api_key or os.getenv("OPENAI_API_KEY")
        )

    async def embed_text(self, text: str) -> list[float]:
        """
        Generate embedding for text.

        Args:
            text: Text to embed.

        Returns:
            List of floats representing the embedding vector.
        """
        response = await self.client.embeddings.create(
            model=self.model,
            input=text,
        )
        return response.data[0].embedding

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """
        Generate embeddings for multiple texts.

        Args:
            texts: List of texts to embed.

        Returns:
            List of embedding vectors.
        """
        if not texts:
            return []

        # OpenAI has a limit on batch size, chunk if needed
        max_batch_size = 2048
        all_embeddings = []

        for i in range(0, len(texts), max_batch_size):
            batch = texts[i:i + max_batch_size]
            response = await self.client.embeddings.create(
                model=self.model,
                input=batch,
            )
            batch_embeddings = [item.embedding for item in response.data]
            all_embeddings.extend(batch_embeddings)

        return all_embeddings

    async def embed_context(self, context: dict) -> list[float]:
        """
        Generate embedding for market context.

        Args:
            context: Enriched context dict from ContextBuilder.

        Returns:
            Embedding vector.
        """
        text = self._context_to_text(context)
        return await self.embed_text(text)

    def _context_to_text(self, context: dict) -> str:
        """
        Convert context dict to searchable text for embedding.

        The text representation should capture the key characteristics
        that make contexts similar or different.

        Args:
            context: Context dictionary.

        Returns:
            Text representation suitable for embedding.
        """
        parts = []

        # Market state with semantic descriptions
        for symbol, data in context.get("market_prices", {}).items():
            price = data.get("price", 0)
            change = data.get("change_24h", 0)
            funding = data.get("funding_rate", 0)

            # Add price level
            parts.append(f"{symbol} price={price:.2f}")

            # Add momentum description
            if change > 5:
                parts.append(f"{symbol} strong_bullish_momentum change={change:.2f}%")
            elif change > 2:
                parts.append(f"{symbol} bullish_momentum change={change:.2f}%")
            elif change < -5:
                parts.append(f"{symbol} strong_bearish_momentum change={change:.2f}%")
            elif change < -2:
                parts.append(f"{symbol} bearish_momentum change={change:.2f}%")
            else:
                parts.append(f"{symbol} sideways change={change:.2f}%")

            # Add funding sentiment
            if funding > 0.0005:
                parts.append(f"{symbol} high_positive_funding={funding:.4f}%")
            elif funding < -0.0005:
                parts.append(f"{symbol} negative_funding={funding:.4f}%")

        # Technical indicators with semantic labels
        for symbol, ind in context.get("indicators", {}).items():
            rsi = ind.get("rsi_14")
            sma_pct = ind.get("price_vs_sma20")
            macd = ind.get("macd")
            bb = ind.get("bollinger")

            if rsi:
                if rsi < 30:
                    parts.append(f"{symbol} RSI_oversold={rsi:.1f}")
                elif rsi > 70:
                    parts.append(f"{symbol} RSI_overbought={rsi:.1f}")
                elif rsi < 45:
                    parts.append(f"{symbol} RSI_weak={rsi:.1f}")
                elif rsi > 55:
                    parts.append(f"{symbol} RSI_strong={rsi:.1f}")
                else:
                    parts.append(f"{symbol} RSI_neutral={rsi:.1f}")

            if sma_pct:
                if sma_pct > 5:
                    parts.append(f"{symbol} well_above_SMA20={sma_pct:.1f}%")
                elif sma_pct > 0:
                    parts.append(f"{symbol} above_SMA20={sma_pct:.1f}%")
                elif sma_pct < -5:
                    parts.append(f"{symbol} well_below_SMA20={sma_pct:.1f}%")
                else:
                    parts.append(f"{symbol} below_SMA20={sma_pct:.1f}%")

            if macd:
                hist = macd.get("histogram", 0)
                if hist and hist > 0:
                    parts.append(f"{symbol} MACD_bullish histogram={hist:.2f}")
                elif hist and hist < 0:
                    parts.append(f"{symbol} MACD_bearish histogram={hist:.2f}")

            if bb:
                percent_b = bb.get("percent_b", 0.5)
                if percent_b > 0.9:
                    parts.append(f"{symbol} at_upper_bollinger_band")
                elif percent_b < 0.1:
                    parts.append(f"{symbol} at_lower_bollinger_band")
                elif percent_b > 0.7:
                    parts.append(f"{symbol} near_upper_bollinger_band")
                elif percent_b < 0.3:
                    parts.append(f"{symbol} near_lower_bollinger_band")

        # Portfolio state
        portfolio = context.get("portfolio_state", {})
        equity = portfolio.get("equity", 10000)
        positions = portfolio.get("positions", [])

        # Equity state
        starting = 10000
        equity_change = ((equity / starting) - 1) * 100
        if equity_change > 10:
            parts.append(f"portfolio highly_profitable equity_change={equity_change:.1f}%")
        elif equity_change > 5:
            parts.append(f"portfolio profitable equity_change={equity_change:.1f}%")
        elif equity_change < -10:
            parts.append(f"portfolio significant_loss equity_change={equity_change:.1f}%")
        elif equity_change < -5:
            parts.append(f"portfolio losing equity_change={equity_change:.1f}%")
        else:
            parts.append(f"portfolio near_breakeven equity_change={equity_change:.1f}%")

        # Position state
        parts.append(f"open_positions={len(positions)}")
        if positions:
            for pos in positions[:3]:  # Limit to 3 positions
                symbol = pos.get("symbol", "?")
                side = pos.get("side", "?")
                pnl = pos.get("unrealized_pnl", 0)
                if pnl > 0:
                    parts.append(f"holding_{side}_{symbol}_profitable")
                else:
                    parts.append(f"holding_{side}_{symbol}_underwater")

        # Market regime
        regime = context.get("regime", "unknown")
        vol_pct = context.get("volatility_percentile", 50)
        parts.append(f"regime={regime}")

        if vol_pct > 80:
            parts.append("high_volatility_environment")
        elif vol_pct > 60:
            parts.append("elevated_volatility")
        elif vol_pct < 20:
            parts.append("low_volatility_environment")
        elif vol_pct < 40:
            parts.append("subdued_volatility")
        else:
            parts.append("normal_volatility")

        return " ".join(parts)


class MockEmbeddingService:
    """
    Mock embedding service for testing without OpenAI API.

    Generates deterministic pseudo-embeddings based on text content.
    """

    def __init__(self, dimensions: int = 1536):
        self.dimensions = dimensions

    async def embed_text(self, text: str) -> list[float]:
        """Generate mock embedding."""
        import hashlib

        # Create deterministic embedding from text hash
        hash_bytes = hashlib.sha256(text.encode()).digest()

        # Expand hash to required dimensions
        embedding = []
        for i in range(self.dimensions):
            byte_idx = i % len(hash_bytes)
            # Normalize to [-1, 1] range
            value = (hash_bytes[byte_idx] / 127.5) - 1.0
            # Add some variation based on position
            value *= (1.0 + (i % 10) * 0.01)
            embedding.append(value)

        # Normalize to unit vector
        magnitude = sum(v ** 2 for v in embedding) ** 0.5
        embedding = [v / magnitude for v in embedding]

        return embedding

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Generate mock embeddings for batch."""
        return [await self.embed_text(t) for t in texts]

    async def embed_context(self, context: dict) -> list[float]:
        """Generate mock embedding for context."""
        # Simple text representation for mock
        text = str(context.get("regime", "")) + str(context.get("tick", 0))
        return await self.embed_text(text)


def get_embedding_service(
    use_mock: bool = False,
    **kwargs,
) -> EmbeddingService | MockEmbeddingService:
    """
    Get embedding service instance.

    Args:
        use_mock: Use mock service for testing.
        **kwargs: Arguments passed to EmbeddingService.

    Returns:
        Embedding service instance.
    """
    if use_mock or os.getenv("EMBEDDING_MODEL") == "mock":
        return MockEmbeddingService(kwargs.get("dimensions", 1536))

    return EmbeddingService(**kwargs)
