"""Index Fund agent - passive buy and hold strategy."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from agent_arena.core.agent import BaseAgent
from agent_arena.core.models import Decision


class IndexFundAgent(BaseAgent):
    """
    Passive index fund strategy.

    Buys and holds equal dollar amounts of each available symbol.
    Acts as a benchmark for active trading strategies.

    With $10,000 capital and 5 symbols, allocates $2,000 to each.
    Uses low leverage (1x) and never sells.
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        super().__init__(agent_id, name, config)
        config = config or {}

        self.model = "index-fund"  # Not an LLM
        self.allocation_per_symbol = Decimal(str(config.get("allocation_per_symbol", 2000)))
        self.leverage = config.get("leverage", 1)

        # Track which symbols we've already bought
        self._positions_opened: set[str] = set()
        self._initialized = False

    async def decide(self, context: dict) -> Decision:
        """
        Simple strategy:
        - On first tick, buy equal amounts of all symbols
        - After that, hold forever
        """
        market = context.get("market", {})
        portfolio = context.get("portfolio", {})
        tick = context.get("tick", 0)

        # Get list of symbols we already have positions in
        current_positions = {pos["symbol"] for pos in portfolio.get("positions", [])}

        # Find symbols we haven't bought yet
        symbols_to_buy = [s for s in market.keys() if s not in self._positions_opened]

        if not symbols_to_buy:
            # Already holding all symbols - just hold
            return Decision(
                action="hold",
                confidence=1.0,
                reasoning=f"Index fund fully allocated. Holding {len(self._positions_opened)} positions.",
                timestamp=datetime.now(timezone.utc),
            )

        # Buy the next symbol we haven't bought yet
        symbol = symbols_to_buy[0]
        price = market[symbol].get("price", Decimal("0"))

        if price <= 0:
            return Decision(
                action="hold",
                confidence=0.5,
                reasoning=f"Waiting for valid price data for {symbol}",
                timestamp=datetime.now(timezone.utc),
            )

        # Calculate position size
        # size = allocation / price
        size = self.allocation_per_symbol / price

        # Mark as opened (even before execution, to avoid re-trying)
        self._positions_opened.add(symbol)

        return Decision(
            action="open_long",
            symbol=symbol,
            size=size,
            leverage=self.leverage,
            confidence=1.0,
            reasoning=f"Index fund allocation: ${self.allocation_per_symbol} into {symbol} at ${float(price):,.2f}",
            timestamp=datetime.now(timezone.utc),
        )
