"""LangChain-based trading agent."""

from __future__ import annotations

import json
import re
from datetime import datetime
from decimal import Decimal
from typing import Optional

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage

from agent_arena.core.agent import BaseAgent
from agent_arena.core.models import Decision


class LangChainTrader(BaseAgent):
    """
    Trading agent using LangChain for LLM orchestration.

    Demonstrates LangChain integration as an alternative to direct API calls.
    Uses structured output parsing and conversation memory.
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        super().__init__(agent_id, name, config)
        config = config or {}

        self.model = config.get("model", "claude-sonnet-4-20250514")
        self.character = config.get("character", "A balanced trader using LangChain.")
        self.temperature = config.get("temperature", 0.7)

        # Initialize LangChain chat model
        self._llm = ChatAnthropic(
            model=self.model,
            temperature=self.temperature,
            max_tokens=1024,
        )

        # Conversation history for context
        self._history: list[dict] = []

    async def decide(self, context: dict) -> Decision:
        """Make a trading decision using LangChain."""
        try:
            # Build the prompt
            system_prompt = self._build_system_prompt()
            user_prompt = self._build_user_prompt(context)

            # Create messages
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_prompt),
            ]

            # Invoke the LLM
            response = await self._llm.ainvoke(messages)
            response_text = response.content

            # Parse the response
            decision = self._parse_response(response_text, context)

            # Store in history for context
            self._history.append({
                "tick": context.get("tick"),
                "decision": decision.action,
                "reasoning": decision.reasoning[:100] if decision.reasoning else "",
            })

            # Keep history manageable
            if len(self._history) > 10:
                self._history = self._history[-10:]

            return decision

        except Exception as e:
            return Decision(
                action="hold",
                confidence=0.0,
                reasoning=f"LangChain error: {str(e)}",
            )

    def _build_system_prompt(self) -> str:
        """Build the system prompt."""
        return f"""You are an AI trading agent competing in Agent Arena.

PERSONALITY: {self.character}

RULES:
- You trade crypto futures with $10,000 starting capital
- Maximum 25% of equity per position
- Maximum 10x leverage
- Trading fees are 0.04% per trade

ACTIONS:
- hold: Do nothing
- open_long: Buy/go long on a symbol
- open_short: Sell/go short on a symbol
- close: Close an existing position

RESPONSE FORMAT (JSON only):
{{
  "action": "hold|open_long|open_short|close",
  "symbol": "BTCUSDT",
  "size": 0.1,
  "leverage": 5,
  "confidence": 0.75,
  "reasoning": "Brief explanation"
}}

Recent history: {json.dumps(self._history[-5:]) if self._history else "No previous decisions"}
"""

    def _build_user_prompt(self, context: dict) -> str:
        """Build the user prompt with market context."""
        market = context.get("market", {})
        portfolio = context.get("portfolio", {})
        tick = context.get("tick", 0)

        market_lines = []
        for symbol, data in market.items():
            price = data.get("price", 0)
            change = data.get("change_24h", 0)
            funding = data.get("funding_rate", 0)
            market_lines.append(
                f"  {symbol}: ${float(price):,.2f} ({change:+.2f}% 24h, funding: {float(funding)*100:.4f}%)"
            )

        positions_lines = []
        for pos in portfolio.get("positions", []):
            positions_lines.append(
                f"  {pos['symbol']}: {pos['side']} {pos['size']} @ {pos['entry_price']} "
                f"(PnL: ${pos['unrealized_pnl']:.2f}, ROE: {pos['roe_percent']:.2f}%)"
            )

        return f"""TICK {tick} - Make your trading decision.

MARKET DATA:
{chr(10).join(market_lines) if market_lines else "  No data available"}

PORTFOLIO:
  Equity: ${portfolio.get('equity', 10000):,.2f}
  Available Margin: ${portfolio.get('available_margin', 10000):,.2f}
  Realized P&L: ${portfolio.get('realized_pnl', 0):,.2f}

CURRENT POSITIONS:
{chr(10).join(positions_lines) if positions_lines else "  No open positions"}

What is your decision? Respond with JSON only."""

    def _parse_response(self, response: str, context: dict) -> Decision:
        """Parse the LLM response into a Decision."""
        # Extract JSON from response
        json_match = re.search(r'\{[^{}]*\}', response, re.DOTALL)
        if not json_match:
            return Decision(action="hold", confidence=0.3, reasoning="Could not parse response")

        try:
            data = json.loads(json_match.group())

            action = data.get("action", "hold").lower()
            if action not in ["hold", "open_long", "open_short", "close"]:
                action = "hold"

            symbol = data.get("symbol")
            size = data.get("size")
            leverage = data.get("leverage", 1)
            confidence = float(data.get("confidence", 0.5))
            reasoning = data.get("reasoning", "")

            # Validate symbol
            market = context.get("market", {})
            if symbol and symbol not in market:
                symbol = None

            return Decision(
                action=action,
                symbol=symbol,
                size=Decimal(str(size)) if size else None,
                leverage=int(leverage) if leverage else 1,
                confidence=max(0.0, min(1.0, confidence)),
                reasoning=reasoning,
                timestamp=datetime.utcnow(),
            )

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            return Decision(
                action="hold",
                confidence=0.2,
                reasoning=f"Parse error: {str(e)}",
            )
