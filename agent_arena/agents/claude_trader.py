"""Claude-based trading agent."""

from __future__ import annotations

import json
import re
from datetime import datetime
from decimal import Decimal
from typing import Optional

import anthropic

from agent_arena.core.agent import BaseAgent
from agent_arena.core.models import Decision


class ClaudeTrader(BaseAgent):
    """
    Straightforward Claude-based trader.
    No framework, no abstraction - just API calls.
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        super().__init__(agent_id, name, config)
        self.client = anthropic.Anthropic()
        # Default to Sonnet 4.5 for better reasoning (no tools to assist)
        self.model = config.get("model", "claude-sonnet-4-5-20250929") if config else "claude-sonnet-4-5-20250929"
        self.character = config.get("character", "") if config else ""

    async def decide(self, context: dict) -> Decision:
        """Make a trading decision based on market context."""
        prompt = self._build_prompt(context)

        start = datetime.utcnow()
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )
            latency = (datetime.utcnow() - start).total_seconds() * 1000

            # Parse response into Decision
            raw_text = response.content[0].text
            parsed = self._parse_response(raw_text)

            return Decision(
                action=parsed.get("action", "hold"),
                symbol=parsed.get("symbol"),
                size=Decimal(str(parsed["size"])) if parsed.get("size") else None,
                leverage=parsed.get("leverage", 1),
                confidence=parsed.get("confidence", 0.5),
                reasoning=parsed.get("reasoning", ""),
                metadata={
                    "model": self.model,
                    "tokens_used": response.usage.input_tokens + response.usage.output_tokens,
                    "latency_ms": latency,
                    "raw_response": raw_text,
                },
            )
        except Exception as e:
            return Decision(
                action="hold",
                reasoning=f"Error calling Claude API: {str(e)}",
                metadata={"error": str(e)},
            )

    def _build_prompt(self, context: dict) -> str:
        """Build the prompt for Claude."""
        market = context.get("market", {})
        portfolio = context.get("portfolio", {})
        tick = context.get("tick", 0)

        market_str = self._format_market(market)
        positions_str = self._format_positions(portfolio.get("positions", []))

        character_section = ""
        if self.character:
            character_section = f"\nYOUR TRADING STYLE:\n{self.character}\n"

        return f"""You are a crypto futures trader competing in Agent Arena.
{character_section}
CURRENT TICK: {tick}

MARKET DATA:
{market_str}

YOUR PORTFOLIO:
Equity: ${portfolio.get('equity', 10000):,.2f}
Available Margin: ${portfolio.get('available_margin', 10000):,.2f}
Total P&L: ${portfolio.get('total_pnl', 0):,.2f} ({portfolio.get('pnl_percent', 0):+.2f}%)

CURRENT POSITIONS:
{positions_str}

RULES:
- You start with $10,000
- Maximum leverage is 10x
- Maximum position size is 25% of equity
- Trading fee is 0.04% per trade
- You can only have one position per symbol

AVAILABLE ACTIONS:
- "hold": Do nothing
- "open_long": Open a long position (bet price goes up)
- "open_short": Open a short position (bet price goes down)
- "close": Close an existing position

Analyze the market and make a decision. Consider:
1. Current price trends and 24h changes
2. Funding rates (positive = longs paying shorts)
3. Your current positions and P&L
4. Risk management

Respond ONLY with valid JSON (no markdown, no explanation outside JSON):
{{
    "action": "hold" | "open_long" | "open_short" | "close",
    "symbol": "BTCUSDT" (required if action is not hold),
    "size": 0.01 (position size in base currency, required for open_long/open_short),
    "leverage": 2 (1-10, default 1),
    "confidence": 0.75 (0.0-1.0, how confident you are),
    "reasoning": "Brief explanation of your thinking (1-2 sentences)"
}}"""

    def _format_market(self, market: dict) -> str:
        """Format market data for the prompt."""
        if not market:
            return "No market data available"

        lines = []
        for symbol, data in market.items():
            price = data.get("price", 0)
            change = data.get("change_24h", 0)
            funding = data.get("funding_rate")

            line = f"{symbol}: ${float(price):,.2f} ({change:+.2f}%)"
            if funding is not None:
                line += f" | Funding: {float(funding)*100:.4f}%"
            lines.append(line)

        return "\n".join(lines)

    def _format_positions(self, positions: list) -> str:
        """Format positions for the prompt."""
        if not positions:
            return "No open positions"

        lines = []
        for pos in positions:
            pnl = pos.get("unrealized_pnl", 0)
            roe = pos.get("roe_percent", 0)
            lines.append(
                f"  {pos['symbol']} {pos['side'].upper()} "
                f"Size: {pos['size']} @ {pos['leverage']}x | "
                f"Entry: ${pos['entry_price']:,.2f} | "
                f"P&L: ${pnl:+,.2f} ({roe:+.2f}%)"
            )

        return "\n".join(lines)

    def _parse_response(self, text: str) -> dict:
        """Extract JSON from response."""
        # Try to find JSON in the response
        try:
            # First try direct parse
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Try to extract JSON from markdown code block
        match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass

        # Try to find raw JSON object
        match = re.search(r"\{[^{}]*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass

        # Default to hold if parsing fails
        return {"action": "hold", "reasoning": "Failed to parse response"}
