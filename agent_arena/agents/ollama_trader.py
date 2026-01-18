"""Ollama-based trading agent for local inference."""

from __future__ import annotations

import json
import re
from datetime import datetime
from decimal import Decimal
from typing import Optional

import httpx

from agent_arena.core.agent import BaseAgent
from agent_arena.core.models import Decision


class OllamaTrader(BaseAgent):
    """
    Local inference trader using Ollama.
    Cost-free, runs on local hardware.
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        super().__init__(agent_id, name, config)
        self.model = config.get("model", "qwen2.5:7b") if config else "qwen2.5:7b"
        self.base_url = config.get("ollama_url", "http://localhost:11434") if config else "http://localhost:11434"
        self.character = config.get("character", "") if config else ""

    async def decide(self, context: dict) -> Decision:
        """Make a trading decision based on market context."""
        prompt = self._build_prompt(context)

        start = datetime.utcnow()
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(
                    f"{self.base_url}/api/generate",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {
                            "temperature": 0.7,
                            "num_predict": 1024,
                        },
                    },
                )
                response.raise_for_status()
                result = response.json()

            latency = (datetime.utcnow() - start).total_seconds() * 1000

            raw_text = result.get("response", "")
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
                    "eval_count": result.get("eval_count", 0),
                    "latency_ms": latency,
                    "raw_response": raw_text,
                },
            )
        except httpx.ConnectError:
            return Decision(
                action="hold",
                reasoning="Ollama server not available",
                metadata={"error": "Connection refused - is Ollama running?"},
            )
        except Exception as e:
            return Decision(
                action="hold",
                reasoning=f"Error calling Ollama: {str(e)}",
                metadata={"error": str(e)},
            )

    def _build_prompt(self, context: dict) -> str:
        """Build the prompt for Ollama."""
        market = context.get("market", {})
        portfolio = context.get("portfolio", {})
        tick = context.get("tick", 0)

        market_str = self._format_market(market)
        positions_str = self._format_positions(portfolio.get("positions", []))

        character_section = ""
        if self.character:
            character_section = f"\nYOUR TRADING STYLE:\n{self.character}\n"

        return f"""You are a crypto futures trader competing in Agent Arena. You must respond with valid JSON only.
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

Respond with JSON only, no other text:
{{
    "action": "hold",
    "symbol": "BTCUSDT",
    "size": 0.01,
    "leverage": 2,
    "confidence": 0.75,
    "reasoning": "Brief explanation"
}}

Your JSON response:"""

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
        try:
            return json.loads(text.strip())
        except json.JSONDecodeError:
            pass

        match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                pass

        match = re.search(r"\{[^{}]*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass

        return {"action": "hold", "reasoning": "Failed to parse response"}
