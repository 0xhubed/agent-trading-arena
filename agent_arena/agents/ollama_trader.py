"""Ollama-based trading agent for local inference."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import httpx

from agent_arena.agents.prompt_utils import (
    format_market,
    format_positions,
    parse_json_response,
)
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
        self._client: httpx.AsyncClient | None = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=120.0)
        return self._client

    async def decide(self, context: dict) -> Decision:
        """Make a trading decision based on market context."""
        prompt = self._build_prompt(context)

        start = datetime.now(timezone.utc)
        try:
            client = self._get_client()
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
            latency = (datetime.now(timezone.utc) - start).total_seconds() * 1000

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

        market_str = format_market(market)
        positions_str = format_positions(portfolio.get("positions", []))

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
    "symbol": "PF_XBTUSD",
    "size": 0.01,
    "leverage": 2,
    "confidence": 0.75,
    "reasoning": "Brief explanation"
}}

Your JSON response:"""

    def _parse_response(self, text: str) -> dict:
        """Extract JSON from response."""
        return parse_json_response(text)
