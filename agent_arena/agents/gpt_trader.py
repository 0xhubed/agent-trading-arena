"""GPT-4 based trading agent."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from decimal import Decimal
from typing import Optional

import httpx

from agent_arena.core.agent import BaseAgent
from agent_arena.core.models import Decision


class GPTTrader(BaseAgent):
    """
    GPT-based trader using OpenAI API.
    Direct API calls, no framework.
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        super().__init__(agent_id, name, config)
        self.api_key = os.environ.get("OPENAI_API_KEY", "")
        # Default to GPT-5.1
        self.model = config.get("model", "gpt-5.1") if config else "gpt-5.1"
        self.character = config.get("character", "") if config else ""
        self.base_url = "https://api.openai.com/v1/chat/completions"

    async def decide(self, context: dict) -> Decision:
        """Make a trading decision based on market context."""
        prompt = self._build_prompt(context)

        start = datetime.utcnow()
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    self.base_url,
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": self.model,
                        "messages": [
                            {
                                "role": "system",
                                "content": "You are a crypto futures trader. Respond only with valid JSON.",
                            },
                            {"role": "user", "content": prompt},
                        ],
                        "max_completion_tokens": 1024,
                        "temperature": 0.7,
                    },
                )

                # Check for errors with detailed response body
                if response.status_code != 200:
                    error_body = response.text
                    try:
                        error_json = response.json()
                        error_msg = error_json.get("error", {}).get("message", error_body)
                        error_type = error_json.get("error", {}).get("type", "unknown")
                        error_code = error_json.get("error", {}).get("code", "unknown")
                    except Exception:
                        error_msg = error_body
                        error_type = "unknown"
                        error_code = "unknown"

                    return Decision(
                        action="hold",
                        reasoning=f"OpenAI API error ({response.status_code}): {error_msg}",
                        metadata={
                            "error": error_msg,
                            "error_type": error_type,
                            "error_code": error_code,
                            "status_code": response.status_code,
                            "model": self.model,
                        },
                    )

                data = response.json()

            latency = (datetime.utcnow() - start).total_seconds() * 1000

            raw_text = data["choices"][0]["message"]["content"]
            parsed = self._parse_response(raw_text)

            tokens_used = data.get("usage", {})
            total_tokens = tokens_used.get("total_tokens", 0)

            return Decision(
                action=parsed.get("action", "hold"),
                symbol=parsed.get("symbol"),
                size=Decimal(str(parsed["size"])) if parsed.get("size") else None,
                leverage=parsed.get("leverage", 1),
                confidence=parsed.get("confidence", 0.5),
                reasoning=parsed.get("reasoning", ""),
                metadata={
                    "model": self.model,
                    "tokens_used": total_tokens,
                    "latency_ms": latency,
                    "raw_response": raw_text,
                },
            )
        except Exception as e:
            return Decision(
                action="hold",
                reasoning=f"Error calling OpenAI API: {str(e)}",
                metadata={"error": str(e)},
            )

    def _build_prompt(self, context: dict) -> str:
        """Build the prompt for GPT."""
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

Respond ONLY with valid JSON:
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
        try:
            return json.loads(text)
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
