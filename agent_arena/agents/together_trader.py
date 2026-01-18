"""Together AI based trading agent for open-source models."""

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

# Popular models available on Together AI (updated Dec 2025)
# For agentic traders with tool calling, use AgenticTogetherTrader with gpt-oss-20b
TOGETHER_MODELS = {
    # GPT-OSS models (for agentic use AgenticTogetherTrader)
    "gpt-oss-20b": "openai/gpt-oss-20b",  # $0.10/$0.30 - can run locally, good tools
    "gpt-oss-120b": "openai/gpt-oss-120b",  # $0.15/$0.60 - larger
    # Llama models
    "llama-4-scout": "meta-llama/Llama-4-Scout-17B-16E-Instruct",  # $0.18/$0.59 - fast
    "llama-4-maverick": "meta-llama/Llama-4-Maverick-17B-128E-Instruct-FP8",  # $0.27/$0.85
    "llama-3.3-70b": "meta-llama/Llama-3.3-70B-Instruct-Turbo",  # $0.88/$0.88
    # Qwen models
    "qwen3-235b": "Qwen/Qwen3-235B-A22B-Instruct-2507-tput",  # $0.20/$0.60
    "qwen-2.5-72b": "Qwen/Qwen2.5-72B-Instruct-Turbo",
    "qwen-2.5-7b": "Qwen/Qwen2.5-7B-Instruct-Turbo",
    # DeepSeek models (best reasoning)
    "deepseek-v3": "deepseek-ai/DeepSeek-V3",  # $1.25/$1.25
    "deepseek-v3.1": "deepseek-ai/DeepSeek-V3.1",  # latest - excellent reasoning
    "deepseek-r1": "deepseek-ai/DeepSeek-R1",  # $3.00/$7.00 - deep reasoning
}


class TogetherTrader(BaseAgent):
    """
    Trading agent using Together AI's OpenAI-compatible API.
    Supports Llama, Qwen, Mistral, and other open-source models.
    Can easily switch to local Ollama by changing base_url.
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        super().__init__(agent_id, name, config)
        self.api_key = os.environ.get("TOGETHER_API_KEY", "")

        # Allow model shorthand or full model path
        model_input = config.get("model", "llama-3.1-70b") if config else "llama-3.1-70b"
        self.model = TOGETHER_MODELS.get(model_input, model_input)

        # Support switching between Together AI and local Ollama
        self.base_url = config.get("base_url", "https://api.together.xyz/v1") if config else "https://api.together.xyz/v1"
        self.character = config.get("character", "") if config else ""

    async def decide(self, context: dict) -> Decision:
        """Make a trading decision based on market context."""
        prompt = self._build_prompt(context)

        start = datetime.utcnow()
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(
                    f"{self.base_url}/chat/completions",
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
                        "max_tokens": 1024,
                        "temperature": 0.7,
                    },
                )
                response.raise_for_status()
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
        except httpx.HTTPStatusError as e:
            error_msg = f"Together API error: {e.response.status_code}"
            try:
                error_detail = e.response.json()
                error_msg = f"Together API error: {error_detail}"
            except Exception:
                pass
            return Decision(
                action="hold",
                reasoning=error_msg,
                metadata={"error": error_msg},
            )
        except Exception as e:
            return Decision(
                action="hold",
                reasoning=f"Error calling Together API: {str(e)}",
                metadata={"error": str(e)},
            )

    def _build_prompt(self, context: dict) -> str:
        """Build the prompt for the model."""
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
