"""LLM-based trading agent using any OpenAI-compatible API endpoint."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import httpx

from agent_arena.agents.model_registry import resolve_model
from agent_arena.agents.prompt_utils import format_market, parse_json_response
from agent_arena.core.agent import BaseAgent
from agent_arena.core.indicators import compute_all_indicators
from agent_arena.core.models import Decision
from agent_arena.llm_utils import strip_think_blocks


class LLMTrader(BaseAgent):
    """
    Trading agent using any OpenAI-compatible API endpoint.
    Supports local inference (LiteLLM, Ollama) and cloud providers
    (Together AI, OpenRouter, etc.) via configurable base_url.
    """

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        super().__init__(agent_id, name, config)
        config = config or {}

        # API key: configurable env var name for local inference compatibility
        self.api_key_env = config.get("api_key_env", "TOGETHER_API_KEY")
        self.api_key = os.environ.get(self.api_key_env, "")

        # Allow model shorthand or full model path
        model_input = config.get("model", "llama-3.1-70b")
        self.model = resolve_model(model_input)

        # OpenAI-compatible endpoint (local or cloud)
        self.base_url = config.get("base_url", "https://api.together.xyz/v1")
        self.character = config.get("character", "")
        self.max_tokens = config.get("max_tokens", 1024)

        # Extra params merged into request body (e.g. {think: false} for Ollama)
        self.extra_params = config.get("extra_params", {})

        # Persistent HTTP client (created lazily)
        self._client: httpx.AsyncClient | None = None

    async def decide(self, context: dict) -> Decision:
        """Make a trading decision based on market context."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=120.0)

        prompt = self._build_prompt(context)

        start = datetime.now(timezone.utc)
        try:
            response = await self._client.post(
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
                    "max_tokens": self.max_tokens,
                    "temperature": 0.7,
                    **self.extra_params,
                },
            )
            response.raise_for_status()
            data = response.json()

            latency = (datetime.now(timezone.utc) - start).total_seconds() * 1000

            message = data["choices"][0]["message"]
            raw_text = message.get("content") or ""
            # GLM-5 via Together AI sometimes puts all output in the
            # separate "reasoning" field, leaving content empty.
            reasoning_text = message.get("reasoning") or ""
            if not raw_text.strip() and reasoning_text:
                raw_text = reasoning_text
            parsed = self._parse_response(raw_text)

            tokens_used = data.get("usage", {})
            total_tokens = tokens_used.get("total_tokens", 0)

            confidence = min(
                parsed.get("confidence", 0.5), self.max_confidence
            )

            return Decision(
                action=parsed.get("action", "hold"),
                symbol=parsed.get("symbol"),
                size=Decimal(str(parsed["size"])) if parsed.get("size") else None,
                leverage=parsed.get("leverage", 1),
                confidence=confidence,
                reasoning=parsed.get("reasoning", ""),
                metadata={
                    "model": self.model,
                    "tokens_used": total_tokens,
                    "latency_ms": latency,
                    "raw_response": raw_text,
                },
            )
        except httpx.HTTPStatusError as e:
            error_msg = f"LLM API error: {e.response.status_code}"
            try:
                error_detail = e.response.json()
                error_msg = f"LLM API error: {error_detail}"
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
                reasoning=f"Error calling LLM API: {str(e)}",
                metadata={"error": str(e)},
            )

    async def on_stop(self) -> None:
        """Close the persistent HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    def _build_prompt(self, context: dict) -> str:
        """Build the prompt for the model."""
        market = context.get("market", {})
        portfolio = context.get("portfolio", {})
        candles = context.get("candles", {})
        tick = context.get("tick", 0)
        recent_decisions = context.get("recent_decisions", [])

        regime = context.get("regime")
        regime_guidance = context.get("regime_guidance", {})

        market_str = format_market(market)
        positions_str = self._format_positions(portfolio.get("positions", []))
        history_str = self._format_decision_history(recent_decisions)
        ta_str = self._format_technical_analysis(candles)
        perf_str = self._format_trade_performance(portfolio)
        regime_str = self._format_regime(regime, regime_guidance)

        character_section = ""
        if self.character:
            character_section = f"\nYOUR TRADING STYLE:\n{self.character}\n"

        return f"""You are a crypto futures trader competing in Agent Arena.
{character_section}
CURRENT TICK: {tick}

MARKET DATA:
{market_str}

TECHNICAL ANALYSIS (1h candles):
{ta_str}

{regime_str}
YOUR PORTFOLIO:
Equity: ${portfolio.get('equity', 10000):,.2f}
Available Margin: ${portfolio.get('available_margin', 10000):,.2f}
Total P&L: ${portfolio.get('total_pnl', 0):,.2f} ({portfolio.get('pnl_percent', 0):+.2f}%)

TRADE PERFORMANCE: {perf_str}

CURRENT POSITIONS:
{positions_str}

YOUR RECENT DECISIONS:
{history_str}

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
    "symbol": "PF_XBTUSD" (required if action is not hold),
    "size": 0.01 (position size in base currency, required for open_long/open_short),
    "leverage": 2 (1-10, default 1),
    "confidence": 0.75 (0.0-1.0, how confident you are),
    "reasoning": "Brief explanation of your thinking (1-2 sentences)"
}}"""

    def _format_technical_analysis(self, candles: dict) -> str:
        """Compute and format technical indicators from candle data."""
        if not candles:
            return "No candle data available"

        lines = []
        for symbol, intervals in candles.items():
            # Prefer 1h candles, fall back to 15m
            candle_list = intervals.get("1h", intervals.get("15m", []))
            if not candle_list:
                continue

            ind = compute_all_indicators(candle_list)
            if not ind:
                continue

            parts = [f"  {symbol}:"]

            # RSI
            rsi = ind.get("rsi_14")
            if rsi is not None:
                signal = ind.get("rsi_signal", "neutral")
                parts.append(f"RSI(14)={rsi:.1f} ({signal})")

            # SMA trend
            ma_trend = ind.get("ma_trend")
            sma_pct = ind.get("price_vs_sma20")
            if ma_trend and sma_pct is not None:
                parts.append(f"SMA20={sma_pct:+.1f}% ({ma_trend})")

            # MACD
            macd = ind.get("macd")
            if macd:
                hist = macd.get("histogram", 0)
                momentum = "bullish" if hist > 0 else "bearish"
                parts.append(f"MACD={momentum}")

            # Bollinger
            bb = ind.get("bollinger")
            if bb:
                pct_b = bb.get("percent_b", 0.5)
                if pct_b > 0.8:
                    bb_signal = "near upper (overbought)"
                elif pct_b < 0.2:
                    bb_signal = "near lower (oversold)"
                else:
                    bb_signal = f"%B={pct_b:.2f}"
                parts.append(f"BB: {bb_signal}")

            # ADX (trend strength)
            adx = ind.get("adx")
            if adx:
                adx_val = adx.get("adx", 0)
                strength = "strong" if adx_val > 25 else "weak"
                parts.append(f"ADX={adx_val:.0f} ({strength} trend)")

            lines.append(" | ".join(parts))

        return "\n".join(lines) if lines else "Insufficient candle data for analysis"

    def _format_decision_history(self, decisions: list[dict]) -> str:
        """Format recent decision history for the prompt."""
        if not decisions:
            return "No previous decisions (first tick)"

        lines = []
        for d in decisions:
            pnl_str = f" → P&L: ${d['trade_pnl']:+,.2f}" if d.get("trade_pnl") is not None else ""
            # Don't show symbol for hold actions — "hold PF_XBTUSD" confuses models
            action = d.get("action", "hold")
            symbol_str = f" {d['symbol']}" if d.get("symbol") and action != "hold" else ""
            lines.append(
                f"  Tick {d['tick']}: {action}{symbol_str} "
                f"(conf: {d.get('confidence', 0):.2f}){pnl_str}"
                f" — {d.get('reasoning', '')[:80]}"
            )
        return "\n".join(lines)

    def _format_positions(self, positions: list) -> str:
        """Format positions for the prompt."""
        if not positions:
            return "No open positions"

        lines = []
        for pos in positions:
            pnl = pos.get("unrealized_pnl", 0)
            roe = pos.get("roe_percent", 0)
            hold_hours = pos.get("hold_hours", 0)
            liq_dist = pos.get("liq_distance_pct", 100)
            advisory = pos.get("advisory", "")

            line = (
                f"  {pos['symbol']} {pos['side'].upper()} "
                f"Size: {pos['size']} @ {pos['leverage']}x | "
                f"Entry: ${pos['entry_price']:,.2f} | "
                f"P&L: ${pnl:+,.2f} ({roe:+.2f}%) | "
                f"Held: {hold_hours:.1f}h | Liq distance: {liq_dist:.1f}%"
            )
            if pos.get("stop_loss"):
                line += f" | SL: ${pos['stop_loss']:,.2f}"
            if pos.get("take_profit"):
                line += f" | TP: ${pos['take_profit']:,.2f}"
            lines.append(line)

            if advisory and advisory != "Position healthy":
                lines.append(f"    ⚠ {advisory}")

        return "\n".join(lines)

    def _format_trade_performance(self, portfolio: dict) -> str:
        """Format recent trade performance summary."""
        perf = portfolio.get("trade_performance", {})
        return perf.get("summary", "No closed trades yet")

    def _format_regime(self, regime: str | None, guidance: dict) -> str:
        """Format market regime classification for the prompt."""
        if not regime or regime == "unknown":
            return ""

        preferred = ", ".join(guidance.get("preferred_actions", []))
        avoid = ", ".join(guidance.get("avoid_actions", []))
        leverage = guidance.get("suggested_leverage", "moderate")
        description = guidance.get("description", "")
        entry = guidance.get("entry_timing", "")

        lines = [
            f"MARKET REGIME: {regime.upper().replace('_', ' ')}",
            f"  {description}",
            f"  Preferred actions: {preferred}",
        ]
        if avoid:
            lines.append(f"  Avoid: {avoid}")
        lines.append(f"  Suggested leverage: {leverage}")
        if entry:
            lines.append(f"  Entry timing: {entry}")
        lines.append("")  # trailing newline before YOUR PORTFOLIO
        return "\n".join(lines)

    def _parse_response(self, text: str) -> dict:
        """Extract JSON from response."""
        # Strip <think>...</think> chain-of-thought blocks
        cleaned = strip_think_blocks(text)

        result = parse_json_response(cleaned)
        if result.get("action") != "hold" or "Failed" not in result.get("reasoning", ""):
            return result

        # Match JSON object allowing one level of nesting (extra fallback)
        match = re.search(
            r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", cleaned, re.DOTALL
        )
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass

        # Last resort: extract trade intent from reasoning text
        return self._extract_from_reasoning(cleaned)

    def _extract_from_reasoning(self, text: str) -> dict:
        """Extract trade intent from unstructured reasoning text."""
        text_lower = text.lower()

        actions = ["open_long", "open_short", "close", "hold"]
        action = "hold"
        for a in actions:
            if a in text_lower:
                action = a
                break

        symbol = None
        for sym in ["PF_XBTUSD", "PF_ETHUSD", "PF_SOLUSD", "PF_DOGEUSD", "PF_XRPUSD"]:
            if sym in text or sym.lower() in text_lower:
                symbol = sym
                break

        confidence = 0.5
        conf_match = re.search(
            r"confidence[:\s]*(\d+\.?\d*)", text_lower
        )
        if conf_match:
            try:
                confidence = min(float(conf_match.group(1)), 1.0)
            except ValueError:
                pass

        size = None
        size_match = re.search(
            r"size[:\s]*(\d+\.?\d*)", text_lower
        )
        if size_match:
            try:
                size = float(size_match.group(1))
            except ValueError:
                pass

        leverage = 1
        lev_match = re.search(r"leverage[:\s]*(\d+)", text_lower)
        if lev_match:
            try:
                leverage = min(int(lev_match.group(1)), 10)
            except ValueError:
                pass

        # Open actions without a size are ghost decisions — the arena
        # silently drops them but they pollute decision history.
        # Fall back to hold when the extracted action can't actually execute.
        if action in ("open_long", "open_short") and size is None:
            action = "hold"

        result = {
            "action": action,
            "confidence": confidence,
            "reasoning": f"Extracted from reasoning (no JSON): {text[-200:]}",
        }
        # Only attach symbol/size/leverage for non-hold actions.
        # "hold PF_XBTUSD" in decision history confuses models on subsequent ticks.
        if action != "hold":
            if symbol:
                result["symbol"] = symbol
            if size is not None:
                result["size"] = size
            if leverage > 1:
                result["leverage"] = leverage
        return result
