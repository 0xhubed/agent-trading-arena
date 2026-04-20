"""Contrarian discussion agent - challenges consensus when detected."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional
from uuid import UUID

import httpx

from agent_arena.agents.model_registry import resolve_model
from agent_arena.llm_utils import strip_reasoning_preamble, strip_think_blocks

if TYPE_CHECKING:
    from agent_arena.forum.service import ForumService

logger = logging.getLogger(__name__)


class ContrarianAgent:
    """Challenges consensus views in the forum.

    Monitors recent forum messages and posts contrarian perspectives when:
    - Consensus is detected (>70% agreement on direction)
    - Counter-evidence is available (funding rates, historical patterns)
    - Risk of crowded trade is high
    """

    def __init__(self, agent_id: str, config: dict, forum: ForumService):
        """Initialize Contrarian agent.

        Args:
            agent_id: Unique agent identifier
            config: Configuration dict with:
                - name: Display name
                - consensus_threshold: Agreement % to trigger (default 0.70)
                - check_interval_ticks: Ticks between consensus checks (default 3)
                - lookback_messages: Number of recent messages to analyze (default 20)
            forum: ForumService instance
        """
        self.agent_id = agent_id
        self.name = config.get("name", "Contrarian")
        self.forum = forum

        # LLM config
        self.model = resolve_model(config["model"]) if config.get("model") else None
        self.base_url = config.get("base_url")
        self.api_key_env = config.get("api_key_env")
        self.api_key = os.environ.get(self.api_key_env or "", "") if self.api_key_env else ""

        # Extra params merged into request body (e.g. {think: false})
        self.extra_params = config.get("extra_params", {})

        # Persistent HTTP client (created lazily)
        self._client: httpx.AsyncClient | None = None

        # Configuration
        self.consensus_threshold = config.get("consensus_threshold", 0.70)
        self.check_interval = config.get("check_interval_ticks", 3)
        self.lookback_messages = config.get("lookback_messages", 20)

        # State
        self.last_check_tick = 0
        self.last_challenge_tick = 0
        self.challenges_posted = 0

    async def on_tick(self, context: dict) -> None:
        """Called every tick to potentially challenge consensus.

        Args:
            context: Trading context with market data
        """
        tick = context["tick"]

        # Rate limit consensus checks
        if tick - self.last_check_tick < self.check_interval:
            return

        self.last_check_tick = tick

        # Don't challenge too frequently
        if tick - self.last_challenge_tick < self.check_interval:
            return

        # Get recent forum messages
        recent_messages = await self.forum.get_recent_messages(
            channels=["market", "strategy"],
            limit=self.lookback_messages,
        )

        if not recent_messages:
            return

        # Analyze consensus
        consensus = await self.forum.analyze_consensus(recent_messages)

        # Check if consensus is strong enough to challenge
        if consensus["agreement_pct"] >= self.consensus_threshold:
            # Generate and post challenge
            posted = await self._post_challenge(consensus, context, recent_messages)
            if posted:
                self.last_challenge_tick = tick
                self.challenges_posted += 1
            else:
                logger.debug(
                    "Contrarian: consensus %.0f%% %s but insufficient counter-evidence",
                    consensus["agreement_pct"] * 100,
                    consensus["direction"],
                )
        else:
            logger.debug(
                "Contrarian: consensus %.0f%% %s (threshold %.0f%%)",
                consensus["agreement_pct"] * 100,
                consensus["direction"],
                self.consensus_threshold * 100,
            )

    async def _post_challenge(
        self,
        consensus: dict,
        context: dict,
        recent_messages: list,
    ) -> bool:
        """Generate and post a contrarian challenge.

        Args:
            consensus: Consensus analysis from ForumService
            context: Trading context
            recent_messages: Recent forum messages

        Returns:
            True if a challenge was posted, False otherwise
        """
        direction = consensus["direction"]
        agreement_pct = consensus["agreement_pct"]
        strongest_id = consensus["strongest_message_id"]

        # Generate counter-arguments based on market data
        counter_args = self._generate_counter_arguments(direction, context)

        if not counter_args:
            # No strong counter-evidence, skip
            return False

        # Generate challenge message (LLM with template fallback)
        content = await self._generate_challenge(
            direction, agreement_pct, counter_args, strongest_id
        )

        # Post as reply to strongest consensus message if available
        reply_to = strongest_id if isinstance(strongest_id, UUID) else None

        await self.forum.post_message(
            channel="strategy",
            agent_id=self.agent_id,
            agent_name=self.name,
            agent_type="discussion",
            content=content,
            reply_to=reply_to,
            metadata={
                "tick": context["tick"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "contrarian_trigger": True,
                "consensus_challenged": direction,
                "agreement_pct": agreement_pct,
                "challenge_count": self.challenges_posted,
            },
        )

        return True

    def _generate_counter_arguments(
        self, direction: str, context: dict
    ) -> Optional[dict]:
        """Generate counter-arguments to consensus view.

        Args:
            direction: Consensus direction (bullish/bearish)
            context: Trading context

        Returns:
            Dict with counter-arguments or None if weak case
        """
        market = context.get("market", {})

        # Get primary symbol data
        symbol = "PF_XBTUSD"
        if symbol not in market:
            symbol = list(market.keys())[0] if market else None

        if not symbol:
            return None

        symbol_data = market[symbol]
        funding_rate = symbol_data.get("funding_rate", 0)
        change_24h = symbol_data.get("change_24h", 0)

        counter_args = []

        # Funding rate counter-argument
        # Kraken rates are normalized to per-8h fraction (same as arena.py convention)
        if direction == "bullish" and funding_rate > 0.0001:
            counter_args.append(
                f"Elevated funding rate ({funding_rate*100:.3f}%) suggests longs "
                "are overcrowded. Historically, this precedes corrections."
            )
        elif direction == "bearish" and funding_rate < -0.0001:
            counter_args.append(
                f"Negative funding rate ({funding_rate*100:.3f}%) shows shorts "
                "are paying. This often marks bottoms."
            )

        # Overextension counter-argument (change_24h is a percentage, e.g. 5.0 = 5%)
        if direction == "bullish" and change_24h > 2.5:
            counter_args.append(
                f"Price up {change_24h:.1f}% in 24h - likely overextended. "
                "Pullbacks are healthy and expected."
            )
        elif direction == "bearish" and change_24h < -2.5:
            counter_args.append(
                f"Price down {abs(change_24h):.1f}% in 24h - oversold conditions. "
                "Bounce candidates often emerge here."
            )

        # Contrarian positioning
        if direction == "bullish":
            counter_args.append(
                "When everyone is bullish, who's left to buy? "
                "Contrarian positioning suggests caution."
            )
        elif direction == "bearish":
            counter_args.append(
                "Maximum bearish sentiment often coincides with local bottoms. "
                "Consider the other side."
            )

        # Need at least 2 arguments for a strong challenge
        if len(counter_args) < 2:
            return None

        return {
            "direction": direction,
            "symbol": symbol,
            "arguments": counter_args,
            "funding_rate": funding_rate,
            "change_24h": change_24h,
        }

    async def _generate_challenge(
        self,
        consensus_direction: str,
        agreement_pct: float,
        counter_args: dict,
        reply_to_id: Optional[UUID],
    ) -> str:
        """Generate challenge via LLM, falling back to template on failure.

        Args:
            consensus_direction: The consensus view being challenged
            agreement_pct: Consensus agreement percentage
            counter_args: Counter-arguments dict
            reply_to_id: Message ID being replied to

        Returns:
            Challenge content string
        """
        # If no LLM configured, use template
        if not self.base_url or not self.model:
            return self._format_challenge_template(
                consensus_direction, agreement_pct, counter_args, reply_to_id
            )

        symbol = counter_args["symbol"]
        arguments = counter_args["arguments"]
        funding_rate = counter_args.get("funding_rate", 0)
        change_24h = counter_args.get("change_24h", 0)

        data_summary = (
            f"Symbol: {symbol}\n"
            f"Consensus: {agreement_pct * 100:.0f}% {consensus_direction}\n"
            f"24h Change: {change_24h:+.2f}%\n"
            f"Funding Rate: {funding_rate * 100:.4f}%\n"
            f"Is reply to another post: {'yes' if reply_to_id else 'no'}\n"
            f"\nCounter-evidence:\n"
            + "\n".join(f"- {arg}" for arg in arguments)
        )

        try:
            if self._client is None:
                self._client = httpx.AsyncClient(timeout=30.0)

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
                            "content": (
                                "You are a contrarian trading agent in a crypto "
                                "futures forum. Challenge the consensus with "
                                "sharp, well-reasoned counter-arguments. "
                                "Be provocative but substantive. Use the "
                                "counter-evidence provided to build your case. "
                                "Use markdown formatting. "
                                "Keep posts under 150 words. No emojis. "
                                "CRITICAL: Use the EXACT numeric values from "
                                "the data (prices, percentages, rates). "
                                "Do NOT recalculate or change any numbers. "
                                "Output ONLY the forum post. Do NOT include "
                                "any reasoning steps, analysis process, "
                                "numbered plans, or drafts."
                            ),
                        },
                        {
                            "role": "user",
                            "content": (
                                f"Here is the current consensus and counter-evidence:\n\n"
                                f"{data_summary}\n\n"
                                "Write a contrarian challenge post for the trading forum."
                            ),
                        },
                    ],
                    "max_tokens": 300,
                    "temperature": 0.5,
                    "chat_template_kwargs": {"enable_thinking": False},
                    **self.extra_params,
                },
            )
            response.raise_for_status()
            data = response.json()
            raw = data["choices"][0]["message"]["content"]
            content = strip_think_blocks(raw)
            content = strip_reasoning_preamble(content)
            if content:
                return content
        except Exception as e:
            logger.warning("Contrarian LLM call failed, using template: %s", e)

        # Fallback to template
        return self._format_challenge_template(
            consensus_direction, agreement_pct, counter_args, reply_to_id
        )

    def _format_challenge_template(
        self,
        consensus_direction: str,
        agreement_pct: float,
        counter_args: dict,
        reply_to_id: Optional[UUID],
    ) -> str:
        """Format contrarian challenge message (template fallback).

        Args:
            consensus_direction: The consensus view being challenged
            agreement_pct: Consensus agreement percentage
            counter_args: Counter-arguments dict
            reply_to_id: Message ID being replied to

        Returns:
            Formatted markdown content
        """
        symbol = counter_args["symbol"]
        arguments = counter_args["arguments"]

        # Opening
        lines = []

        if reply_to_id:
            lines.append("I see a different picture here.")
        else:
            lines.append(
                f"**Contrarian View: {symbol}**"
            )

        lines.append("")

        # Note consensus
        lines.append(
            f"Consensus is {agreement_pct*100:.0f}% {consensus_direction}, "
            "but consider these points:"
        )
        lines.append("")

        # List counter-arguments
        for i, arg in enumerate(arguments, 1):
            lines.append(f"{i}. {arg}")

        lines.append("")

        # Closing
        if consensus_direction == "bullish":
            lines.append("The crowd might be getting greedy. Be cautious with new longs.")
        else:
            lines.append("Peak fear can create opportunity. Don't dismiss the long side.")

        return "\n".join(lines)

    async def on_stop(self) -> None:
        """Clean up resources."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
