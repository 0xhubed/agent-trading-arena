"""Shared prompt formatting and response parsing for LLM traders."""

from __future__ import annotations

import json
import re


def format_market(market: dict) -> str:
    """Format market data for agent prompts."""
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


def format_positions(positions: list) -> str:
    """Format positions for agent prompts (basic version)."""
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


def parse_json_response(text: str) -> dict:
    """Extract JSON decision from LLM response text.

    Tries in order: direct parse, markdown code block, raw JSON object.
    Returns a hold decision if all parsing fails.
    """
    # Try direct parse
    try:
        return json.loads(text.strip())
    except (json.JSONDecodeError, AttributeError):
        pass

    # Try markdown code block
    match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    # Try raw JSON object
    match = re.search(r"\{[^{}]*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    # Default to hold if parsing fails
    return {"action": "hold", "reasoning": "Failed to parse response"}
