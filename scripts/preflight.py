#!/usr/bin/env python3
"""Preflight checks for Agent Arena.

Tests all LLM-dependent agent types against the local inference server
before starting the full competition. Run after vLLM warmup.

Usage:
    python scripts/preflight.py
    python scripts/preflight.py --base-url http://192.168.0.42:4000/v1
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time

import httpx

# Defaults
DEFAULT_BASE_URL = "http://192.168.0.42:4000/v1"
DEFAULT_API_KEY_ENV = "LOCAL_API_KEY"
MODELS = {
    "nemotron": "nemotron",
    "glm": "glm-4.7-flash",
}
TIMEOUT = 30.0
PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
SKIP = "\033[93mSKIP\033[0m"


def strip_think(text: str) -> str:
    """Strip <think>...</think> blocks."""
    return re.sub(r".*?</think>\s*", "", text, count=1, flags=re.DOTALL)


def extract_json(text: str) -> dict | None:
    """Extract JSON from LLM response (same logic as LLMTrader)."""
    cleaned = strip_think(text)
    try:
        return json.loads(cleaned)
    except (json.JSONDecodeError, ValueError):
        pass
    match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", cleaned, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except (json.JSONDecodeError, ValueError):
            pass
    match = re.search(
        r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", cleaned, re.DOTALL
    )
    if match:
        try:
            return json.loads(match.group())
        except (json.JSONDecodeError, ValueError):
            pass
    return None


async def check_connectivity(
    client: httpx.AsyncClient, base_url: str, api_key: str
) -> dict[str, bool]:
    """Test basic connectivity to both models."""
    results = {}
    for label, model in MODELS.items():
        t0 = time.monotonic()
        try:
            r = await client.post(
                f"{base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": "Say ok"}],
                    "max_tokens": 10,
                },
            )
            r.raise_for_status()
            elapsed = time.monotonic() - t0
            content = r.json()["choices"][0]["message"]["content"]
            results[label] = True
            print(f"  [{PASS}] {label:10s} responded in {elapsed:.2f}s")
        except Exception as e:
            elapsed = time.monotonic() - t0
            results[label] = False
            print(f"  [{FAIL}] {label:10s} error after {elapsed:.2f}s: {e}")
    return results


async def check_json_parsing(
    client: httpx.AsyncClient, base_url: str, api_key: str
) -> dict[str, bool]:
    """Test simple trader JSON parsing (both models)."""
    prompt = (
        "MARKET: PF_XBTUSD $97000 (+1.2%)\n"
        'Respond ONLY with JSON: {"action":"hold"|"open_long"|"open_short",'
        '"symbol":"PF_XBTUSD","size":0.01,"leverage":2,'
        '"confidence":0.75,"reasoning":"brief"}'
    )
    results = {}
    for label, model in MODELS.items():
        t0 = time.monotonic()
        try:
            r = await client.post(
                f"{base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a crypto futures trader. "
                            "Respond only with valid JSON.",
                        },
                        {"role": "user", "content": prompt},
                    ],
                    "max_tokens": 4096,
                    "temperature": 0.7,
                },
            )
            r.raise_for_status()
            elapsed = time.monotonic() - t0
            raw = r.json()["choices"][0]["message"]["content"]
            parsed = extract_json(raw)
            if parsed and "action" in parsed:
                results[label] = True
                print(
                    f"  [{PASS}] {label:10s} "
                    f"action={parsed['action']} "
                    f"({elapsed:.2f}s)"
                )
            else:
                results[label] = False
                print(
                    f"  [{FAIL}] {label:10s} "
                    f"could not parse JSON ({elapsed:.2f}s)"
                )
                print(f"         raw: {raw[:120]}...")
        except Exception as e:
            elapsed = time.monotonic() - t0
            results[label] = False
            print(f"  [{FAIL}] {label:10s} error ({elapsed:.2f}s): {e}")
    return results


async def check_tool_calling(
    client: httpx.AsyncClient, base_url: str, api_key: str
) -> dict[str, bool]:
    """Test agentic tool calling (both models)."""
    tools = [
        {
            "type": "function",
            "function": {
                "name": "calculate_rsi",
                "description": "Calculate RSI for a trading symbol",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "symbol": {
                            "type": "string",
                            "description": "Trading symbol",
                        },
                        "period": {
                            "type": "integer",
                            "description": "RSI period",
                            "default": 14,
                        },
                    },
                    "required": ["symbol"],
                },
            },
        }
    ]
    results = {}
    for label, model in MODELS.items():
        t0 = time.monotonic()
        try:
            r = await client.post(
                f"{base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a trading assistant. "
                            "Use tools when needed.",
                        },
                        {
                            "role": "user",
                            "content": "Calculate RSI for PF_XBTUSD",
                        },
                    ],
                    "tools": tools,
                    "tool_choice": "auto",
                    "max_tokens": 512,
                },
            )
            r.raise_for_status()
            elapsed = time.monotonic() - t0
            data = r.json()
            msg = data["choices"][0]["message"]

            # Check for proper tool_calls in response
            tool_calls = msg.get("tool_calls", [])
            if tool_calls:
                tc = tool_calls[0]
                fn = tc.get("function", {})
                results[label] = True
                print(
                    f"  [{PASS}] {label:10s} "
                    f"tool={fn.get('name')} "
                    f"args={fn.get('arguments')} "
                    f"({elapsed:.2f}s)"
                )
            else:
                # Tool call not parsed — check if it's in the content
                content = msg.get("content", "")
                has_xml_tool = (
                    "<tool_call>" in content
                    or "<function=" in content
                    or "<arg_key>" in content
                )
                results[label] = False
                if has_xml_tool:
                    print(
                        f"  [{FAIL}] {label:10s} "
                        f"tool call in content (parser not working) "
                        f"({elapsed:.2f}s)"
                    )
                else:
                    print(
                        f"  [{FAIL}] {label:10s} "
                        f"no tool call in response ({elapsed:.2f}s)"
                    )
                    print(f"         content: {content[:120]}...")
        except Exception as e:
            elapsed = time.monotonic() - t0
            results[label] = False
            print(f"  [{FAIL}] {label:10s} error ({elapsed:.2f}s): {e}")
    return results


async def check_forum_generation(
    client: httpx.AsyncClient, base_url: str, api_key: str
) -> dict[str, bool]:
    """Test forum post generation (MarketAnalyst + Contrarian)."""
    tests = {
        "analyst": {
            "model": MODELS["nemotron"],
            "system": (
                "You are a market analyst agent posting in a crypto "
                "trading forum. Write concise technical analysis. "
                "Keep under 200 words. No emojis."
            ),
            "user": (
                "Symbol: PF_XBTUSD\nPrice: $97,000\n"
                "24h Change: +1.20%\nRSI(14): 62.3\n"
                "Trend: bullish\n\n"
                "Write a market analysis post."
            ),
        },
        "contrarian": {
            "model": MODELS["glm"],
            "system": (
                "You are a contrarian trading agent in a crypto "
                "futures forum. Challenge the consensus. "
                "Keep under 150 words. No emojis."
            ),
            "user": (
                "Consensus: 80% bullish\n"
                "Funding Rate: 0.0400%\n"
                "24h Change: +5.20%\n\n"
                "Write a contrarian challenge post."
            ),
        },
    }
    results = {}
    for label, cfg in tests.items():
        t0 = time.monotonic()
        try:
            r = await client.post(
                f"{base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": cfg["model"],
                    "messages": [
                        {"role": "system", "content": cfg["system"]},
                        {"role": "user", "content": cfg["user"]},
                    ],
                    "max_tokens": 400,
                    "temperature": 0.8,
                },
            )
            r.raise_for_status()
            elapsed = time.monotonic() - t0
            content = r.json()["choices"][0]["message"]["content"]
            cleaned = strip_think(content).strip()
            word_count = len(cleaned.split())
            if word_count >= 10:
                results[label] = True
                print(
                    f"  [{PASS}] {label:10s} "
                    f"{word_count} words ({elapsed:.2f}s)"
                )
            else:
                results[label] = False
                print(
                    f"  [{FAIL}] {label:10s} "
                    f"too short ({word_count} words, {elapsed:.2f}s)"
                )
        except Exception as e:
            elapsed = time.monotonic() - t0
            results[label] = False
            print(f"  [{FAIL}] {label:10s} error ({elapsed:.2f}s): {e}")
    return results


async def main(base_url: str, api_key: str) -> int:
    """Run all preflight checks. Returns 0 if all pass, 1 otherwise."""
    print(f"\n{'='*60}")
    print("  AGENT ARENA — PREFLIGHT CHECKS")
    print(f"  Server: {base_url}")
    print(f"{'='*60}\n")

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        # 1. Connectivity
        print("1. Connectivity")
        conn = await check_connectivity(client, base_url, api_key)
        print()

        # Bail early if both models are down
        if not any(conn.values()):
            print(f"  [{FAIL}] Both models unreachable. Aborting.\n")
            return 1

        # 2. JSON Parsing (Simple Traders)
        print("2. JSON Parsing (Simple Traders)")
        json_ok = await check_json_parsing(client, base_url, api_key)
        print()

        # 3. Tool Calling (Agentic Traders)
        print("3. Tool Calling (Agentic Traders)")
        tools_ok = await check_tool_calling(client, base_url, api_key)
        print()

        # 4. Forum Post Generation (Discussion Agents)
        print("4. Forum Generation (Discussion Agents)")
        forum_ok = await check_forum_generation(client, base_url, api_key)
        print()

    # Summary — use list to avoid key collisions
    all_results = (
        [("conn/" + k, v) for k, v in conn.items()]
        + [("json/" + k, v) for k, v in json_ok.items()]
        + [("tools/" + k, v) for k, v in tools_ok.items()]
        + [("forum/" + k, v) for k, v in forum_ok.items()]
    )
    passed = sum(1 for _, v in all_results if v)
    total = len(all_results)
    failed = total - passed

    print(f"{'='*60}")
    if failed == 0:
        print(f"  ALL {total} CHECKS PASSED — ready to start")
    else:
        print(f"  {passed}/{total} passed, {failed} failed")
    print(f"{'='*60}\n")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Agent Arena preflight checks"
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"LiteLLM proxy URL (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--api-key-env",
        default=DEFAULT_API_KEY_ENV,
        help=f"Env var for API key (default: {DEFAULT_API_KEY_ENV})",
    )
    args = parser.parse_args()

    api_key = os.environ.get(args.api_key_env, "")

    exit_code = asyncio.run(main(args.base_url, api_key))
    sys.exit(exit_code)
