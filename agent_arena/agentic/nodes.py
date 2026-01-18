"""LangGraph nodes for agentic trading loop."""

from __future__ import annotations

import json
import re
from typing import Any, Optional

from langchain_core.messages import HumanMessage

from agent_arena.agentic.state import AgentState


class AgentNodes:
    """
    Nodes for the agentic trading graph.

    Implements ReAct-style reasoning:
    - think: Analyze situation, decide whether to use tools or make decision
    - execute_tools: Run requested tools and collect results
    - decide: Make final trading decision based on analysis
    """

    # Control token patterns that some models leak
    CONTROL_TOKEN_PATTERNS = [
        r'<\|start\|>',
        r'<\|end\|>',
        r'<\|channel\|>',
        r'<\|message\|>',
        r'<\|constrain\|>',
        r'<\|call\|>',
        r'<\|im_start\|>',
        r'<\|im_end\|>',
        r'<\|eot_id\|>',
        r'<\|begin_of_text\|>',
        r'<\|end_of_text\|>',
    ]

    def __init__(
        self,
        llm: Any,
        tools: list,
        memory_store: Optional[Any] = None,
        system_prompt: Optional[str] = None,
    ):
        """
        Initialize agent nodes.

        Args:
            llm: LangChain LLM instance (should have tools bound)
            tools: List of TradingTool instances
            memory_store: Optional AgentMemoryStore for persistence
            system_prompt: Optional custom system prompt to prepend
        """
        self.llm = llm
        self.tools = {t.name: t for t in tools}
        self.memory_store = memory_store
        self.custom_system_prompt = system_prompt

    def _sanitize_response(self, content: str) -> tuple[str, list[dict]]:
        """
        Sanitize model response by stripping control tokens and extracting tool calls.

        Some models (especially smaller open-source ones) leak internal control
        tokens instead of properly formatting tool calls. This method:
        1. Extracts tool call intent from control token patterns
        2. Strips control tokens from the content
        3. Returns cleaned content and any extracted tool calls

        Args:
            content: Raw model response content

        Returns:
            Tuple of (sanitized_content, extracted_tool_calls)
        """
        if not content:
            return "", []

        extracted_calls = []

        # Pattern to extract tool calls from control token format
        # Matches: commentary to=functions.tool_name <|constrain|>json<|message|>{...}<|call|>
        tool_pattern = r'(?:commentary\s+)?to=functions\.(\w+)\s*(?:<\|constrain\|>json)?(?:<\|message\|>)?(\{[^}]*\})?(?:<\|call\|>)?'
        matches = re.findall(tool_pattern, content, re.IGNORECASE)

        for match in matches:
            tool_name = match[0]
            args_str = match[1] if len(match) > 1 else "{}"

            # Only add if it's a valid tool
            if tool_name in self.tools:
                try:
                    args = json.loads(args_str) if args_str else {}
                except json.JSONDecodeError:
                    args = {}
                extracted_calls.append({"name": tool_name, "args": args})

        # Also try to extract from json blocks that might contain tool calls
        json_block_pattern = r'```(?:json)?\s*(\{[^`]*"name"\s*:\s*"(\w+)"[^`]*\})\s*```'
        json_matches = re.findall(json_block_pattern, content, re.DOTALL)
        for json_str, tool_name in json_matches:
            if tool_name in self.tools:
                try:
                    parsed = json.loads(json_str)
                    if isinstance(parsed, dict) and "name" in parsed:
                        extracted_calls.append({
                            "name": parsed["name"],
                            "args": parsed.get("args", parsed.get("arguments", {}))
                        })
                except json.JSONDecodeError:
                    pass

        # Strip all control tokens from content
        sanitized = content
        for pattern in self.CONTROL_TOKEN_PATTERNS:
            sanitized = re.sub(pattern, '', sanitized)

        # Clean up the commentary/function call syntax
        sanitized = re.sub(r'commentary\s+to=functions\.\w+\s*', '', sanitized)
        sanitized = re.sub(r'\{}\s*$', '', sanitized)  # Remove trailing empty braces

        # Clean up multiple spaces and newlines
        sanitized = re.sub(r'\n{3,}', '\n\n', sanitized)
        sanitized = re.sub(r' {2,}', ' ', sanitized)
        sanitized = sanitized.strip()

        # Deduplicate tool calls
        seen = set()
        unique_calls = []
        for call in extracted_calls:
            key = (call["name"], json.dumps(call["args"], sort_keys=True))
            if key not in seen:
                seen.add(key)
                unique_calls.append(call)

        return sanitized, unique_calls

    async def think_node(self, state: AgentState) -> dict:
        """
        Analyze the situation and decide next action.

        This is the reasoning step of ReAct - the agent considers
        what information it needs and whether to use tools.
        """
        context = state["context"]
        iteration = state.get("iteration", 0)
        tool_results = state.get("tool_results", [])
        memories = state.get("memories_retrieved", [])

        # Build the prompt
        system_prompt = self._build_system_prompt(context, memories)
        user_prompt = self._build_think_prompt(context, iteration, tool_results)

        messages = [HumanMessage(content=system_prompt + "\n\n" + user_prompt)]

        # Call LLM with tools bound
        response = await self.llm.ainvoke(messages)

        # Check if LLM wants to use tools (proper tool call format)
        tool_calls = []
        if hasattr(response, "tool_calls") and response.tool_calls:
            tool_calls = [
                {"name": tc["name"], "args": tc.get("args", {})}
                for tc in response.tool_calls
            ]

        # Sanitize response content and extract tool calls from control tokens
        # This handles models that leak internal tokens instead of proper tool calls
        raw_content = response.content or ""
        sanitized_content, extracted_calls = self._sanitize_response(raw_content)

        # If no proper tool calls but we extracted some from control tokens, use those
        if not tool_calls and extracted_calls:
            tool_calls = extracted_calls

        # Use sanitized content for thought (cleaner output)
        thought_content = sanitized_content[:500] if sanitized_content else "No reasoning provided"
        thought = f"Iteration {iteration + 1}: {thought_content}"
        thoughts = [thought]

        return {
            "messages": [response],
            "thoughts": thoughts,
            "tool_calls": tool_calls,
            "iteration": iteration + 1,
        }

    async def execute_tools_node(self, state: AgentState) -> dict:
        """Execute requested tools and collect results."""
        tool_calls = state.get("tool_calls", [])
        context = state["context"]

        if not tool_calls:
            return {"tool_results": [], "tool_calls": []}

        results = []

        for call in tool_calls:
            tool_name = call.get("name")
            tool_args = call.get("args", {})

            if tool_name in self.tools:
                tool = self.tools[tool_name]
                # Set context for tool
                tool.set_context(context)

                try:
                    # Try async first, fall back to sync
                    if hasattr(tool, "_arun"):
                        result = await tool._arun(**tool_args)
                    else:
                        result = tool._run(**tool_args)

                    results.append(f"[{tool_name}]\n{result}")
                except Exception as e:
                    results.append(f"[{tool_name}] Error: {str(e)}")
            else:
                results.append(f"[{tool_name}] Tool not found")

        return {
            "tool_results": results,
            "tool_calls": [],  # Clear after execution
        }

    async def decide_node(self, state: AgentState) -> dict:
        """Make final trading decision based on analysis."""
        context = state["context"]
        thoughts = state.get("thoughts", [])
        tool_results = state.get("tool_results", [])

        # Build decision prompt
        prompt = self._build_decision_prompt(context, thoughts, tool_results)

        response = await self.llm.ainvoke([HumanMessage(content=prompt)])

        # Parse decision from response
        decision = self._parse_decision(response.content, context)

        return {
            "decision": decision,
            "should_continue": False,
            "messages": [response],
        }

    def should_continue(self, state: AgentState) -> str:
        """
        Determine whether to continue the think-act loop or decide.

        Enforces minimum tool usage to prevent agents from skipping analysis.

        Returns:
            "execute_tools" if there are tool calls to execute
            "decide" if ready to make final decision
        """
        iteration = state.get("iteration", 0)
        max_iterations = state.get("max_iterations", 3)
        tool_calls = state.get("tool_calls", [])
        tool_results = state.get("tool_results", [])

        # Minimum tool usage requirement
        # Agents must use at least 2 tools before deciding
        min_tool_calls = 2

        # Continue if:
        # 1. Haven't reached max iterations
        # 2. There are tool calls to execute
        if iteration < max_iterations and tool_calls:
            return "execute_tools"

        # If agent hasn't used enough tools and hasn't maxed iterations,
        # the think node will nudge them to use more tools
        if len(tool_results) < min_tool_calls and iteration < max_iterations:
            # Agent didn't request tools but hasn't met minimum
            # Return to think to encourage tool usage
            return "execute_tools"  # Will be empty but forces another think cycle

        return "decide"

    def _build_system_prompt(self, context: dict, memories: list) -> str:
        """Build system prompt for the agent."""
        # Start with custom prompt if provided
        custom_section = ""
        if self.custom_system_prompt:
            custom_section = self.custom_system_prompt + "\n\n---\n\n"

        portfolio = context.get("portfolio", {})
        positions = portfolio.get("positions", [])

        positions_str = "None"
        if positions:
            positions_str = "\n".join([
                f"  - {p['symbol']} {p['side'].upper()} @ ${p['entry_price']:,.2f} "
                f"(P&L: ${p['unrealized_pnl']:+,.2f}, {p['roe_percent']:+.1f}%)"
                for p in positions
            ])

        # Format recent memories if available
        memories_str = ""
        if memories:
            memories_str = "\n\nRECENT MEMORIES:\n" + "\n".join([
                f"- [{m.get('memory_type', 'unknown')}] {m.get('content', '')[:100]}..."
                for m in memories[:5]
            ])

        base_prompt = f"""You are an AI trading agent in Agent Arena, a crypto futures competition.

CURRENT PORTFOLIO STATUS:
- Equity: ${portfolio.get('equity', 10000):,.2f}
- Available Margin: ${portfolio.get('available_margin', 10000):,.2f}
- Total P&L: ${portfolio.get('total_pnl', 0):+,.2f} ({portfolio.get('pnl_percent', 0):+.2f}%)

CURRENT POSITIONS:
{positions_str}

AVAILABLE TOOLS (must use at least 2 before trading):
1. validate_trade - REQUIRED before any open/close. Checks hold periods, trends
2. reflect_on_performance - Analyzes patterns, detects churning and symbol bias
3. technical_analysis - Calculate RSI, SMA, MACD, Bollinger Bands for any symbol
4. multi_timeframe_analysis - Check trend alignment across 15m, 1h, 4h timeframes
5. portfolio_risk_analysis - Analyze exposure, concentration, and position sizing
6. risk_calculator - Calculate position sizing, stop-loss, and risk/reward ratio
7. trade_history - Query your past trades and performance metrics
8. market_search - Get Fear & Greed Index, funding rates, and market sentiment

MANDATORY WORKFLOW:
1. Call reflect_on_performance FIRST to learn from recent trades
2. Call validate_trade before any open/close action
3. Use technical_analysis or multi_timeframe_analysis to confirm direction

TRADING RULES:
- Maximum leverage: 10x
- Maximum position size: 25% of equity per symbol
- Trading fee: 0.04% per trade
- One position per symbol at a time
{memories_str}

Think step by step. Use tools to gather information before making trading decisions.
If you need more information, call the appropriate tool.
If you have enough information, explain your reasoning and make a decision."""

        return custom_section + base_prompt

    def _build_think_prompt(
        self,
        context: dict,
        iteration: int,
        tool_results: list[str],
    ) -> str:
        """Build prompt for thinking step."""
        market = context.get("market", {})
        tick = context.get("tick", 0)

        # Format market data
        market_lines = []
        for symbol, data in market.items():
            price = float(data.get("price", 0))
            change = data.get("change_24h", 0)
            funding = data.get("funding_rate")
            funding_str = f" | Funding: {float(funding)*100:.4f}%" if funding else ""
            market_lines.append(f"  {symbol}: ${price:,.2f} ({change:+.2f}%){funding_str}")

        market_str = "\n".join(market_lines) if market_lines else "No market data"

        # Format tool results if any
        results_context = ""
        if tool_results:
            results_context = "\n\nPREVIOUS TOOL RESULTS:\n" + "\n\n".join(tool_results[-3:])

        return f"""TICK {tick} - Analyze the market and decide your next action.

CURRENT MARKET DATA:
{market_str}
{results_context}

This is iteration {iteration + 1}. You MUST use at least 2 tools before making a decision.

RECOMMENDED TOOL SEQUENCE:
1. reflect_on_performance - Check for patterns to avoid
2. validate_trade - Validate any trade you're considering
3. technical_analysis or multi_timeframe_analysis - Confirm direction

What tools do you want to use? Call at least one tool now."""

    def _build_decision_prompt(
        self,
        context: dict,
        thoughts: list[str],
        tool_results: list[str],
    ) -> str:
        """Build prompt for final decision."""
        market = context.get("market", {})
        symbols = list(market.keys())

        # Format analysis summary
        analysis = "\n".join(thoughts) if thoughts else "No analysis recorded"

        # Format tool results (last 5)
        results = "\n\n".join(tool_results[-5:]) if tool_results else "No tool results"

        return f"""Based on your analysis, make a final trading decision.

YOUR ANALYSIS:
{analysis}

TOOL RESULTS:
{results}

AVAILABLE SYMBOLS: {', '.join(symbols)}

RESPOND WITH ONLY A JSON OBJECT. NO OTHER TEXT BEFORE OR AFTER.

Example valid response:
{{"action": "hold", "symbol": null, "size": null, "leverage": 1, "confidence": 0.5, "reasoning": "Market uncertain"}}

Required fields:
- action: "hold" | "open_long" | "open_short" | "close"
- symbol: one of {', '.join(symbols)} (null for hold)
- size: number (null for hold/close)
- leverage: 1-10
- confidence: 0.0-1.0
- reasoning: brief explanation

YOUR JSON RESPONSE:"""

    def _parse_decision(self, response: Any, context: dict) -> dict:
        """Parse decision from LLM response with robust error handling."""
        market = context.get("market", {})
        valid_symbols = list(market.keys())

        # Default decision
        default = {
            "action": "hold",
            "symbol": None,
            "size": None,
            "leverage": 1,
            "confidence": 0.0,
            "reasoning": "Failed to parse decision",
        }

        if not response:
            return default

        # Handle list response (Claude can return list of content blocks)
        if isinstance(response, list):
            # Extract text content from list
            text_parts = []
            for item in response:
                if isinstance(item, dict) and item.get("type") == "text":
                    text_parts.append(item.get("text", ""))
                elif isinstance(item, str):
                    text_parts.append(item)
            response = "\n".join(text_parts)

        if not isinstance(response, str):
            default["reasoning"] = f"Unexpected response type: {type(response)}"
            return default

        # Try multiple parsing strategies
        json_str = self._extract_json_string(response)
        if json_str:
            try:
                decision = json.loads(json_str)
                return self._validate_decision(decision, valid_symbols)
            except json.JSONDecodeError:
                # Try to fix common JSON errors
                fixed_json = self._fix_json_errors(json_str)
                if fixed_json:
                    try:
                        decision = json.loads(fixed_json)
                        return self._validate_decision(decision, valid_symbols)
                    except json.JSONDecodeError:
                        pass

        # Last resort: try to extract decision from natural language
        extracted = self._extract_decision_from_text(response, valid_symbols)
        if extracted:
            return extracted

        # Complete failure - return default with raw response
        default["reasoning"] = f"Could not parse JSON. Raw response: {response}"
        return default

    def _extract_json_string(self, response: str) -> str | None:
        """Extract JSON string from response using multiple strategies."""
        # Strategy 1: Direct JSON parse (response is pure JSON)
        response_stripped = response.strip()
        if response_stripped.startswith("{") and response_stripped.endswith("}"):
            return response_stripped

        # Strategy 2: Extract from markdown code block
        match = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", response)
        if match:
            return match.group(1)

        # Strategy 3: Find JSON object with nested braces support
        # Use a more sophisticated regex that handles nested structures
        match = re.search(r"\{(?:[^{}]|\{[^{}]*\})*\}", response, re.DOTALL)
        if match:
            return match.group()

        # Strategy 4: Find JSON starting with known fields
        for field in ["action", "symbol", "confidence"]:
            pattern = rf'\{{\s*"{field}"[^{{}}]*\}}'
            match = re.search(pattern, response, re.DOTALL | re.IGNORECASE)
            if match:
                return match.group()

        # Strategy 5: Extract JSON that may be preceded/followed by text
        # Look for pattern like: ... {"action": ... } ...
        match = re.search(r'(\{[^{}]*"action"[^{}]*\})', response, re.DOTALL)
        if match:
            return match.group(1)

        return None

    def _fix_json_errors(self, json_str: str) -> str | None:
        """Attempt to fix common JSON errors from cheap models."""
        if not json_str:
            return None

        fixed = json_str

        # Fix 1: Remove trailing commas before closing brace
        fixed = re.sub(r",\s*}", "}", fixed)
        fixed = re.sub(r",\s*]", "]", fixed)

        # Fix 2: Replace single quotes with double quotes (careful with apostrophes)
        # Only replace single quotes that look like JSON string delimiters
        fixed = re.sub(r"(?<=[{,:\[])\s*'([^']*?)'\s*(?=[},:\]])", r'"\1"', fixed)

        # Fix 3: Add quotes around unquoted keys
        fixed = re.sub(r'(?<=[{,])\s*(\w+)\s*:', r'"\1":', fixed)

        # Fix 4: Replace Python None/True/False with JSON equivalents
        fixed = re.sub(r"\bNone\b", "null", fixed)
        fixed = re.sub(r"\bTrue\b", "true", fixed)
        fixed = re.sub(r"\bFalse\b", "false", fixed)

        # Fix 5: Remove newlines inside string values
        # This is tricky - we need to preserve the JSON structure
        # Simple approach: replace \n inside quoted strings
        def fix_newlines_in_strings(match):
            return match.group().replace("\n", " ").replace("  ", " ")

        fixed = re.sub(r'"[^"]*"', fix_newlines_in_strings, fixed)

        # Fix 6: Handle escaped quotes that shouldn't be escaped
        fixed = fixed.replace('\\"', '"').replace('""', '"')
        # Re-escape internal quotes properly
        # This is complex, skip for now

        return fixed if fixed != json_str else None

    def _extract_decision_from_text(self, response: str, valid_symbols: list) -> dict | None:
        """Extract decision from natural language when JSON parsing fails."""
        response_lower = response.lower()

        # Try to determine action
        action = "hold"
        if any(word in response_lower for word in ["open long", "buy", "go long", "bullish"]):
            action = "open_long"
        elif any(word in response_lower for word in ["open short", "sell", "go short", "bearish"]):
            action = "open_short"
        elif any(word in response_lower for word in ["close", "exit", "take profit", "stop loss"]):
            action = "close"

        # Try to find symbol
        symbol = None
        for sym in valid_symbols:
            if sym.lower() in response_lower or sym.replace("USDT", "").lower() in response_lower:
                symbol = sym
                break

        # Try to find confidence (look for percentages or decimals)
        confidence = 0.5
        conf_match = re.search(r"confidence[:\s]*(\d+(?:\.\d+)?)\s*%?", response_lower)
        if conf_match:
            conf_val = float(conf_match.group(1))
            confidence = conf_val / 100 if conf_val > 1 else conf_val

        # Try to find leverage
        leverage = 1
        lev_match = re.search(r"leverage[:\s]*(\d+)x?", response_lower)
        if lev_match:
            leverage = min(10, max(1, int(lev_match.group(1))))

        # If we found an action other than hold, return the extracted decision
        if action != "hold" or "hold" in response_lower:
            return {
                "action": action,
                "symbol": symbol,
                "size": None,
                "leverage": leverage,
                "confidence": confidence,
                "reasoning": f"Extracted from text: {response[:200]}...",
            }

        return None

    def _validate_decision(self, decision: dict, valid_symbols: list) -> dict:
        """Validate and normalize decision."""
        # Validate action
        action = decision.get("action", "hold").lower()
        if action not in ["hold", "open_long", "open_short", "close"]:
            action = "hold"

        # Validate symbol
        symbol = decision.get("symbol")
        if symbol and symbol not in valid_symbols:
            # Try to match partial
            for vs in valid_symbols:
                if symbol.upper() in vs or vs in symbol.upper():
                    symbol = vs
                    break
            else:
                symbol = None

        # Validate size
        size = decision.get("size")
        if size is not None:
            try:
                size = float(size)
                if size <= 0:
                    size = None
            except (ValueError, TypeError):
                size = None

        # Validate leverage
        leverage = decision.get("leverage", 1)
        try:
            leverage = int(leverage)
            leverage = max(1, min(10, leverage))
        except (ValueError, TypeError):
            leverage = 1

        # Validate confidence
        confidence = decision.get("confidence", 0.5)
        try:
            confidence = float(confidence)
            confidence = max(0.0, min(1.0, confidence))
        except (ValueError, TypeError):
            confidence = 0.5

        return {
            "action": action,
            "symbol": symbol,
            "size": size,
            "leverage": leverage,
            "confidence": confidence,
            "reasoning": decision.get("reasoning", ""),
        }
