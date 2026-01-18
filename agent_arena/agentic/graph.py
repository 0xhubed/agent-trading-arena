"""LangGraph state machine for agentic trading."""

from __future__ import annotations

from typing import Any, Optional

from langgraph.graph import END, StateGraph

from agent_arena.agentic.nodes import AgentNodes
from agent_arena.agentic.state import AgentState


def create_trading_graph(
    llm: Any,
    tools: list,
    memory_store: Optional[Any] = None,
    system_prompt: Optional[str] = None,
) -> Any:
    """
    Create the LangGraph state machine for agentic trading.

    Graph structure:

    START
      |
      v
    [think] <--.
      |        |
      v        |
    [should_continue?]
      |        |
      | tools  | no tools / max iterations
      v        |
    [execute_tools]
      |        |
      '--------'
               |
               v
            [decide]
               |
               v
              END

    Args:
        llm: LangChain LLM with tools bound
        tools: List of TradingTool instances
        memory_store: Optional AgentMemoryStore for persistence
        system_prompt: Optional custom system prompt to prepend

    Returns:
        Compiled LangGraph graph
    """
    # Initialize nodes handler
    nodes = AgentNodes(llm=llm, tools=tools, memory_store=memory_store, system_prompt=system_prompt)

    # Create graph
    graph = StateGraph(AgentState)

    # Add nodes
    graph.add_node("think", nodes.think_node)
    graph.add_node("execute_tools", nodes.execute_tools_node)
    graph.add_node("decide", nodes.decide_node)

    # Set entry point
    graph.set_entry_point("think")

    # Add conditional edges from think node
    graph.add_conditional_edges(
        "think",
        nodes.should_continue,
        {
            "execute_tools": "execute_tools",
            "decide": "decide",
        },
    )

    # Tool execution loops back to think
    graph.add_edge("execute_tools", "think")

    # Decide ends the graph
    graph.add_edge("decide", END)

    return graph.compile()
