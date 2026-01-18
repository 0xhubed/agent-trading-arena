"""Agent state definition for LangGraph."""

from typing import Annotated, Optional, Sequence

from langchain_core.messages import BaseMessage
from typing_extensions import TypedDict


def add_messages(left: list, right: list) -> list:
    """Merge message lists."""
    return left + right


def add_strings(left: list, right: list) -> list:
    """Merge string lists."""
    return left + right


class AgentState(TypedDict):
    """
    State for the agentic trading loop.

    This state flows through the graph:
    think -> select_tool -> execute_tool -> observe -> (loop or decide)

    Attributes:
        context: Market data, portfolio, tick info
        agent_id: Unique agent identifier
        messages: Conversation history (LangChain messages)
        tool_calls: Tools requested by LLM this iteration
        tool_results: Results from tool executions
        thoughts: Agent's reasoning steps
        iteration: Current iteration in think-act loop
        max_iterations: Maximum allowed iterations (default 3)
        should_continue: Whether to continue the loop
        decision: Final trading decision
        memories_retrieved: Memories loaded from store
    """

    # Input context (set at start)
    context: dict
    agent_id: str

    # Conversation/reasoning (accumulated)
    messages: Annotated[Sequence[BaseMessage], add_messages]

    # Tool execution (reset each iteration)
    tool_calls: list[dict]
    tool_results: Annotated[list[str], add_strings]

    # Reasoning trace (accumulated)
    thoughts: Annotated[list[str], add_strings]

    # Loop control
    iteration: int
    max_iterations: int
    should_continue: bool

    # Output
    decision: Optional[dict]

    # Memory
    memories_retrieved: list[dict]
