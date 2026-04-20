"""Base agent interface for Agent Arena."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from agent_arena.core.models import Decision


class BaseAgent(ABC):
    """
    Minimal interface for trading agents.

    The system doesn't care how you implement decide().
    Use any LLM, any prompting strategy, any architecture.
    Just return a Decision.
    """

    # Agent type mapping based on class name patterns
    # Order matters - more specific patterns should come first
    _TYPE_MAP = {
        # Learning agents (RAG-based)
        "LearningTrader": ("Learning", "RAG + Pattern Matching + Meta-Learning"),
        # Journal-aware agents (must be before ForumAware)
        "JournalAware": ("Journal-Aware", "Skills + Forum Witness + Journal + Tools"),
        # Forum-aware agents (must be before SkillAware and LLMTrader)
        "ForumAware": ("Forum-Aware", "Skills + Forum Witness + Tools"),
        # Skill-based agents
        "SkillOnly": ("Skill-Only", "Pure Skill Guidance"),
        "SkillAware": ("Skill-Aware", "Learned Skills + Tools"),
        # Agentic agents
        "Agentic": ("Agentic", "LangGraph ReAct + Tools"),
        "LangChain": ("LangChain", "LangChain Framework"),
        # Rule-based agents
        "TATrader": ("TA", "RSI + SMA Technical Analysis"),
        "IndexFund": ("Passive", "Equal-Weight Buy & Hold"),
        # Simple LLM agents
        "ClaudeTrader": ("LLM", "Claude API"),
        "GPTTrader": ("LLM", "OpenAI API"),
        "LLMTrader": ("LLM", "OpenAI-Compatible API"),
        "OllamaTrader": ("LLM", "Local Ollama"),
        # Fallbacks (less specific)
        "Claude": ("LLM", "Claude API"),
        "GPT": ("LLM", "OpenAI API"),
        "Together": ("LLM", "OpenAI-Compatible API"),
        "Ollama": ("LLM", "Local Inference"),
    }

    def __init__(self, agent_id: str, name: str, config: Optional[dict] = None):
        self.agent_id = agent_id
        self.name = name
        self.config = config or {}
        self.max_confidence = self.config.get("max_confidence", 1.0)

    @property
    def agent_type(self) -> str:
        """Short agent type category (e.g., 'Agentic', 'LLM', 'TA', 'Passive')."""
        class_name = self.__class__.__name__
        for pattern, (type_name, _) in self._TYPE_MAP.items():
            if pattern in class_name:
                return type_name
        return "Custom"

    @property
    def agent_type_description(self) -> str:
        """Short description of agent approach (e.g., 'LangGraph ReAct + Tools')."""
        class_name = self.__class__.__name__
        for pattern, (_, description) in self._TYPE_MAP.items():
            if pattern in class_name:
                return description
        return "Custom Implementation"

    @abstractmethod
    async def decide(self, context: dict) -> Decision:
        """
        Make a trading decision based on context.

        Args:
            context: Dict containing market data, portfolio state,
                    and any configured enrichments (news, memory, etc.)

        Returns:
            Decision with action, reasoning, and confidence
        """
        pass

    async def on_start(self) -> None:
        """Called when competition starts. Override for setup."""
        pass

    async def on_stop(self) -> None:
        """Called when competition ends. Override for cleanup."""
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.agent_id!r}, name={self.name!r})"
