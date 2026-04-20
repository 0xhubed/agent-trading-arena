"""Agent implementations for Agent Arena."""

from agent_arena.agents.claude_trader import ClaudeTrader
from agent_arena.agents.gpt_trader import GPTTrader
from agent_arena.agents.ollama_trader import OllamaTrader
from agent_arena.agents.agentic_claude import AgenticClaudeTrader
from agent_arena.agents.agentic_llm import AgenticLLMTrader
from agent_arena.agents.llm_trader import LLMTrader
from agent_arena.agents.learning_trader import LearningTraderAgent
from agent_arena.agents.learning_trader_llm import LearningTraderLLM
from agent_arena.agents.skill_aware_trader import SkillAwareTrader, SkillOnlyTrader
from agent_arena.agents.skill_aware_llm import SkillAwareLLMTrader, SkillOnlyLLMTrader
from agent_arena.agents.observer_agent import ObserverAgent

# Backwards compatibility aliases
TogetherTrader = LLMTrader
AgenticTogetherTrader = AgenticLLMTrader
SkillAwareTogetherTrader = SkillAwareLLMTrader
SkillOnlyTogetherTrader = SkillOnlyLLMTrader
LearningTraderTogether = LearningTraderLLM

__all__ = [
    "ClaudeTrader",
    "GPTTrader",
    "OllamaTrader",
    "AgenticClaudeTrader",
    "AgenticLLMTrader",
    "LLMTrader",
    "LearningTraderAgent",
    "LearningTraderLLM",
    "SkillAwareTrader",
    "SkillOnlyTrader",
    "SkillAwareLLMTrader",
    "SkillOnlyLLMTrader",
    "ObserverAgent",
    # Backwards compatibility
    "TogetherTrader",
    "AgenticTogetherTrader",
    "SkillAwareTogetherTrader",
    "SkillOnlyTogetherTrader",
    "LearningTraderTogether",
]
