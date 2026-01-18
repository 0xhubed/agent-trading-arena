"""Agent implementations for Agent Arena."""

from agent_arena.agents.claude_trader import ClaudeTrader
from agent_arena.agents.gpt_trader import GPTTrader
from agent_arena.agents.ollama_trader import OllamaTrader
from agent_arena.agents.agentic_claude import AgenticClaudeTrader
from agent_arena.agents.agentic_together import AgenticTogetherTrader
from agent_arena.agents.learning_trader import LearningTraderAgent
from agent_arena.agents.learning_trader_together import LearningTraderTogether
from agent_arena.agents.skill_aware_trader import SkillAwareTrader, SkillOnlyTrader
from agent_arena.agents.skill_aware_together import SkillAwareTogetherTrader, SkillOnlyTogetherTrader
from agent_arena.agents.observer_agent import ObserverAgent

__all__ = [
    "ClaudeTrader",
    "GPTTrader",
    "OllamaTrader",
    "AgenticClaudeTrader",
    "AgenticTogetherTrader",
    "LearningTraderAgent",
    "LearningTraderTogether",
    "SkillAwareTrader",
    "SkillOnlyTrader",
    "SkillAwareTogetherTrader",
    "SkillOnlyTogetherTrader",
    "ObserverAgent",
]
