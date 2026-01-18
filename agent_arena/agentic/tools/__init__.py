"""Trading tools for agentic traders."""

from agent_arena.agentic.tools.base import TradingTool
from agent_arena.agentic.tools.technical import TechnicalAnalysisTool
from agent_arena.agentic.tools.risk import RiskCalculatorTool
from agent_arena.agentic.tools.history import TradeHistoryTool
from agent_arena.agentic.tools.search import MarketSearchTool
from agent_arena.agentic.tools.rules import TradeRulesTool
from agent_arena.agentic.tools.multi_tf import MultiTimeframeTool
from agent_arena.agentic.tools.reflection import ReflectionTool
from agent_arena.agentic.tools.portfolio_risk import PortfolioRiskTool

# Learning-specific tools
from agent_arena.agentic.tools.similar_situations import SimilarSituationsTool
from agent_arena.agentic.tools.pattern_matcher import PatternMatcherTool
from agent_arena.agentic.tools.agent_performance import AgentPerformanceTool

# Skills tools (for Observer Agent integration)
from agent_arena.agentic.tools.skills import TradingSkillsTool, SkillRecommendationTool

__all__ = [
    "TradingTool",
    "TechnicalAnalysisTool",
    "RiskCalculatorTool",
    "TradeHistoryTool",
    "MarketSearchTool",
    "TradeRulesTool",
    "MultiTimeframeTool",
    "ReflectionTool",
    "PortfolioRiskTool",
    # Learning tools
    "SimilarSituationsTool",
    "PatternMatcherTool",
    "AgentPerformanceTool",
    # Skills tools
    "TradingSkillsTool",
    "SkillRecommendationTool",
]
