"""Analysis module for Agent Arena."""

from .bias_models import BiasProfile, BiasScore
from .bias_scan import (
    analyze_agent_biases,
    calculate_disposition_effect,
    calculate_loss_aversion,
    calculate_overconfidence,
)
from .contagion import (
    ContagionScore,
    ContagionSnapshot,
    analyze_contagion,
    calculate_position_diversity,
    calculate_reasoning_entropy,
    compute_system_health,
)
from .statistics import (
    AgentComparison,
    PatternValidation,
    calculate_max_drawdown,
    calculate_sharpe_ratio,
    compare_agents,
    validate_pattern,
)

__all__ = [
    "validate_pattern",
    "compare_agents",
    "calculate_sharpe_ratio",
    "calculate_max_drawdown",
    "PatternValidation",
    "AgentComparison",
    "BiasScore",
    "BiasProfile",
    "analyze_agent_biases",
    "calculate_disposition_effect",
    "calculate_loss_aversion",
    "calculate_overconfidence",
    "ContagionScore",
    "ContagionSnapshot",
    "analyze_contagion",
    "calculate_position_diversity",
    "calculate_reasoning_entropy",
    "compute_system_health",
]
