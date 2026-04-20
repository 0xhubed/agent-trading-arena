"""AgentGenome — encodes a trading agent's full configuration as an evolvable genome.

Extended ("HarnessGenome") with prompt architecture, tool selection, integration
toggles, and risk envelope parameters for the self-evolving experiment loop.
"""

from __future__ import annotations

import copy
import random
import uuid
from dataclasses import dataclass, field
from typing import Any

# Character templates — personality axes for mutation without LLM-in-the-loop.
# Each template is a short trading personality description.
CHARACTER_TEMPLATES = [
    (
        "Aggressive momentum trader. Follows trends hard, "
        "sizes up on conviction, cuts losers fast."
    ),
    (
        "Conservative value seeker. Waits for deep pullbacks, "
        "uses tight position sizes, prioritizes capital preservation."
    ),
    (
        "Contrarian thinker. Fades crowd sentiment, "
        "buys fear and sells greed. Patience is the edge."
    ),
    (
        "Technical precision trader. Relies on RSI, MACD, "
        "and Bollinger Bands. Executes mechanically on signals."
    ),
    (
        "Risk-adjusted optimizer. Maximizes Sharpe ratio "
        "over raw returns. Never risks more than 2% per trade."
    ),
    (
        "Scalp-oriented trader. Takes small profits frequently, "
        "avoids overnight exposure, loves high-volume setups."
    ),
    (
        "Macro-aware swing trader. Reads funding rates "
        "and market structure. Holds positions for multiple ticks."
    ),
    (
        "Adaptive regime trader. Trends in trending markets, "
        "mean-reverts in ranging markets. Adjusts strategy."
    ),
    (
        "Volatility harvester. Sells premium in calm markets, "
        "reduces size in chaos. Thrives on mean reversion."
    ),
    (
        "Breakout specialist. Watches consolidation patterns, "
        "enters on volume confirmation, rides momentum."
    ),
]

# Prompt templates — different system prompt structures for trading agents.
PROMPT_TEMPLATES = [
    "default",          # Standard prompt (agent's built-in)
    "concise",          # Shorter, more direct instructions
    "analytical",       # Emphasizes technical analysis reasoning
    "risk_first",       # Leads with risk constraints before opportunity
    "contrarian",       # Frames decisions as contrarian evaluation
]

# Available agentic tools (subset selection for agentic traders)
ALL_AGENTIC_TOOLS = [
    "technical",        # RSI, SMA, MACD, Bollinger Bands
    "risk",             # Position sizing, stop-loss, R:R ratio
    "history",          # Trade history queries
    "search",           # Fear & Greed Index, sentiment
    "reflection",       # Trade reflection and lesson extraction
    "multi_tf",         # Multi-timeframe analysis
    "skills",           # Skill reading/recommendation
    "rules",            # Trading rules enforcement
    "portfolio_risk",   # Portfolio-level risk analysis
    "agent_performance",  # Self-performance analysis
    "pattern_matcher",  # Recurring pattern recognition
    "similar_situations",  # RAG-based situation retrieval
]

# Agent classes allowed in genome evolution
ALLOWED_AGENT_CLASSES = [
    "agent_arena.agents.llm_trader.LLMTrader",
    "agent_arena.agents.skill_aware_llm.SkillAwareLLMTrader",
    "agent_arena.agents.forum_aware_llm.ForumAwareLLMTrader",
    "agent_arena.agents.journal_aware_llm.JournalAwareLLMTrader",
    "agent_arena.agents.agentic_llm.AgenticLLMTrader",
]


# Parameter bounds: (min, max) for continuous, list of options for categorical.
PARAM_BOUNDS: dict[str, Any] = {
    # Original 9 parameters
    "model": ["glm-5", "gpt-oss-120b"],
    "temperature": (0.1, 1.0),
    "max_tokens": (512, 4000),
    "character": CHARACTER_TEMPLATES,
    "confidence_threshold": (0.3, 0.9),
    "position_size_pct": (0.05, 0.25),
    "sl_pct": (0.01, 0.05),
    "tp_pct": (0.02, 0.10),
    "max_leverage": (1, 10),
    # Prompt architecture
    "prompt_template": PROMPT_TEMPLATES,
    "system_prefix": [
        "",
        "You are a cautious trader. ",
        "You are an aggressive trader. ",
        "Think step by step before every trade. ",
        "Focus on risk-reward ratio above all else. ",
    ],
    # Tool selection (agentic agents only)
    "enabled_tools": ALL_AGENTIC_TOOLS,  # subset selection
    "max_iterations": (1, 5),
    # Integration toggles
    "use_skills": [True, False],
    "skill_weight": (0.0, 1.0),
    "use_forum": [True, False],
    "forum_weight": (0.0, 1.0),
    "use_journal": [True, False],
    # Risk envelope
    "max_concurrent_positions": (1, 5),
    "correlation_threshold": (0.3, 0.9),
    "funding_sensitivity": (0.0, 1.0),
    # Agent class
    "agent_class": ALLOWED_AGENT_CLASSES,
}


@dataclass
class AgentGenome:
    """Evolvable representation of a trading agent's configuration.

    Extended with prompt architecture, tool selection, integration toggles,
    and risk envelope parameters beyond the original 9.
    """

    genome_id: str = ""
    agent_class: str = "agent_arena.agents.llm_trader.LLMTrader"
    generation: int = 0

    # Model config (original)
    model: str = "glm-5"
    temperature: float = 0.7
    max_tokens: int = 1024
    character: str = ""

    # Trading parameters (original)
    confidence_threshold: float = 0.5
    position_size_pct: float = 0.15
    sl_pct: float = 0.02
    tp_pct: float = 0.04
    max_leverage: int = 5

    # Prompt architecture (new)
    prompt_template: str = "default"
    system_prefix: str = ""

    # Tool selection for agentic agents (new)
    enabled_tools: list[str] = field(default_factory=lambda: list(ALL_AGENTIC_TOOLS))
    max_iterations: int = 3

    # Integration toggles (new)
    use_skills: bool = True
    skill_weight: float = 0.5
    use_forum: bool = True
    forum_weight: float = 0.5
    use_journal: bool = True

    # Risk envelope (new)
    max_concurrent_positions: int = 3
    correlation_threshold: float = 0.7
    funding_sensitivity: float = 0.5

    # Lineage
    parent_ids: list[str] = field(default_factory=list)
    mutations: list[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.genome_id:
            self.genome_id = f"g_{uuid.uuid4().hex[:12]}"

    @classmethod
    def randomize(
        cls,
        agent_class: str = "agent_arena.agents.llm_trader.LLMTrader",
        generation: int = 0,
    ) -> AgentGenome:
        """Create a random genome within parameter bounds."""
        # Random tool subset (at least 3 tools)
        num_tools = random.randint(3, len(ALL_AGENTIC_TOOLS))
        tools = random.sample(ALL_AGENTIC_TOOLS, num_tools)

        return cls(
            agent_class=agent_class,
            generation=generation,
            # Original params
            model=random.choice(PARAM_BOUNDS["model"]),
            temperature=round(random.uniform(*PARAM_BOUNDS["temperature"]), 2),
            max_tokens=random.randint(*PARAM_BOUNDS["max_tokens"]),
            character=random.choice(PARAM_BOUNDS["character"]),
            confidence_threshold=round(random.uniform(*PARAM_BOUNDS["confidence_threshold"]), 2),
            position_size_pct=round(random.uniform(*PARAM_BOUNDS["position_size_pct"]), 3),
            sl_pct=round(random.uniform(*PARAM_BOUNDS["sl_pct"]), 3),
            tp_pct=round(random.uniform(*PARAM_BOUNDS["tp_pct"]), 3),
            max_leverage=random.randint(*PARAM_BOUNDS["max_leverage"]),
            # Prompt architecture
            prompt_template=random.choice(PROMPT_TEMPLATES),
            system_prefix=random.choice(PARAM_BOUNDS["system_prefix"]),
            # Tool selection
            enabled_tools=tools,
            max_iterations=random.randint(*PARAM_BOUNDS["max_iterations"]),
            # Integration toggles
            use_skills=random.choice([True, False]),
            skill_weight=round(random.uniform(*PARAM_BOUNDS["skill_weight"]), 2),
            use_forum=random.choice([True, False]),
            forum_weight=round(random.uniform(*PARAM_BOUNDS["forum_weight"]), 2),
            use_journal=random.choice([True, False]),
            # Risk envelope
            max_concurrent_positions=random.randint(*PARAM_BOUNDS["max_concurrent_positions"]),
            correlation_threshold=round(random.uniform(*PARAM_BOUNDS["correlation_threshold"]), 2),
            funding_sensitivity=round(random.uniform(*PARAM_BOUNDS["funding_sensitivity"]), 2),
        )

    def mutate(self, rate: float = 0.15) -> AgentGenome:
        """Create a mutated copy. Each gene has `rate` probability of mutation."""
        child = copy.deepcopy(self)
        child.genome_id = f"g_{uuid.uuid4().hex[:12]}"
        child.parent_ids = [self.genome_id]
        child.mutations = []

        # --- Categorical mutations ---

        # Model
        if random.random() < rate:
            old = child.model
            child.model = random.choice(PARAM_BOUNDS["model"])
            if child.model != old:
                child.mutations.append(f"model: {old} -> {child.model}")

        # Character
        if random.random() < rate:
            old_idx = (
                CHARACTER_TEMPLATES.index(child.character)
                if child.character in CHARACTER_TEMPLATES
                else -1
            )
            child.character = random.choice(PARAM_BOUNDS["character"])
            new_idx = CHARACTER_TEMPLATES.index(child.character)
            if old_idx != new_idx:
                child.mutations.append(f"character: template[{old_idx}] -> template[{new_idx}]")

        # Prompt template
        if random.random() < rate:
            old = child.prompt_template
            child.prompt_template = random.choice(PROMPT_TEMPLATES)
            if child.prompt_template != old:
                child.mutations.append(f"prompt_template: {old} -> {child.prompt_template}")

        # System prefix
        if random.random() < rate:
            old = child.system_prefix
            child.system_prefix = random.choice(PARAM_BOUNDS["system_prefix"])
            if child.system_prefix != old:
                child.mutations.append("system_prefix: changed")

        # Agent class
        if random.random() < rate:
            old = child.agent_class
            child.agent_class = random.choice(ALLOWED_AGENT_CLASSES)
            if child.agent_class != old:
                child.mutations.append(f"agent_class: {old.split('.')[-1]} -> {child.agent_class.split('.')[-1]}")

        # --- Boolean toggle mutations (flip) ---
        for toggle in ("use_skills", "use_forum", "use_journal"):
            if random.random() < rate:
                old_val = getattr(child, toggle)
                new_val = not old_val
                setattr(child, toggle, new_val)
                child.mutations.append(f"{toggle}: {old_val} -> {new_val}")

        # --- Subset mutation for enabled_tools ---
        if random.random() < rate:
            old_tools = set(child.enabled_tools)
            # Randomly add or remove 1-2 tools
            if random.random() < 0.5 and len(child.enabled_tools) > 3:
                # Remove a random tool
                tool_to_remove = random.choice(child.enabled_tools)
                child.enabled_tools = [t for t in child.enabled_tools if t != tool_to_remove]
                child.mutations.append(f"enabled_tools: removed {tool_to_remove}")
            else:
                # Add a random tool not already present
                missing = [t for t in ALL_AGENTIC_TOOLS if t not in child.enabled_tools]
                if missing:
                    tool_to_add = random.choice(missing)
                    child.enabled_tools.append(tool_to_add)
                    child.mutations.append(f"enabled_tools: added {tool_to_add}")

        # --- Continuous parameters: Gaussian perturbation ---
        continuous_params = {
            "temperature", "confidence_threshold", "position_size_pct",
            "sl_pct", "tp_pct", "skill_weight", "forum_weight",
            "correlation_threshold", "funding_sensitivity",
        }
        for param in continuous_params:
            if random.random() >= rate:
                continue
            bounds = PARAM_BOUNDS[param]
            old_val = getattr(child, param)
            lo, hi = bounds
            span = hi - lo
            sigma = span * 0.10
            new_val = old_val + random.gauss(0, sigma)
            new_val = round(max(lo, min(hi, new_val)), 3)
            if new_val != old_val:
                setattr(child, param, new_val)
                child.mutations.append(f"{param}: {old_val} -> {new_val}")

        # --- Discrete integer parameters ---
        int_params = {"max_tokens", "max_leverage", "max_iterations", "max_concurrent_positions"}
        for param in int_params:
            if random.random() >= rate:
                continue
            bounds = PARAM_BOUNDS[param]
            old_val = getattr(child, param)
            lo, hi = bounds
            span = hi - lo
            delta = random.randint(-max(1, span // 5), max(1, span // 5))
            new_val = max(lo, min(hi, old_val + delta))
            if new_val != old_val:
                setattr(child, param, new_val)
                child.mutations.append(f"{param}: {old_val} -> {new_val}")

        return child

    def crossover(self, other: AgentGenome) -> AgentGenome:
        """Uniform crossover: each gene picked 50/50 from self or other."""
        child = AgentGenome(
            agent_class=random.choice([self.agent_class, other.agent_class]),
            generation=max(self.generation, other.generation) + 1,
            parent_ids=[self.genome_id, other.genome_id],
        )

        # Simple scalar/categorical params — 50/50 from each parent
        scalar_params = [
            "model", "temperature", "max_tokens", "character",
            "confidence_threshold", "position_size_pct", "sl_pct", "tp_pct",
            "max_leverage", "prompt_template", "system_prefix",
            "max_iterations", "use_skills", "skill_weight",
            "use_forum", "forum_weight", "use_journal",
            "max_concurrent_positions", "correlation_threshold", "funding_sensitivity",
        ]
        for param in scalar_params:
            source = self if random.random() < 0.5 else other
            setattr(child, param, getattr(source, param))

        # List merge for enabled_tools: union then random subset
        combined_tools = list(set(self.enabled_tools) | set(other.enabled_tools))
        min_tools = min(len(self.enabled_tools), len(other.enabled_tools))
        max_tools = max(len(self.enabled_tools), len(other.enabled_tools))
        num_tools = random.randint(max(3, min_tools), max_tools)
        child.enabled_tools = random.sample(combined_tools, min(num_tools, len(combined_tools)))

        return child

    def to_agent_config(
        self, agent_id: str, agent_name: str,
        base_url: str, api_key_env: str,
    ) -> dict:
        """Convert genome to the dict format expected by cli.load_agent()."""
        config = {
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "character": (self.system_prefix + self.character) if self.system_prefix else self.character,
            "base_url": base_url,
            "api_key_env": api_key_env,
            "confidence_threshold": self.confidence_threshold,
            "position_size_pct": self.position_size_pct,
            "sl_pct": self.sl_pct,
            "tp_pct": self.tp_pct,
            "max_leverage": self.max_leverage,
        }

        # Add integration toggles for skill/forum/journal-aware agents
        if "skill_aware" in self.agent_class or "forum_aware" in self.agent_class or "journal_aware" in self.agent_class:
            config["use_skills"] = self.use_skills
            config["skill_weight"] = self.skill_weight

        if "forum_aware" in self.agent_class or "journal_aware" in self.agent_class:
            config["use_forum"] = self.use_forum
            config["forum_weight"] = self.forum_weight

        if "journal_aware" in self.agent_class:
            config["use_journal"] = self.use_journal

        # Add tool config for agentic agents
        if "agentic" in self.agent_class:
            config["enabled_tools"] = self.enabled_tools
            config["max_iterations"] = self.max_iterations

        # Risk envelope
        config["max_concurrent_positions"] = self.max_concurrent_positions
        config["correlation_threshold"] = self.correlation_threshold
        config["funding_sensitivity"] = self.funding_sensitivity

        return {
            "id": agent_id,
            "name": agent_name,
            "class": self.agent_class,
            "config": config,
        }

    def to_dict(self) -> dict:
        """Serialize to dict for DB storage."""
        return {
            "genome_id": self.genome_id,
            "agent_class": self.agent_class,
            "generation": self.generation,
            # Original params
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "character": self.character,
            "confidence_threshold": self.confidence_threshold,
            "position_size_pct": self.position_size_pct,
            "sl_pct": self.sl_pct,
            "tp_pct": self.tp_pct,
            "max_leverage": self.max_leverage,
            # Prompt architecture
            "prompt_template": self.prompt_template,
            "system_prefix": self.system_prefix,
            # Tool selection
            "enabled_tools": self.enabled_tools,
            "max_iterations": self.max_iterations,
            # Integration toggles
            "use_skills": self.use_skills,
            "skill_weight": self.skill_weight,
            "use_forum": self.use_forum,
            "forum_weight": self.forum_weight,
            "use_journal": self.use_journal,
            # Risk envelope
            "max_concurrent_positions": self.max_concurrent_positions,
            "correlation_threshold": self.correlation_threshold,
            "funding_sensitivity": self.funding_sensitivity,
            # Lineage
            "parent_ids": self.parent_ids,
            "mutations": self.mutations,
        }

    @classmethod
    def from_dict(cls, data: dict) -> AgentGenome:
        """Deserialize from dict (backwards-compatible with old 9-param genomes)."""
        return cls(
            genome_id=data.get("genome_id", ""),
            agent_class=data.get(
                "agent_class",
                "agent_arena.agents.llm_trader.LLMTrader",
            ),
            generation=data.get("generation", 0),
            # Original params
            model=data.get("model", "glm-5"),
            temperature=data.get("temperature", 0.7),
            max_tokens=data.get("max_tokens", 1024),
            character=data.get("character", ""),
            confidence_threshold=data.get("confidence_threshold", 0.5),
            position_size_pct=data.get("position_size_pct", 0.15),
            sl_pct=data.get("sl_pct", 0.02),
            tp_pct=data.get("tp_pct", 0.04),
            max_leverage=data.get("max_leverage", 5),
            # Prompt architecture (defaults for backwards compat)
            prompt_template=data.get("prompt_template", "default"),
            system_prefix=data.get("system_prefix", ""),
            # Tool selection
            enabled_tools=data.get("enabled_tools", list(ALL_AGENTIC_TOOLS)),
            max_iterations=data.get("max_iterations", 3),
            # Integration toggles
            use_skills=data.get("use_skills", True),
            skill_weight=data.get("skill_weight", 0.5),
            use_forum=data.get("use_forum", True),
            forum_weight=data.get("forum_weight", 0.5),
            use_journal=data.get("use_journal", True),
            # Risk envelope
            max_concurrent_positions=data.get("max_concurrent_positions", 3),
            correlation_threshold=data.get("correlation_threshold", 0.7),
            funding_sensitivity=data.get("funding_sensitivity", 0.5),
            # Lineage
            parent_ids=data.get("parent_ids", []),
            mutations=data.get("mutations", []),
        )

    def __repr__(self) -> str:
        return (
            f"AgentGenome(id={self.genome_id[:8]}..., model={self.model}, "
            f"class={self.agent_class.split('.')[-1]}, "
            f"temp={self.temperature}, conf_thresh={self.confidence_threshold}, "
            f"sl={self.sl_pct}, tp={self.tp_pct}, lev={self.max_leverage}, "
            f"tools={len(self.enabled_tools)}, skills={self.use_skills}, "
            f"forum={self.use_forum}, journal={self.use_journal})"
        )
