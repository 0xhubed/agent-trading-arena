"""Contagion Tracker — measures system health and echo chamber risk.

Detects whether the learning loop produces genuine intelligence or
uniformity by tracking position diversity and reasoning entropy
across agents.
"""

from __future__ import annotations

import math
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class ContagionScore:
    """Score for a single contagion metric."""

    metric_type: str  # "position_diversity", "reasoning_entropy"
    value: Optional[float]  # 0-1, None if insufficient data
    sample_size: int
    sufficient_data: bool
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "metric_type": self.metric_type,
            "value": round(self.value, 4) if self.value is not None else None,
            "sample_size": self.sample_size,
            "sufficient_data": self.sufficient_data,
            "details": self.details,
        }

    def summary(self) -> str:
        if not self.sufficient_data:
            return (
                f"{self.metric_type}: INSUFFICIENT DATA "
                f"({self.sample_size} samples)"
            )
        rating = _health_rating(self.value)
        return (
            f"{self.metric_type}: {self.value:.2f} ({rating}) "
            f"[{self.sample_size} samples]"
        )


@dataclass
class ContagionSnapshot:
    """Full contagion analysis for a single point in time."""

    timestamp: datetime
    tick: Optional[int]
    position_diversity: ContagionScore
    reasoning_entropy: ContagionScore
    agent_count: int

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp.isoformat(),
            "tick": self.tick,
            "position_diversity": self.position_diversity.to_dict(),
            "reasoning_entropy": self.reasoning_entropy.to_dict(),
            "agent_count": self.agent_count,
        }

    @property
    def scores(self) -> list[ContagionScore]:
        return [self.position_diversity, self.reasoning_entropy]

    @property
    def system_health(self) -> Optional[float]:
        """Composite health: average of available metrics (0-1, higher=healthier)."""
        values = [s.value for s in self.scores if s.sufficient_data and s.value is not None]
        if not values:
            return None
        return sum(values) / len(values)

    @property
    def health_label(self) -> str:
        h = self.system_health
        if h is None:
            return "UNKNOWN"
        return _health_rating(h)


def compute_system_health(
    values: list[float],
) -> tuple[Optional[float], str]:
    """Compute composite system health from metric values.

    Args:
        values: List of metric scores (0-1, higher = healthier).

    Returns:
        (health_score, health_label) tuple.
    """
    if not values:
        return None, "UNKNOWN"
    health = sum(values) / len(values)
    return health, _health_rating(health)


def _health_rating(value: Optional[float]) -> str:
    """Convert 0-1 score to a health rating (higher = healthier)."""
    if value is None:
        return "N/A"
    if value >= 0.6:
        return "HEALTHY"
    if value >= 0.3:
        return "MODERATE"
    return "WARNING"


# ---------------------------------------------------------------------------
# Position Diversity Index
# ---------------------------------------------------------------------------

def calculate_position_diversity(
    decisions_by_agent: dict[str, list[dict]],
    min_agents: int = 2,
) -> ContagionScore:
    """Measure how diverse agent positions are at each tick.

    Computes the position diversity per tick by encoding each agent's
    action as a numeric vector, then computing 1 - mean(pairwise cosine
    similarity). Averaged across all ticks.

    Args:
        decisions_by_agent: {agent_id: [decision_dicts]} — each decision
            must have 'tick', 'action', 'symbol'.
        min_agents: Minimum agents required for meaningful diversity.

    Returns:
        ContagionScore with value 0-1 (1 = fully diverse, 0 = identical).
    """
    agent_ids = list(decisions_by_agent.keys())
    if len(agent_ids) < min_agents:
        return ContagionScore(
            metric_type="position_diversity",
            value=None,
            sample_size=len(agent_ids),
            sufficient_data=False,
            details={"reason": "not enough agents"},
        )

    # Group decisions by tick across agents
    tick_actions: dict[int, dict[str, str]] = defaultdict(dict)
    for agent_id, decisions in decisions_by_agent.items():
        for d in decisions:
            tick = d.get("tick")
            action = d.get("action", "hold")
            symbol = d.get("symbol") or ""
            if tick is not None:
                # Encode as "action:symbol" for richer position signal
                tick_actions[tick][agent_id] = f"{action}:{symbol}"

    if not tick_actions:
        return ContagionScore(
            metric_type="position_diversity",
            value=None,
            sample_size=0,
            sufficient_data=False,
            details={"reason": "no tick data"},
        )

    # For each tick, compute pairwise agreement rate
    diversities = []
    for tick, agent_actions in tick_actions.items():
        agents_at_tick = [a for a in agent_ids if a in agent_actions]
        if len(agents_at_tick) < min_agents:
            continue

        # Count pairwise disagreements
        n = len(agents_at_tick)
        pairs = 0
        disagreements = 0
        for i in range(n):
            for j in range(i + 1, n):
                pairs += 1
                if agent_actions[agents_at_tick[i]] != agent_actions[agents_at_tick[j]]:
                    disagreements += 1

        diversity = disagreements / pairs if pairs > 0 else 0.0
        diversities.append(diversity)

    if not diversities:
        return ContagionScore(
            metric_type="position_diversity",
            value=None,
            sample_size=0,
            sufficient_data=False,
            details={"reason": "no ticks with enough agents"},
        )

    avg_diversity = sum(diversities) / len(diversities)
    return ContagionScore(
        metric_type="position_diversity",
        value=max(0.0, min(1.0, avg_diversity)),
        sample_size=len(diversities),
        sufficient_data=True,
        details={
            "ticks_analyzed": len(diversities),
            "min_diversity": round(min(diversities), 4),
            "max_diversity": round(max(diversities), 4),
            "agents": len(agent_ids),
        },
    )


# ---------------------------------------------------------------------------
# Reasoning Entropy
# ---------------------------------------------------------------------------

_WORD_SPLIT = re.compile(r"[a-z0-9]+")


def _tokenize(text: str) -> list[str]:
    """Simple lowercased word tokenizer."""
    return _WORD_SPLIT.findall(text.lower())


def _cosine_similarity(a: dict[str, float], b: dict[str, float]) -> float:
    """Cosine similarity between two sparse TF vectors."""
    common = set(a.keys()) & set(b.keys())
    if not common:
        return 0.0
    dot = sum(a[k] * b[k] for k in common)
    norm_a = math.sqrt(sum(v * v for v in a.values()))
    norm_b = math.sqrt(sum(v * v for v in b.values()))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def calculate_reasoning_entropy(
    decisions_by_agent: dict[str, list[dict]],
    min_agents: int = 2,
    min_ticks: int = 5,
) -> ContagionScore:
    """Measure reasoning text diversity across agents per tick.

    Computes TF vectors per agent per tick from reasoning text, then
    1 - mean(pairwise cosine similarity). Averaged across ticks.

    Args:
        decisions_by_agent: {agent_id: [decision_dicts]} — each decision
            must have 'tick' and 'reasoning'.
        min_agents: Minimum agents per tick for a valid comparison.
        min_ticks: Minimum ticks required for sufficient data.

    Returns:
        ContagionScore with value 0-1 (1 = diverse reasoning, 0 = identical).
    """
    agent_ids = list(decisions_by_agent.keys())
    if len(agent_ids) < min_agents:
        return ContagionScore(
            metric_type="reasoning_entropy",
            value=None,
            sample_size=len(agent_ids),
            sufficient_data=False,
            details={"reason": "not enough agents"},
        )

    # Group reasoning text by tick
    tick_reasoning: dict[int, dict[str, str]] = defaultdict(dict)
    for agent_id, decisions in decisions_by_agent.items():
        for d in decisions:
            tick = d.get("tick")
            reasoning = d.get("reasoning") or ""
            if tick is not None and reasoning.strip():
                tick_reasoning[tick][agent_id] = reasoning

    entropies = []
    for tick, agent_texts in tick_reasoning.items():
        agents_at_tick = [a for a in agent_ids if a in agent_texts]
        if len(agents_at_tick) < min_agents:
            continue

        # Build TF vectors
        tf_vectors: dict[str, dict[str, float]] = {}
        for agent_id in agents_at_tick:
            tokens = _tokenize(agent_texts[agent_id])
            if not tokens:
                continue
            tf: dict[str, float] = {}
            for t in tokens:
                tf[t] = tf.get(t, 0.0) + 1.0
            # Normalize by total tokens
            total = len(tokens)
            tf = {k: v / total for k, v in tf.items()}
            tf_vectors[agent_id] = tf

        valid_agents = list(tf_vectors.keys())
        if len(valid_agents) < min_agents:
            continue

        # Mean pairwise cosine similarity
        n = len(valid_agents)
        total_sim = 0.0
        pairs = 0
        for i in range(n):
            for j in range(i + 1, n):
                sim = _cosine_similarity(
                    tf_vectors[valid_agents[i]],
                    tf_vectors[valid_agents[j]],
                )
                total_sim += sim
                pairs += 1

        avg_sim = total_sim / pairs if pairs > 0 else 0.0
        # Entropy = 1 - similarity (higher = more diverse)
        entropies.append(1.0 - avg_sim)

    if len(entropies) < min_ticks:
        return ContagionScore(
            metric_type="reasoning_entropy",
            value=None,
            sample_size=len(entropies),
            sufficient_data=False,
            details={
                "reason": f"need {min_ticks}+ ticks, got {len(entropies)}",
                "agents": len(agent_ids),
            },
        )

    avg_entropy = sum(entropies) / len(entropies)
    return ContagionScore(
        metric_type="reasoning_entropy",
        value=max(0.0, min(1.0, avg_entropy)),
        sample_size=len(entropies),
        sufficient_data=True,
        details={
            "ticks_analyzed": len(entropies),
            "min_entropy": round(min(entropies), 4),
            "max_entropy": round(max(entropies), 4),
            "agents": len(agent_ids),
        },
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def analyze_contagion(
    decisions_by_agent: dict[str, list[dict]],
    tick: Optional[int] = None,
) -> ContagionSnapshot:
    """Run all contagion metrics and return a complete snapshot.

    Args:
        decisions_by_agent: {agent_id: [decision_dicts]} — all decisions
            for each agent in the analysis window.
        tick: Optional current tick number for metadata.

    Returns:
        ContagionSnapshot with position_diversity, reasoning_entropy.
    """
    diversity = calculate_position_diversity(decisions_by_agent)
    entropy = calculate_reasoning_entropy(decisions_by_agent)

    return ContagionSnapshot(
        timestamp=datetime.now(timezone.utc),
        tick=tick,
        position_diversity=diversity,
        reasoning_entropy=entropy,
        agent_count=len(decisions_by_agent),
    )
