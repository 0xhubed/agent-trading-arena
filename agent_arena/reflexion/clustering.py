"""Failure Clusterer — groups losing trade reflections by regime/signal/failure mode."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Optional

from agent_arena.llm_utils import extract_json_from_llm, strip_think_blocks
from agent_arena.reflexion.models import TradeReflection

logger = logging.getLogger(__name__)

CLUSTER_PROMPT = """You are analyzing a group of {count} losing trades that share common characteristics.

Trades:
{trade_summaries}

Identify the common failure pattern and propose a concise trading rule that would help avoid this failure in the future.

Respond in JSON:
{{
  "cluster_label": "Short descriptive label for this failure pattern",
  "failure_mode": "What specifically goes wrong in these trades",
  "proposed_skill": "A concise trading rule (1-2 sentences) that addresses this failure"
}}"""


@dataclass
class FailureCluster:
    """A group of related losing trades with a common failure pattern."""

    cluster_label: str
    regime: str = ""
    failure_mode: str = ""
    reflection_ids: list[int] = field(default_factory=list)
    sample_size: int = 0
    proposed_skill: str = ""
    proposed_skill_validated: bool = False
    improvement_pct: float = 0.0


class FailureClusterer:
    """Clusters losing trade reflections to identify systematic failure patterns.

    Groups reflections by regime + signal similarity, then uses LLM to
    label clusters and propose corrective skills.
    """

    def __init__(
        self,
        storage: Any,
        model: str = "claude-sonnet-4-6",
        min_cluster_size: int = 3,
    ):
        self.storage = storage
        self.model = model
        self.min_cluster_size = min_cluster_size

    async def cluster_failures(
        self,
        lookback_days: int = 14,
    ) -> list[FailureCluster]:
        """Cluster recent losing trades across all agents.

        Returns list of FailureCluster objects with proposed skills.
        """
        reflections = await self._get_losing_reflections(lookback_days)
        if len(reflections) < self.min_cluster_size:
            logger.info(
                "Only %d losing reflections (need %d), skipping clustering",
                len(reflections), self.min_cluster_size,
            )
            return []

        # Group by regime (simple pre-clustering)
        regime_groups = self._group_by_regime(reflections)

        clusters = []
        for regime, group in regime_groups.items():
            if len(group) < self.min_cluster_size:
                continue

            # Use LLM to identify failure pattern and propose skill
            cluster = await self._analyze_cluster(regime, group)
            if cluster:
                await self._save_cluster(cluster)
                clusters.append(cluster)

        logger.info(
            "Found %d failure clusters from %d reflections",
            len(clusters), len(reflections),
        )
        return clusters

    def _group_by_regime(
        self, reflections: list[dict],
    ) -> dict[str, list[dict]]:
        """Group reflections by market regime."""
        groups: dict[str, list[dict]] = defaultdict(list)
        for r in reflections:
            regime = r.get("market_regime", "unknown") or "unknown"
            groups[regime].append(r)
        return dict(groups)

    async def _analyze_cluster(
        self, regime: str, reflections: list[dict],
    ) -> Optional[FailureCluster]:
        """Use LLM to analyze a cluster of failing trades."""
        from langchain_anthropic import ChatAnthropic
        from langchain_core.messages import HumanMessage

        # Format trade summaries
        summaries = []
        reflection_ids = []
        for r in reflections[:10]:  # Limit context
            summaries.append(
                f"- {r.get('symbol', '?')} {r.get('side', '?')}: "
                f"PnL ${float(r.get('realized_pnl', 0)):+.2f}, "
                f"Signal: {r.get('entry_signal', 'unknown')}, "
                f"Mistake: {r.get('what_went_wrong', 'unknown')}"
            )
            if r.get("id"):
                reflection_ids.append(r["id"])

        prompt = CLUSTER_PROMPT.format(
            count=len(reflections),
            trade_summaries="\n".join(summaries),
        )

        try:
            llm = ChatAnthropic(model=self.model, max_tokens=400, temperature=0.3)
            response = await llm.ainvoke([HumanMessage(content=prompt)])
            content = strip_think_blocks(response.content)

            # Parse JSON
            parsed = extract_json_from_llm(content)
            if not parsed:
                return None

            return FailureCluster(
                cluster_label=parsed.get("cluster_label", "Unknown pattern"),
                regime=regime,
                failure_mode=parsed.get("failure_mode", ""),
                reflection_ids=reflection_ids,
                sample_size=len(reflections),
                proposed_skill=parsed.get("proposed_skill", ""),
            )
        except Exception:
            logger.exception("Failed to analyze cluster for regime %s", regime)
            return None

    async def _get_losing_reflections(self, lookback_days: int) -> list[dict]:
        """Get losing trade reflections from the database."""
        if not hasattr(self.storage, "pool"):
            return []

        try:
            async with self.storage.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, agent_id, symbol, side, realized_pnl,
                           entry_signal, what_went_wrong, market_regime, created_at
                    FROM trade_reflections
                    WHERE outcome = 'loss'
                    AND created_at >= NOW() - INTERVAL '1 day' * $1
                    AND is_digested = FALSE
                    ORDER BY created_at DESC
                    """,
                    lookback_days,
                )
                return [dict(r) for r in rows]
        except Exception:
            return []

    async def _save_cluster(self, cluster: FailureCluster) -> None:
        """Save a failure cluster to the database."""
        if not hasattr(self.storage, "pool"):
            return

        try:
            async with self.storage.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO failure_clusters (
                        cluster_label, regime, failure_mode,
                        reflection_ids, sample_size, proposed_skill
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    """,
                    cluster.cluster_label,
                    cluster.regime,
                    cluster.failure_mode,
                    cluster.reflection_ids,
                    cluster.sample_size,
                    cluster.proposed_skill,
                )
        except Exception:
            logger.exception("Failed to save failure cluster")
