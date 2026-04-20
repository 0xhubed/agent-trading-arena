"""Pareto/NSGA-II optimization — multi-objective ranking with crowding distance."""

from __future__ import annotations

from dataclasses import dataclass

from agent_arena.evolution.genome import AgentGenome


@dataclass
class ParetoMetrics:
    """Multi-objective metrics for Pareto ranking."""

    sharpe_ratio: float = 0.0
    total_return: float = 0.0
    max_drawdown: float = 0.0  # Lower is better (stored as positive %)
    win_rate: float = 0.0


class ParetoOptimizer:
    """NSGA-II style multi-objective optimizer.

    Ranks genomes by Pareto dominance (non-dominated sorting) and uses
    crowding distance to promote diversity within each front.
    """

    def calculate_pareto_rank(
        self,
        genomes: list[AgentGenome],
        metrics: dict[str, ParetoMetrics],
    ) -> dict[str, int]:
        """Non-dominated sorting: assign Pareto rank to each genome.

        Rank 1 = Pareto front (not dominated by anyone).
        Rank 2 = dominated only by rank-1, etc.
        """
        ids = [g.genome_id for g in genomes if g.genome_id in metrics]
        ranks: dict[str, int] = {}
        remaining = set(ids)
        rank = 1

        while remaining:
            # Find non-dominated set in remaining
            front: list[str] = []
            for gid in remaining:
                dominated = False
                for other_id in remaining:
                    if other_id == gid:
                        continue
                    if self._dominates(metrics[other_id], metrics[gid]):
                        dominated = True
                        break
                if not dominated:
                    front.append(gid)

            if not front:
                # Safety: assign remaining to current rank
                for gid in remaining:
                    ranks[gid] = rank
                break

            for gid in front:
                ranks[gid] = rank
            remaining -= set(front)
            rank += 1

        return ranks

    def _dominates(self, a: ParetoMetrics, b: ParetoMetrics) -> bool:
        """Check if metrics A dominates B.

        A dominates B if A is >= B on all objectives and strictly > on at least one.
        Note: for max_drawdown, lower is better, so we invert the comparison.
        """
        objectives_a = [a.sharpe_ratio, a.total_return, -a.max_drawdown, a.win_rate]
        objectives_b = [b.sharpe_ratio, b.total_return, -b.max_drawdown, b.win_rate]

        at_least_as_good = all(oa >= ob for oa, ob in zip(objectives_a, objectives_b))
        strictly_better = any(oa > ob for oa, ob in zip(objectives_a, objectives_b))

        return at_least_as_good and strictly_better

    def calculate_crowding_distance(
        self,
        genomes: list[AgentGenome],
        metrics: dict[str, ParetoMetrics],
        ranks: dict[str, int],
    ) -> dict[str, float]:
        """Compute crowding distance for each genome within its Pareto front.

        Higher crowding distance = more isolated = more diverse.
        """
        distances: dict[str, float] = {g.genome_id: 0.0 for g in genomes}

        # Group by rank
        rank_groups: dict[int, list[str]] = {}
        for gid, rank in ranks.items():
            rank_groups.setdefault(rank, []).append(gid)

        objectives = ["sharpe_ratio", "total_return", "max_drawdown", "win_rate"]

        for _rank, group in rank_groups.items():
            if len(group) <= 2:
                for gid in group:
                    distances[gid] = float("inf")
                continue

            for obj in objectives:
                # Sort by this objective
                sorted_group = sorted(
                    group,
                    key=lambda gid: getattr(metrics.get(gid, ParetoMetrics()), obj, 0.0),
                )

                # Boundary points get infinite distance
                distances[sorted_group[0]] = float("inf")
                distances[sorted_group[-1]] = float("inf")

                # Range for normalization
                min_val = getattr(metrics.get(sorted_group[0], ParetoMetrics()), obj, 0.0)
                max_val = getattr(metrics.get(sorted_group[-1], ParetoMetrics()), obj, 0.0)
                obj_range = max_val - min_val

                if obj_range == 0:
                    continue

                for i in range(1, len(sorted_group) - 1):
                    prev_val = getattr(metrics.get(sorted_group[i - 1], ParetoMetrics()), obj, 0.0)
                    next_val = getattr(metrics.get(sorted_group[i + 1], ParetoMetrics()), obj, 0.0)
                    distances[sorted_group[i]] += (next_val - prev_val) / obj_range

        return distances

    def rank_population(
        self,
        genomes: list[AgentGenome],
        metrics: dict[str, ParetoMetrics],
    ) -> list[AgentGenome]:
        """Sort population by (pareto_rank ASC, crowding_distance DESC)."""
        ranks = self.calculate_pareto_rank(genomes, metrics)
        crowding = self.calculate_crowding_distance(genomes, metrics, ranks)

        return sorted(
            genomes,
            key=lambda g: (
                ranks.get(g.genome_id, 999),
                -crowding.get(g.genome_id, 0.0),
            ),
        )
