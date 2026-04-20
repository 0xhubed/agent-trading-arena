"""Novelty search — rewards behavioral diversity alongside fitness."""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass, field

from agent_arena.backtest.results import AgentResult
from agent_arena.evolution.genome import AgentGenome


@dataclass
class NoveltySearch:
    """Behavioral novelty search that rewards agents for exploring new strategies.

    Maintains an archive of novel behavioral signatures and computes novelty
    as the average distance to k-nearest neighbors.
    """

    k_nearest: int = 15
    archive_threshold: float = 0.8
    max_archive: int = 100

    # Normalization constants for behavioral signature dimensions
    TRADE_FREQ_NORMALIZER: float = 50.0  # Expected max trades per backtest
    MAX_LEVERAGE_NORMALIZER: float = 10.0  # Max leverage bound
    SL_PCT_NORMALIZER: float = 0.05  # Max stop-loss percentage

    # Archive: list of (genome_id, signature) pairs
    archive: list[tuple[str, list[float]]] = field(default_factory=list)

    def calculate_behavioral_signature(
        self,
        genome: AgentGenome,
        agent_result: AgentResult,
    ) -> list[float]:
        """Compute an 8-dimensional behavioral descriptor from backtest results.

        Dimensions:
        0: trade_frequency (trades per tick, normalized)
        1: avg_position_size (genome param)
        2: avg_leverage (genome param)
        3: long_short_ratio (0 = all short, 1 = all long)
        4-6: symbol_distribution (fraction of trades in each of top 3 symbols)
        7: risk_profile (SL distance as fraction)
        """
        total_trades = max(agent_result.total_trades, 1)

        # Trade frequency: normalize to ~[0, 1]
        trade_freq = min(1.0, total_trades / self.TRADE_FREQ_NORMALIZER)

        # Position size and leverage from genome
        avg_pos = genome.position_size_pct
        avg_lev = genome.max_leverage / self.MAX_LEVERAGE_NORMALIZER

        # Long/short ratio from trades
        long_count = sum(
            1 for t in agent_result.trades
            if t.side == "long" and t.action == "open"
        )
        short_count = sum(
            1 for t in agent_result.trades
            if t.side == "short" and t.action == "open"
        )
        total_opens = long_count + short_count
        ls_ratio = long_count / max(total_opens, 1)

        # Symbol distribution (up to 3 dimensions)
        symbol_counts: dict[str, int] = {}
        for t in agent_result.trades:
            if t.action == "open":
                symbol_counts[t.symbol] = symbol_counts.get(t.symbol, 0) + 1

        sorted_symbols = sorted(symbol_counts.items(), key=lambda x: -x[1])
        sym_dist = [0.0, 0.0, 0.0]
        for i, (_, count) in enumerate(sorted_symbols[:3]):
            sym_dist[i] = count / max(total_opens, 1)

        # Risk profile: SL distance
        risk = genome.sl_pct / self.SL_PCT_NORMALIZER

        return [trade_freq, avg_pos, avg_lev, ls_ratio, *sym_dist, risk]

    def calculate_novelty_score(
        self,
        signature: list[float],
        population_signatures: list[list[float]],
    ) -> float:
        """Compute novelty as average Euclidean distance to k-nearest neighbors.

        Considers both the current population and the novelty archive.
        """
        all_sigs = population_signatures + [s for _, s in self.archive]
        if not all_sigs:
            return 1.0

        distances = [
            self._euclidean_distance(signature, other)
            for other in all_sigs
            if other != signature
        ]

        if not distances:
            return 1.0

        k = min(self.k_nearest, len(distances))
        if k == 0:
            return 1.0
        k_nearest_distances = heapq.nsmallest(k, distances)
        return sum(k_nearest_distances) / k

    def update_archive(
        self,
        genome_id: str,
        signature: list[float],
        novelty_score: float,
    ) -> None:
        """Add highly novel behaviors to the archive."""
        if novelty_score >= self.archive_threshold:
            self.archive.append((genome_id, signature))
            # Trim if over max
            if len(self.archive) > self.max_archive:
                self.archive = self.archive[-self.max_archive:]

    def get_combined_score(
        self,
        fitness: float,
        novelty: float,
        novelty_weight: float = 0.3,
    ) -> float:
        """Combine fitness and novelty into a single score."""
        return (1 - novelty_weight) * fitness + novelty_weight * novelty

    @staticmethod
    def _euclidean_distance(a: list[float], b: list[float]) -> float:
        """Euclidean distance between two vectors."""
        return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))
