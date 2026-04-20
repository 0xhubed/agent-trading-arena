"""Island model — parallel sub-populations with periodic migration."""

from __future__ import annotations

from dataclasses import dataclass

from agent_arena.evolution.genome import AgentGenome


@dataclass
class IslandModel:
    """Island model GA: splits population into sub-populations (islands) that
    evolve independently with periodic migration of top individuals.

    Supports ring topology where each island sends its best to the next.
    """

    num_islands: int = 4
    migration_interval: int = 5
    migration_count: int = 2
    topology: str = "ring"  # "ring" or "full"

    def split_population(
        self, population: list[AgentGenome]
    ) -> list[list[AgentGenome]]:
        """Split a flat population into islands of roughly equal size."""
        islands: list[list[AgentGenome]] = [[] for _ in range(self.num_islands)]
        for i, genome in enumerate(population):
            islands[i % self.num_islands].append(genome)
        return islands

    def should_migrate(self, generation: int) -> bool:
        """Check if migration should occur this generation."""
        return generation > 0 and generation % self.migration_interval == 0

    def perform_migration(
        self,
        islands: list[list[AgentGenome]],
        fitness_maps: list[dict[str, float]],
    ) -> list[list[AgentGenome]]:
        """Migrate top genomes between islands.

        Ring topology: island i sends top N to island (i+1) % num_islands,
        replacing the worst N on the receiving island.
        """
        n = self.migration_count
        num = len(islands)

        if self.topology == "ring":
            # Collect migrants from each island
            migrants: list[list[AgentGenome]] = []
            for i in range(num):
                fm = fitness_maps[i]
                ranked = sorted(
                    islands[i],
                    key=lambda g: fm.get(g.genome_id, 0.0),
                    reverse=True,
                )
                migrants.append(ranked[:n])

            # Perform migration: island i's migrants go to island (i+1)
            for i in range(num):
                target = (i + 1) % num
                target_fm = fitness_maps[target]

                # Remove worst N from target, but don't empty the island
                ranked_target = sorted(
                    islands[target],
                    key=lambda g: target_fm.get(g.genome_id, 0.0),
                )
                n_to_remove = min(n, len(ranked_target) - 1, len(migrants[i]))
                if n_to_remove > 0:
                    to_remove = {g.genome_id for g in ranked_target[:n_to_remove]}
                    islands[target] = [
                        g for g in islands[target]
                        if g.genome_id not in to_remove
                    ]

                islands[target].extend(migrants[i])

        elif self.topology == "full":
            # Full topology: every island sends to every other
            for i in range(num):
                fm = fitness_maps[i]
                ranked = sorted(
                    islands[i],
                    key=lambda g: fm.get(g.genome_id, 0.0),
                    reverse=True,
                )
                best = ranked[:n]

                for j in range(num):
                    if i == j:
                        continue
                    target_fm = fitness_maps[j]
                    ranked_target = sorted(
                        islands[j],
                        key=lambda g: target_fm.get(g.genome_id, 0.0),
                    )
                    n_to_remove = min(n, len(ranked_target) - 1, len(best))
                    if n_to_remove > 0:
                        to_remove = {
                            g.genome_id for g in ranked_target[:n_to_remove]
                        }
                        islands[j] = [
                            g for g in islands[j]
                            if g.genome_id not in to_remove
                        ]
                    islands[j].extend(best)

        return islands

    def merge_islands(self, islands: list[list[AgentGenome]]) -> list[AgentGenome]:
        """Flatten all islands back into a single population."""
        merged: list[AgentGenome] = []
        for island in islands:
            merged.extend(island)
        return merged
