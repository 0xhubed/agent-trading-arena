"""EvolutionStorage — DB persistence for evolution runs and genomes."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)


class EvolutionStorage:
    """Thin layer over PostgresStorage for evolution-specific CRUD."""

    def __init__(self, postgres_storage: Any):
        self.storage = postgres_storage

    async def create_run(
        self,
        run_id: str,
        name: str,
        population_size: int,
        max_generations: int,
        agent_class: str,
        backtest_start: str,
        backtest_end: str,
        tick_interval: str,
        symbols: list[str],
        fitness_weights: dict,
        config: Optional[dict] = None,
    ) -> None:
        """Create a new evolution run record."""
        async with self.storage.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO evolution_runs (
                    id, name, status, population_size, max_generations,
                    agent_class, backtest_start, backtest_end, tick_interval,
                    symbols, fitness_weights, config
                ) VALUES ($1, $2, 'running', $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                run_id,
                name,
                population_size,
                max_generations,
                agent_class,
                backtest_start,
                backtest_end,
                tick_interval,
                symbols,
                json.dumps(fitness_weights),
                json.dumps(config or {}),
            )

    # Explicit whitelist with expected types for update_run
    ALLOWED_UPDATES: dict[str, type] = {
        "status": str,
        "current_generation": int,
        "completed_at": datetime,
        "best_fitness": (int, float),  # type: ignore[assignment]
        "best_genome_id": str,
    }

    async def update_run(self, run_id: str, **kwargs: Any) -> None:
        """Update fields on an evolution run."""
        sets = []
        values = []
        idx = 2  # $1 is always run_id

        for key, val in kwargs.items():
            if key not in self.ALLOWED_UPDATES:
                raise ValueError(f"Invalid update field: {key}")
            if val is not None and not isinstance(val, self.ALLOWED_UPDATES[key]):
                raise TypeError(
                    f"Invalid type for {key}: expected {self.ALLOWED_UPDATES[key]}, got {type(val)}"
                )
            sets.append(f"{key} = ${idx}")
            values.append(val)
            idx += 1

        if not sets:
            return

        query = f"UPDATE evolution_runs SET {', '.join(sets)} WHERE id = $1"
        async with self.storage.pool.acquire() as conn:
            await conn.execute(query, run_id, *values)

    async def save_generation(
        self,
        run_id: str,
        generation: int,
        genomes_with_fitness: list[dict],
    ) -> None:
        """Persist all genomes from a generation in a single transaction.

        Each entry in genomes_with_fitness should have:
            genome_id, genome (dict), fitness (float), metrics (dict),
            backtest_run_id (str), parent_ids (list), mutations (list),
            is_elite (bool)
        """
        async with self.storage.pool.acquire() as conn:
            async with conn.transaction():
                for entry in genomes_with_fitness:
                    await conn.execute(
                        """
                        INSERT INTO evolution_genomes (
                            id, run_id, generation, genome, fitness,
                            metrics, backtest_run_id, parent_ids,
                            mutations, is_elite
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                        ON CONFLICT (id) DO UPDATE SET
                            fitness = EXCLUDED.fitness,
                            metrics = EXCLUDED.metrics,
                            is_elite = EXCLUDED.is_elite
                        """,
                        entry["genome_id"],
                        run_id,
                        generation,
                        json.dumps(entry["genome"]),
                        entry.get("fitness"),
                        json.dumps(entry.get("metrics", {})),
                        entry.get("backtest_run_id"),
                        entry.get("parent_ids", []),
                        entry.get("mutations", []),
                        entry.get("is_elite", False),
                    )

    async def get_best_genome(self, run_id: str) -> Optional[dict]:
        """Get the highest-fitness genome from a run."""
        async with self.storage.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, generation, genome, fitness, metrics, is_elite
                FROM evolution_genomes
                WHERE run_id = $1 AND fitness IS NOT NULL
                ORDER BY fitness DESC
                LIMIT 1
                """,
                run_id,
            )
        if not row:
            return None
        return {
            "genome_id": row["id"],
            "generation": row["generation"],
            "genome": json.loads(row["genome"]),
            "fitness": row["fitness"],
            "metrics": json.loads(row["metrics"]) if row["metrics"] else {},
            "is_elite": row["is_elite"],
        }

    async def get_generation(self, run_id: str, generation: int) -> list[dict]:
        """Get all genomes from a specific generation."""
        async with self.storage.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, genome, fitness, metrics, parent_ids, mutations, is_elite
                FROM evolution_genomes
                WHERE run_id = $1 AND generation = $2
                ORDER BY fitness DESC NULLS LAST
                """,
                run_id,
                generation,
            )
        return [
            {
                "genome_id": row["id"],
                "genome": json.loads(row["genome"]),
                "fitness": row["fitness"],
                "metrics": json.loads(row["metrics"]) if row["metrics"] else {},
                "parent_ids": row["parent_ids"],
                "mutations": row["mutations"],
                "is_elite": row["is_elite"],
            }
            for row in rows
        ]

    async def get_run_summary(self, run_id: str) -> Optional[dict]:
        """Get run metadata and per-generation fitness stats."""
        async with self.storage.pool.acquire() as conn:
            run = await conn.fetchrow(
                "SELECT * FROM evolution_runs WHERE id = $1", run_id
            )
            if not run:
                return None

            # Per-generation stats
            gen_stats = await conn.fetch(
                """
                SELECT generation,
                       COUNT(*) as pop_size,
                       MAX(fitness) as best_fitness,
                       AVG(fitness) as avg_fitness,
                       MIN(fitness) as worst_fitness
                FROM evolution_genomes
                WHERE run_id = $1 AND fitness IS NOT NULL
                GROUP BY generation
                ORDER BY generation
                """,
                run_id,
            )

        return {
            "run_id": run["id"],
            "name": run["name"],
            "status": run["status"],
            "population_size": run["population_size"],
            "max_generations": run["max_generations"],
            "current_generation": run["current_generation"],
            "agent_class": run["agent_class"],
            "backtest_start": str(run["backtest_start"]),
            "backtest_end": str(run["backtest_end"]),
            "tick_interval": run["tick_interval"],
            "symbols": run["symbols"],
            "fitness_weights": json.loads(run["fitness_weights"]),
            "config": json.loads(run["config"]) if run["config"] else {},
            "created_at": run["created_at"].isoformat() if run["created_at"] else None,
            "completed_at": run["completed_at"].isoformat() if run["completed_at"] else None,
            "best_fitness": run["best_fitness"],
            "best_genome_id": run["best_genome_id"],
            "generations": [
                {
                    "generation": row["generation"],
                    "pop_size": row["pop_size"],
                    "best_fitness": row["best_fitness"],
                    "avg_fitness": row["avg_fitness"],
                    "worst_fitness": row["worst_fitness"],
                }
                for row in gen_stats
            ],
        }

    async def get_all_genomes(self, run_id: str, limit: int = 1000) -> list[dict]:
        """Get all genomes across all generations for a run (for lineage/family tree).

        Args:
            run_id: Evolution run ID
            limit: Maximum number of genomes to return (default 1000 to prevent memory issues)

        Returns:
            List of genome dicts, ordered by generation and fitness
        """
        async with self.storage.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, generation, genome, fitness, metrics,
                       parent_ids, mutations, is_elite
                FROM evolution_genomes
                WHERE run_id = $1
                ORDER BY generation, fitness DESC NULLS LAST
                LIMIT $2
                """,
                run_id,
                limit,
            )

        if len(rows) == limit:
            logger.warning(
                "get_all_genomes returned %d genomes (limit reached). "
                "Results may be truncated for run_id=%s",
                limit,
                run_id,
            )

        return [
            {
                "genome_id": row["id"],
                "generation": row["generation"],
                "genome": json.loads(row["genome"]),
                "fitness": row["fitness"],
                "metrics": json.loads(row["metrics"]) if row["metrics"] else {},
                "parent_ids": row["parent_ids"],
                "mutations": row["mutations"],
                "is_elite": row["is_elite"],
            }
            for row in rows
        ]

    async def get_diversity_metrics(self, run_id: str) -> list[dict]:
        """Get per-generation diversity metrics (parameter variance, unique strategy count)."""
        async with self.storage.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT generation, genome, fitness
                FROM evolution_genomes
                WHERE run_id = $1 AND fitness IS NOT NULL
                ORDER BY generation
                """,
                run_id,
            )

        # Group by generation
        gen_groups: dict[int, list[dict]] = {}
        for row in rows:
            gen = row["generation"]
            gen_groups.setdefault(gen, []).append(json.loads(row["genome"]))

        results = []
        numeric_params = [
            "temperature", "confidence_threshold", "position_size_pct",
            "sl_pct", "tp_pct", "max_leverage",
        ]

        for gen in sorted(gen_groups.keys()):
            genomes = gen_groups[gen]
            n = len(genomes)
            if n == 0:
                continue

            # Calculate variance for each numeric parameter
            param_variance: dict[str, float] = {}
            for param in numeric_params:
                values = [g.get(param, 0) for g in genomes]
                if not values:
                    param_variance[param] = 0.0
                    continue
                mean = sum(values) / n
                variance = sum((v - mean) ** 2 for v in values) / n
                param_variance[param] = round(variance, 6)

            total_variance = sum(param_variance.values())

            # Count unique strategy "fingerprints" (model + character combos)
            fingerprints = set()
            for g in genomes:
                fp = f"{g.get('model', '')}:{g.get('character', '')[:20]}"
                fingerprints.add(fp)

            results.append({
                "generation": gen,
                "population_size": n,
                "param_variance": param_variance,
                "total_variance": round(total_variance, 6),
                "unique_strategies": len(fingerprints),
            })

        return results

    async def list_runs(self, limit: int = 20) -> list[dict]:
        """List recent evolution runs."""
        async with self.storage.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, name, status, population_size, max_generations,
                       current_generation, best_fitness, created_at, completed_at
                FROM evolution_runs
                ORDER BY created_at DESC
                LIMIT $1
                """,
                limit,
            )
        return [
            {
                "run_id": row["id"],
                "name": row["name"],
                "status": row["status"],
                "population_size": row["population_size"],
                "max_generations": row["max_generations"],
                "current_generation": row["current_generation"],
                "best_fitness": row["best_fitness"],
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                "completed_at": row["completed_at"].isoformat() if row["completed_at"] else None,
            }
            for row in rows
        ]
