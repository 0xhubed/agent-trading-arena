"""EvolutionEngine — genetic algorithm loop that evolves trading agents via backtesting."""

from __future__ import annotations

import logging
import random
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from agent_arena.backtest.results import AgentResult, BacktestResult
from agent_arena.backtest.runner import BacktestRunner
from agent_arena.core.config import CandleConfig, CompetitionConfig
from agent_arena.core.loader import load_agent
from agent_arena.evolution.fitness import FitnessEvaluator
from agent_arena.evolution.genome import AgentGenome
from agent_arena.evolution.storage import EvolutionStorage

logger = logging.getLogger(__name__)


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Convert value to float, handling None and invalid values."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class EvolutionEngine:
    """Genetic algorithm engine that evolves agent configurations.

    Key insight: each generation runs as a single BacktestRunner with N agents
    (one per genome), sharing the same market data. This is ~Nx more efficient
    than running N separate backtests.
    """

    def __init__(
        self,
        population_size: int = 20,
        generations: int = 10,
        elite_count: int = 3,
        mutation_rate: float = 0.15,
        tournament_size: int = 3,
        backtest_start: str = "",
        backtest_end: str = "",
        tick_interval: str = "4h",
        symbols: Optional[list[str]] = None,
        storage: Any = None,
        fitness_weights: Optional[dict] = None,
        agent_class: str = "agent_arena.agents.llm_trader.LLMTrader",
        base_url: str = "http://192.168.0.42:8001/v1",
        api_key_env: str = "LOCAL_API_KEY",
        candle_intervals: Optional[list[str]] = None,
        validation_split: float = 0.3,
        event_emitter: Optional[Callable] = None,
        on_completion_callback: Optional[Callable] = None,
        # Advanced GA features
        use_llm_operators: bool = False,
        llm_operator_prob: float = 0.3,
        use_novelty: bool = False,
        novelty_weight: float = 0.3,
        use_islands: bool = False,
        num_islands: int = 4,
        migration_interval: int = 5,
        use_pareto: bool = False,
    ):
        self.population_size = population_size
        self.generations = generations
        self.elite_count = min(elite_count, population_size)
        self.mutation_rate = mutation_rate
        self.tournament_size = min(tournament_size, population_size)
        self.backtest_start = backtest_start
        self.backtest_end = backtest_end
        self.tick_interval = tick_interval
        self.symbols = symbols or ["PF_XBTUSD", "PF_ETHUSD", "PF_SOLUSD"]
        self.storage = storage
        self.agent_class = agent_class
        self.base_url = base_url
        self.api_key_env = api_key_env
        self.candle_intervals = candle_intervals or ["1h", "4h"]
        self.validation_split = validation_split
        self.emit = event_emitter or (lambda *a, **kw: None)
        self.on_completion = on_completion_callback

        self.fitness = FitnessEvaluator(weights=fitness_weights or {
            "sharpe": 0.40, "return": 0.25, "win_rate": 0.15, "drawdown": 0.20,
        })
        self.evo_storage: Optional[EvolutionStorage] = None

        # Advanced GA features
        self.use_llm_operators = use_llm_operators
        self.llm_operator_prob = llm_operator_prob
        self.use_novelty = use_novelty
        self.novelty_weight = novelty_weight
        self.use_islands = use_islands
        self.num_islands = num_islands
        self.migration_interval = migration_interval
        self.use_pareto = use_pareto

        # Conditionally initialize advanced modules
        self.llm_ops = None
        if use_llm_operators:
            from agent_arena.evolution.llm_operators import LLMOperators
            self.llm_ops = LLMOperators(
                base_url=base_url,
                api_key_env=api_key_env,
            )

        self.novelty_search = None
        if use_novelty:
            from agent_arena.evolution.novelty import NoveltySearch
            self.novelty_search = NoveltySearch()

        self.pareto_optimizer = None
        if use_pareto:
            from agent_arena.evolution.pareto import ParetoOptimizer
            self.pareto_optimizer = ParetoOptimizer()

        # State — run_id assigned eagerly so callers can read it
        # before awaiting run()
        self.run_id: str = f"evo_{uuid.uuid4().hex[:12]}"
        self.population: list[AgentGenome] = []
        self._cancelled = False

    def cancel(self) -> None:
        """Cancel a running evolution."""
        self._cancelled = True

    async def run(self, name: str = "Evolution Run") -> dict:
        """Execute the full GA loop.

        Returns a summary dict with best genome, fitness curve, and validation results.
        """
        self._cancelled = False
        try:
            return await self._run_evolution(name)
        except Exception as e:
            logger.error(
                "Evolution run %s failed: %s",
                self.run_id, e, exc_info=True,
            )
            if self.evo_storage:
                try:
                    await self.evo_storage.update_run(
                        self.run_id,
                        status="failed",
                        completed_at=datetime.now(timezone.utc),
                    )
                except Exception:
                    logger.error("Failed to mark run %s as failed", self.run_id)
            raise

    async def _run_evolution(self, name: str) -> dict:
        """Internal evolution loop (wrapped by run() for error handling)."""

        # Split date range for walk-forward validation
        train_start, train_end, val_start, val_end = self._split_date_range()

        # Initialize storage
        if self.storage and hasattr(self.storage, "pool"):
            self.evo_storage = EvolutionStorage(self.storage)
            await self.evo_storage.create_run(
                run_id=self.run_id,
                name=name,
                population_size=self.population_size,
                max_generations=self.generations,
                agent_class=self.agent_class,
                backtest_start=self.backtest_start,
                backtest_end=self.backtest_end,
                tick_interval=self.tick_interval,
                symbols=self.symbols,
                fitness_weights=self.fitness.weights,
                config={
                    "elite_count": self.elite_count,
                    "mutation_rate": self.mutation_rate,
                    "tournament_size": self.tournament_size,
                    "base_url": self.base_url,
                    "api_key_env": self.api_key_env,
                    "validation_split": self.validation_split,
                    "train_start": train_start,
                    "train_end": train_end,
                    "val_start": val_start,
                    "val_end": val_end,
                    "use_llm_operators": self.use_llm_operators,
                    "use_novelty": self.use_novelty,
                    "use_islands": self.use_islands,
                    "use_pareto": self.use_pareto,
                },
            )

        # Initialize random population
        self.population = [
            AgentGenome.randomize(agent_class=self.agent_class, generation=0)
            for _ in range(self.population_size)
        ]

        best_genome: Optional[AgentGenome] = None
        best_fitness: float = -1.0
        fitness_history: list[dict] = []

        logger.info(
            "Evolution started: run_id=%s, pop=%d, gens=%d, train=%s..%s, val=%s..%s",
            self.run_id, self.population_size, self.generations,
            train_start, train_end, val_start, val_end,
        )

        for gen in range(self.generations):
            if self._cancelled:
                logger.info("Evolution cancelled at generation %d", gen)
                break

            logger.info("Generation %d/%d — evaluating %d genomes",
                        gen, self.generations - 1, len(self.population))

            # Evaluate population on training period
            fitness_map, metrics_map, agent_results_map, bt_run_id = (
                await self._evaluate_generation(
                    self.population, train_start, train_end, gen
                )
            )

            # --- Novelty scoring ---
            novelty_scores: dict[str, float] = {}
            if self.novelty_search and agent_results_map:
                signatures: dict[str, list[float]] = {}
                for genome in self.population:
                    ar = agent_results_map.get(genome.genome_id)
                    if ar:
                        sig = self.novelty_search.calculate_behavioral_signature(genome, ar)
                        signatures[genome.genome_id] = sig

                pop_sigs = list(signatures.values())
                for gid, sig in signatures.items():
                    ns = self.novelty_search.calculate_novelty_score(sig, pop_sigs)
                    novelty_scores[gid] = ns
                    self.novelty_search.update_archive(gid, sig, ns)

                # Override fitness_map with combined scores
                for gid in fitness_map:
                    if gid in novelty_scores:
                        fitness_map[gid] = self.novelty_search.get_combined_score(
                            fitness_map[gid],
                            novelty_scores[gid],
                            self.novelty_weight,
                        )

            # --- Pareto ranking ---
            pareto_ranks: dict[str, int] = {}
            crowding_distances: dict[str, float] = {}
            if self.pareto_optimizer:
                from agent_arena.evolution.pareto import ParetoMetrics
                pareto_metrics = {}
                for gid, m in metrics_map.items():
                    pareto_metrics[gid] = ParetoMetrics(
                        sharpe_ratio=_safe_float(m.get("sharpe_ratio")),
                        total_return=_safe_float(m.get("total_pnl_pct")),
                        max_drawdown=abs(_safe_float(m.get("max_drawdown_pct"))),
                        win_rate=_safe_float(m.get("win_rate")),
                    )
                pareto_ranks = self.pareto_optimizer.calculate_pareto_rank(
                    self.population, pareto_metrics
                )
                crowding_distances = self.pareto_optimizer.calculate_crowding_distance(
                    self.population, pareto_metrics, pareto_ranks
                )
                # Use Pareto ranking for selection
                ranked = self.pareto_optimizer.rank_population(
                    self.population, pareto_metrics
                )
            else:
                # Rank by fitness
                ranked = sorted(
                    self.population,
                    key=lambda g: fitness_map.get(g.genome_id, 0.0),
                    reverse=True,
                )

            gen_best = ranked[0]
            gen_best_fitness = fitness_map.get(gen_best.genome_id, 0.0)
            gen_avg = sum(fitness_map.values()) / max(len(fitness_map), 1)

            if gen_best_fitness > best_fitness:
                best_fitness = gen_best_fitness
                best_genome = gen_best

            fitness_history.append({
                "generation": gen,
                "best_fitness": gen_best_fitness,
                "avg_fitness": round(gen_avg, 6),
                "best_genome_id": gen_best.genome_id,
            })

            self.emit("evolution_generation", {
                "run_id": self.run_id,
                "generation": gen,
                "best_fitness": gen_best_fitness,
                "avg_fitness": gen_avg,
                "best_genome_id": gen_best.genome_id,
            })

            # Persist generation
            if self.evo_storage:
                elites_set = {g.genome_id for g in ranked[:self.elite_count]}
                await self.evo_storage.save_generation(
                    run_id=self.run_id,
                    generation=gen,
                    genomes_with_fitness=[
                        {
                            "genome_id": g.genome_id,
                            "genome": g.to_dict(),
                            "fitness": fitness_map.get(g.genome_id, 0.0),
                            "metrics": {
                                **metrics_map.get(g.genome_id, {}),
                                "novelty_score": novelty_scores.get(g.genome_id),
                                "pareto_rank": pareto_ranks.get(g.genome_id),
                                "crowding_distance": crowding_distances.get(g.genome_id),
                                "island_id": None,  # Set below if islands enabled
                            },
                            "backtest_run_id": bt_run_id,
                            "parent_ids": g.parent_ids,
                            "mutations": g.mutations,
                            "is_elite": g.genome_id in elites_set,
                        }
                        for g in self.population
                    ],
                )
                await self.evo_storage.update_run(
                    self.run_id,
                    current_generation=gen,
                    best_fitness=best_fitness,
                    best_genome_id=best_genome.genome_id if best_genome else None,
                )

            logger.info(
                "Gen %d complete: best=%.4f avg=%.4f genome=%s",
                gen, gen_best_fitness, gen_avg, gen_best.genome_id[:8],
            )

            # Emit elite selection events
            self.emit("evolution_event", {
                "run_id": self.run_id,
                "generation": gen,
                "type": "elite_selection",
                "elite_ids": [g.genome_id for g in ranked[:self.elite_count]],
            })

            # Build next generation (skip for final generation)
            if gen < self.generations - 1:
                gen_stats = {
                    "generation": gen,
                    "best_fitness": gen_best_fitness,
                    "avg_fitness": gen_avg,
                }
                self.population = await self._build_next_generation(
                    ranked, fitness_map, gen_stats
                )

        # Walk-forward validation on the best genome
        validation_result = None
        overfit_warning = False

        if best_genome and val_start and val_end:
            logger.info("Running walk-forward validation for best genome on %s..%s",
                        val_start, val_end)
            val_fitness_map, val_metrics_map, _, _ = await self._evaluate_generation(
                [best_genome], val_start, val_end, gen=-1
            )
            val_fitness = val_fitness_map.get(best_genome.genome_id, 0.0)
            validation_result = {
                "train_fitness": best_fitness,
                "val_fitness": val_fitness,
                "metrics": val_metrics_map.get(best_genome.genome_id, {}),
            }
            if best_fitness > 0 and val_fitness < best_fitness * 0.7:
                overfit_warning = True
                logger.warning(
                    "Overfitting detected: train=%.4f val=%.4f (%.0f%% drop)",
                    best_fitness, val_fitness,
                    (1 - val_fitness / best_fitness) * 100,
                )

        # Finalize
        if self.evo_storage:
            await self.evo_storage.update_run(
                self.run_id,
                status="completed" if not self._cancelled else "cancelled",
                completed_at=datetime.now(timezone.utc),
                best_fitness=best_fitness,
                best_genome_id=best_genome.genome_id if best_genome else None,
            )

        # Trigger Observer analysis (M3.5) if evolution completed successfully
        if self.evo_storage and not self._cancelled and best_genome and self.on_completion:
            logger.info("Triggering completion callback for evolution run: %s", self.run_id)
            try:
                await self.on_completion(self.run_id)
            except Exception as e:
                logger.error("Failed to run completion callback: %s", e, exc_info=True)

        summary = {
            "run_id": self.run_id,
            "status": "completed" if not self._cancelled else "cancelled",
            "generations_completed": len(fitness_history),
            "best_genome": best_genome.to_dict() if best_genome else None,
            "best_fitness": best_fitness,
            "fitness_history": fitness_history,
            "validation": validation_result,
            "overfit_warning": overfit_warning,
        }

        logger.info("Evolution complete: best_fitness=%.4f, overfit=%s",
                     best_fitness, overfit_warning)
        return summary

    async def _evaluate_generation(
        self,
        genomes: list[AgentGenome],
        start_date: str,
        end_date: str,
        gen: int,
    ) -> tuple[dict[str, float], dict[str, dict], dict[str, AgentResult], str]:
        """Run one backtest with all genomes as concurrent agents.

        Returns (fitness_map, metrics_map, agent_results_map, backtest_run_id).
        """
        # Convert genomes to agents
        agents = []
        genome_to_agent: dict[str, str] = {}  # genome_id -> agent_id

        for i, genome in enumerate(genomes):
            agent_id = f"evo_{self.run_id[-8:]}_{gen}_{i}"
            agent_name = f"Genome {genome.genome_id[:8]}"
            agent_config = genome.to_agent_config(
                agent_id=agent_id,
                agent_name=agent_name,
                base_url=self.base_url,
                api_key_env=self.api_key_env,
            )
            agent = load_agent(agent_config)
            agents.append(agent)
            genome_to_agent[genome.genome_id] = agent_id

        # Build competition config
        config = CompetitionConfig(
            name=f"evo_{self.run_id}_gen{gen}",
            symbols=self.symbols,
            interval_seconds=self._interval_to_seconds(),
            candles=CandleConfig(
                enabled=True,
                intervals=self.candle_intervals,
                limit=100,
            ),
        )

        # Run backtest
        runner = BacktestRunner(
            config=config,
            agents=agents,
            storage=self.storage,
            start_date=start_date,
            end_date=end_date,
            tick_interval=self.tick_interval,
        )

        bt_run_id = f"bt_evo_{self.run_id[-8:]}_g{gen}_{uuid.uuid4().hex[:6]}"
        result: BacktestResult = await runner.run(
            name=f"Evolution gen {gen}",
            save_results=False,
            run_id=bt_run_id,
        )

        # Map backtest results back to genome IDs
        agent_to_genome = {v: k for k, v in genome_to_agent.items()}

        fitness_map: dict[str, float] = {}
        metrics_map: dict[str, dict] = {}
        agent_results_map: dict[str, AgentResult] = {}

        for agent_id, agent_result in result.agent_results.items():
            genome_id = agent_to_genome.get(agent_id)
            if genome_id is None:
                continue
            fitness_map[genome_id] = self.fitness.evaluate(agent_result)
            metrics_map[genome_id] = {
                "total_pnl_pct": agent_result.total_pnl_pct,
                "sharpe_ratio": agent_result.sharpe_ratio,
                "win_rate": agent_result.win_rate,
                "max_drawdown_pct": agent_result.max_drawdown_pct,
                "total_trades": agent_result.total_trades,
                "final_equity": float(agent_result.final_equity),
            }
            agent_results_map[genome_id] = agent_result

        return fitness_map, metrics_map, agent_results_map, bt_run_id

    async def _build_next_generation(
        self,
        ranked: list[AgentGenome],
        fitness_map: dict[str, float],
        gen_stats: Optional[dict] = None,
    ) -> list[AgentGenome]:
        """Selection, crossover, and mutation to produce the next generation."""
        next_gen: list[AgentGenome] = []
        new_generation = ranked[0].generation + 1

        # Elitism: carry top genomes unchanged
        for elite in ranked[:self.elite_count]:
            elite_copy = AgentGenome.from_dict(elite.to_dict())
            elite_copy.generation = new_generation
            next_gen.append(elite_copy)

        # Fill the rest via tournament selection + crossover + mutation
        while len(next_gen) < self.population_size:
            parent_a = self._tournament_select(ranked, fitness_map)
            parent_b = self._tournament_select(ranked, fitness_map)

            # Crossover
            use_llm = (
                self.llm_ops is not None
                and random.random() < self.llm_operator_prob
            )
            if use_llm:
                child = await self.llm_ops.llm_crossover(
                    parent_a, parent_b,
                    fitness_map.get(parent_a.genome_id, 0.0),
                    fitness_map.get(parent_b.genome_id, 0.0),
                )
                operator_type = "llm_crossover"
            else:
                child = parent_a.crossover(parent_b)
                operator_type = "crossover"
            child.generation = new_generation

            logger.debug(
                "%s: %s + %s -> %s",
                operator_type, parent_a.genome_id[:8],
                parent_b.genome_id[:8], child.genome_id[:8],
            )
            self.emit("evolution_event", {
                "run_id": self.run_id,
                "type": operator_type,
                "parent_ids": [parent_a.genome_id, parent_b.genome_id],
                "child_id": child.genome_id,
            })

            # Mutation
            use_llm_mut = (
                self.llm_ops is not None
                and random.random() < self.llm_operator_prob
            )
            if use_llm_mut:
                child = await self.llm_ops.llm_mutation(
                    child,
                    fitness_map.get(parent_a.genome_id, 0.0),
                    gen_stats,
                )
                mut_type = "llm_mutation"
            else:
                child = child.mutate(rate=self.mutation_rate)
                mut_type = "mutation"
            child.generation = new_generation

            logger.debug(
                "%s: %s mutations=%s",
                mut_type, child.genome_id[:8], child.mutations,
            )
            self.emit("evolution_event", {
                "run_id": self.run_id,
                "type": mut_type,
                "genome_id": child.genome_id,
                "mutations": child.mutations,
            })

            next_gen.append(child)

        return next_gen[:self.population_size]

    def _tournament_select(
        self,
        population: list[AgentGenome],
        fitness_map: dict[str, float],
    ) -> AgentGenome:
        """Select a genome via tournament selection."""
        contestants = random.sample(population, min(self.tournament_size, len(population)))
        return max(contestants, key=lambda g: fitness_map.get(g.genome_id, 0.0))

    def _split_date_range(self) -> tuple[str, str, str, str]:
        """Split backtest date range into 70% train / 30% validation."""
        from datetime import timedelta

        try:
            start = datetime.strptime(self.backtest_start, "%Y-%m-%d")
            end = datetime.strptime(self.backtest_end, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(
                f"Invalid date format (expected YYYY-MM-DD): "
                f"start={self.backtest_start}, end={self.backtest_end}"
            ) from e

        if end <= start:
            raise ValueError(
                f"End date must be after start date: {self.backtest_start} >= {self.backtest_end}"
            )

        total_days = (end - start).days

        if total_days < 7 or self.validation_split <= 0:
            # Too short to split meaningfully
            fmt = "%Y-%m-%d"
            return self.backtest_start, self.backtest_end, "", ""

        train_days = int(total_days * (1 - self.validation_split))
        train_end = start + timedelta(days=train_days)
        val_start = train_end + timedelta(days=1)

        fmt = "%Y-%m-%d"
        return (
            start.strftime(fmt),
            train_end.strftime(fmt),
            val_start.strftime(fmt),
            end.strftime(fmt),
        )

    def _interval_to_seconds(self) -> int:
        """Convert tick interval string to seconds."""
        multipliers = {"m": 60, "h": 3600, "d": 86400}
        for suffix, mult in multipliers.items():
            if self.tick_interval.endswith(suffix):
                return int(self.tick_interval[:-len(suffix)]) * mult
        return 3600
