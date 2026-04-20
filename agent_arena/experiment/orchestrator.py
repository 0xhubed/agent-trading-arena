"""Experiment Orchestrator — runs evolution experiments on scenarios.

Orchestrates: scenario selection → population init → evolution (via EvolutionEngine)
→ validation on held-out scenario → promotion candidates.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from agent_arena.evolution.engine import EvolutionEngine
from agent_arena.evolution.genome import AgentGenome
from agent_arena.experiment.cost_tracker import BudgetExceededError, CostTracker

logger = logging.getLogger(__name__)


@dataclass
class ExperimentConfig:
    """Configuration for an experiment run."""

    name: str = "Experiment"
    population_size: int = 16
    generations: int = 5
    budget_limit_usd: float = 5.0

    # Scenario IDs for training
    scenario_ids: list[str] = field(default_factory=list)
    # Held-out scenario for validation
    validation_scenario_id: str = ""

    # Fitness weights (passed to EvolutionEngine)
    fitness_weights: dict[str, float] = field(default_factory=lambda: {
        "sharpe": 0.3,
        "return": 0.3,
        "win_rate": 0.2,
        "drawdown": 0.2,
    })

    # Evolution params
    elite_count: int = 3
    mutation_rate: float = 0.15
    agent_class: str = "agent_arena.agents.llm_trader.LLMTrader"
    base_url: str = ""
    api_key_env: str = "TOGETHER_API_KEY"

    # Backtest params (used if no scenarios provided)
    backtest_start: str = ""
    backtest_end: str = ""
    tick_interval: str = "4h"
    symbols: list[str] = field(default_factory=lambda: ["PF_XBTUSD", "PF_ETHUSD", "PF_SOLUSD"])

    # Validation
    validation_threshold: float = 0.7  # min validation/train fitness ratio
    promotion_count: int = 3  # max promotion candidates per run

    def validate(self) -> list[str]:
        """Validate config, returning list of error strings."""
        errors = []
        if self.population_size < 4:
            errors.append("population_size must be >= 4")
        if self.generations < 1:
            errors.append("generations must be >= 1")
        if self.budget_limit_usd <= 0:
            errors.append("budget_limit_usd must be > 0")
        if not self.backtest_start and not self.scenario_ids:
            errors.append("Must provide either backtest_start/end or scenario_ids")
        if self.elite_count >= self.population_size:
            errors.append("elite_count must be < population_size")
        return errors


@dataclass
class ExperimentResult:
    """Result of a completed experiment."""

    experiment_id: str = ""
    status: str = "pending"  # pending, running, completed, failed, budget_exceeded
    config: Optional[ExperimentConfig] = None

    # Results
    best_genome: Optional[dict] = None
    best_fitness: float = 0.0
    validation_fitness: float = 0.0
    overfit_warning: bool = False
    total_cost_usd: float = 0.0
    generations_completed: int = 0

    # Promotion candidates (top N genomes that passed validation)
    promotion_candidates: list[dict] = field(default_factory=list)

    error: str = ""

    def to_dict(self) -> dict:
        return {
            "experiment_id": self.experiment_id,
            "status": self.status,
            "best_fitness": self.best_fitness,
            "validation_fitness": self.validation_fitness,
            "overfit_warning": self.overfit_warning,
            "total_cost_usd": round(self.total_cost_usd, 4),
            "generations_completed": self.generations_completed,
            "promotion_candidates": self.promotion_candidates,
            "best_genome": self.best_genome,
            "error": self.error,
        }


class ExperimentOrchestrator:
    """Orchestrates experiment cycles: evolve → validate → promote.

    Delegates actual evolution to the existing EvolutionEngine, wrapping it
    with budget tracking, validation, and promotion candidate selection.
    """

    def __init__(
        self,
        config: ExperimentConfig,
        storage: Any,
        event_emitter: Any = None,
    ):
        self.config = config
        self.storage = storage
        self.event_emitter = event_emitter
        self.cost_tracker = CostTracker(budget_limit_usd=config.budget_limit_usd)
        self.experiment_id = f"exp_{uuid.uuid4().hex[:12]}"

    async def run(self) -> ExperimentResult:
        """Run the full experiment cycle."""
        result = ExperimentResult(
            experiment_id=self.experiment_id,
            status="running",
            config=self.config,
        )

        # Validate config
        errors = self.config.validate()
        if errors:
            result.status = "failed"
            result.error = "; ".join(errors)
            return result

        try:
            # Save experiment run to DB
            await self._save_run(result)

            # Phase 1: Run evolution
            logger.info(
                "Starting experiment %s: pop=%d, gens=%d, budget=$%.2f",
                self.experiment_id,
                self.config.population_size,
                self.config.generations,
                self.config.budget_limit_usd,
            )

            evo_result = await self._run_evolution()

            result.best_fitness = evo_result.get("best_fitness", 0.0)
            result.best_genome = evo_result.get("best_genome")
            result.generations_completed = evo_result.get("generations_completed", 0)
            result.total_cost_usd = self.cost_tracker.total_spent

            # Phase 2: Validate on held-out data (if configured)
            if self.config.validation_scenario_id or (
                self.config.backtest_start and self.config.backtest_end
            ):
                val_fitness = await self._run_validation(evo_result)
                result.validation_fitness = val_fitness

                if result.best_fitness > 0:
                    ratio = val_fitness / result.best_fitness
                    result.overfit_warning = ratio < self.config.validation_threshold
                    if result.overfit_warning:
                        logger.warning(
                            "Overfitting detected: train=%.4f, val=%.4f (ratio=%.2f)",
                            result.best_fitness, val_fitness, ratio,
                        )

            # Phase 3: Select promotion candidates
            result.promotion_candidates = self._select_candidates(evo_result)

            result.status = "completed"
            logger.info(
                "Experiment %s completed: best_fitness=%.4f, candidates=%d, cost=$%.2f",
                self.experiment_id,
                result.best_fitness,
                len(result.promotion_candidates),
                result.total_cost_usd,
            )

        except BudgetExceededError as e:
            result.status = "budget_exceeded"
            result.error = str(e)
            result.total_cost_usd = self.cost_tracker.total_spent
            logger.warning("Experiment %s: %s", self.experiment_id, e)

        except Exception as e:
            result.status = "failed"
            result.error = str(e)
            result.total_cost_usd = self.cost_tracker.total_spent
            logger.exception("Experiment %s failed", self.experiment_id)

        # Update DB
        await self._save_run(result)
        return result

    async def _run_evolution(self) -> dict:
        """Run the evolution engine with budget tracking."""

        def cost_aware_emitter(event_type: str, data: dict) -> None:
            """Track costs from evolution events."""
            if event_type == "evolution_generation":
                estimated_cost = self.cost_tracker.estimate_generation_cost(
                    self.config.population_size,
                    data.get("total_ticks", 100),
                )
                self.cost_tracker.record(
                    estimated_cost,
                    f"Generation {data.get('generation', '?')}",
                )
                self.cost_tracker.check_budget()

            if self.event_emitter:
                self.event_emitter(event_type, data)

        engine = EvolutionEngine(
            population_size=self.config.population_size,
            generations=self.config.generations,
            elite_count=self.config.elite_count,
            mutation_rate=self.config.mutation_rate,
            backtest_start=self.config.backtest_start,
            backtest_end=self.config.backtest_end,
            tick_interval=self.config.tick_interval,
            symbols=self.config.symbols,
            storage=self.storage,
            fitness_weights=self.config.fitness_weights,
            agent_class=self.config.agent_class,
            base_url=self.config.base_url,
            api_key_env=self.config.api_key_env,
            event_emitter=cost_aware_emitter,
        )

        return await engine.run(name=self.config.name)

    async def _run_validation(self, evo_result: dict) -> float:
        """Validate best genome on held-out data. Returns validation fitness."""
        best_genome_data = evo_result.get("best_genome")
        if not best_genome_data:
            return 0.0

        # For now, use the validation fitness reported by EvolutionEngine
        # (which already does an 80/20 train/val split internally)
        if evo_result.get("validation"):
            return evo_result["validation"].get("val_fitness", 0.0)

        return 0.0

    def _select_candidates(self, evo_result: dict) -> list[dict]:
        """Select top genomes as promotion candidates."""
        candidates = []
        top_genomes = evo_result.get("top_genomes", [])

        if not top_genomes and evo_result.get("best_genome"):
            top_genomes = [evo_result["best_genome"]]

        for genome_data in top_genomes[: self.config.promotion_count]:
            candidates.append({
                "genome": genome_data,
                "fitness": genome_data.get("fitness", 0.0),
                "experiment_id": self.experiment_id,
                "status": "pending",
            })

        return candidates

    async def _save_run(self, result: ExperimentResult) -> None:
        """Save experiment run state to database."""
        if not hasattr(self.storage, "pool"):
            return  # SQLite — skip for now

        try:
            async with self.storage.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO experiment_runs (
                        id, name, status, config, best_fitness,
                        validation_fitness, overfit_warning,
                        total_cost_usd, generations_completed,
                        promotion_candidates, best_genome, error,
                        created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $13)
                    ON CONFLICT (id) DO UPDATE SET
                        status = $3,
                        best_fitness = $5,
                        validation_fitness = $6,
                        overfit_warning = $7,
                        total_cost_usd = $8,
                        generations_completed = $9,
                        promotion_candidates = $10,
                        best_genome = $11,
                        error = $12,
                        updated_at = $13
                    """,
                    self.experiment_id,
                    self.config.name,
                    result.status,
                    json.dumps(self.config.__dict__
                        if self.config else {}),
                    result.best_fitness,
                    result.validation_fitness,
                    result.overfit_warning,
                    result.total_cost_usd,
                    result.generations_completed,
                    json.dumps(result.promotion_candidates),
                    json.dumps(result.best_genome) if result.best_genome else None,
                    result.error,
                    datetime.now(timezone.utc),
                )
        except Exception:
            logger.exception("Failed to save experiment run %s", self.experiment_id)
