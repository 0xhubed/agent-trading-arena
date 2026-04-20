"""LLM-powered genetic operators — intelligent crossover and mutation via local inference."""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import random
import uuid
from typing import Any, Optional

import httpx

from agent_arena.evolution.genome import PARAM_BOUNDS, AgentGenome

logger = logging.getLogger(__name__)


class LLMOperators:
    """Uses a local LLM to perform intelligent crossover and mutation.

    Falls back to standard operators on any failure (timeout, parse error, etc.).
    """

    def __init__(
        self,
        base_url: str = "http://100.104.221.46:4000/v1",
        model: str = "nemotron",
        api_key_env: str = "LOCAL_API_KEY",
        timeout: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key = os.environ.get(api_key_env, "sk-placeholder")
        self.timeout = timeout

    async def llm_crossover(
        self,
        parent1: AgentGenome,
        parent2: AgentGenome,
        fitness1: float,
        fitness2: float,
    ) -> AgentGenome:
        """Intelligently combine two parent genomes using LLM reasoning.

        Falls back to uniform crossover on failure.
        """
        try:
            prompt = self._build_crossover_prompt(parent1, parent2, fitness1, fitness2)
            response = await self._call_llm(prompt)
            params = self._parse_genome_response(response)

            child = AgentGenome(
                agent_class=parent1.agent_class,
                generation=max(parent1.generation, parent2.generation) + 1,
                parent_ids=[parent1.genome_id, parent2.genome_id],
                mutations=["llm_crossover"],
                **params,
            )
            return child
        except Exception as e:
            logger.debug("LLM crossover failed (%s), falling back to uniform", e)
            return parent1.crossover(parent2)

    async def llm_mutation(
        self,
        genome: AgentGenome,
        fitness: float,
        generation_stats: Optional[dict] = None,
    ) -> AgentGenome:
        """LLM suggests parameter tweaks based on performance context.

        Falls back to Gaussian mutation on failure.
        """
        try:
            prompt = self._build_mutation_prompt(genome, fitness, generation_stats)
            response = await self._call_llm(prompt)
            mutations_data = self._parse_mutation_response(response)
            return self._apply_mutations(genome, mutations_data)
        except Exception as e:
            logger.debug("LLM mutation failed (%s), falling back to Gaussian", e)
            return genome.mutate(rate=0.15)

    def _build_crossover_prompt(
        self,
        parent1: AgentGenome,
        parent2: AgentGenome,
        fitness1: float,
        fitness2: float,
    ) -> str:
        return f"""You are a genetic algorithm operator for trading agent optimization.

Combine the best traits from two parent trading agents into one child.

Parent 1 (fitness: {fitness1:.4f}):
- temperature: {parent1.temperature}
- confidence_threshold: {parent1.confidence_threshold}
- position_size_pct: {parent1.position_size_pct}
- sl_pct: {parent1.sl_pct}
- tp_pct: {parent1.tp_pct}
- max_leverage: {parent1.max_leverage}
- model: {parent1.model}

Parent 2 (fitness: {fitness2:.4f}):
- temperature: {parent2.temperature}
- confidence_threshold: {parent2.confidence_threshold}
- position_size_pct: {parent2.position_size_pct}
- sl_pct: {parent2.sl_pct}
- tp_pct: {parent2.tp_pct}
- max_leverage: {parent2.max_leverage}
- model: {parent2.model}

Favor parameters from the higher-fitness parent but introduce intelligent blending.
Respond with ONLY a JSON object with these keys:
temperature, confidence_threshold, position_size_pct, sl_pct, tp_pct, max_leverage, model.
JSON only, no explanation."""

    def _build_mutation_prompt(
        self,
        genome: AgentGenome,
        fitness: float,
        generation_stats: Optional[dict] = None,
    ) -> str:
        stats_str = ""
        if generation_stats:
            stats_str = f"""
Generation stats:
- Best fitness: {generation_stats.get('best_fitness', 'N/A')}
- Avg fitness: {generation_stats.get('avg_fitness', 'N/A')}
- Generation: {generation_stats.get('generation', 'N/A')}"""

        return f"""You are a genetic algorithm mutation operator for trading agent optimization.

Suggest 1-3 parameter changes to improve this genome's performance.

Current genome (fitness: {fitness:.4f}):
- temperature: {genome.temperature} (range: 0.1-1.0)
- confidence_threshold: {genome.confidence_threshold} (range: 0.3-0.9)
- position_size_pct: {genome.position_size_pct} (range: 0.05-0.25)
- sl_pct: {genome.sl_pct} (range: 0.01-0.05)
- tp_pct: {genome.tp_pct} (range: 0.02-0.10)
- max_leverage: {genome.max_leverage} (range: 1-10)
{stats_str}

Respond with ONLY a JSON object containing the parameters you want to change and their new values.
Only include parameters you want to modify (1-3 changes).
JSON only, no explanation."""

    async def _call_llm(self, prompt: str, retries: int = 3) -> str:
        """Call local inference endpoint via OpenAI-compatible API with retries."""
        last_error: Exception | None = None

        for attempt in range(retries):
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    resp = await client.post(
                        f"{self.base_url}/chat/completions",
                        headers={
                            "Authorization": f"Bearer {self.api_key}",
                            "Content-Type": "application/json",
                        },
                        json={
                            "model": self.model,
                            "messages": [{"role": "user", "content": prompt}],
                            "temperature": 0.7,
                            "max_tokens": 256,
                        },
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    return data["choices"][0]["message"]["content"]
            except (httpx.TimeoutException, httpx.HTTPStatusError) as e:
                last_error = e
                if attempt < retries - 1:
                    wait = 2 ** attempt
                    logger.debug(
                        "LLM call failed (attempt %d/%d), retrying in %ds: %s",
                        attempt + 1, retries, wait, e,
                    )
                    await asyncio.sleep(wait)

        raise last_error  # type: ignore[misc]

    def _extract_json(self, response: str) -> str:
        """Extract JSON object string from LLM response text."""
        text = response.strip()
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end < start:
            raise ValueError("No JSON object found in LLM response")
        return text[start:end + 1]

    def _parse_genome_response(self, response: str) -> dict:
        """Extract and validate genome parameters from LLM response."""
        text = self._extract_json(response)
        data = json.loads(text)
        return self._clamp_params(data)

    def _parse_mutation_response(self, response: str) -> dict:
        """Extract mutation suggestions from LLM response."""
        text = self._extract_json(response)
        return json.loads(text)

    def _clamp_params(self, data: dict) -> dict:
        """Clamp all parameters to PARAM_BOUNDS."""
        result: dict[str, Any] = {}
        for param, bounds in PARAM_BOUNDS.items():
            if param not in data:
                continue
            val = data[param]
            if isinstance(bounds, list):
                # Categorical: must be in list
                result[param] = val if val in bounds else random.choice(bounds)
            elif isinstance(bounds, tuple):
                lo, hi = bounds
                if isinstance(lo, int) and isinstance(hi, int):
                    result[param] = max(lo, min(hi, int(val)))
                else:
                    result[param] = round(max(lo, min(hi, float(val))), 3)
        return result

    def _apply_mutations(self, genome: AgentGenome, mutations_data: dict) -> AgentGenome:
        """Apply LLM-suggested mutations to a genome copy."""
        child = copy.deepcopy(genome)
        child.genome_id = f"g_{uuid.uuid4().hex[:12]}"
        child.parent_ids = [genome.genome_id]
        child.mutations = []

        clamped = self._clamp_params(mutations_data)
        for param, new_val in clamped.items():
            if not hasattr(child, param):
                continue
            old_val = getattr(child, param)
            if new_val != old_val:
                setattr(child, param, new_val)
                child.mutations.append(f"llm_mutate {param}: {old_val} -> {new_val}")

        if not child.mutations:
            child.mutations.append("llm_mutate (no change)")

        return child
