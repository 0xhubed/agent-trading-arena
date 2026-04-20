"""Memory Digester — compresses low-scoring memories into abstract principles."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from typing import Any, Optional

from agent_arena.llm_utils import extract_json_from_llm, strip_think_blocks
from agent_arena.memory.models import AbstractPrinciple, DigestionResult, ScoredMemory
from agent_arena.memory.scoring import MemoryScorer

logger = logging.getLogger(__name__)

DIGESTION_PROMPT = """You are distilling {count} episodic trade memories into a single abstract trading principle.

Memories:
{memories}

Extract ONE concise, actionable principle that captures the common lesson.
The principle should be general enough to apply in similar future situations,
not tied to specific prices or dates.

Respond in JSON:
{{
  "principle": "The abstract principle (1-2 sentences)",
  "regime": "Market regime this applies to, or 'all'",
  "confidence": 0.0-1.0
}}

Example: "Low RSI alone is insufficient for long entries — check funding rate direction first."
"""


class MemoryDigester:
    """Compresses low-scoring memories into abstract principles.

    Pipeline:
    1. Score all memories (via MemoryScorer)
    2. Group digest-candidates by topic similarity
    3. LLM extracts one principle per group
    4. Save principles, mark originals as digested
    5. Prune memories below prune threshold
    """

    def __init__(
        self,
        storage: Any,
        model: str = "claude-sonnet-4-6",
        min_group_size: int = 3,
    ):
        self.storage = storage
        self.scorer = MemoryScorer(storage)
        self.model = model
        self.min_group_size = min_group_size

    async def run_digestion_cycle(self, agent_id: str) -> DigestionResult:
        """Run a full digestion cycle for an agent.

        Returns DigestionResult with counts and details.
        """
        result = DigestionResult(agent_id=agent_id)

        # Step 1: Score all memories
        scored = await self.scorer.score_memories(agent_id)
        result.memories_scored = len(scored)

        if not scored:
            return result

        # Update scores in DB
        await self.scorer.update_scores_in_db(scored)

        # Step 2: Separate by action
        to_digest = [m for m in scored if m.action == "digest"]
        to_prune = [m for m in scored if m.action == "prune"]

        # Step 3: Group digest candidates by topic
        if len(to_digest) >= self.min_group_size:
            groups = self._group_by_topic(to_digest)

            for topic, group in groups.items():
                if len(group) < self.min_group_size:
                    continue

                # Extract principle via LLM
                principle = await self._extract_principle(agent_id, group)
                if principle:
                    await self._save_principle(principle)
                    result.principles_created += 1

                    # Mark originals as digested
                    await self._mark_digested([m.memory_id for m in group])
                    result.memories_digested += len(group)

        # Step 4: Prune very low-scoring memories
        if to_prune:
            await self._mark_digested([m.memory_id for m in to_prune])
            result.memories_pruned = len(to_prune)

        # Save digestion history
        await self._save_history(result)

        logger.info(
            "Digestion for %s: scored=%d, digested=%d, pruned=%d, principles=%d",
            agent_id,
            result.memories_scored,
            result.memories_digested,
            result.memories_pruned,
            result.principles_created,
        )

        return result

    def _group_by_topic(self, memories: list[ScoredMemory]) -> dict[str, list[ScoredMemory]]:
        """Group memories by simple keyword/topic similarity.

        Uses the first significant word of the lesson as a grouping key.
        In production, this would use embedding clustering.
        """
        groups: dict[str, list[ScoredMemory]] = defaultdict(list)

        for m in memories:
            # Simple topic extraction: first 3 significant words
            words = [w.lower() for w in m.content.split() if len(w) > 3][:3]
            topic = " ".join(words) if words else "misc"
            groups[topic].append(m)

        return dict(groups)

    async def _extract_principle(
        self, agent_id: str, memories: list[ScoredMemory],
    ) -> Optional[AbstractPrinciple]:
        """Use LLM to extract an abstract principle from a group of memories."""
        from langchain_anthropic import ChatAnthropic
        from langchain_core.messages import HumanMessage

        memory_texts = []
        reflection_ids = []
        for m in memories:
            memory_texts.append(f"- {m.content} (PnL: ${m.pnl:+.2f})")
            reflection_ids.append(m.memory_id)

        prompt = DIGESTION_PROMPT.format(
            count=len(memories),
            memories="\n".join(memory_texts),
        )

        try:
            llm = ChatAnthropic(model=self.model, max_tokens=300, temperature=0.3)
            response = await llm.ainvoke([HumanMessage(content=prompt)])
            content = strip_think_blocks(response.content)

            parsed = extract_json_from_llm(content)
            if not parsed or not parsed.get("principle"):
                return None

            return AbstractPrinciple(
                agent_id=agent_id,
                principle=parsed["principle"],
                source_type="trade_reflection",
                regime=parsed.get("regime", "all"),
                confidence=parsed.get("confidence", 0.5),
                source_reflection_ids=reflection_ids,
            )
        except Exception:
            logger.exception("Failed to extract principle for %s", agent_id)
            return None

    async def _save_principle(self, principle: AbstractPrinciple) -> None:
        """Save an abstract principle to the database."""
        if not hasattr(self.storage, "pool"):
            return

        try:
            async with self.storage.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO abstract_principles (
                        agent_id, principle, source_type, regime,
                        confidence, source_reflection_ids
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    """,
                    principle.agent_id,
                    principle.principle,
                    principle.source_type,
                    principle.regime,
                    principle.confidence,
                    principle.source_reflection_ids,
                )
        except Exception:
            logger.exception("Failed to save principle")

    async def _mark_digested(self, memory_ids: list[int]) -> None:
        """Mark memories as digested in the database."""
        if not memory_ids or not hasattr(self.storage, "pool"):
            return

        try:
            async with self.storage.pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE trade_reflections
                    SET is_digested = TRUE
                    WHERE id = ANY($1)
                    """,
                    memory_ids,
                )
        except Exception:
            logger.exception("Failed to mark memories as digested")

    async def _save_history(self, result: DigestionResult) -> None:
        """Save digestion history record."""
        if not hasattr(self.storage, "pool"):
            return

        try:
            async with self.storage.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO digestion_history (
                        agent_id, memories_scored, memories_digested,
                        memories_pruned, principles_created, details
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    """,
                    result.agent_id,
                    result.memories_scored,
                    result.memories_digested,
                    result.memories_pruned,
                    result.principles_created,
                    json.dumps(result.details),
                )
        except Exception:
            logger.exception("Failed to save digestion history")
