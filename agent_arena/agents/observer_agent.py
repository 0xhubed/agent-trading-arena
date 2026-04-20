"""Observer Agent - Watches competition and distills knowledge into skills.

This agent doesn't trade. Instead, it:
1. Observes all agent decisions, trades, and outcomes
2. Analyzes patterns across agents and market conditions
3. Distills learnings into SKILL.md files
4. Updates skills periodically (e.g., daily)
5. Maintains a self-correcting memory in PostgreSQL:
   - Patterns tracked across runs with confidence lifecycle
   - Confirmed patterns gain confidence, contradicted ones weaken
   - Deprecated patterns are excluded from skill generation

The generated skills can be used by trading agents to improve their decisions.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, Protocol, runtime_checkable

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage

from agent_arena.agents.skill_writer import SkillUpdate, SkillWriter
# DEACTIVATED: bias analysis — implementation preserved in analysis/
# from agent_arena.analysis.bias_scan import analyze_agent_biases
from agent_arena.storage.observer_memory import ObserverMemoryStorage

logger = logging.getLogger(__name__)


def _extract_json_object(text: str) -> dict | None:
    """Extract a JSON object from LLM output, handling markdown fences.

    Tries in order:
    1. JSON inside ```json ... ``` fences
    2. JSON inside ``` ... ``` fences
    3. Bracket-balanced extraction starting from first '{'
    Returns parsed dict or None.
    """
    # 1. Try ```json ... ``` fence
    m = re.search(r"```json\s*\n?(\{[\s\S]*?\})\s*\n?```", text)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    # 2. Try ``` ... ``` fence (any language or none)
    m = re.search(r"```\w*\s*\n?(\{[\s\S]*?\})\s*\n?```", text)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    # 3. Bracket-balanced extraction from first '{'
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(text)):
        c = text[i]
        if escape:
            escape = False
            continue
        if c == "\\":
            escape = True
            continue
        if c == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start : i + 1])
                except json.JSONDecodeError:
                    return None
    return None


# Evolution analysis constants
MIN_GENOMES_FOR_ANALYSIS = 10  # Minimum sample size for statistical significance
MIN_PARAM_VALUES_FOR_QUARTILES = 4  # Need at least 4 values for quartile analysis
MIN_GROUP_SIZE_FOR_ARCHETYPE = 2  # Minimum genomes to form an archetype group
MAX_KEYWORDS_PER_CHARACTER = 2  # Maximum keyword matches to extract per character
MAX_ARCHETYPES_TO_DISPLAY = 5  # Top N archetypes to include in skill output
CHARACTER_TRUNCATION_LENGTH = 200  # Max character string length for display
SKILL_TOKEN_LIMIT = 4000  # Maximum tokens for skill content to stay under context limits

# Confidence calculation parameters
MAX_CONFIDENCE_SCORE = 0.95  # Maximum confidence score for parameter insights
BASE_CONFIDENCE = 0.5  # Baseline confidence with minimal samples
CONFIDENCE_SAMPLE_DENOMINATOR = 50  # Sample size for full confidence

# Evolution parameter list to analyze
EVOLUTION_PARAMETERS_TO_ANALYZE = [
    "temperature",
    "max_tokens",
    "sl_pct",
    "tp_pct",
    "position_size_pct",
    "max_leverage",
    "confidence_threshold",
]

# Character archetype keywords (categorized)
CHARACTER_ARCHETYPE_KEYWORDS = [
    # Trading styles
    "aggressive", "conservative", "contrarian",
    # Strategies
    "momentum", "mean-revert", "technical", "scalp", "macro", "breakout", "value",
    # Characteristics
    "precision", "risk-adjusted", "adaptive", "regime", "volatility",
]


@runtime_checkable
class SupportsEvolution(Protocol):
    """Protocol for storage backends that support evolution analysis.

    Evolution analysis requires PostgreSQL for storing run metadata and genomes.
    This protocol defines the interface required for evolution-aware storage.
    """

    @property
    def pool(self) -> Any:
        """PostgreSQL connection pool."""
        ...


@dataclass
class ObservationWindow:
    """Time window for analysis."""

    start: datetime
    end: datetime
    tick_start: int
    tick_end: int


@dataclass
class AgentPerformance:
    """Performance metrics for a single agent."""

    agent_id: str
    agent_name: str
    total_pnl: float
    win_rate: float
    trade_count: int
    avg_confidence: float
    best_regime: str
    worst_regime: str
    notable_decisions: list[dict] = field(default_factory=list)


@dataclass
class MarketRegimeStats:
    """Statistics for a market regime period."""

    regime: str
    duration_ticks: int
    price_change_pct: float
    volatility: float
    best_performing_agents: list[str] = field(default_factory=list)
    worst_performing_agents: list[str] = field(default_factory=list)
    winning_strategies: list[str] = field(default_factory=list)
    losing_strategies: list[str] = field(default_factory=list)


@dataclass
class PatternInsight:
    """A discovered pattern from observation."""

    pattern_type: str  # entry_signal, exit_signal, risk_rule, regime_strategy
    description: str
    conditions: dict
    success_rate: float
    sample_size: int
    confidence: float
    supporting_examples: list[dict] = field(default_factory=list)


class ObserverAgent:
    """
    Meta-learning agent that observes competition and writes skills.

    Unlike trading agents, this agent:
    - Doesn't participate in trading
    - Runs on a schedule (e.g., daily)
    - Has read access to all agent data
    - Writes knowledge to .claude/skills/

    Architecture:
    1. Data Collection: Query storage for decisions, trades, outcomes
    2. Analysis: Use LLM to identify patterns, strategies, insights
    3. Synthesis: Aggregate findings into coherent knowledge
    4. Skill Writing: Generate/update SKILL.md files

    Example usage:
        observer = ObserverAgent(storage, skills_dir=".claude/skills")
        await observer.run_daily_analysis()
    """

    def __init__(
        self,
        storage: Any,
        skills_dir: str | Path = ".claude/skills",
        model: str = "claude-opus-4-6",  # Opus for best analysis
        min_confidence: float = 0.6,
        min_sample_size: int = 10,
    ):
        self.storage = storage
        self.skills_dir = Path(skills_dir)
        self.model = model
        self.min_confidence = min_confidence
        self.min_sample_size = min_sample_size

        # Create LLM for analysis
        self._llm = ChatAnthropic(
            model=model,
            temperature=0.3,  # Lower temperature for analytical tasks
            max_tokens=32768,
        )

        # Skill writer handles file generation
        self._skill_writer = SkillWriter(skills_dir)

        # Track observation state
        self._last_analysis: Optional[datetime] = None
        self._analysis_history: list[dict] = []

        # PostgreSQL-backed self-correcting memory
        self._memory: Optional[ObserverMemoryStorage] = None
        if isinstance(storage, SupportsEvolution):
            self._memory = ObserverMemoryStorage(storage.pool)

    async def run_daily_analysis(self, lookback_hours: int = 24) -> dict:
        """
        Run the daily observation and skill update cycle.

        Integrates self-correcting memory:
        1. Creates a run record in PostgreSQL
        2. Loads existing memory patterns from DB
        3. Collects fresh observation data
        4. Analyzes with LLM, providing existing patterns for lifecycle tracking
        5. Applies confidence evolution (confirm/contradict/decay)
        6. Saves updated patterns to DB
        7. Generates skills excluding deprecated patterns
        8. Updates run record with stats

        Args:
            lookback_hours: How many hours of data to analyze

        Returns:
            Summary of analysis and skill updates
        """
        now = datetime.now(timezone.utc)
        window = ObservationWindow(
            start=now - timedelta(hours=lookback_hours),
            end=now,
            tick_start=0,  # Will be filled from data
            tick_end=0,
        )

        # Phase 0: Create run record & load existing memory
        run_id = None
        existing_memory_patterns = []
        if self._memory:
            run_id = await self._memory.create_run(
                window_start=window.start,
                window_end=window.end,
            )
            existing_memory_patterns = await self._memory.get_latest_patterns()
            logger.info(
                "Observer run %s: loaded %d existing patterns from memory",
                run_id, len(existing_memory_patterns),
            )

        # Phase 1: Collect data
        data = await self._collect_observation_data(window)
        if not data["decisions"]:
            return {"status": "no_data", "message": "No decisions in observation window"}

        # Update run with data counts
        if self._memory and run_id:
            await self._memory.update_run(
                run_id,
                decisions_analyzed=len(data["decisions"]),
                trades_analyzed=len(data["trades"]),
                agents_observed=len(data["agent_ids"]),
            )

        # Persist bias profiles for historical tracking (Step 5 Lab Tab)
        if hasattr(self.storage, "save_bias_profile"):
            for profile in data.get("bias_profiles", {}).values():
                try:
                    await self.storage.save_bias_profile(profile.to_dict())
                except Exception:
                    logger.warning("Failed to save bias profile for %s", profile.agent_id)

        # Phase 2-6: LLM analysis, memory updates, skill generation
        # Wrapped so failures here don't block journal/forum/codegen
        analysis = {}
        memory_stats = {"confirmed": 0, "contradicted": 0, "new": 0, "deprecated": 0}
        written_skills: list = []
        analysis_ok = False
        try:
            # Phase 2: Analyze patterns (LLM call)
            analysis = await self._analyze_patterns(
                data, window, memory_patterns=existing_memory_patterns,
            )

            # Phase 3: Apply confidence evolution and save to DB
            if self._memory and run_id:
                memory_stats = await self._apply_memory_updates(
                    run_id, now, window, analysis, existing_memory_patterns,
                )

            # Phase 4: Generate skill updates (only active/confirmed patterns)
            skill_updates = await self._generate_skill_updates(analysis)

            # Phase 5: Write skills
            written_skills = await self._write_skills(skill_updates)

            # Phase 6: Update run record
            if self._memory and run_id:
                await self._memory.update_run(
                    run_id,
                    raw_analysis=analysis.get("raw", ""),
                    skills_updated=[str(s) for s in written_skills],
                    patterns_confirmed=memory_stats["confirmed"],
                    patterns_contradicted=memory_stats["contradicted"],
                    patterns_new=memory_stats["new"],
                    patterns_deprecated=memory_stats["deprecated"],
                    summary={
                        "decisions_analyzed": len(data["decisions"]),
                        "trades_analyzed": len(data["trades"]),
                        "agents_observed": len(data["agent_ids"]),
                        "patterns_found": len(analysis.get("patterns", [])),
                        "skills_updated": [str(s) for s in written_skills],
                    },
                )
            analysis_ok = True
        except Exception:
            logger.exception("Pattern analysis failed (non-fatal, continuing to journal)")

        # Record analysis
        summary = {
            "timestamp": now.isoformat(),
            "run_id": run_id,
            "window": {
                "start": window.start.isoformat(),
                "end": window.end.isoformat(),
            },
            "data_summary": {
                "decisions_analyzed": len(data["decisions"]),
                "trades_analyzed": len(data["trades"]),
                "agents_observed": len(data["agent_ids"]),
            },
            "memory_stats": memory_stats,
            "patterns_found": len(analysis.get("patterns", [])),
            "skills_updated": written_skills,
            "status": "success" if analysis_ok else "partial",
        }

        self._last_analysis = now
        self._analysis_history.append(summary)

        # Phase 7: Forum witness generation (before journal so it can reference)
        try:
            witness_result = await self.analyze_forum_window(
                hours=lookback_hours,
            )
            summary["forum_witness"] = witness_result
        except Exception:
            logger.warning("Forum witness generation failed (non-fatal)", exc_info=True)
            summary["forum_witness"] = {"status": "error"}

        # Phase 8: Generate journal entry (runs after skills + witness)
        try:
            journal_result = await self.generate_journal(lookback_hours)
            summary["journal"] = journal_result
        except Exception:
            logger.warning("Journal generation failed (non-fatal)", exc_info=True)
            summary["journal"] = {"status": "error"}

        # Phase 9: Trigger codegen from journal findings
        try:
            codegen_result = await self._trigger_codegen()
            summary["codegen"] = codegen_result
        except Exception:
            logger.warning("Codegen trigger failed (non-fatal)", exc_info=True)
            summary["codegen"] = {"status": "error"}

        return summary

    async def generate_journal(self, lookback_hours: int = 24) -> dict:
        """Generate a daily journal entry using JournalService.

        Can be called standalone or as part of run_daily_analysis().
        """
        try:
            from agent_arena.journal.service import JournalService

            service = JournalService(
                storage=self.storage,
                model=self.model,
            )
            entry = await service.generate_daily_journal(lookback_hours)
            return {
                "status": "success",
                "journal_date": entry.journal_date.isoformat(),
                "entry_id": entry.id,
            }
        except Exception:
            logger.exception("Journal generation failed")
            return {"status": "error"}

    async def _trigger_codegen(self) -> dict:
        """Run codegen as a background subprocess.

        Launches ``agent-arena codegen`` in a detached process so it
        doesn't block the observer.  The codegen command creates its
        own git worktree, so the running codebase is never modified.
        """
        import asyncio
        import sys
        from pathlib import Path

        # Use the same Python that's running this process
        cli_bin = Path(sys.executable).parent / "agent-arena"
        if not cli_bin.exists():
            logger.info(
                "agent-arena CLI not found at %s, skipping codegen",
                cli_bin,
            )
            return {"status": "skipped", "reason": "cli_not_found"}

        try:
            proc = await asyncio.create_subprocess_exec(
                str(cli_bin), "codegen",
                "--lookback-days", "5",
                "--max-changes", "3",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            # Wait up to 5 minutes — codegen calls an LLM
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=300,
            )
            if proc.returncode == 0:
                logger.info("Codegen completed successfully")
                return {
                    "status": "success",
                    "output": stdout.decode()[-500:],
                }
            else:
                logger.warning(
                    "Codegen exited %d: %s",
                    proc.returncode, stderr.decode()[-300:],
                )
                return {
                    "status": "error",
                    "returncode": proc.returncode,
                    "stderr": stderr.decode()[-300:],
                }
        except asyncio.TimeoutError:
            logger.warning("Codegen timed out after 300s")
            if proc:
                proc.kill()
            return {"status": "timeout"}
        except Exception:
            logger.warning(
                "Codegen trigger failed (non-fatal)", exc_info=True,
            )
            return {"status": "error"}

    async def _collect_observation_data(self, window: ObservationWindow) -> dict:
        """Collect all relevant data from storage."""
        data = {
            "decisions": [],
            "trades": [],
            "snapshots": [],
            "agent_ids": set(),
            "market_data": [],
        }

        # Get all decisions in window
        if hasattr(self.storage, "pool"):
            # PostgreSQL - ensure timestamps are timezone-aware
            start_ts = window.start if window.start.tzinfo else window.start.replace(tzinfo=timezone.utc)
            end_ts = window.end if window.end.tzinfo else window.end.replace(tzinfo=timezone.utc)

            async with self.storage.pool.acquire() as conn:
                # Get decisions
                rows = await conn.fetch(
                    """
                    SELECT * FROM decisions
                    WHERE timestamp >= $1
                    AND timestamp <= $2
                    ORDER BY tick ASC
                    """,
                    start_ts,
                    end_ts,
                )
                data["decisions"] = [dict(row) for row in rows]

                # Get trades
                rows = await conn.fetch(
                    """
                    SELECT * FROM trades
                    WHERE timestamp >= $1
                    AND timestamp <= $2
                    ORDER BY timestamp ASC
                    """,
                    start_ts,
                    end_ts,
                )
                data["trades"] = [dict(row) for row in rows]

                # Get snapshots for market data
                rows = await conn.fetch(
                    """
                    SELECT * FROM snapshots
                    WHERE timestamp >= $1
                    AND timestamp <= $2
                    ORDER BY tick ASC
                    """,
                    start_ts,
                    end_ts,
                )
                data["snapshots"] = [dict(row) for row in rows]

        elif hasattr(self.storage, "_connection"):
            # SQLite
            async with self.storage._connection.execute(
                """
                SELECT * FROM decisions
                WHERE datetime(timestamp) >= datetime(?)
                AND datetime(timestamp) <= datetime(?)
                ORDER BY tick ASC
                """,
                (window.start.isoformat(), window.end.isoformat()),
            ) as cursor:
                rows = await cursor.fetchall()
                columns = [d[0] for d in cursor.description]
                data["decisions"] = [dict(zip(columns, row)) for row in rows]

            # Get trades
            async with self.storage._connection.execute(
                """
                SELECT * FROM trades
                WHERE datetime(timestamp) >= datetime(?)
                AND datetime(timestamp) <= datetime(?)
                ORDER BY timestamp ASC
                """,
                (window.start.isoformat(), window.end.isoformat()),
            ) as cursor:
                rows = await cursor.fetchall()
                columns = [d[0] for d in cursor.description]
                data["trades"] = [dict(zip(columns, row)) for row in rows]

            # Get snapshots for market data
            async with self.storage._connection.execute(
                """
                SELECT * FROM snapshots
                WHERE datetime(timestamp) >= datetime(?)
                AND datetime(timestamp) <= datetime(?)
                ORDER BY tick ASC
                """,
                (window.start.isoformat(), window.end.isoformat()),
            ) as cursor:
                rows = await cursor.fetchall()
                columns = [d[0] for d in cursor.description]
                data["snapshots"] = [dict(zip(columns, row)) for row in rows]

        # Extract unique agents
        data["agent_ids"] = {d["agent_id"] for d in data["decisions"]}

        # Group decisions and trades by agent (single pass each)
        decisions_by_agent: dict[str, list] = {}
        for d in data["decisions"]:
            decisions_by_agent.setdefault(d["agent_id"], []).append(d)
        data["decisions_by_agent"] = decisions_by_agent

        trades_by_agent: dict[str, list] = {}
        for t in data["trades"]:
            trades_by_agent.setdefault(t["agent_id"], []).append(t)
        data["trades_by_agent"] = trades_by_agent

        # Parse JSON fields
        for decision in data["decisions"]:
            if isinstance(decision.get("metadata"), str):
                try:
                    decision["metadata"] = json.loads(decision["metadata"])
                except (json.JSONDecodeError, TypeError):
                    decision["metadata"] = {}

        for snapshot in data["snapshots"]:
            if isinstance(snapshot.get("leaderboard"), str):
                try:
                    snapshot["leaderboard"] = json.loads(snapshot["leaderboard"])
                except (json.JSONDecodeError, TypeError):
                    snapshot["leaderboard"] = []
            if isinstance(snapshot.get("market_data"), str):
                try:
                    snapshot["market_data"] = json.loads(snapshot["market_data"])
                except (json.JSONDecodeError, TypeError):
                    snapshot["market_data"] = {}

        # DEACTIVATED: bias profiling — reactivate by uncommenting below and the import
        # bias_profiles = {}
        # for agent_id in data["agent_ids"]:
        #     bias_profiles[agent_id] = analyze_agent_biases(
        #         agent_id,
        #         decisions_by_agent.get(agent_id, []),
        #         trades_by_agent.get(agent_id, []),
        #     )
        data["bias_profiles"] = {}

        return data

    async def _analyze_patterns(
        self,
        data: dict,
        window: ObservationWindow,
        memory_patterns: list[dict] | None = None,
    ) -> dict:
        """Use LLM to analyze patterns in the data."""
        # Load existing patterns from skill files (backwards compat)
        existing_patterns = await self._load_existing_patterns()

        # Prepare analysis prompt with both skill patterns and DB memory
        analysis_prompt = self._build_analysis_prompt(
            data, window, existing_patterns, memory_patterns=memory_patterns,
        )

        # Call LLM for analysis
        response = await self._llm.ainvoke([HumanMessage(content=analysis_prompt)])
        raw_analysis = response.content

        # Parse analysis into structured format
        analysis = await self._parse_analysis(raw_analysis, data)

        return analysis

    async def _load_existing_patterns(self) -> dict[str, str]:
        """Load summaries of existing patterns from all skills."""
        patterns = {}
        skill_names = [
            "trading-wisdom",
            "market-regimes",
            "risk-management",
            "entry-signals",
            "exit-signals",
        ]

        for skill_name in skill_names:
            summary = self._skill_writer.get_existing_patterns_summary(skill_name)
            if summary and "No existing patterns" not in summary:
                patterns[skill_name] = summary

        return patterns

    @staticmethod
    def _format_bias_section(data: dict) -> str:
        """Format bias profiles into a prompt section for the LLM."""
        profiles = data.get("bias_profiles", {})
        if not profiles:
            return ""

        lines = [
            "",
            "AGENT BEHAVIORAL BIASES (statistical, from BiasScan):",
        ]
        for agent_id, profile in sorted(profiles.items()):
            parts = []
            for score in profile.scores:
                if score.sufficient_data:
                    parts.append(f"{score.bias_type}={score.value:.2f}")
                else:
                    parts.append(f"{score.bias_type}=N/A({score.sample_size} samples)")
            lines.append(f"  {agent_id}: {', '.join(parts)}")

        lines.append("")
        lines.append(
            "Use these bias scores when generating insights. "
            "If a bias is HIGH (>0.6), generate a specific corrective skill. "
            "For example: disposition_effect>0.6 → 'set tighter stop-losses to cut losers faster'. "
            "If a bias is LOW (<0.3), note it as a strength."
        )
        lines.append("")

        return "\n".join(lines)

    def _build_analysis_prompt(
        self,
        data: dict,
        window: ObservationWindow,
        existing_patterns: dict[str, str] | None = None,
        memory_patterns: list[dict] | None = None,
    ) -> str:
        """Build the analysis prompt for the LLM."""
        # Use pre-grouped data from _collect_observation_data
        decisions_by_agent = data.get("decisions_by_agent", {})
        trades_by_agent = data.get("trades_by_agent", {})

        # Calculate basic stats per agent
        agent_summaries = []
        for agent_id, decisions in decisions_by_agent.items():
            actions = [d["action"] for d in decisions]
            holds = actions.count("hold")
            trades = len(actions) - holds

            agent_trades = trades_by_agent.get(agent_id, [])
            total_pnl = sum(
                float(t.get("realized_pnl") or 0) for t in agent_trades
            )

            agent_summaries.append(
                f"- {agent_id}: {len(decisions)} decisions, {trades} trades, "
                f"PnL: ${total_pnl:+.2f}"
            )

        # Sample notable decisions (high confidence trades)
        notable_decisions = []
        for d in data["decisions"]:
            if d["action"] not in ("hold",) and (d.get("confidence") or 0) > 0.7:
                notable_decisions.append({
                    "agent": d["agent_id"],
                    "action": d["action"],
                    "symbol": d["symbol"],
                    "confidence": d["confidence"],
                    "reasoning": d.get("reasoning", "")[:200],
                })
        notable_decisions = notable_decisions[:20]  # Limit for context

        # Get market price changes from snapshots
        market_summary = "No market data available"
        if data["snapshots"]:
            first = data["snapshots"][0]
            last = data["snapshots"][-1]
            if first.get("market_data") and last.get("market_data"):
                changes = []
                for symbol in first["market_data"].keys():
                    if symbol in last["market_data"]:
                        start_price = float(first["market_data"][symbol].get("price", 0))
                        end_price = float(last["market_data"][symbol].get("price", 0))
                        if start_price > 0:
                            change = ((end_price - start_price) / start_price) * 100
                            changes.append(f"{symbol}: {change:+.2f}%")
                market_summary = ", ".join(changes) if changes else "No price changes"

        # Build memory patterns section (PostgreSQL-backed lifecycle tracking)
        memory_section = ""
        if memory_patterns:
            memory_section = """
SELF-CORRECTING MEMORY — PATTERN LIFECYCLE:
The following patterns are tracked across runs with confidence scores.
For EACH pattern below, you MUST classify it based on new evidence:
  - CONFIRMED: New data supports this pattern (confidence will increase)
  - CONTRADICTED: New data contradicts this pattern (confidence will decrease)
  - NO_DATA: No relevant data in this window (pattern decays slowly)

"""
            for p in memory_patterns:
                status_emoji = {"active": "[ACT]", "confirmed": "[OK!]", "weakened": "[WEAK]"}.get(
                    p.get("status", "active"), "[?]"
                )
                memory_section += (
                    f"  {status_emoji} pattern_id={p['pattern_id']} | "
                    f"skill={p['skill_name']} | conf={p.get('confidence', 0):.2f} | "
                    f"confirmed={p.get('times_confirmed', 0)}x / "
                    f"contradicted={p.get('times_contradicted', 0)}x\n"
                    f"    {p['description'][:200]}\n\n"
                )

            memory_section += """
RESPOND with a "pattern_verdicts" section that maps each pattern_id to its verdict:
{
    "pattern_verdicts": {
        "<pattern_id>": {"verdict": "confirmed|contradicted|no_data", "evidence": "..."}
    }
}
"""

        # Build existing skill patterns section (file-based, backwards compat)
        existing_patterns_section = ""
        if existing_patterns:
            existing_patterns_section = """
EXISTING SKILL FILE PATTERNS (from .claude/skills/):
"""
            for skill_name, summary in existing_patterns.items():
                existing_patterns_section += f"### {skill_name}\n{summary}\n\n"
        elif not memory_patterns:
            existing_patterns_section = """
EXISTING PATTERNS: None yet. This is the first analysis or no patterns have been learned.
"""

        prompt = f"""Analyze this trading competition data and identify patterns.

OBSERVATION WINDOW: {window.start.isoformat()} to {window.end.isoformat()}

MARKET SUMMARY:
{market_summary}

AGENT PERFORMANCE SUMMARY:
{chr(10).join(agent_summaries)}

NOTABLE HIGH-CONFIDENCE DECISIONS (sample):
{json.dumps(notable_decisions, indent=2)}

TOTAL STATISTICS:
- Decisions: {len(data["decisions"])}
- Trades: {len(data["trades"])}
- Unique agents: {len(data["agent_ids"])}
{memory_section}{existing_patterns_section}{self._format_bias_section(data)}
ANALYSIS TASKS:
1. WINNING STRATEGIES: What patterns do successful agents follow?
2. LOSING PATTERNS: What behaviors lead to losses?
3. REGIME INSIGHTS: How do agents perform in different market conditions?
4. RISK PATTERNS: What risk management approaches work best?
5. ENTRY/EXIT SIGNALS: What technical or sentiment signals are agents using?
6. PATTERN LIFECYCLE: For each existing memory pattern, state CONFIRMED / CONTRADICTED / NO_DATA.
7. BIAS CORRECTIONS: For agents with HIGH biases (>0.6), generate specific corrective actions.

OUTPUT FORMAT:
Return your analysis as JSON with this structure:
{{
    "winning_strategies": [
        {{"description": "...", "agents": ["..."], "confidence": 0.8, "sample_size": 10}}
    ],
    "losing_patterns": [
        {{"description": "...", "agents": ["..."], "confidence": 0.7, "sample_size": 5}}
    ],
    "regime_insights": [
        {{"regime": "trending_up|...", "best_approach": "...", "confidence": 0.75}}
    ],
    "risk_rules": [
        {{"rule": "...", "success_rate": 0.65, "sample_size": 20}}
    ],
    "signals": [
        {{"type": "entry|exit", "description": "...", "success_rate": 0.7, "sample_size": 15}}
    ],
    "key_learnings": ["...", "..."],
    "bias_corrections": [
        {{"agent_id": "...", "bias_type": "disposition_effect",
          "score": 0.7, "correction": "...",
          "target_skill": "risk-management"}}
    ],
    "pattern_verdicts": {{
        "<pattern_id>": {{"verdict": "confirmed|contradicted|no_data", "evidence": "..."}}
    }},
    "pattern_confirmations": ["List patterns from EXISTING that were confirmed by new data"],
    "pattern_contradictions": ["List any contradictions found with existing patterns"]
}}

Focus on patterns with statistical significance (multiple occurrences, clear outcomes).
Be specific and actionable. Avoid vague generalizations.
Re-include patterns from EXISTING LEARNED PATTERNS that you see confirmed in the new data."""

        return prompt

    async def _parse_analysis(self, raw_analysis: str, data: dict) -> dict:
        """Parse LLM analysis into structured format."""
        # Extract JSON from response (handles markdown fences + bracket balance)
        analysis = _extract_json_object(raw_analysis)
        if analysis is None:
            logger.warning(
                "Observer analysis: failed to extract JSON, raw: %.500s",
                raw_analysis,
            )
            analysis = {"raw": raw_analysis, "patterns": []}

        # Convert to PatternInsight objects
        patterns = []

        for strategy in analysis.get("winning_strategies", []):
            if strategy.get("confidence", 0) >= self.min_confidence:
                patterns.append(PatternInsight(
                    pattern_type="winning_strategy",
                    description=strategy["description"],
                    conditions={"agents": strategy.get("agents", [])},
                    success_rate=strategy.get("confidence", 0.5),
                    sample_size=strategy.get("sample_size", 0),
                    confidence=strategy.get("confidence", 0.5),
                ))

        for pattern in analysis.get("losing_patterns", []):
            if pattern.get("confidence", 0) >= self.min_confidence:
                patterns.append(PatternInsight(
                    pattern_type="losing_pattern",
                    description=pattern["description"],
                    conditions={"agents": pattern.get("agents", [])},
                    success_rate=1 - pattern.get("confidence", 0.5),  # Invert for "what to avoid"
                    sample_size=pattern.get("sample_size", 0),
                    confidence=pattern.get("confidence", 0.5),
                ))

        for insight in analysis.get("regime_insights", []):
            patterns.append(PatternInsight(
                pattern_type="regime_strategy",
                description=f"{insight['regime']}: {insight['best_approach']}",
                conditions={"regime": insight["regime"]},
                success_rate=insight.get("confidence", 0.5),
                sample_size=insight.get("sample_size", 0),
                confidence=insight.get("confidence", 0.5),
            ))

        for rule in analysis.get("risk_rules", []):
            # LLM-reported sample_size is approximate; use confidence as
            # primary quality filter, accept rules with any sample_size >= 3
            if rule.get("sample_size", 0) >= 3 or rule.get(
                "confidence", 0
            ) >= self.min_confidence:
                sample = rule.get("sample_size", 0)
                patterns.append(PatternInsight(
                    pattern_type="risk_rule",
                    description=rule["rule"],
                    conditions={},
                    success_rate=rule.get("success_rate", 0.5),
                    sample_size=sample,
                    confidence=min(0.95, 0.5 + (sample * 0.02)),
                ))

        for signal in analysis.get("signals", []):
            # Same relaxed filter: LLM can't count samples accurately
            if signal.get("sample_size", 0) >= 3 or signal.get(
                "confidence", 0
            ) >= self.min_confidence:
                sample = signal.get("sample_size", 0)
                patterns.append(PatternInsight(
                    pattern_type=f"{signal['type']}_signal",
                    description=signal["description"],
                    conditions={"type": signal["type"]},
                    success_rate=signal.get("success_rate", 0.5),
                    sample_size=sample,
                    confidence=min(0.95, 0.5 + (sample * 0.02)),
                ))

        analysis["patterns"] = patterns
        analysis["key_learnings"] = analysis.get("key_learnings", [])

        return analysis

    async def _apply_memory_updates(
        self,
        run_id: str,
        now: datetime,
        window: ObservationWindow,
        analysis: dict,
        existing_memory_patterns: list[dict],
    ) -> dict:
        """
        Apply confidence evolution to memory patterns based on LLM verdicts.

        Computes all updates as pure logic, then persists in a single transaction.

        Returns:
            Dict with counts: {confirmed, contradicted, new, deprecated}
        """
        stats = {"confirmed": 0, "contradicted": 0, "new": 0, "deprecated": 0}
        verdicts = analysis.get("pattern_verdicts", {})

        # Build lookup of existing patterns by pattern_id
        existing_by_id = {p["pattern_id"]: p for p in existing_memory_patterns}

        # Compute all updates (pure logic, no I/O)
        pattern_rows: list[dict] = []

        for pattern_id, existing in existing_by_id.items():
            verdict_info = verdicts.get(pattern_id, {})
            verdict = verdict_info.get("verdict", "no_data")
            evidence = verdict_info.get("evidence", "")

            update = ObserverMemoryStorage.compute_verdict_update(
                existing, verdict, evidence, now,
            )

            if verdict == "confirmed":
                stats["confirmed"] += 1
            elif verdict == "contradicted":
                stats["contradicted"] += 1
            if update["status"] == "deprecated":
                stats["deprecated"] += 1

            pattern_rows.append({
                "run_id": run_id,
                "run_timestamp": now,
                "window_start": window.start,
                "window_end": window.end,
                "pattern_id": pattern_id,
                "skill_name": existing.get("skill_name", "unknown"),
                "pattern_type": existing.get("pattern_type", "unknown"),
                "description": existing.get("description", ""),
                "status": update["status"],
                "confidence": update["confidence"],
                "sample_size": existing.get("sample_size", 0),
                "times_confirmed": update["times_confirmed"],
                "times_contradicted": update["times_contradicted"],
                "first_seen": existing.get("first_seen", now),
                "last_confirmed": update["last_confirmed"],
                "last_contradicted": update["last_contradicted"],
                "reasoning": update["reasoning"],
                "supporting_evidence": update["supporting_evidence"],
                "contradiction_evidence": update["contradiction_evidence"],
            })

        # Save NEW patterns discovered in this analysis
        for pattern in analysis.get("patterns", []):
            pid = self._pattern_to_id(pattern)
            if pid not in existing_by_id:
                skill_name = self._pattern_type_to_skill(pattern.pattern_type)
                pattern_rows.append({
                    "run_id": run_id,
                    "run_timestamp": now,
                    "window_start": window.start,
                    "window_end": window.end,
                    "pattern_id": pid,
                    "skill_name": skill_name,
                    "pattern_type": pattern.pattern_type,
                    "description": pattern.description,
                    "status": "active",
                    "confidence": pattern.confidence,
                    "sample_size": pattern.sample_size,
                    "times_confirmed": 0,
                    "times_contradicted": 0,
                    "first_seen": now,
                })
                stats["new"] += 1

        # Persist all updates in a single transaction
        if pattern_rows:
            await self._memory.batch_upsert_patterns(pattern_rows)

        logger.info(
            "Observer memory update: +%d new, %d confirmed, %d contradicted, %d deprecated",
            stats["new"], stats["confirmed"], stats["contradicted"], stats["deprecated"],
        )
        return stats

    @staticmethod
    def _pattern_to_id(pattern: PatternInsight) -> str:
        """Generate a stable ID for a pattern based on its type and description."""
        raw = f"{pattern.pattern_type}:{pattern.description}"
        return hashlib.sha256(raw.encode()).hexdigest()[:24]

    @staticmethod
    def _pattern_type_to_skill(pattern_type: str) -> str:
        """Map a pattern type to a skill name."""
        mapping = {
            "winning_strategy": "trading-wisdom",
            "losing_pattern": "trading-wisdom",
            "regime_strategy": "market-regimes",
            "risk_rule": "risk-management",
            "entry_signal": "entry-signals",
            "exit_signal": "entry-signals",
            "backtest_validated_strategy": "trading-wisdom",
            "backtest_invalidated_pattern": "trading-wisdom",
            "statistical_finding": "trading-wisdom",
            "backtest_risk_rule": "risk-management",
        }
        return mapping.get(pattern_type, "trading-wisdom")

    async def _generate_skill_updates(self, analysis: dict) -> list[SkillUpdate]:
        """Generate skill updates from analysis."""
        updates = []

        # Group patterns by type for skill organization
        patterns_by_type = {}
        for pattern in analysis.get("patterns", []):
            ptype = pattern.pattern_type
            if ptype not in patterns_by_type:
                patterns_by_type[ptype] = []
            patterns_by_type[ptype].append(pattern)

        # Generate trading-wisdom skill (master skill)
        # Write whenever we have key_learnings OR any patterns at all
        if analysis.get("key_learnings") or analysis.get("patterns"):
            updates.append(SkillUpdate(
                skill_name="trading-wisdom",
                description="Core trading insights learned from Agent Arena competition. "
                           "Use when making any trading decision to apply institutional knowledge.",
                sections={
                    "key_learnings": analysis["key_learnings"],
                    "last_updated": datetime.now(timezone.utc).isoformat(),
                },
                patterns=analysis.get("patterns", []),
            ))

        # Generate market-regimes skill
        regime_patterns = patterns_by_type.get("regime_strategy", [])
        if regime_patterns:
            updates.append(SkillUpdate(
                skill_name="market-regimes",
                description="Market regime detection and regime-specific trading strategies. "
                           "Use when analyzing market conditions to select appropriate strategy.",
                sections={
                    "regimes": [
                        {
                            "regime": p.conditions.get("regime"),
                            "strategy": p.description,
                            "confidence": p.confidence,
                        }
                        for p in regime_patterns
                    ],
                },
                patterns=regime_patterns,
            ))

        # Generate risk-management skill
        risk_patterns = patterns_by_type.get("risk_rule", [])
        if risk_patterns:
            updates.append(SkillUpdate(
                skill_name="risk-management",
                description="Risk management rules learned from competition outcomes. "
                           "Use when sizing positions or setting stop-losses.",
                sections={
                    "rules": [
                        {
                            "rule": p.description,
                            "success_rate": p.success_rate,
                            "sample_size": p.sample_size,
                        }
                        for p in risk_patterns
                    ],
                },
                patterns=risk_patterns,
            ))

        # Generate entry-signals skill
        entry_patterns = patterns_by_type.get("entry_signal", [])
        if entry_patterns:
            updates.append(SkillUpdate(
                skill_name="entry-signals",
                description="Entry signal patterns with historical success rates. "
                           "Use when deciding whether to open a position.",
                sections={
                    "signals": [
                        {
                            "signal": p.description,
                            "success_rate": p.success_rate,
                            "sample_size": p.sample_size,
                        }
                        for p in entry_patterns
                    ],
                },
                patterns=entry_patterns,
            ))

        if not updates:
            logger.warning(
                "No skill updates generated. key_learnings=%s, patterns=%d",
                bool(analysis.get("key_learnings")),
                len(analysis.get("patterns", [])),
            )

        return updates

    async def _write_skills(self, updates: list[SkillUpdate]) -> list[str]:
        """Write skill updates to files and save snapshots to PostgreSQL."""

        written = []
        for update in updates:
            try:
                path = await self._skill_writer.write_skill(update)
                written.append(str(path))

                # Save skill version to PostgreSQL if available
                if hasattr(self.storage, "save_skill_version"):
                    try:
                        # Read the written content
                        content = path.read_text()
                        # Generate version hash from content
                        version_hash = hashlib.sha256(content.encode()).hexdigest()[:64]

                        # Load metadata from .skill_meta.json
                        meta_file = path.parent / ".skill_meta.json"
                        metadata = {}
                        if meta_file.exists():
                            metadata = json.loads(meta_file.read_text())

                        # Load pattern history for counts
                        history_file = path.parent / ".pattern_history.json"
                        pattern_count = 0
                        active_patterns = 0
                        total_samples = 0
                        if history_file.exists():
                            history = json.loads(history_file.read_text())
                            pattern_count = len(history)
                            active_patterns = sum(
                                1 for p in history.values() if p.get("is_active", True)
                            )
                            total_samples = sum(
                                p.get("sample_size", 0) for p in history.values()
                            )

                        metadata.update({
                            "pattern_count": pattern_count,
                            "active_patterns": active_patterns,
                            "total_samples": total_samples,
                        })

                        await self.storage.save_skill_version(
                            skill_name=update.skill_name,
                            version_hash=version_hash,
                            content=content,
                            metadata=metadata,
                        )
                    except Exception as e:
                        logger.error(
                            "Failed to save skill version to DB: %s: %s",
                            update.skill_name, e,
                        )

            except Exception as e:
                logger.error("Failed to write skill %s: %s", update.skill_name, e)
        return written

    async def get_skill_summary(self) -> dict:
        """Get summary of all generated skills."""
        skills = {}
        if self.skills_dir.exists():
            for skill_dir in self.skills_dir.iterdir():
                if skill_dir.is_dir():
                    skill_file = skill_dir / "SKILL.md"
                    if skill_file.exists():
                        content = skill_file.read_text()
                        skills[skill_dir.name] = {
                            "path": str(skill_file),
                            "size": len(content),
                            "exists": True,
                        }
        return skills

    async def get_memory_summary(self) -> dict:
        """Get summary of self-correcting memory state."""
        if not self._memory:
            return {"available": False, "message": "No PostgreSQL memory backend"}

        all_patterns = await self._memory.get_all_patterns_latest()
        run_count = await self._memory.get_run_count()

        by_status = {}
        by_skill = {}
        for p in all_patterns:
            status = p.get("status", "unknown")
            by_status[status] = by_status.get(status, 0) + 1
            skill = p.get("skill_name", "unknown")
            by_skill[skill] = by_skill.get(skill, 0) + 1

        return {
            "available": True,
            "total_runs": run_count,
            "total_patterns": len(all_patterns),
            "patterns_by_status": by_status,
            "patterns_by_skill": by_skill,
        }

    async def analyze_backtest_run(self, run_id: str) -> dict:
        """
        Analyze a specific backtest run and extract insights for skills.

        This method:
        1. Fetches backtest results from storage
        2. Analyzes agent performance across the historical period
        3. Extracts patterns validated by historical data
        4. Updates skills with backtest-validated insights

        Args:
            run_id: The backtest run ID to analyze

        Returns:
            Summary of analysis and skill updates
        """
        now = datetime.now(timezone.utc)

        # Fetch backtest data
        backtest_data = await self._fetch_backtest_data(run_id)
        if not backtest_data:
            return {"status": "error", "message": f"Backtest run {run_id} not found"}

        # Analyze backtest patterns
        analysis = await self._analyze_backtest_patterns(backtest_data)

        # Generate skill updates with backtest validation markers
        skill_updates = await self._generate_backtest_skill_updates(analysis, backtest_data)

        # Write skills
        written_skills = await self._write_skills(skill_updates)

        # Record analysis
        summary = {
            "timestamp": now.isoformat(),
            "backtest_run_id": run_id,
            "backtest_period": {
                "start": backtest_data.get("start_date"),
                "end": backtest_data.get("end_date"),
            },
            "data_summary": {
                "agents_analyzed": len(backtest_data.get("agents", [])),
                "total_ticks": backtest_data.get("total_ticks", 0),
                "tick_interval": backtest_data.get("tick_interval"),
            },
            "patterns_found": len(analysis.get("patterns", [])),
            "skills_updated": written_skills,
            "status": "success",
        }

        self._last_analysis = now
        self._analysis_history.append(summary)

        return summary

    async def _fetch_backtest_data(self, run_id: str) -> Optional[dict]:
        """Fetch backtest results from storage."""
        try:
            # Try SQLite storage first (uses _connection attribute)
            if hasattr(self.storage, "_connection"):
                from agent_arena.storage.candles import CandleStorage
                candle_storage = CandleStorage(self.storage._connection)

                run_data = await candle_storage.get_backtest_run(run_id)
                if not run_data:
                    return None

                agents = await candle_storage.get_backtest_results(run_id)
                comparisons = await candle_storage.get_comparisons(run_id)

                return {
                    "run_id": run_id,
                    "name": run_data.get("name"),
                    "start_date": run_data.get("start_date"),
                    "end_date": run_data.get("end_date"),
                    "tick_interval": run_data.get("tick_interval"),
                    "total_ticks": run_data.get("total_ticks"),
                    "config": run_data.get("config"),
                    "agents": agents,
                    "comparisons": comparisons,
                }

            # Try PostgreSQL storage (uses pool attribute)
            elif hasattr(self.storage, "pool"):
                async with self.storage.pool.acquire() as conn:
                    # Get backtest run metadata
                    run_row = await conn.fetchrow(
                        "SELECT * FROM backtest_runs WHERE id = $1",
                        run_id,
                    )
                    if not run_row:
                        return None

                    run_data = dict(run_row)

                    # Get agent results
                    results_rows = await conn.fetch(
                        "SELECT * FROM backtest_results WHERE run_id = $1",
                        run_id,
                    )
                    agents = [dict(row) for row in results_rows]

                    # Get comparisons
                    comparisons_rows = await conn.fetch(
                        "SELECT * FROM backtest_comparisons WHERE run_id = $1",
                        run_id,
                    )
                    comparisons = [dict(row) for row in comparisons_rows]

                    return {
                        "run_id": run_id,
                        "name": run_data.get("name"),
                        "start_date": run_data.get("start_date"),
                        "end_date": run_data.get("end_date"),
                        "tick_interval": run_data.get("tick_interval"),
                        "total_ticks": run_data.get("total_ticks"),
                        "config": run_data.get("config"),
                        "agents": agents,
                        "comparisons": comparisons,
                    }

            return None
        except Exception as e:
            logger.error("Error fetching backtest data: %s", e)
            return None

    async def _analyze_backtest_patterns(self, backtest_data: dict) -> dict:
        """Analyze patterns from backtest results using LLM."""
        # Build analysis prompt
        prompt = self._build_backtest_analysis_prompt(backtest_data)

        # Run LLM analysis
        response = await self._llm.ainvoke([HumanMessage(content=prompt)])
        raw_analysis = response.content

        # Parse analysis
        analysis = await self._parse_backtest_analysis(raw_analysis, backtest_data)

        return analysis

    def _build_backtest_analysis_prompt(self, backtest_data: dict) -> str:
        """Build prompt for backtest analysis."""
        agents = backtest_data.get("agents", [])
        comparisons = backtest_data.get("comparisons", [])

        # Build agent summary
        agent_summary = []
        for agent in agents:
            agent_summary.append({
                "agent_id": agent.get("agent_id"),
                "agent_name": agent.get("agent_name"),
                "final_equity": float(agent.get("final_equity", 10000)),
                "total_return": float(agent.get("total_return", 0)),
                "total_trades": agent.get("total_trades", 0),
                "win_rate": float(agent.get("win_rate", 0)),
                "sharpe_ratio": float(agent.get("sharpe_ratio", 0)) if agent.get("sharpe_ratio") else None,
                "max_drawdown_pct": float(agent.get("max_drawdown_pct", 0)),
                "profit_factor": float(agent.get("profit_factor", 0)) if agent.get("profit_factor") else None,
            })

        # Sort by return
        agent_summary.sort(key=lambda x: x["total_return"], reverse=True)

        # Build comparison summary
        comparison_summary = []
        for comp in comparisons:
            comparison_summary.append({
                "agent": comp.get("agent_id"),
                "baseline": comp.get("baseline_id"),
                "outperformance": float(comp.get("outperformance", 0)),
                "p_value": float(comp.get("p_value", 1)) if comp.get("p_value") else None,
                "is_significant": comp.get("is_significant", False),
            })

        prompt = f"""You are analyzing a BACKTEST RUN from Agent Arena trading competition.

BACKTEST PERIOD: {backtest_data.get('start_date')} to {backtest_data.get('end_date')}
TICK INTERVAL: {backtest_data.get('tick_interval')}
TOTAL TICKS: {backtest_data.get('total_ticks')}

AGENT PERFORMANCE (sorted by return):
{json.dumps(agent_summary, indent=2)}

STATISTICAL COMPARISONS VS BASELINES:
{json.dumps(comparison_summary, indent=2)}

ANALYZE THIS BACKTEST DATA TO EXTRACT:

1. WINNING STRATEGIES: Which agents performed best and why?
   - What characteristics do the top performers share?
   - What trading approaches work over this historical period?

2. LOSING PATTERNS: What caused underperformance?
   - What should be avoided based on historical evidence?

3. STATISTICAL INSIGHTS: Focus on statistically significant results (p_value < 0.05)
   - Which agents beat baselines with confidence?
   - What's the magnitude of outperformance?

4. RISK-ADJUSTED PERFORMANCE:
   - Which agents had the best Sharpe ratios?
   - Which had acceptable drawdowns?

5. BACKTEST-VALIDATED RULES:
   - What rules can we confidently apply based on this historical validation?
   - Include sample size and confidence level.

OUTPUT FORMAT:
Return your analysis as JSON with this structure:
{{
    "backtest_validated_strategies": [
        {{"description": "...", "agents": ["..."], "return_pct": 0.15, "sharpe": 1.2, "confidence": 0.9}}
    ],
    "patterns_to_avoid": [
        {{"description": "...", "agents": ["..."], "loss_pct": -0.08, "reason": "..."}}
    ],
    "statistically_significant_findings": [
        {{"finding": "...", "p_value": 0.02, "magnitude": 0.05}}
    ],
    "risk_rules_validated": [
        {{"rule": "...", "supporting_evidence": "...", "confidence": 0.85}}
    ],
    "key_learnings": ["...", "..."],
    "recommended_strategies": ["Strategy recommendations based on backtest..."]
}}

Focus on ACTIONABLE insights with STATISTICAL backing. These will be used to improve trading skills."""

        return prompt

    async def _parse_backtest_analysis(self, raw_analysis: str, backtest_data: dict) -> dict:
        """Parse LLM backtest analysis into structured format."""
        analysis = _extract_json_object(raw_analysis)
        if analysis is None:
            logger.warning(
                "Backtest analysis: failed to extract JSON, raw: %.500s",
                raw_analysis,
            )
            analysis = {"raw": raw_analysis, "patterns": []}

        # Convert to PatternInsight objects with backtest validation marker
        patterns = []

        for strategy in analysis.get("backtest_validated_strategies", []):
            patterns.append(PatternInsight(
                pattern_type="backtest_validated_strategy",
                description=strategy["description"],
                conditions={
                    "agents": strategy.get("agents", []),
                    "return_pct": strategy.get("return_pct"),
                    "sharpe": strategy.get("sharpe"),
                    "validation_source": "backtest",
                    "backtest_period": f"{backtest_data.get('start_date')} to {backtest_data.get('end_date')}",
                },
                success_rate=strategy.get("confidence", 0.5),
                sample_size=backtest_data.get("total_ticks", 0),
                confidence=strategy.get("confidence", 0.5),
            ))

        for pattern in analysis.get("patterns_to_avoid", []):
            patterns.append(PatternInsight(
                pattern_type="backtest_invalidated_pattern",
                description=pattern["description"],
                conditions={
                    "agents": pattern.get("agents", []),
                    "loss_pct": pattern.get("loss_pct"),
                    "reason": pattern.get("reason"),
                    "validation_source": "backtest",
                },
                success_rate=0.0,
                sample_size=backtest_data.get("total_ticks", 0),
                confidence=0.8,
            ))

        for finding in analysis.get("statistically_significant_findings", []):
            patterns.append(PatternInsight(
                pattern_type="statistical_finding",
                description=finding["finding"],
                conditions={
                    "p_value": finding.get("p_value"),
                    "magnitude": finding.get("magnitude"),
                    "validation_source": "backtest",
                },
                success_rate=1 - (finding.get("p_value", 0.5) or 0.5),
                sample_size=backtest_data.get("total_ticks", 0),
                confidence=1 - (finding.get("p_value", 0.5) or 0.5),
            ))

        for rule in analysis.get("risk_rules_validated", []):
            patterns.append(PatternInsight(
                pattern_type="backtest_risk_rule",
                description=rule["rule"],
                conditions={
                    "supporting_evidence": rule.get("supporting_evidence"),
                    "validation_source": "backtest",
                },
                success_rate=rule.get("confidence", 0.5),
                sample_size=backtest_data.get("total_ticks", 0),
                confidence=rule.get("confidence", 0.5),
            ))

        analysis["patterns"] = patterns
        analysis["key_learnings"] = analysis.get("key_learnings", [])
        analysis["recommended_strategies"] = analysis.get("recommended_strategies", [])

        return analysis

    async def _generate_backtest_skill_updates(
        self, analysis: dict, backtest_data: dict
    ) -> list[SkillUpdate]:
        """Generate skill updates from backtest analysis."""
        updates = []
        backtest_period = f"{backtest_data.get('start_date')} to {backtest_data.get('end_date')}"

        # Trading wisdom with backtest validation
        if analysis.get("key_learnings") or analysis.get("recommended_strategies"):
            updates.append(SkillUpdate(
                skill_name="trading-wisdom",
                description="Core trading insights learned from Agent Arena competition. "
                           "Use when making any trading decision to apply institutional knowledge.",
                sections={
                    "key_learnings": analysis.get("key_learnings", []),
                    "backtest_validated": True,
                    "backtest_period": backtest_period,
                    "recommended_strategies": analysis.get("recommended_strategies", []),
                    "last_updated": datetime.now(timezone.utc).isoformat(),
                },
                patterns=analysis.get("patterns", []),
            ))

        # Risk management with backtest validation
        risk_patterns = [p for p in analysis.get("patterns", [])
                        if "risk" in p.pattern_type.lower()]
        if risk_patterns:
            updates.append(SkillUpdate(
                skill_name="risk-management",
                description="Risk management rules learned from competition outcomes. "
                           "Use when sizing positions or setting stop-losses.",
                sections={
                    "validated_rules": [
                        {
                            "rule": p.description,
                            "confidence": p.confidence,
                            "validation_source": "backtest",
                            "backtest_period": backtest_period,
                        }
                        for p in risk_patterns
                    ],
                    "last_updated": datetime.now(timezone.utc).isoformat(),
                },
                patterns=risk_patterns,
            ))

        # Entry signals with backtest validation
        strategy_patterns = [p for p in analysis.get("patterns", [])
                           if "strategy" in p.pattern_type.lower() or "signal" in p.pattern_type.lower()]
        if strategy_patterns:
            updates.append(SkillUpdate(
                skill_name="entry-signals",
                description="Entry signal patterns with historical success rates. "
                           "Use when deciding whether to open a position.",
                sections={
                    "validated_signals": [
                        {
                            "signal": p.description,
                            "success_rate": p.success_rate,
                            "confidence": p.confidence,
                            "validation_source": "backtest",
                            "backtest_period": backtest_period,
                        }
                        for p in strategy_patterns
                    ],
                    "last_updated": datetime.now(timezone.utc).isoformat(),
                },
                patterns=strategy_patterns,
            ))

        return updates
    # =========================================================================
    # Forum Analysis Methods (M3)
    # =========================================================================

    async def analyze_forum_window(
        self,
        hours: int = 3,
        symbols: Optional[list[str]] = None,
    ) -> dict:
        """Analyze forum discussions and correlate with trading outcomes.

        This method is called every N hours to:
        1. Load forum messages from the window
        2. Load trades/outcomes from the window
        3. Correlate forum discussions with profitable trades
        4. Generate witness summaries via LLM
        5. Save witness records to database

        Args:
            hours: Analysis window in hours (default 3)
            symbols: Symbols to analyze (None = all)

        Returns:
            Dict with:
                - messages_analyzed: int
                - trades_analyzed: int
                - witness_generated: int
                - summaries: list of witness dicts
        """
        from uuid import uuid4
        from datetime import datetime, timedelta, timezone
        from agent_arena.forum.service import ForumService

        # Define time window
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=hours)
        run_id = uuid4()

        logger.info(
            f"Observer forum analysis: {start_time.isoformat()} to {end_time.isoformat()}"
        )

        # Initialize forum service
        forum = ForumService(self.storage)

        # 1. Load forum messages
        messages = await forum.get_recent_messages(
            channels=["market", "strategy"],
            limit=200,
            since=start_time,
            symbols=symbols,
        )

        logger.info("Loaded %d forum messages", len(messages))

        # 2. Load recent trades and outcomes
        trades = await self._load_forum_window_trades(start_time, end_time, symbols)

        logger.info("Loaded %d trades", len(trades))

        if not messages or not trades:
            logger.info("Insufficient data for forum analysis")
            return {
                "messages_analyzed": len(messages),
                "trades_analyzed": len(trades),
                "witness_generated": 0,
                "summaries": [],
            }

        # 3. Correlate forum messages with outcomes
        correlations = await self._correlate_forum_and_outcomes(messages, trades)

        # 4. Generate witness summaries via LLM
        witness_summaries = await self._generate_witness_summaries(
            messages, trades, correlations
        )

        logger.info("Generated %d witness summaries", len(witness_summaries))

        # 5. Save witness records to database
        witness_ids = []
        for witness in witness_summaries:
            witness_id = await self.storage.save_witness_summary(
                witness_type=witness["type"],
                insight=witness["insight"],
                confidence=witness["confidence"],
                symbols=witness.get("symbols", []),
                timeframe=witness.get("timeframe"),
                based_on=witness.get("based_on", {}),
                metadata=witness.get("metadata", {}),
                valid_until=witness.get("valid_until"),
            )
            witness_ids.append(witness_id)

        # 6. Save run metadata
        await self.storage.save_observer_forum_run(
            run_id=run_id,
            timestamp=end_time,
            window_start=start_time,
            window_end=end_time,
            messages_analyzed=len(messages),
            trades_analyzed=len(trades),
            witness_generated=len(witness_summaries),
            summary={
                "correlations": len(correlations),
                "witness_ids": witness_ids,
            },
        )

        return {
            "messages_analyzed": len(messages),
            "trades_analyzed": len(trades),
            "witness_generated": len(witness_summaries),
            "summaries": witness_summaries,
        }

    async def _load_forum_window_trades(
        self,
        start_time: datetime,
        end_time: datetime,
        symbols: Optional[list[str]] = None,
    ) -> list[dict]:
        """Load trades within the analysis window.

        Args:
            start_time: Window start
            end_time: Window end
            symbols: Filter by symbols (None = all)

        Returns:
            List of trade dicts with outcomes
        """
        try:
            # Query trades with outcomes
            query = """
                SELECT
                    t.id,
                    t.agent_id,
                    t.symbol,
                    t.side,
                    t.size,
                    t.price,
                    t.leverage,
                    t.realized_pnl,
                    t.timestamp,
                    d.reasoning,
                    d.confidence
                FROM trades t
                LEFT JOIN decisions d ON t.decision_id = d.id
                WHERE t.timestamp >= $1 AND t.timestamp <= $2
            """
            params = [start_time, end_time]

            if symbols:
                query += " AND t.symbol = ANY($3)"
                params.append(symbols)

            query += " ORDER BY t.timestamp ASC"

            async with self.storage.pool.acquire() as conn:
                rows = await conn.fetch(query, *params)

            trades = []
            for row in rows:
                trades.append({
                    "id": row["id"],
                    "agent_id": row["agent_id"],
                    "symbol": row["symbol"],
                    "side": row["side"],
                    "size": float(row["size"]) if row["size"] else 0,
                    "price": float(row["price"]) if row["price"] else 0,
                    "leverage": row["leverage"],
                    "realized_pnl": float(row["realized_pnl"]) if row["realized_pnl"] else None,
                    "timestamp": row["timestamp"],
                    "reasoning": row["reasoning"],
                    "confidence": float(row["confidence"]) if row["confidence"] else None,
                })

            return trades

        except Exception as e:
            logger.error("Error loading forum window trades: %s", e)
            return []

    async def _correlate_forum_and_outcomes(
        self,
        messages: list,
        trades: list[dict],
    ) -> list[dict]:
        """Correlate forum messages with trade outcomes.

        Identifies which forum discussions preceded profitable trades.
        Optimized with time-bucketing for O(M + T) complexity instead of O(M × T).

        Args:
            messages: Forum messages
            trades: Trades with outcomes

        Returns:
            List of correlation dicts
        """
        from collections import defaultdict

        correlations = []

        # Build time-based index: hour → [trades in that hour]
        # This reduces O(M × T) to O(M + T)
        trade_index = defaultdict(list)
        for trade in trades:
            trade_time = trade["timestamp"]
            # Bucket by hour
            hour = trade_time.replace(minute=0, second=0, microsecond=0)
            trade_index[hour].append(trade)

        # For each message, only check relevant time buckets (2-hour window)
        for msg in messages:
            msg_time = msg.created_at
            msg_hour = msg_time.replace(minute=0, second=0, microsecond=0)
            related_trades = []

            # Check current hour + next 2 hours (covers 2-hour window)
            for h in range(3):
                check_hour = msg_hour + timedelta(hours=h)
                hour_trades = trade_index.get(check_hour, [])

                # Filter to exact 2-hour window
                for trade in hour_trades:
                    trade_time = trade["timestamp"]
                    time_diff = (trade_time - msg_time).total_seconds() / 3600

                    if 0 <= time_diff <= 2:
                        related_trades.append(trade)

            if related_trades:
                # Calculate success metrics
                profitable_count = sum(
                    1 for t in related_trades if t.get("realized_pnl", 0) > 0
                )
                total_pnl = sum(t.get("realized_pnl", 0) for t in related_trades)

                correlations.append({
                    "message_id": str(msg.id),
                    "message_content": msg.content[:200],  # Truncate
                    "message_time": msg.created_at.isoformat(),
                    "agent_id": msg.agent_id,
                    "agent_name": msg.agent_name,
                    "related_trades": len(related_trades),
                    "profitable_trades": profitable_count,
                    "total_pnl": total_pnl,
                    "success_rate": profitable_count / len(related_trades) if related_trades else 0,
                })

        # Sort by success rate and PnL
        correlations.sort(key=lambda x: (x["success_rate"], x["total_pnl"]), reverse=True)

        return correlations

    async def _generate_witness_summaries(
        self,
        messages: list,
        trades: list[dict],
        correlations: list[dict],
    ) -> list[dict]:
        """Generate witness summaries using LLM.

        Args:
            messages: Forum messages
            trades: Trades with outcomes
            correlations: Correlation analysis

        Returns:
            List of witness summary dicts
        """
        # Build analysis prompt
        prompt = self._build_forum_analysis_prompt(messages, trades, correlations)

        try:
            # Call LLM for analysis
            response = await self._llm.ainvoke([HumanMessage(content=prompt)])
            raw_analysis = response.content

            # Parse witness summaries from response
            witness_summaries = self._parse_witness_response(raw_analysis)

            return witness_summaries

        except Exception as e:
            logger.error("Error generating witness summaries: %s", e)
            return []

    def _build_forum_analysis_prompt(
        self,
        messages: list,
        trades: list[dict],
        correlations: list[dict],
    ) -> str:
        """Build prompt for LLM forum analysis.

        Args:
            messages: Forum messages
            trades: Trades with outcomes
            correlations: Correlation data

        Returns:
            Analysis prompt string
        """
        # Format messages
        messages_text = []
        for msg in messages[:20]:  # Top 20 recent messages
            messages_text.append(
                f"[{msg.created_at.isoformat()}] {msg.agent_name}: {msg.content[:300]}"
            )

        # Format trades
        profitable_trades = [t for t in trades if t.get("realized_pnl", 0) > 0]
        losing_trades = [t for t in trades if t.get("realized_pnl", 0) < 0]

        trades_summary = f"""
Profitable trades: {len(profitable_trades)} (avg PnL: {sum(t.get('realized_pnl', 0) for t in profitable_trades) / len(profitable_trades) if profitable_trades else 0:.2f})
Losing trades: {len(losing_trades)} (avg PnL: {sum(t.get('realized_pnl', 0) for t in losing_trades) / len(losing_trades) if losing_trades else 0:.2f})
Total trades: {len(trades)}
"""

        # Format correlations
        top_correlations = correlations[:5] if correlations else []
        correlations_text = []
        for corr in top_correlations:
            correlations_text.append(
                f"- {corr['agent_name']}: {corr['related_trades']} trades, "
                f"{corr['success_rate']*100:.0f}% win rate, ${corr['total_pnl']:.2f} PnL"
            )

        prompt = f"""You are analyzing forum discussions from AI trading agents and correlating them with trading outcomes.

FORUM MESSAGES (last 3 hours):
{chr(10).join(messages_text)}

TRADING OUTCOMES (last 3 hours):
{trades_summary}

TOP CORRELATIONS (which discussions preceded profitable trades):
{chr(10).join(correlations_text) if correlations_text else "No significant correlations found."}

Your task: Generate 2-4 witness summaries that capture actionable insights.

Each witness should answer:
1. Which forum discussions preceded profitable trades?
2. What patterns emerged from the discussion?
3. What actionable insight can traders use?

Format each witness as JSON:
{{
  "type": "exit_timing" | "entry_signal" | "risk_warning" | "regime_insight",
  "insight": "One sentence actionable insight (max 200 chars)",
  "confidence": 0.0-1.0,
  "based_on": {{"trades_analyzed": N, "forum_posts": N, "win_rate": 0.0-1.0}},
  "symbols": ["PF_XBTUSD"],
  "timeframe": "2-3h" | "intraday" | "4-6h"
}}

Only generate witness summaries if there is clear evidence. If correlations are weak, return empty array.

Output ONLY a JSON array of witness objects, no other text."""

        return prompt

    def _parse_witness_response(self, raw_response: str) -> list[dict]:
        """Parse LLM response into witness summaries.

        Args:
            raw_response: Raw LLM output

        Returns:
            List of witness summary dicts
        """
        try:
            # Extract JSON array from response
            # Handle markdown code blocks
            content = raw_response.strip()

            # Remove markdown code blocks if present
            if content.startswith("```"):
                lines = content.split("\n")
                # Remove first and last lines (```)
                content = "\n".join(lines[1:-1])
                # Remove json language identifier if present
                if content.startswith("json"):
                    content = content[4:].strip()

            # Parse JSON
            witness_summaries = json.loads(content)

            if not isinstance(witness_summaries, list):
                logger.warning("Witness response is not a list")
                return []

            # Validate and clean summaries
            validated = []
            for witness in witness_summaries:
                if not isinstance(witness, dict):
                    continue

                # Required fields
                if "type" not in witness or "insight" not in witness:
                    continue

                # Ensure confidence is set
                if "confidence" not in witness:
                    witness["confidence"] = 0.5

                # Ensure symbols list
                if "symbols" not in witness:
                    witness["symbols"] = []

                # Ensure based_on dict
                if "based_on" not in witness:
                    witness["based_on"] = {}

                validated.append(witness)

            return validated

        except json.JSONDecodeError as e:
            logger.error("Failed to parse witness JSON: %s", e)
            logger.debug("Raw response: %s", raw_response)
            return []
        except Exception as e:
            logger.error("Error parsing witness response: %s", e)
            return []

    # =========================================================================
    # Evolution Analysis Methods (M3.5)
    # =========================================================================

    async def analyze_evolution_run(self, run_id: str) -> dict:
        """Analyze completed evolution run for parameter insights.

        This method:
        1. Loads evolution run metadata and all genomes
        2. Performs statistical analysis on parameter ranges
        3. Identifies character archetypes that succeeded/failed
        4. Analyzes regime-specific patterns (if applicable)
        5. Generates LLM summary of findings
        6. Writes evolved-parameters/SKILL.md
        7. Stores analysis metadata

        Args:
            run_id: The evolution run ID to analyze

        Returns:
            Dict with:
                - run_id: str
                - genomes_analyzed: int
                - patterns_found: int
                - skill_written: str (path)
                - summary: dict
        """
        from agent_arena.evolution.storage import EvolutionStorage

        logger.info("Analyzing evolution run: %s", run_id)

        # Initialize evolution storage
        if not isinstance(self.storage, SupportsEvolution):
            return {
                "status": "error",
                "message": "Evolution analysis requires PostgreSQL storage",
            }

        evo_storage = EvolutionStorage(self.storage)

        # 1. Load evolution run metadata
        run_summary = await evo_storage.get_run_summary(run_id)
        if not run_summary:
            return {
                "status": "error",
                "message": f"Evolution run {run_id} not found",
            }

        # 2. Load all genomes with fitness/metrics
        genomes = await evo_storage.get_all_genomes(run_id)
        logger.info("Loaded %d genomes from evolution run", len(genomes))

        if not genomes:
            return {
                "status": "error",
                "message": "No genomes found for evolution run",
            }

        # Filter out genomes without fitness scores
        scored_genomes = [g for g in genomes if g.get("fitness") is not None]
        logger.info("Found %d genomes with fitness scores", len(scored_genomes))

        if len(scored_genomes) < MIN_GENOMES_FOR_ANALYSIS:
            return {
                "status": "error",
                "message": f"Insufficient scored genomes ({len(scored_genomes)}), need at least {MIN_GENOMES_FOR_ANALYSIS}",
            }

        # 3. Statistical analysis
        param_analysis = self._analyze_parameter_ranges(scored_genomes)
        character_analysis = self._analyze_character_archetypes(scored_genomes)
        regime_analysis = self._analyze_regime_patterns(scored_genomes, run_summary)

        # 4. Generate LLM summary
        analysis_prompt = self._build_evolution_analysis_prompt(
            run_summary=run_summary,
            param_analysis=param_analysis,
            character_analysis=character_analysis,
            regime_analysis=regime_analysis,
        )

        try:
            response = await self._llm.ainvoke([HumanMessage(content=analysis_prompt)])
            llm_summary = response.content
        except Exception as e:
            logger.error("Error generating LLM summary: %s", e)
            llm_summary = "Error generating summary"

        # 5. Write evolved-parameters/SKILL.md
        skill_path = await self._write_evolution_skill(
            run_id=run_id,
            run_summary=run_summary,
            param_analysis=param_analysis,
            character_analysis=character_analysis,
            llm_summary=llm_summary,
        )

        logger.info("Evolution analysis complete: %s", run_id)

        return {
            "status": "success",
            "run_id": run_id,
            "genomes_analyzed": len(scored_genomes),
            "patterns_found": len(param_analysis),
            "skill_written": str(skill_path),
            "summary": {
                "best_fitness": run_summary.get("best_fitness"),
                "parameter_insights": len(param_analysis),
                "character_archetypes": len(character_analysis),
            },
        }

    def _analyze_parameter_ranges(self, genomes: list[dict]) -> dict:
        """Analyze which parameter ranges correlate with high fitness.

        Uses quartile analysis to find optimal parameter ranges.

        Args:
            genomes: List of genomes with fitness scores

        Returns:
            Dict mapping parameter names to analysis results:
            {
                "temperature": {
                    "optimal_range": [0.7, 0.8],
                    "avg_sharpe": 1.8,
                    "avg_fitness": 0.85,
                    "sample_size": 34,
                    "confidence": 0.85
                },
                ...
            }
        """
        import numpy as np

        def _safe_float(value, default=0.0):
            """Safely convert value to float, returning default if conversion fails."""
            try:
                return float(value) if value is not None else default
            except (ValueError, TypeError):
                return default

        results = {}

        for param in EVOLUTION_PARAMETERS_TO_ANALYZE:
            # Extract parameter values and fitness scores
            param_values = []
            for g in genomes:
                genome_dict = g.get("genome", {})
                value = genome_dict.get(param)
                if value is not None:
                    param_values.append({
                        "value": _safe_float(value),
                        "fitness": _safe_float(g.get("fitness")),
                        "sharpe": _safe_float(g.get("metrics", {}).get("sharpe_ratio")),
                        "win_rate": _safe_float(g.get("metrics", {}).get("win_rate")),
                    })

            if len(param_values) < MIN_PARAM_VALUES_FOR_QUARTILES:
                continue

            # Sort by parameter value
            param_values.sort(key=lambda x: x["value"])

            # Calculate quartile boundaries using numpy percentiles
            values_array = np.array([x["value"] for x in param_values])
            q1, q2, q3 = np.percentile(values_array, [25, 50, 75])

            # Split into quartiles based on percentile boundaries
            quartiles = [
                [x for x in param_values if x["value"] <= q1],
                [x for x in param_values if q1 < x["value"] <= q2],
                [x for x in param_values if q2 < x["value"] <= q3],
                [x for x in param_values if x["value"] > q3],
            ]

            # Calculate metrics for each quartile
            quartile_stats = []
            for q in quartiles:
                if not q:
                    continue
                quartile_stats.append({
                    "min": min(x["value"] for x in q),
                    "max": max(x["value"] for x in q),
                    "avg_fitness": np.mean([x["fitness"] for x in q]),
                    "avg_sharpe": np.mean([x["sharpe"] for x in q]),
                    "avg_win_rate": np.mean([x["win_rate"] for x in q]),
                    "count": len(q),
                })

            # Find best quartile by fitness
            if not quartile_stats:
                continue

            best_quartile = max(quartile_stats, key=lambda q: q["avg_fitness"])

            # Calculate confidence based on sample size and variance
            confidence = min(
                MAX_CONFIDENCE_SCORE,
                BASE_CONFIDENCE + (best_quartile["count"] / CONFIDENCE_SAMPLE_DENOMINATOR)
            )

            results[param] = {
                "optimal_range": [round(best_quartile["min"], 3), round(best_quartile["max"], 3)],
                "avg_fitness": round(best_quartile["avg_fitness"], 3),
                "avg_sharpe": round(best_quartile["avg_sharpe"], 3),
                "avg_win_rate": round(best_quartile["avg_win_rate"], 3),
                "sample_size": best_quartile["count"],
                "confidence": round(confidence, 2),
                "all_quartiles": quartile_stats,
            }

        return results

    def _analyze_character_archetypes(self, genomes: list[dict]) -> dict:
        """Cluster genomes by character archetype, identify winners.

        Uses keyword matching to group similar character strings.

        Args:
            genomes: List of genomes with character strings

        Returns:
            Dict mapping archetype names to analysis:
            {
                "conservative contrarian": {
                    "avg_fitness": 0.85,
                    "avg_sharpe": 2.1,
                    "avg_win_rate": 0.68,
                    "sample_size": 8,
                    "best_genome_id": "g_abc123"
                },
                ...
            }
        """
        import numpy as np

        # Extract character strings
        character_groups = {}

        for g in genomes:
            genome_dict = g.get("genome", {})
            character = genome_dict.get("character", "")
            if not character:
                continue

            # Build archetype label from keywords found in character
            character_lower = character.lower()  # Pre-lowercase once for efficiency
            found_keywords = [kw for kw in CHARACTER_ARCHETYPE_KEYWORDS if kw in character_lower]
            archetype = " ".join(found_keywords[:MAX_KEYWORDS_PER_CHARACTER]) if found_keywords else "general trader"

            if archetype not in character_groups:
                character_groups[archetype] = []

            metrics = g.get("metrics") or {}  # Handle None metrics safely
            character_groups[archetype].append({
                "genome_id": g.get("genome_id"),
                "fitness": float(g.get("fitness", 0)),
                "sharpe": float(metrics.get("sharpe_ratio", 0) or 0),
                "win_rate": float(metrics.get("win_rate", 0) or 0),
                "character": character,
            })

        # Calculate stats for each archetype
        results = {}
        for archetype, group in character_groups.items():
            if len(group) < MIN_GROUP_SIZE_FOR_ARCHETYPE:
                continue

            best_genome = max(group, key=lambda x: x["fitness"])

            results[archetype] = {
                "avg_fitness": round(np.mean([x["fitness"] for x in group]), 3),
                "avg_sharpe": round(np.mean([x["sharpe"] for x in group]), 3),
                "avg_win_rate": round(np.mean([x["win_rate"] for x in group]), 3),
                "sample_size": len(group),
                "best_genome_id": best_genome["genome_id"],
                "best_fitness": round(best_genome["fitness"], 3),
                "example_character": group[0]["character"][:CHARACTER_TRUNCATION_LENGTH],
            }

        # Sort by avg fitness
        sorted_results = dict(
            sorted(results.items(), key=lambda x: x[1]["avg_fitness"], reverse=True)
        )

        return sorted_results

    def _analyze_regime_patterns(self, genomes: list[dict], run_summary: dict) -> dict:
        """Analyze regime-specific patterns (if applicable).

        For MVP, this returns a placeholder. Future versions can analyze
        multi-regime backtests to identify regime-specific optimal parameters.

        Args:
            genomes: List of genomes
            run_summary: Evolution run metadata

        Returns:
            Dict with regime analysis (currently placeholder)
        """
        # Placeholder for regime analysis
        # In future, this could:
        # 1. Split backtest period into regimes (trending/ranging/volatile)
        # 2. Analyze genome performance in each regime
        # 3. Recommend regime-specific parameters

        return {
            "status": "not_implemented",
            "message": "Regime-specific analysis not yet implemented",
            "recommendation": "Use overall parameter insights for now",
        }

    def _build_evolution_analysis_prompt(
        self,
        run_summary: dict,
        param_analysis: dict,
        character_analysis: dict,
        regime_analysis: dict,
    ) -> str:
        """Build LLM prompt for evolution analysis.

        Args:
            run_summary: Evolution run metadata
            param_analysis: Parameter range analysis results
            character_analysis: Character archetype analysis results
            regime_analysis: Regime pattern analysis results

        Returns:
            Formatted prompt string for LLM
        """
        # Format parameter analysis
        param_text = []
        for param, data in param_analysis.items():
            param_text.append(
                f"\n{param.upper()}:\n"
                f"  Optimal range: {data['optimal_range']}\n"
                f"  Avg fitness: {data['avg_fitness']:.3f}\n"
                f"  Avg Sharpe: {data['avg_sharpe']:.3f}\n"
                f"  Sample size: {data['sample_size']}\n"
                f"  Confidence: {data['confidence']:.2f}"
            )

        # Format character analysis
        char_text = []
        for archetype, data in character_analysis.items():
            char_text.append(
                f"\n{archetype.upper()}:\n"
                f"  Avg fitness: {data['avg_fitness']:.3f}\n"
                f"  Avg Sharpe: {data['avg_sharpe']:.3f}\n"
                f"  Win rate: {data['avg_win_rate']:.1%}\n"
                f"  Sample size: {data['sample_size']}\n"
                f"  Best genome: {data['best_genome_id']}"
            )

        # Safe formatting for best_fitness (can be None from database)
        best_fitness = run_summary.get('best_fitness')
        best_fitness_str = f"{best_fitness:.3f}" if best_fitness is not None else "N/A"

        prompt = f"""You are analyzing the results of a genetic algorithm that evolved
trading agent configurations through backtesting.

EVOLUTION RUN SUMMARY:
- Run ID: {run_summary['run_id']}
- Period: {run_summary['backtest_start']} to {run_summary['backtest_end']}
- Genomes tested: {run_summary['population_size']} × {run_summary['max_generations']} = {run_summary['population_size'] * run_summary['max_generations']}
- Best fitness: {best_fitness_str}
- Symbols: {', '.join(run_summary['symbols'])}

PARAMETER ANALYSIS:
{''.join(param_text)}

CHARACTER ARCHETYPE ANALYSIS:
{''.join(char_text)}

Based on this analysis, generate a comprehensive trading skill document with these sections:

1. **Executive Summary**
   - Key takeaways from this evolution run
   - 3-5 bullet points with actionable insights

2. **High-Fitness Parameter Ranges**
   - For each parameter, explain the optimal range and why
   - Include confidence level and sample size
   - Highlight ranges to avoid (if evident)

3. **Winning Character Archetypes**
   - Describe the top 3 character types by fitness
   - Explain what makes them successful in this backtest period
   - Best use cases for each

4. **Patterns to Avoid**
   - Character archetypes that consistently underperformed
   - Parameter ranges that led to poor outcomes

5. **Transfer Considerations**
   - Important caveats about backtest vs live trading
   - Known limitations and degradation factors
   - How to use these insights responsibly

Format as markdown with clear structure. Use tables where appropriate.
Be specific with numbers. Include confidence levels.
Keep the total output under {SKILL_TOKEN_LIMIT} tokens - this is a skill file, not a research paper."""

        return prompt

    async def _write_evolution_skill(
        self,
        run_id: str,
        run_summary: dict,
        param_analysis: dict,
        character_analysis: dict,
        llm_summary: str,
    ) -> Path:
        """Write the evolved-parameters/SKILL.md file.

        Args:
            run_id: Evolution run ID
            run_summary: Run metadata
            param_analysis: Parameter analysis results
            character_analysis: Character analysis results
            llm_summary: LLM-generated summary

        Returns:
            Path to the written skill file
        """
        from datetime import datetime, timezone

        # Create skill directory
        skill_dir = self.skills_dir / "evolved-parameters"
        skill_dir.mkdir(parents=True, exist_ok=True)

        skill_path = skill_dir / "SKILL.md"

        # Build skill content
        now = datetime.now(timezone.utc)

        # Safe formatting for symbols (ensure it's a list)
        symbols = run_summary.get('symbols', [])
        symbols_str = ', '.join(symbols) if isinstance(symbols, list) else str(symbols)

        # Build parameter tables
        param_tables = []
        for param, data in param_analysis.items():
            param_tables.append(
                f"### {param}\n\n"
                f"| Metric | Value |\n"
                f"|--------|-------|\n"
                f"| **Optimal Range** | **{data['optimal_range'][0]} - {data['optimal_range'][1]}** |\n"
                f"| Avg Fitness | {data['avg_fitness']:.3f} |\n"
                f"| Avg Sharpe | {data['avg_sharpe']:.3f} |\n"
                f"| Avg Win Rate | {data['avg_win_rate']:.1%} |\n"
                f"| Sample Size | {data['sample_size']} |\n"
                f"| Confidence | {data['confidence']:.0%} |\n"
            )

        # Build character tables
        char_sections = []
        for i, (archetype, data) in enumerate(list(character_analysis.items())[:MAX_ARCHETYPES_TO_DISPLAY], 1):
            char_sections.append(
                f"### {i}. \"{archetype.title()}\"\n\n"
                f"**Avg Fitness**: {data['avg_fitness']:.3f} (n={data['sample_size']} genomes)\n"
                f"**Avg Sharpe**: {data['avg_sharpe']:.3f} | **Win Rate**: {data['avg_win_rate']:.1%}\n\n"
                f"**Example**: {data['example_character']}\n\n"
                f"**Best genome**: `{data['best_genome_id']}` (Fitness: {data['best_fitness']:.3f})\n"
            )

        content = f"""---
name: evolved-parameters
description: Parameter configurations discovered through genetic evolution on historical data
evolution_run_id: {run_id}
backtest_period: {run_summary['backtest_start']} to {run_summary['backtest_end']}
genomes_analyzed: {run_summary['population_size'] * run_summary['max_generations']}
last_updated: {now.isoformat()}
---

# Evolved Parameter Insights

> **Source**: Evolution run `{run_id}`
> **Backtest**: {run_summary['backtest_start']} to {run_summary['backtest_end']}
> **Symbols**: {symbols_str}
> **Genomes tested**: {run_summary['population_size']} × {run_summary['max_generations']} = {run_summary['population_size'] * run_summary['max_generations']}
> **Best fitness**: {best_fitness_str}

---

## LLM Analysis

{llm_summary}

---

## High-Fitness Parameter Ranges

{''.join(param_tables)}

---

## Winning Character Archetypes

{''.join(char_sections)}

---

## How to Use This Skill

**✅ DO:**
- Use parameter ranges as guidance for manual config tuning
- Prefer character archetypes that succeeded in this backtest
- Consider these insights as one data point among many

**❌ DON'T:**
- Treat these as gospel - markets evolve
- Blindly deploy best genome without live validation
- Ignore your own risk management rules

**⚠️ TRANSFER WARNING:**
- These insights are from backtest on **historical data**
- Live markets may behave differently
- Typical degradation: backtest Sharpe 1.8 → live Sharpe ~1.4 (22% drop)
- Always validate parameters in live conditions with small position sizes first

---

## Metadata

- **Generated by**: Observer Agent (M3.5)
- **Last updated**: {now.strftime('%Y-%m-%d %H:%M UTC')}
- **Evolution run**: {run_id}
- **Confidence**: Statistical analysis based on {len(param_analysis)} parameters and {len(character_analysis)} character archetypes

---

*This skill is automatically generated by the Observer Agent after each evolution run.*
"""

        # Write file
        skill_path.write_text(content)
        logger.info("Wrote evolution skill to %s", skill_path)

        # Also save metadata
        meta_file = skill_dir / ".skill_meta.json"
        best_fitness_val = run_summary.get("best_fitness")
        meta_file.write_text(json.dumps({
            "evolution_run_id": run_id,
            "generated_at": now.isoformat(),
            "backtest_period": {
                "start": run_summary["backtest_start"],
                "end": run_summary["backtest_end"],
            },
            "genomes_analyzed": run_summary['population_size'] * run_summary['max_generations'],
            "best_fitness": float(best_fitness_val) if best_fitness_val is not None else None,
            "parameters_analyzed": list(param_analysis.keys()),
            "character_archetypes": list(character_analysis.keys()),
        }, indent=2))

        return skill_path
