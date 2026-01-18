"""Observer Agent - Watches competition and distills knowledge into skills.

This agent doesn't trade. Instead, it:
1. Observes all agent decisions, trades, and outcomes
2. Analyzes patterns across agents and market conditions
3. Distills learnings into SKILL.md files
4. Updates skills periodically (e.g., daily)

The generated skills can be used by trading agents to improve their decisions.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from langchain_anthropic import ChatAnthropic

from agent_arena.agents.skill_writer import SkillUpdate, SkillWriter


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
        model: str = "claude-opus-4-5-20251101",  # Opus for best analysis
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
            max_tokens=4096,
        )

        # Skill writer handles file generation
        self._skill_writer = SkillWriter(skills_dir)

        # Track observation state
        self._last_analysis: Optional[datetime] = None
        self._analysis_history: list[dict] = []

    async def run_daily_analysis(self, lookback_hours: int = 24) -> dict:
        """
        Run the daily observation and skill update cycle.

        Args:
            lookback_hours: How many hours of data to analyze

        Returns:
            Summary of analysis and skill updates
        """
        now = datetime.utcnow()
        window = ObservationWindow(
            start=now - timedelta(hours=lookback_hours),
            end=now,
            tick_start=0,  # Will be filled from data
            tick_end=0,
        )

        # Phase 1: Collect data
        data = await self._collect_observation_data(window)
        if not data["decisions"]:
            return {"status": "no_data", "message": "No decisions in observation window"}

        # Phase 2: Analyze patterns
        analysis = await self._analyze_patterns(data, window)

        # Phase 3: Generate skill updates
        skill_updates = await self._generate_skill_updates(analysis)

        # Phase 4: Write skills
        written_skills = await self._write_skills(skill_updates)

        # Record analysis
        summary = {
            "timestamp": now.isoformat(),
            "window": {
                "start": window.start.isoformat(),
                "end": window.end.isoformat(),
            },
            "data_summary": {
                "decisions_analyzed": len(data["decisions"]),
                "trades_analyzed": len(data["trades"]),
                "agents_observed": len(data["agent_ids"]),
            },
            "patterns_found": len(analysis.get("patterns", [])),
            "skills_updated": written_skills,
            "status": "success",
        }

        self._last_analysis = now
        self._analysis_history.append(summary)

        return summary

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

        return data

    async def _analyze_patterns(self, data: dict, window: ObservationWindow) -> dict:
        """Use LLM to analyze patterns in the data."""
        # Load existing patterns from all skills
        existing_patterns = await self._load_existing_patterns()

        # Prepare analysis prompt with existing patterns
        analysis_prompt = self._build_analysis_prompt(data, window, existing_patterns)

        # Call LLM for analysis
        response = await self._llm.ainvoke(analysis_prompt)
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

    def _build_analysis_prompt(
        self,
        data: dict,
        window: ObservationWindow,
        existing_patterns: dict[str, str] | None = None,
    ) -> str:
        """Build the analysis prompt for the LLM."""
        # Summarize data for prompt
        decisions_by_agent = {}
        for d in data["decisions"]:
            agent_id = d["agent_id"]
            if agent_id not in decisions_by_agent:
                decisions_by_agent[agent_id] = []
            decisions_by_agent[agent_id].append(d)

        # Calculate basic stats per agent
        agent_summaries = []
        for agent_id, decisions in decisions_by_agent.items():
            actions = [d["action"] for d in decisions]
            holds = actions.count("hold")
            trades = len(actions) - holds

            # Get trades for this agent
            agent_trades = [t for t in data["trades"] if t["agent_id"] == agent_id]
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

        # Build existing patterns section
        existing_patterns_section = ""
        if existing_patterns:
            existing_patterns_section = """
EXISTING LEARNED PATTERNS (from previous analyses):
These patterns have been learned from prior observation windows. For each pattern,
you should CONFIRM (if you see supporting evidence), UPDATE (if new data suggests
refinement), or note CONTRADICTION (if new data contradicts the pattern).

"""
            for skill_name, summary in existing_patterns.items():
                existing_patterns_section += f"### {skill_name}\n{summary}\n\n"

            existing_patterns_section += """
IMPORTANT: When analyzing, consider whether the new data:
1. CONFIRMS existing patterns (include them in output with increased confidence)
2. REFINES existing patterns (update description/success_rate based on new evidence)
3. CONTRADICTS existing patterns (note the contradiction in key_learnings)
4. REVEALS NEW patterns not previously identified

Patterns that are confirmed multiple times become more reliable.
Patterns not seen in new data will naturally decay over time.
"""
        else:
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
{existing_patterns_section}
ANALYSIS TASKS:
1. WINNING STRATEGIES: What patterns do successful agents follow?
2. LOSING PATTERNS: What behaviors lead to losses?
3. REGIME INSIGHTS: How do agents perform in different market conditions?
4. RISK PATTERNS: What risk management approaches work best?
5. ENTRY/EXIT SIGNALS: What technical or sentiment signals are agents using?

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
    "pattern_confirmations": ["List patterns from EXISTING that were confirmed by new data"],
    "pattern_contradictions": ["List any contradictions found with existing patterns"]
}}

Focus on patterns with statistical significance (multiple occurrences, clear outcomes).
Be specific and actionable. Avoid vague generalizations.
Re-include patterns from EXISTING LEARNED PATTERNS that you see confirmed in the new data."""

        return prompt

    async def _parse_analysis(self, raw_analysis: str, data: dict) -> dict:
        """Parse LLM analysis into structured format."""
        # Extract JSON from response
        try:
            # Try to find JSON in the response
            import re
            json_match = re.search(r"\{[\s\S]*\}", raw_analysis)
            if json_match:
                analysis = json.loads(json_match.group())
            else:
                analysis = {"raw": raw_analysis, "patterns": []}
        except json.JSONDecodeError:
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
            if rule.get("sample_size", 0) >= self.min_sample_size:
                patterns.append(PatternInsight(
                    pattern_type="risk_rule",
                    description=rule["rule"],
                    conditions={},
                    success_rate=rule.get("success_rate", 0.5),
                    sample_size=rule.get("sample_size", 0),
                    confidence=min(0.95, 0.5 + (rule.get("sample_size", 0) * 0.02)),
                ))

        for signal in analysis.get("signals", []):
            if signal.get("sample_size", 0) >= self.min_sample_size:
                patterns.append(PatternInsight(
                    pattern_type=f"{signal['type']}_signal",
                    description=signal["description"],
                    conditions={"type": signal["type"]},
                    success_rate=signal.get("success_rate", 0.5),
                    sample_size=signal.get("sample_size", 0),
                    confidence=min(0.95, 0.5 + (signal.get("sample_size", 0) * 0.02)),
                ))

        analysis["patterns"] = patterns
        analysis["key_learnings"] = analysis.get("key_learnings", [])

        return analysis

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
        if analysis.get("key_learnings"):
            updates.append(SkillUpdate(
                skill_name="trading-wisdom",
                description="Core trading insights learned from Agent Arena competition. "
                           "Use when making any trading decision to apply institutional knowledge.",
                sections={
                    "key_learnings": analysis["key_learnings"],
                    "last_updated": datetime.utcnow().isoformat(),
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

        return updates

    async def _write_skills(self, updates: list[SkillUpdate]) -> list[str]:
        """Write skill updates to files and save snapshots to PostgreSQL."""
        import hashlib

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
                        print(f"Failed to save skill version to DB: {update.skill_name}: {e}")

            except Exception as e:
                print(f"Failed to write skill {update.skill_name}: {e}")
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
