"""Journal Service — generates daily diagnostic editorials.

Gathers competition data, computes metrics (pure Python), calls LLM for
editorial article, parses per-agent sections, and saves to DB + markdown.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from agent_arena.journal.models import AgentDailyStats, JournalEntry, JournalMetrics
from agent_arena.llm_utils import strip_think_blocks

logger = logging.getLogger(__name__)


class JournalService:
    """Generates daily journal entries with diagnostic analysis."""

    def __init__(
        self,
        storage: Any,
        journal_dir: str | Path = "data/journal",
        model: str = "claude-opus-4-6",
        config_path: str | Path = "configs/production.yaml",
    ):
        self.storage = storage
        self.journal_dir = Path(journal_dir)
        self.journal_dir.mkdir(parents=True, exist_ok=True)
        self.model = model
        self._active_agent_ids = self._load_active_agents(config_path)

    @staticmethod
    def _load_active_agents(config_path: str | Path) -> set[str]:
        """Load active agent IDs from config YAML."""
        try:
            import yaml

            path = Path(config_path)
            if not path.exists():
                logger.warning("Config %s not found, no agent filtering", path)
                return set()
            with open(path) as f:
                raw = yaml.safe_load(f)
            agents = raw.get("agents", [])
            ids = {a["id"] for a in agents if "id" in a}
            logger.info("Active agents from config: %s", ids)
            return ids
        except Exception:
            logger.warning("Failed to load active agents from %s", config_path)
            return set()

    async def generate_daily_journal(
        self, lookback_hours: int = 24
    ) -> JournalEntry:
        """Generate a complete journal entry."""
        now = datetime.now(timezone.utc)

        # 1. Gather data
        data = await self._gather_data(lookback_hours, now)

        # 2. Compute metrics (pure Python, no LLM)
        metrics = self._compute_metrics(data)

        # 3. Build LLM prompt
        prompt = self._build_journal_prompt(data, metrics)

        # 4. Call LLM for editorial
        article = await self._generate_article(prompt)

        # 5. Parse sections from article
        sections = self._parse_sections(article)
        agent_reports = self._parse_agent_reports(article)

        # 6. Build entry
        entry = JournalEntry(
            id=str(uuid.uuid4()),
            journal_date=now.date(),
            generated_at=now,
            lookback_hours=lookback_hours,
            full_markdown=article,
            market_summary=sections.get("market_recap", ""),
            forum_summary=sections.get("forum_quality_assessment", ""),
            learning_summary=sections.get("learning_loop_assessment", ""),
            recommendations=sections.get("recommendations", ""),
            agent_reports=agent_reports,
            metrics=metrics.to_dict(),
            model=self.model,
        )

        # 7. Save to DB + markdown
        await self._save_journal(entry)

        return entry

    # ------------------------------------------------------------------
    # Data Gathering
    # ------------------------------------------------------------------

    async def _gather_data(self, lookback_hours: int, now: datetime) -> dict:
        """Collect all data needed for journal generation."""
        start = now - timedelta(hours=lookback_hours)
        data: dict[str, Any] = {
            "start": start,
            "end": now,
            "lookback_hours": lookback_hours,
            "decisions": [],
            "trades": [],
            "snapshots": [],
            "forum_messages": [],

            "agent_ids": set(),
            "agent_names": {},
        }

        if hasattr(self.storage, "pool") and self.storage.pool is not None:
            await self._gather_postgres(data, start, now)
        elif (
            hasattr(self.storage, "_connection")
            and self.storage._connection is not None
        ):
            await self._gather_sqlite(data, start, now)
        else:
            logger.warning(
                "No usable storage backend for journal data gathering"
            )

        # Group by agent
        data["decisions_by_agent"] = {}
        for d in data["decisions"]:
            data["decisions_by_agent"].setdefault(d["agent_id"], []).append(d)

        data["trades_by_agent"] = {}
        for t in data["trades"]:
            data["trades_by_agent"].setdefault(t["agent_id"], []).append(t)

        data["agent_ids"] = {d["agent_id"] for d in data["decisions"]}

        # Filter to only active agents from config (skip retired agents)
        if self._active_agent_ids:
            retired = data["agent_ids"] - self._active_agent_ids
            if retired:
                logger.info("Filtering out retired agents: %s", retired)
                data["agent_ids"] &= self._active_agent_ids
                data["decisions"] = [
                    d for d in data["decisions"]
                    if d["agent_id"] in self._active_agent_ids
                ]
                data["trades"] = [
                    t for t in data["trades"]
                    if t["agent_id"] in self._active_agent_ids
                ]
                data["decisions_by_agent"] = {
                    k: v for k, v in data["decisions_by_agent"].items()
                    if k in self._active_agent_ids
                }
                data["trades_by_agent"] = {
                    k: v for k, v in data["trades_by_agent"].items()
                    if k in self._active_agent_ids
                }

        return data

    async def _gather_postgres(
        self, data: dict, start: datetime, end: datetime
    ) -> None:
        """Gather data from PostgreSQL."""
        start_ts = start if start.tzinfo else start.replace(tzinfo=timezone.utc)
        end_ts = end if end.tzinfo else end.replace(tzinfo=timezone.utc)

        async with self.storage.pool.acquire() as conn:
            # Decisions
            rows = await conn.fetch(
                """SELECT * FROM decisions
                   WHERE timestamp >= $1 AND timestamp <= $2
                   ORDER BY tick ASC""",
                start_ts, end_ts,
            )
            data["decisions"] = [dict(r) for r in rows]

            # Trades
            rows = await conn.fetch(
                """SELECT * FROM trades
                   WHERE timestamp >= $1 AND timestamp <= $2
                   ORDER BY timestamp ASC""",
                start_ts, end_ts,
            )
            data["trades"] = [dict(r) for r in rows]

            # Snapshots (for market data)
            rows = await conn.fetch(
                """SELECT * FROM snapshots
                   WHERE timestamp >= $1 AND timestamp <= $2
                   ORDER BY tick ASC""",
                start_ts, end_ts,
            )
            data["snapshots"] = [dict(r) for r in rows]

            # Forum messages
            try:
                rows = await conn.fetch(
                    """SELECT * FROM forum_messages
                       WHERE created_at >= $1 AND created_at <= $2
                       ORDER BY created_at ASC""",
                    start_ts, end_ts,
                )
                data["forum_messages"] = [dict(r) for r in rows]
            except Exception:
                logger.debug("No forum_messages table or query failed")


    async def _gather_sqlite(
        self, data: dict, start: datetime, end: datetime
    ) -> None:
        """Gather data from SQLite."""
        start_iso = start.isoformat()
        end_iso = end.isoformat()

        async with self.storage._connection.execute(
            """SELECT * FROM decisions
               WHERE datetime(timestamp) >= datetime(?)
               AND datetime(timestamp) <= datetime(?)
               ORDER BY tick ASC""",
            (start_iso, end_iso),
        ) as cursor:
            rows = await cursor.fetchall()
            columns = [d[0] for d in cursor.description]
            data["decisions"] = [dict(zip(columns, row)) for row in rows]

        async with self.storage._connection.execute(
            """SELECT * FROM trades
               WHERE datetime(timestamp) >= datetime(?)
               AND datetime(timestamp) <= datetime(?)
               ORDER BY timestamp ASC""",
            (start_iso, end_iso),
        ) as cursor:
            rows = await cursor.fetchall()
            columns = [d[0] for d in cursor.description]
            data["trades"] = [dict(zip(columns, row)) for row in rows]

        async with self.storage._connection.execute(
            """SELECT * FROM snapshots
               WHERE datetime(timestamp) >= datetime(?)
               AND datetime(timestamp) <= datetime(?)
               ORDER BY tick ASC""",
            (start_iso, end_iso),
        ) as cursor:
            rows = await cursor.fetchall()
            columns = [d[0] for d in cursor.description]
            data["snapshots"] = [dict(zip(columns, row)) for row in rows]

    # ------------------------------------------------------------------
    # Metric Computation (Pure Python)
    # ------------------------------------------------------------------

    def _compute_metrics(self, data: dict) -> JournalMetrics:
        """Compute all metrics from gathered data."""
        metrics = JournalMetrics()

        # Parse JSON fields
        for d in data["decisions"]:
            if isinstance(d.get("metadata"), str):
                try:
                    d["metadata"] = json.loads(d["metadata"])
                except (json.JSONDecodeError, TypeError):
                    d["metadata"] = {}

        for s in data["snapshots"]:
            for field in ("leaderboard", "market_data"):
                if isinstance(s.get(field), str):
                    try:
                        s[field] = json.loads(s[field])
                    except (json.JSONDecodeError, TypeError):
                        s[field] = {} if field == "market_data" else []

        # Per-agent stats
        for agent_id in data["agent_ids"]:
            agent_decisions = data["decisions_by_agent"].get(agent_id, [])
            agent_trades = data["trades_by_agent"].get(agent_id, [])
            stats = self._compute_agent_stats(agent_id, agent_decisions, agent_trades)
            metrics.agent_stats[agent_id] = stats

        # Market summary from snapshots
        metrics.price_changes = self._compute_price_changes(data["snapshots"])
        metrics.funding_rates = self._compute_funding_rates(data["snapshots"])

        # Forum quality
        if data["forum_messages"]:
            metrics.forum_post_count = len(data["forum_messages"])
            metrics.forum_accuracy = self._compute_forum_quality(
                data["forum_messages"], data["trades"], data["snapshots"]
            )

        # Overlay unrealized PnL from latest snapshot leaderboard
        self._overlay_unrealized_pnl(data["snapshots"], metrics)

        # Learning delta
        self._compute_learning_delta(metrics)

        return metrics

    def _compute_agent_stats(
        self, agent_id: str, decisions: list[dict], trades: list[dict]
    ) -> AgentDailyStats:
        """Compute stats for a single agent."""
        stats = AgentDailyStats(
            agent_id=agent_id,
            agent_name=agent_id,  # Will be overridden if name available
            total_decisions=len(decisions),
        )

        if not decisions:
            return stats

        # Count actions
        action_counts: Counter = Counter()
        confidences = []
        symbols = set()
        for d in decisions:
            action = d.get("action", "hold")
            action_counts[action] += 1
            if d.get("confidence") is not None:
                try:
                    confidences.append(float(d["confidence"]))
                except (ValueError, TypeError):
                    pass
            if d.get("symbol"):
                symbols.add(d["symbol"])

        stats.hold_count = action_counts.get("hold", 0)
        stats.dominant_action = action_counts.most_common(1)[0][0] if action_counts else "hold"
        stats.symbols_traded = sorted(symbols)
        if confidences:
            stats.avg_confidence = sum(confidences) / len(confidences)

        # Classify agent type from metadata flags (set by each agent's
        # decide() method). Priority order: most specific type wins.
        _type_priority = {
            "journal_aware": 4,
            "forum_aware": 3,
            "skill_aware": 2,
            "agentic": 1,
        }
        best_type = ""
        best_priority = -1
        for d in decisions:
            meta = d.get("metadata", {})
            if not isinstance(meta, dict):
                continue
            for flag, priority in _type_priority.items():
                if meta.get(flag) and priority > best_priority:
                    best_type = flag
                    best_priority = priority
            if best_priority == 4:
                break  # Already at highest priority
        stats.agent_type = best_type

        # Trade stats
        if trades:
            stats.trade_count = len(trades)
            winning = 0
            losing = 0
            total_pnl = 0.0
            for t in trades:
                pnl = float(t.get("realized_pnl", 0) or 0)
                total_pnl += pnl
                if pnl > 0:
                    winning += 1
                elif pnl < 0:
                    losing += 1

            stats.winning_trades = winning
            stats.losing_trades = losing
            stats.pnl = total_pnl
            stats.total_pnl = total_pnl  # baseline; overlay adds unrealized
            stats.win_rate = winning / len(trades) if trades else 0.0

        # Overtrading score: ratio of non-hold decisions to total
        non_hold = stats.total_decisions - stats.hold_count
        if stats.total_decisions > 0:
            stats.overtrading_score = min(1.0, non_hold / max(stats.total_decisions * 0.3, 1))

        return stats

    def _compute_price_changes(self, snapshots: list[dict]) -> dict[str, float]:
        """Compute price changes from first to last snapshot."""
        if len(snapshots) < 2:
            return {}

        first_market = snapshots[0].get("market_data", {})
        last_market = snapshots[-1].get("market_data", {})

        changes = {}
        for symbol in last_market:
            try:
                first_price = float(first_market.get(symbol, {}).get("price", 0))
                last_price = float(last_market.get(symbol, {}).get("price", 0))
                if first_price > 0:
                    changes[symbol] = (last_price - first_price) / first_price
            except (ValueError, TypeError, AttributeError):
                pass

        return changes

    def _compute_funding_rates(self, snapshots: list[dict]) -> dict[str, float]:
        """Get latest funding rates from snapshots."""
        if not snapshots:
            return {}

        last_market = snapshots[-1].get("market_data", {})
        rates = {}
        for symbol, data in last_market.items():
            if isinstance(data, dict):
                rate = data.get("funding_rate")
                if rate is not None:
                    try:
                        rates[symbol] = float(rate)
                    except (ValueError, TypeError):
                        pass
        return rates

    def _overlay_unrealized_pnl(
        self, snapshots: list[dict], metrics: JournalMetrics
    ) -> None:
        """Overlay unrealized PnL from latest snapshot leaderboard."""
        if not snapshots:
            return

        latest = snapshots[-1]
        leaderboard = latest.get("leaderboard", [])
        if not isinstance(leaderboard, list):
            return

        for entry in leaderboard:
            if not isinstance(entry, dict):
                continue
            aid = entry.get("agent_id")
            if not aid or aid not in metrics.agent_stats:
                continue
            stats = metrics.agent_stats[aid]
            stats.unrealized_pnl = float(entry.get("unrealized_pnl", 0))
            stats.total_pnl = stats.pnl + stats.unrealized_pnl
            positions = entry.get("open_positions", [])
            if isinstance(positions, list):
                stats.open_positions_detail = positions
                stats.open_position_count = len(positions)

    def _compute_forum_quality(
        self,
        messages: list[dict],
        trades: list[dict],
        snapshots: list[dict],
    ) -> dict[str, dict]:
        """Evaluate forum post quality (simplified)."""
        by_agent: dict[str, list] = {}
        for msg in messages:
            aid = msg.get("agent_id", "unknown")
            by_agent.setdefault(aid, []).append(msg)

        result = {}
        for agent_id, posts in by_agent.items():
            result[agent_id] = {
                "post_count": len(posts),
                "agent_type": posts[0].get("agent_type", "unknown") if posts else "unknown",
            }

        return result

    def _compute_learning_delta(self, metrics: JournalMetrics) -> None:
        """Compute PnL delta between skill-aware, forum-aware, and simple agents."""
        skill_pnls = []
        forum_pnls = []
        non_skill_pnls = []

        for stats in metrics.agent_stats.values():
            if stats.agent_type in ("skill_aware", "journal_aware"):
                skill_pnls.append(stats.pnl)
            elif stats.agent_type == "forum_aware":
                forum_pnls.append(stats.pnl)
            elif stats.agent_type not in ("ta_rule", "index_fund"):
                non_skill_pnls.append(stats.pnl)

        if skill_pnls:
            metrics.skill_aware_avg_pnl = sum(skill_pnls) / len(skill_pnls)
        if non_skill_pnls:
            metrics.non_skill_avg_pnl = sum(non_skill_pnls) / len(non_skill_pnls)
        if forum_pnls:
            metrics.forum_aware_avg_pnl = sum(forum_pnls) / len(forum_pnls)

    # ------------------------------------------------------------------
    # LLM Prompt & Generation
    # ------------------------------------------------------------------

    def _build_journal_prompt(self, data: dict, metrics: JournalMetrics) -> str:
        """Build the LLM prompt with all computed data."""
        lines = [
            "You are writing a daily journal for a crypto futures trading competition.",
            "Your role is a CRITICAL analyst — like a trading desk's morning report.",
            "Be honest, data-driven, and blunt. Cite specific numbers.",
            "",
            f"Period: last {data['lookback_hours']} hours",
            f"Agents observed: {len(data['agent_ids'])}",
            f"Total decisions: {len(data['decisions'])}",
            f"Total trades: {len(data['trades'])}",
            "",
        ]

        # Market data
        if metrics.price_changes:
            lines.append("MARKET PRICE CHANGES:")
            for sym, chg in sorted(metrics.price_changes.items()):
                lines.append(f"  {sym}: {chg:+.2%}")
            lines.append("")

        if metrics.funding_rates:
            lines.append("FUNDING RATES:")
            for sym, rate in sorted(metrics.funding_rates.items()):
                lines.append(f"  {sym}: {rate:+.4%}")
            lines.append("")

        # Per-agent stats
        lines.append("AGENT PERFORMANCE:")
        for aid, stats in sorted(metrics.agent_stats.items()):
            lines.append(f"  [{aid}] (type={stats.agent_type or 'simple'})")
            lines.append(f"    Decisions: {stats.total_decisions}, Trades: {stats.trade_count}")
            lines.append(f"    Win rate: {stats.win_rate:.1%}, Realized PnL: ${stats.pnl:+.2f}")
            lines.append(
                f"    Unrealized PnL: ${stats.unrealized_pnl:+.2f}"
                f", Total PnL: ${stats.total_pnl:+.2f}"
            )
            lines.append(f"    Holds: {stats.hold_count}/{stats.total_decisions}")
            lines.append(f"    Avg confidence: {stats.avg_confidence:.2f}")
            lines.append(f"    Overtrading score: {stats.overtrading_score:.2f}")
            lines.append(f"    Symbols: {', '.join(stats.symbols_traded) or 'none'}")
            if stats.open_positions_detail:
                lines.append(f"    Open positions ({stats.open_position_count}):")
                for pos in stats.open_positions_detail:
                    sym = pos.get("symbol", "?")
                    side = pos.get("side", "?")
                    upnl = pos.get("unrealized_pnl", 0)
                    roe = pos.get("roe_pct", 0)
                    lev = pos.get("leverage", 1)
                    lines.append(
                        f"      {sym} {side} {lev}x:"
                        f" uPnL ${upnl:+.2f} (ROE {roe:+.1f}%)"
                    )
            lines.append("")

        # Forum data
        if metrics.forum_post_count > 0:
            lines.append(f"FORUM: {metrics.forum_post_count} messages")
            for fid, fdata in metrics.forum_accuracy.items():
                agent_type = fdata.get('agent_type', '?')
                post_count = fdata.get('post_count', 0)
                lines.append(f"  {fid}: {post_count} posts ({agent_type})")
            lines.append("")

        # Learning delta
        if metrics.skill_aware_avg_pnl != 0 or metrics.non_skill_avg_pnl != 0:
            lines.append("LEARNING LOOP:")
            lines.append(f"  Skill-aware avg PnL: ${metrics.skill_aware_avg_pnl:+.2f}")
            lines.append(f"  Forum-aware avg PnL: ${metrics.forum_aware_avg_pnl:+.2f}")
            lines.append(f"  Non-skill avg PnL: ${metrics.non_skill_avg_pnl:+.2f}")
            lines.append("")

        # Instructions
        lines.extend([
            "WRITE A JOURNAL ENTRY WITH THESE SECTIONS:",
            "",
            "## Market Recap",
            "Price action, funding rates, notable moves. 2-3 paragraphs.",
            "",
            "## Agent Report Cards",
            "For EACH agent, write a section delimited by <!-- AGENT: {agent_id} -->",
            "Include: trade count, win rate, realized PnL, unrealized PnL, total PnL.",
            "Include specific critique and specific recommendation.",
            "Be blunt. If they're overtrading, say so with numbers.",
            "If they're ignoring stop-losses, call it out.",
            "If an agent has large unrealized gains without taking profit, critique the timing.",
            "If an agent has deteriorating unrealized PnL, flag missing stop-losses.",
            "Recognize good positioning — agents with large unrealized profits.",
            "",
            "## Forum Quality Assessment",
            "- Post volume and agent participation",
            "- Are forum-aware agents doing better than non-forum agents?",
            "- Echo chamber risk: are all posts converging on same narrative?",
            "",
            "## Learning Loop Assessment",
            "- Skill-aware agents avg PnL vs non-skill agents",
            "- Has performance gap widened or narrowed?",
            "- Any signs of overfitting?",
            "",
            "## Recommendations",
            "Top 3 actionable items. Be specific.",
            "",
            "FORMAT: Markdown. Use tables for comparisons. Keep total under 3000 tokens.",
        ])

        return "\n".join(lines)

    async def _generate_article(self, prompt: str) -> str:
        """Call LLM to generate the journal article."""
        try:
            from langchain_anthropic import ChatAnthropic
            from langchain_core.messages import HumanMessage

            llm = ChatAnthropic(
                model=self.model,
                temperature=0.4,
                max_tokens=4096,
            )
            response = await llm.ainvoke([HumanMessage(content=prompt)])
            content = response.content if hasattr(response, "content") else str(response)
            return strip_think_blocks(content)
        except Exception:
            logger.exception("Failed to generate journal article via LLM")
            return self._generate_fallback_article(prompt)

    def _generate_fallback_article(self, prompt: str) -> str:
        """Generate a basic article from the data if LLM fails."""
        return (
            "# Daily Journal\n\n"
            "*LLM generation failed. Raw data summary below.*\n\n"
            "```\n" + prompt[:3000] + "\n```\n"
        )

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_sections(self, article: str) -> dict[str, str]:
        """Extract named sections from markdown article."""
        sections: dict[str, str] = {}
        current_key = ""
        current_lines: list[str] = []

        for line in article.split("\n"):
            if line.startswith("## "):
                if current_key:
                    sections[current_key] = "\n".join(current_lines).strip()
                heading = line[3:].strip()
                current_key = heading.lower().replace(" ", "_")
                current_lines = []
            else:
                current_lines.append(line)

        if current_key:
            sections[current_key] = "\n".join(current_lines).strip()

        return sections

    def _parse_agent_reports(self, article: str) -> dict[str, str]:
        """Extract per-agent sections delimited by <!-- AGENT: agent_id -->."""
        reports: dict[str, str] = {}
        pattern = r"<!-- AGENT: (\S+) -->"
        parts = re.split(pattern, article)

        # parts alternates: [text_before, agent_id, text, agent_id, text, ...]
        for i in range(1, len(parts) - 1, 2):
            agent_id = parts[i]
            text = parts[i + 1]
            # Trim to next AGENT marker or section header
            end_match = re.search(r"(?=<!-- AGENT:)|(?=\n## )", text)
            if end_match:
                text = text[:end_match.start()]
            reports[agent_id] = text.strip()

        return reports

    @staticmethod
    def get_agent_briefing(entry: JournalEntry, agent_id: str) -> str:
        """Get personalized briefing for an agent."""
        parts = []

        if entry.market_summary:
            parts.append("## Market Overview\n" + entry.market_summary)

        # Agent's own report card
        report = entry.agent_reports.get(agent_id)
        if report:
            parts.append("## Your Report Card\n" + report)
        else:
            parts.append("## Your Report Card\nNo specific report available for this period.")

        if entry.forum_summary:
            parts.append("## Forum Quality\n" + entry.forum_summary)

        if entry.recommendations:
            parts.append("## Recommendations\n" + entry.recommendations)

        return "\n\n".join(parts)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    async def _save_journal(self, entry: JournalEntry) -> None:
        """Save journal entry to DB and markdown file."""
        # Save to DB
        if hasattr(self.storage, "save_journal_entry"):
            try:
                await self.storage.save_journal_entry(entry.to_dict())
            except Exception:
                logger.exception("Failed to save journal entry to DB")

        # Write markdown file
        try:
            md_path = self.journal_dir / f"{entry.journal_date.isoformat()}.md"
            md_path.write_text(entry.full_markdown, encoding="utf-8")
            logger.info("Journal written to %s", md_path)
        except Exception:
            logger.exception("Failed to write journal markdown")
