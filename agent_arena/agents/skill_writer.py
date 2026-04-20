"""Skill Writer - Generates SKILL.md files from observed patterns.

This module handles the conversion of analyzed patterns into properly
formatted SKILL.md files following the Anthropic Agent Skills format.

Key features:
- Accumulates learnings over time (patterns persist across updates)
- Tracks pattern history (first_seen, last_confirmed, times_seen)
- Confidence decay for unconfirmed patterns
- Merges new patterns with existing knowledge
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


@dataclass
class PatternHistory:
    """Tracks the history of a learned pattern."""

    pattern_id: str  # Hash of pattern description for identity
    pattern_type: str
    description: str
    conditions: dict
    success_rate: float
    sample_size: int
    confidence: float
    first_seen: str  # ISO timestamp
    last_confirmed: str  # ISO timestamp
    times_seen: int
    is_active: bool = True

    def to_dict(self) -> dict:
        return {
            "pattern_id": self.pattern_id,
            "pattern_type": self.pattern_type,
            "description": self.description,
            "conditions": self.conditions,
            "success_rate": self.success_rate,
            "sample_size": self.sample_size,
            "confidence": self.confidence,
            "first_seen": self.first_seen,
            "last_confirmed": self.last_confirmed,
            "times_seen": self.times_seen,
            "is_active": self.is_active,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PatternHistory":
        return cls(**data)

    @staticmethod
    def generate_id(description: str, pattern_type: str) -> str:
        """Generate a stable ID for a pattern based on its description."""
        content = f"{pattern_type}:{description[:100].lower().strip()}"
        return hashlib.md5(content.encode()).hexdigest()[:12]


@dataclass
class SkillUpdate:
    """Represents an update to a skill file."""

    skill_name: str
    description: str
    sections: dict[str, Any] = field(default_factory=dict)
    patterns: list[Any] = field(default_factory=list)
    version: str = "1.0"
    confidence_threshold: float = 0.6


class SkillWriter:
    """
    Writes SKILL.md files following the Anthropic Agent Skills format.

    Format:
    ```
    ---
    name: skill-name
    description: When to use this skill
    ---

    # Skill Title

    ## Section 1
    Content...

    ## Section 2
    Content...
    ```

    The writer supports:
    - Creating new skills
    - Updating existing skills (merge or replace)
    - Tracking skill versions and history
    - Accumulating patterns over time with confidence decay
    """

    # Confidence decay rate per day for unconfirmed patterns
    CONFIDENCE_DECAY_RATE = 0.05  # 5% per day
    # Minimum confidence before pattern becomes inactive
    MIN_CONFIDENCE_THRESHOLD = 0.40
    # Days without confirmation before applying decay
    DECAY_GRACE_PERIOD_DAYS = 1

    def __init__(self, skills_dir: str | Path = ".claude/skills"):
        self.skills_dir = Path(skills_dir)
        self.skills_dir.mkdir(parents=True, exist_ok=True)

    def _get_pattern_history_path(self, skill_name: str) -> Path:
        """Get the path to pattern history JSON file."""
        return self.skills_dir / skill_name / ".pattern_history.json"

    def _load_pattern_history(self, skill_name: str) -> dict[str, PatternHistory]:
        """Load existing pattern history for a skill."""
        history_path = self._get_pattern_history_path(skill_name)
        if not history_path.exists():
            return {}

        try:
            data = json.loads(history_path.read_text())
            return {
                pid: PatternHistory.from_dict(pdata)
                for pid, pdata in data.items()
            }
        except (json.JSONDecodeError, KeyError, TypeError):
            return {}

    def _save_pattern_history(
        self, skill_name: str, history: dict[str, PatternHistory]
    ) -> None:
        """Save pattern history to JSON file."""
        history_path = self._get_pattern_history_path(skill_name)
        history_path.parent.mkdir(parents=True, exist_ok=True)
        data = {pid: p.to_dict() for pid, p in history.items()}
        history_path.write_text(json.dumps(data, indent=2))

    def _apply_confidence_decay(
        self, history: dict[str, PatternHistory], confirmed_ids: set[str]
    ) -> dict[str, PatternHistory]:
        """Apply confidence decay to patterns not confirmed in this update."""
        now = datetime.now(timezone.utc)
        updated_history = {}

        for pid, pattern in history.items():
            if pid in confirmed_ids:
                # Pattern was confirmed, no decay
                updated_history[pid] = pattern
                continue

            # Calculate days since last confirmation
            try:
                last_confirmed = datetime.fromisoformat(pattern.last_confirmed)
                days_since = (now - last_confirmed).days
            except (ValueError, TypeError):
                days_since = 1

            # Apply decay if past grace period
            if days_since > self.DECAY_GRACE_PERIOD_DAYS:
                decay_days = days_since - self.DECAY_GRACE_PERIOD_DAYS
                decay = self.CONFIDENCE_DECAY_RATE * decay_days
                new_confidence = max(0, pattern.confidence - decay)

                # Mark inactive if below threshold
                if new_confidence < self.MIN_CONFIDENCE_THRESHOLD:
                    pattern.is_active = False

                pattern.confidence = new_confidence

            updated_history[pid] = pattern

        return updated_history

    def _merge_patterns(
        self,
        existing_history: dict[str, PatternHistory],
        new_patterns: list[Any],
    ) -> tuple[dict[str, PatternHistory], set[str]]:
        """
        Merge new patterns with existing history.

        Returns:
            Tuple of (merged history, set of confirmed pattern IDs)
        """
        now = datetime.now(timezone.utc).isoformat()
        confirmed_ids = set()
        merged = dict(existing_history)

        for pattern in new_patterns:
            # Generate pattern ID
            pattern_id = PatternHistory.generate_id(
                pattern.description, pattern.pattern_type
            )

            if pattern_id in merged:
                # Update existing pattern
                existing = merged[pattern_id]
                confirmed_ids.add(pattern_id)

                # Accumulate sample size
                new_sample_size = existing.sample_size + pattern.sample_size

                # Weighted average of success rate
                total_samples = new_sample_size
                if total_samples > 0:
                    new_success_rate = (
                        existing.success_rate * existing.sample_size
                        + pattern.success_rate * pattern.sample_size
                    ) / total_samples
                else:
                    new_success_rate = pattern.success_rate

                # Boost confidence for reconfirmed patterns
                confidence_boost = min(0.05, pattern.sample_size * 0.005)
                new_confidence = min(0.99, existing.confidence + confidence_boost)

                merged[pattern_id] = PatternHistory(
                    pattern_id=pattern_id,
                    pattern_type=pattern.pattern_type,
                    description=pattern.description,
                    conditions=pattern.conditions,
                    success_rate=new_success_rate,
                    sample_size=new_sample_size,
                    confidence=new_confidence,
                    first_seen=existing.first_seen,
                    last_confirmed=now,
                    times_seen=existing.times_seen + 1,
                    is_active=True,
                )
            else:
                # New pattern
                confirmed_ids.add(pattern_id)
                merged[pattern_id] = PatternHistory(
                    pattern_id=pattern_id,
                    pattern_type=pattern.pattern_type,
                    description=pattern.description,
                    conditions=pattern.conditions,
                    success_rate=pattern.success_rate,
                    sample_size=pattern.sample_size,
                    confidence=pattern.confidence,
                    first_seen=now,
                    last_confirmed=now,
                    times_seen=1,
                    is_active=True,
                )

        return merged, confirmed_ids

    def get_existing_patterns_summary(self, skill_name: str) -> str:
        """
        Get a summary of existing patterns for inclusion in analysis prompts.

        Returns formatted string of existing patterns with their history.
        """
        history = self._load_pattern_history(skill_name)
        if not history:
            return "No existing patterns for this skill."

        active = [p for p in history.values() if p.is_active]
        if not active:
            return "No active patterns (all have decayed below threshold)."

        lines = []
        for p in sorted(active, key=lambda x: -x.confidence):
            lines.append(
                f"- [{p.pattern_type}] {p.description[:80]}... "
                f"(conf: {p.confidence:.0%}, seen {p.times_seen}x, "
                f"samples: {p.sample_size}, first: {p.first_seen[:10]})"
            )

        return "\n".join(lines[:20])  # Limit to top 20

    async def write_skill(
        self,
        update: SkillUpdate,
        mode: str = "merge",
    ) -> Path:
        """
        Write or update a skill file.

        Args:
            update: The skill update to write
            mode: "merge" to combine with existing, "replace" to overwrite

        Returns:
            Path to the written skill file
        """
        skill_dir = self.skills_dir / update.skill_name
        skill_dir.mkdir(parents=True, exist_ok=True)
        skill_file = skill_dir / "SKILL.md"

        # Load and merge pattern history
        if mode == "merge":
            existing_history = self._load_pattern_history(update.skill_name)
            merged_history, confirmed_ids = self._merge_patterns(
                existing_history, update.patterns
            )
            # Apply decay to unconfirmed patterns
            final_history = self._apply_confidence_decay(merged_history, confirmed_ids)
        else:
            # Replace mode - start fresh
            final_history = {}
            now = datetime.now(timezone.utc).isoformat()
            for pattern in update.patterns:
                pattern_id = PatternHistory.generate_id(
                    pattern.description, pattern.pattern_type
                )
                final_history[pattern_id] = PatternHistory(
                    pattern_id=pattern_id,
                    pattern_type=pattern.pattern_type,
                    description=pattern.description,
                    conditions=pattern.conditions,
                    success_rate=pattern.success_rate,
                    sample_size=pattern.sample_size,
                    confidence=pattern.confidence,
                    first_seen=now,
                    last_confirmed=now,
                    times_seen=1,
                    is_active=True,
                )

        # Save pattern history
        self._save_pattern_history(update.skill_name, final_history)

        # Generate content using accumulated history
        content = self._generate_skill_content_from_history(
            update, final_history
        )

        # Write file
        skill_file.write_text(content)

        # Write metadata file for tracking
        meta_file = skill_dir / ".skill_meta.json"
        active_patterns = [p for p in final_history.values() if p.is_active]
        meta = {
            "name": update.skill_name,
            "version": update.version,
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "total_patterns": len(final_history),
            "active_patterns": len(active_patterns),
            "inactive_patterns": len(final_history) - len(active_patterns),
            "confidence_threshold": update.confidence_threshold,
        }
        meta_file.write_text(json.dumps(meta, indent=2))

        return skill_file

    def _generate_skill_content_from_history(
        self,
        update: SkillUpdate,
        history: dict[str, PatternHistory],
    ) -> str:
        """Generate SKILL.md content from accumulated pattern history."""
        lines = []

        # YAML frontmatter
        lines.append("---")
        lines.append(f"name: {update.skill_name}")
        lines.append(f"description: {update.description}")
        lines.append("---")
        lines.append("")

        # Title
        title = update.skill_name.replace("-", " ").title()
        lines.append(f"# {title}")
        lines.append("")

        # Statistics
        active_patterns = [p for p in history.values() if p.is_active]
        total_samples = sum(p.sample_size for p in active_patterns)
        lines.append(f"> Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
        lines.append(f"> Active patterns: {len(active_patterns)}")
        lines.append(f"> Total samples: {total_samples}")
        lines.append(f"> Confidence threshold: {update.confidence_threshold:.0%}")
        lines.append("")

        # Generate sections based on skill type using history
        if update.skill_name == "trading-wisdom":
            lines.extend(self._generate_trading_wisdom_from_history(update, history))
        elif update.skill_name == "market-regimes":
            lines.extend(self._generate_market_regimes_from_history(update, history))
        elif update.skill_name == "risk-management":
            lines.extend(self._generate_risk_management_from_history(update, history))
        elif update.skill_name == "entry-signals":
            lines.extend(self._generate_signals_from_history(history, "entry"))
        elif update.skill_name == "exit-signals":
            lines.extend(self._generate_signals_from_history(history, "exit"))
        else:
            lines.extend(self._generate_generic_from_history(update, history))

        # Add confidence guide
        lines.extend(self._generate_confidence_guide())

        return "\n".join(lines)

    def _generate_trading_wisdom_from_history(
        self, update: SkillUpdate, history: dict[str, PatternHistory]
    ) -> list[str]:
        """Generate trading-wisdom sections from pattern history."""
        lines = []

        # Key learnings from sections
        lines.append("## Key Learnings")
        lines.append("")
        learnings = update.sections.get("key_learnings", [])
        if learnings:
            for i, learning in enumerate(learnings, 1):
                lines.append(f"{i}. {learning}")
            lines.append("")
        else:
            lines.append("*No key learnings extracted yet.*")
            lines.append("")

        # Winning strategies from history
        lines.append("## Winning Strategies")
        lines.append("")
        winning = [
            p for p in history.values()
            if p.is_active and p.pattern_type == "winning_strategy"
        ]
        if winning:
            for pattern in sorted(winning, key=lambda x: (-x.confidence, -x.sample_size)):
                lines.append(f"### {pattern.description[:50]}...")
                lines.append(f"- **Confidence**: {pattern.confidence:.0%}")
                lines.append(f"- **Total samples**: {pattern.sample_size}")
                lines.append(f"- **Times confirmed**: {pattern.times_seen}")
                lines.append(f"- **First seen**: {pattern.first_seen[:10]}")
                lines.append(f"- **Details**: {pattern.description}")
                lines.append("")
        else:
            lines.append("*No winning strategies identified yet.*")
            lines.append("")

        # Patterns to avoid
        lines.append("## Patterns to Avoid")
        lines.append("")
        losing = [
            p for p in history.values()
            if p.is_active and p.pattern_type == "losing_pattern"
        ]
        if losing:
            for pattern in sorted(losing, key=lambda x: -x.confidence):
                lines.append(f"- **AVOID**: {pattern.description}")
                lines.append(
                    f"  - Conf: {pattern.confidence:.0%}, "
                    f"N={pattern.sample_size}, seen {pattern.times_seen}x"
                )
            lines.append("")
        else:
            lines.append("*No losing patterns identified yet.*")
            lines.append("")

        return lines

    def _generate_market_regimes_from_history(
        self, update: SkillUpdate, history: dict[str, PatternHistory]
    ) -> list[str]:
        """Generate market-regimes sections from pattern history."""
        lines = []

        lines.append("## How to Use This Skill")
        lines.append("")
        lines.append("1. Identify the current market regime using price action and volatility")
        lines.append("2. Look up the recommended strategy for that regime below")
        lines.append("3. Adjust your trading approach accordingly")
        lines.append("4. Monitor for regime changes")
        lines.append("")

        lines.append("## Regime Strategies")
        lines.append("")

        regime_patterns = [
            p for p in history.values()
            if p.is_active and p.pattern_type == "regime_strategy"
        ]

        if regime_patterns:
            for pattern in sorted(regime_patterns, key=lambda x: -x.confidence):
                # Extract regime from description (format: "regime: strategy")
                parts = pattern.description.split(":", 1)
                regime = parts[0].strip() if parts else "unknown"
                strategy = parts[1].strip() if len(parts) > 1 else pattern.description

                lines.append(f"### {regime.replace('_', ' ').title()}")
                lines.append("")
                lines.append(
                    f"**Recommended approach** "
                    f"({pattern.confidence:.0%} confidence, seen {pattern.times_seen}x):"
                )
                lines.append(f"> {strategy}")
                lines.append(f"- Total observations: {pattern.sample_size}")
                lines.append(f"- First identified: {pattern.first_seen[:10]}")
                lines.append("")
        else:
            lines.append("*No regime strategies learned yet.*")
            lines.append("")

        return lines

    def _generate_risk_management_from_history(
        self, update: SkillUpdate, history: dict[str, PatternHistory]
    ) -> list[str]:
        """Generate risk-management sections from pattern history."""
        lines = []

        lines.append("## Core Principles")
        lines.append("")
        lines.append("These rules are derived from analyzing profitable vs losing trades:")
        lines.append("")

        risk_patterns = [
            p for p in history.values()
            if p.is_active and p.pattern_type == "risk_rule"
        ]

        if risk_patterns:
            # Sort by success rate
            sorted_patterns = sorted(risk_patterns, key=lambda x: -x.success_rate)

            lines.append("| Rule | Success Rate | Samples | Confidence | Seen |")
            lines.append("|------|-------------|---------|------------|------|")
            for p in sorted_patterns:
                lines.append(
                    f"| {p.description[:40]}... | {p.success_rate:.0%} | "
                    f"{p.sample_size} | {p.confidence:.0%} | {p.times_seen}x |"
                )
            lines.append("")

            # Highlight top rules
            lines.append("## Top Risk Rules")
            lines.append("")
            for p in sorted_patterns[:5]:
                lines.append(f"### {p.description}")
                lines.append(f"- Success rate: {p.success_rate:.0%}")
                lines.append(f"- Based on {p.sample_size} observations")
                lines.append(f"- Confidence: {p.confidence:.0%} (seen {p.times_seen} times)")
                lines.append(f"- First identified: {p.first_seen[:10]}")
                lines.append("")
        else:
            lines.append("*No risk rules learned yet.*")
            lines.append("")

        # Add general guidelines
        lines.append("## General Guidelines")
        lines.append("")
        lines.append("- Never risk more than 2% of equity on a single trade")
        lines.append("- Use stop-losses on every position")
        lines.append("- Reduce position size in high volatility regimes")
        lines.append("- Don't add to losing positions")
        lines.append("")

        return lines

    def _generate_signals_from_history(
        self, history: dict[str, PatternHistory], signal_type: str
    ) -> list[str]:
        """Generate entry/exit signals sections from pattern history."""
        lines = []

        lines.append(f"## {signal_type.title()} Signals")
        lines.append("")
        lines.append(f"These {signal_type} signals have been learned from competition data:")
        lines.append("")

        signal_patterns = [
            p for p in history.values()
            if p.is_active and p.pattern_type == f"{signal_type}_signal"
        ]

        if signal_patterns:
            lines.append("| Signal | Success Rate | Samples | Confidence | Seen |")
            lines.append("|--------|-------------|---------|------------|------|")
            for p in sorted(signal_patterns, key=lambda x: -x.success_rate):
                sig = p.description[:35]
                lines.append(
                    f"| {sig}... | {p.success_rate:.0%} | "
                    f"{p.sample_size} | {p.confidence:.0%} | {p.times_seen}x |"
                )
            lines.append("")

            # Detail top signals
            lines.append("## Signal Details")
            lines.append("")
            for p in sorted(signal_patterns, key=lambda x: -x.success_rate)[:5]:
                lines.append(f"### {p.description[:40]}...")
                lines.append(f"**Success rate**: {p.success_rate:.0%}")
                lines.append(f"**Total samples**: {p.sample_size}")
                lines.append(f"**Confidence**: {p.confidence:.0%}")
                lines.append(f"**Times confirmed**: {p.times_seen}")
                lines.append(f"**First seen**: {p.first_seen[:10]}")
                lines.append(f"**Description**: {p.description}")
                lines.append("")
        else:
            lines.append(f"*No {signal_type} signals learned yet.*")
            lines.append("")

        return lines

    def _generate_generic_from_history(
        self, update: SkillUpdate, history: dict[str, PatternHistory]
    ) -> list[str]:
        """Generate generic skill sections from pattern history."""
        lines = []

        # Include sections from update
        for section_name, section_content in update.sections.items():
            lines.append(f"## {section_name.replace('_', ' ').title()}")
            lines.append("")
            if isinstance(section_content, list):
                for item in section_content:
                    lines.append(f"- {item}")
            elif isinstance(section_content, dict):
                for key, value in section_content.items():
                    lines.append(f"- **{key}**: {value}")
            else:
                lines.append(str(section_content))
            lines.append("")

        # Add patterns from history
        active_patterns = [p for p in history.values() if p.is_active]
        if active_patterns:
            lines.append("## Learned Patterns")
            lines.append("")
            for p in sorted(active_patterns, key=lambda x: -x.confidence):
                lines.append(f"### {p.description[:50]}...")
                lines.append(f"- Type: {p.pattern_type}")
                lines.append(f"- Confidence: {p.confidence:.0%}")
                lines.append(f"- Samples: {p.sample_size}")
                lines.append(f"- Seen: {p.times_seen} times")
                lines.append("")

        return lines

    def _generate_confidence_guide(self) -> list[str]:
        """Add confidence interpretation guide."""
        lines = []

        lines.append("---")
        lines.append("")
        lines.append("## Confidence Guide")
        lines.append("")
        lines.append("| Confidence | Interpretation |")
        lines.append("|------------|----------------|")
        lines.append("| 90%+ | High confidence - strong historical support |")
        lines.append("| 70-90% | Moderate confidence - use with other signals |")
        lines.append("| 60-70% | Low confidence - consider as one input |")
        lines.append("| <60% | Experimental - needs more data |")
        lines.append("")
        lines.append("*This skill is automatically generated and updated by the Observer Agent.*")
        lines.append("")

        return lines

    async def read_skill(self, skill_name: str) -> Optional[str]:
        """Read an existing skill file."""
        skill_file = self.skills_dir / skill_name / "SKILL.md"
        if skill_file.exists():
            return skill_file.read_text()
        return None

    async def list_skills(self) -> list[dict]:
        """List all available skills."""
        skills = []
        if self.skills_dir.exists():
            for skill_dir in self.skills_dir.iterdir():
                if skill_dir.is_dir():
                    skill_file = skill_dir / "SKILL.md"
                    meta_file = skill_dir / ".skill_meta.json"
                    if skill_file.exists():
                        meta = {}
                        if meta_file.exists():
                            import json
                            meta = json.loads(meta_file.read_text())
                        skills.append({
                            "name": skill_dir.name,
                            "path": str(skill_file),
                            **meta,
                        })
        return skills
