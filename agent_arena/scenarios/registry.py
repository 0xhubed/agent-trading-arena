"""Scenario registry — discovers and verifies saved scenarios."""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path

from agent_arena.scenarios.models import Scenario

logger = logging.getLogger(__name__)

BASE_DIR = Path("data/scenarios")


class ScenarioRegistry:
    """Discovers, loads, and verifies scenarios on disk."""

    def __init__(self, base_dir: str | Path = BASE_DIR):
        self.base_dir = Path(base_dir)

    def list_scenarios(self) -> list[Scenario]:
        """Scan the scenarios directory and return all valid scenarios."""
        scenarios = []
        if not self.base_dir.exists():
            return scenarios

        for d in sorted(self.base_dir.iterdir()):
            if not d.is_dir():
                continue
            meta_path = d / "metadata.json"
            if meta_path.exists():
                try:
                    scenario = Scenario.from_json(
                        meta_path.read_text(encoding="utf-8")
                    )
                    scenarios.append(scenario)
                except Exception as e:
                    logger.warning(f"Skipping {d.name}: {e}")

        return scenarios

    def load_scenario(self, scenario_id: str) -> Scenario:
        """Load a single scenario by ID."""
        meta_path = self.base_dir / scenario_id / "metadata.json"
        if not meta_path.exists():
            raise FileNotFoundError(
                f"Scenario '{scenario_id}' not found at {meta_path}"
            )
        return Scenario.from_json(meta_path.read_text(encoding="utf-8"))

    def verify_checksum(self, scenario_id: str) -> bool:
        """Verify the candles.json checksum matches metadata."""
        scenario = self.load_scenario(scenario_id)
        candles_path = self.base_dir / scenario_id / "candles.json"

        if not candles_path.exists():
            return False

        content = candles_path.read_bytes()
        actual = hashlib.sha256(content).hexdigest()
        return actual == scenario.checksum

    def verify_all(self) -> dict[str, bool]:
        """Verify checksums for all scenarios. Returns {id: passed}."""
        results = {}
        for scenario in self.list_scenarios():
            results[scenario.scenario_id] = self.verify_checksum(
                scenario.scenario_id
            )
        return results
