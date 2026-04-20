"""Agent Deployer — converts approved genomes to production config via PR."""

from __future__ import annotations

import json
import logging
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml

from agent_arena.evolution.genome import AgentGenome

logger = logging.getLogger(__name__)


class AgentDeployer:
    """Deploys approved genomes to production by creating a PR.

    Flow:
    1. Load approved genome from promotion_queue
    2. Identify worst-performing agent to replace (via journal metrics)
    3. Generate new YAML agent entry
    4. Write changes to git worktree
    5. Commit + push + create PR
    6. All production changes require human approval (PR review)
    """

    def __init__(
        self,
        storage: Any,
        config_path: str = "configs/production.yaml",
        project_root: str = ".",
    ):
        self.storage = storage
        self.config_path = Path(config_path)
        self.project_root = Path(project_root).resolve()

    async def deploy_approved(self) -> list[dict]:
        """Deploy all approved promotions.

        Returns list of deployment results.
        """
        approved = await self._get_approved_promotions()
        if not approved:
            logger.info("No approved promotions to deploy")
            return []

        results = []
        for promotion in approved:
            try:
                result = await self._deploy_one(promotion)
                results.append(result)
            except Exception:
                logger.exception(
                    "Failed to deploy promotion %s", promotion.get("id")
                )
                results.append({
                    "promotion_id": promotion.get("id"),
                    "status": "failed",
                    "error": "Deployment error",
                })

        return results

    async def _deploy_one(self, promotion: dict) -> dict:
        """Deploy a single approved promotion."""
        genome_data = promotion.get("genome", {})
        if isinstance(genome_data, str):
            genome_data = json.loads(genome_data)

        genome = AgentGenome.from_dict(genome_data)

        # Find worst performer to replace
        worst_agent = await self._find_worst_performer()
        if not worst_agent:
            return {
                "promotion_id": promotion.get("id"),
                "status": "skipped",
                "reason": "No underperforming agent found to replace",
            }

        # Generate new agent config
        new_agent_id = f"evolved_{genome.genome_id[:8]}"
        new_agent_name = f"Evolved {genome.model} (gen {genome.generation})"

        new_config = genome.to_agent_config(
            agent_id=new_agent_id,
            agent_name=new_agent_name,
            base_url="",
            api_key_env="TOGETHER_API_KEY",
        )

        # Create PR with changes
        pr_url = await self._create_deployment_pr(
            worst_agent_id=worst_agent["agent_id"],
            new_config=new_config,
            genome=genome,
            fitness=promotion.get("fitness", 0),
        )

        # Update promotion status
        await self._mark_deployed(promotion.get("id"), new_agent_id)

        return {
            "promotion_id": promotion.get("id"),
            "status": "deployed",
            "replaced_agent": worst_agent["agent_id"],
            "new_agent_id": new_agent_id,
            "pr_url": pr_url,
        }

    async def _find_worst_performer(self) -> Optional[dict]:
        """Find the worst-performing agent from recent journal data."""
        if not hasattr(self.storage, "pool"):
            return None

        try:
            async with self.storage.pool.acquire() as conn:
                # Get the latest journal's agent reports
                row = await conn.fetchrow(
                    """
                    SELECT agent_reports FROM observer_journal
                    ORDER BY journal_date DESC LIMIT 1
                    """
                )
                if not row or not row["agent_reports"]:
                    return None

                reports = row["agent_reports"]
                if isinstance(reports, str):
                    reports = json.loads(reports)

                # Find agent with worst PnL
                worst = None
                worst_pnl = float("inf")
                for agent_id, report in reports.items():
                    pnl = report.get("pnl", 0)
                    if pnl < worst_pnl:
                        worst_pnl = pnl
                        worst = {"agent_id": agent_id, "pnl": pnl}

                return worst
        except Exception:
            logger.exception("Failed to find worst performer")
            return None

    async def _create_deployment_pr(
        self,
        worst_agent_id: str,
        new_config: dict,
        genome: AgentGenome,
        fitness: float,
    ) -> str:
        """Create a PR with the config change. Returns PR URL."""
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        branch_name = f"experiment/deploy-{genome.genome_id[:8]}-{date_str}"

        try:
            # Create worktree
            worktree_dir = tempfile.mkdtemp(prefix="agent-arena-deploy-")

            subprocess.run(
                ["git", "worktree", "add", worktree_dir, "-b", branch_name],
                cwd=str(self.project_root),
                check=True,
                capture_output=True,
            )

            # Read and modify production config
            config_in_worktree = Path(worktree_dir) / self.config_path
            with open(config_in_worktree) as f:
                config = yaml.safe_load(f)

            # Replace worst agent with new one
            agents = config.get("agents", [])
            replaced = False
            for i, agent in enumerate(agents):
                if agent.get("id") == worst_agent_id:
                    agents[i] = new_config
                    replaced = True
                    break

            if not replaced:
                # Append instead
                agents.append(new_config)

            config["agents"] = agents

            with open(config_in_worktree, "w") as f:
                yaml.dump(config, f, default_flow_style=False, sort_keys=False)

            # Commit
            subprocess.run(
                ["git", "add", str(self.config_path)],
                cwd=worktree_dir,
                check=True,
                capture_output=True,
            )
            commit_msg = (
                f"experiment: deploy evolved agent {genome.genome_id[:8]}\n\n"
                f"Replaces {worst_agent_id} with evolved genome.\n"
                f"Fitness: {fitness:.4f}\n"
                f"Model: {genome.model}\n"
                f"Agent class: {genome.agent_class}\n"
                f"Generation: {genome.generation}"
            )
            subprocess.run(
                ["git", "commit", "-m", commit_msg],
                cwd=worktree_dir,
                check=True,
                capture_output=True,
            )

            # Push
            subprocess.run(
                ["git", "push", "-u", "origin", branch_name],
                cwd=worktree_dir,
                check=True,
                capture_output=True,
            )

            # Create PR
            result = subprocess.run(
                [
                    "gh", "pr", "create",
                    "--title", f"Experiment: Deploy evolved agent {genome.genome_id[:8]}",
                    "--body", (
                        f"## Auto-generated by Experiment Deployer\n\n"
                        f"**Replaces**: `{worst_agent_id}`\n"
                        f"**Genome**: `{genome.genome_id}`\n"
                        f"**Fitness**: {fitness:.4f}\n"
                        f"**Model**: {genome.model}\n"
                        f"**Class**: {genome.agent_class}\n\n"
                        f"### Genome Parameters\n"
                        f"```json\n{json.dumps(genome.to_dict(), indent=2)}\n```\n"
                    ),
                ],
                cwd=worktree_dir,
                capture_output=True,
                text=True,
            )

            pr_url = result.stdout.strip() if result.returncode == 0 else ""

            # Cleanup worktree
            subprocess.run(
                ["git", "worktree", "remove", worktree_dir, "--force"],
                cwd=str(self.project_root),
                capture_output=True,
            )

            return pr_url

        except Exception:
            logger.exception("Failed to create deployment PR")
            return ""

    async def _get_approved_promotions(self) -> list[dict]:
        """Get all approved-but-not-deployed promotions."""
        if not hasattr(self.storage, "pool"):
            return []

        try:
            async with self.storage.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT * FROM promotion_queue
                    WHERE status = 'approved'
                    ORDER BY fitness DESC
                    """
                )
                return [dict(r) for r in rows]
        except Exception:
            return []

    async def _mark_deployed(self, promotion_id: int, new_agent_id: str) -> None:
        """Mark a promotion as deployed."""
        if not hasattr(self.storage, "pool"):
            return

        try:
            async with self.storage.pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE promotion_queue
                    SET status = 'deployed',
                        deploy_config = $1,
                        reviewed_at = NOW()
                    WHERE id = $2
                    """,
                    json.dumps({"deployed_as": new_agent_id}),
                    promotion_id,
                )
        except Exception:
            logger.exception("Failed to mark promotion as deployed")
