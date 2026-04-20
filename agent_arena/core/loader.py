"""Agent loading utilities."""

import importlib


ALLOWED_AGENT_PREFIXES = [
    "agent_arena.agents.",
    "agent_arena.agents.baselines.",
]


def load_agent(agent_config: dict):
    """Dynamically load an agent from config."""
    class_path = agent_config["class"]

    # Validate class path against allowlist
    if not any(class_path.startswith(prefix) for prefix in ALLOWED_AGENT_PREFIXES):
        raise ValueError(
            f"Agent class '{class_path}' is not in the allowed module list. "
            "Only classes under agent_arena.agents.* are permitted."
        )

    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    agent_class = getattr(module, class_name)

    return agent_class(
        agent_id=agent_config["id"],
        name=agent_config["name"],
        config=agent_config.get("config", {}),
    )
