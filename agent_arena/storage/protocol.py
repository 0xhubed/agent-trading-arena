"""Storage protocol defining the common interface for all backends."""

from __future__ import annotations

from typing import Any, Optional, Protocol, runtime_checkable


@runtime_checkable
class StorageProtocol(Protocol):
    """Common interface shared by SQLite and PostgreSQL backends.

    All storage backends must implement these methods. Optional capabilities
    (backtest, forum, learning, candles) are defined as separate protocols.
    """

    async def initialize(self) -> None: ...

    async def close(self) -> None: ...

    # --- Decisions ---
    async def save_decision(self, **kwargs: Any) -> None: ...

    async def get_recent_decisions(
        self, limit: int = 50, agent_id: Optional[str] = None,
    ) -> list[dict]: ...

    async def get_all_decisions(
        self, agent_id: Optional[str] = None,
    ) -> list[dict]: ...

    # --- Trades ---
    async def save_trade(self, **kwargs: Any) -> None: ...

    async def get_agent_trades(
        self, agent_id: str, limit: int = 100,
    ) -> list[dict]: ...

    async def get_all_trades(
        self, agent_id: Optional[str] = None,
    ) -> list[dict]: ...

    async def get_agent_trade_count(self, agent_id: str) -> int: ...

    # --- Snapshots ---
    async def save_snapshot(self, **kwargs: Any) -> None: ...

    async def get_leaderboard_history(
        self, limit: int = 100,
    ) -> list[dict]: ...

    # --- Funding ---
    async def save_funding_payment(self, **kwargs: Any) -> None: ...

    async def get_funding_history(
        self, limit: int = 100, agent_id: Optional[str] = None,
    ) -> list[dict]: ...

    async def get_agent_funding_summary(
        self, agent_id: str,
    ) -> dict: ...

    # --- Liquidations ---
    async def save_liquidation(self, **kwargs: Any) -> None: ...

    async def get_liquidation_history(
        self, limit: int = 100, agent_id: Optional[str] = None,
    ) -> list[dict]: ...

    async def get_agent_liquidation_count(
        self, agent_id: str,
    ) -> int: ...

    # --- SL/TP ---
    async def save_sl_tp_trigger(self, **kwargs: Any) -> None: ...

    # --- Behavioral ---
    async def get_agent_behavioral_stats(
        self, agent_id: str,
    ) -> dict: ...

    async def get_all_agent_ids(self) -> list[str]: ...

    # --- Bias / Contagion ---
    async def save_bias_profile(self, **kwargs: Any) -> None: ...

    async def get_bias_profiles(self, **kwargs: Any) -> list[dict]: ...

    async def get_bias_history(
        self, agent_id: str, **kwargs: Any,
    ) -> list[dict]: ...

    async def save_contagion_snapshot(
        self, **kwargs: Any,
    ) -> None: ...

    async def get_contagion_snapshots(
        self, **kwargs: Any,
    ) -> list[dict]: ...

    async def get_contagion_latest(self) -> Optional[dict]: ...


@runtime_checkable
class BacktestStorageProtocol(Protocol):
    """Optional protocol for backends that support backtest data."""

    async def save_candles(self, **kwargs: Any) -> None: ...

    async def get_candles(self, **kwargs: Any) -> list[dict]: ...

    async def get_data_status(self) -> dict: ...

    async def create_backtest_run(self, **kwargs: Any) -> None: ...

    async def update_backtest_run(self, **kwargs: Any) -> None: ...

    async def get_backtest_run(self, run_id: str) -> Optional[dict]: ...

    async def get_backtest_runs(self, **kwargs: Any) -> list[dict]: ...


@runtime_checkable
class ForumStorageProtocol(Protocol):
    """Optional protocol for backends that support forum data."""

    async def save_forum_message(self, **kwargs: Any) -> None: ...

    async def get_forum_messages(self, **kwargs: Any) -> list[dict]: ...

    async def save_witness_summary(self, **kwargs: Any) -> None: ...

    async def get_witness_summaries(self, **kwargs: Any) -> list[dict]: ...


@runtime_checkable
class StatePersistenceProtocol(Protocol):
    """Optional protocol for backends that support arena state."""

    async def save_arena_state(self, **kwargs: Any) -> None: ...

    async def load_arena_state(self) -> Optional[dict]: ...

    async def has_saved_state(self) -> bool: ...
