"""Base data provider interface."""

from __future__ import annotations

from abc import ABC, abstractmethod


class DataProvider(ABC):
    """
    Base class for any data source.
    Providers fetch external data that gets merged into context.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider identifier for config."""
        pass

    @abstractmethod
    async def get_data(self, symbols: list[str]) -> dict:
        """
        Fetch data for the given symbols.

        Returns:
            Dict to merge into agent context
        """
        pass

    async def start(self) -> None:
        """Initialize connections, caches, etc."""
        pass

    async def stop(self) -> None:
        """Cleanup resources."""
        pass
