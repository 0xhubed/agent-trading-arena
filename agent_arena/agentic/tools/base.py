"""Base interface for trading tools."""

from abc import ABC, abstractmethod
from typing import Any

from langchain_core.tools import BaseTool
from pydantic import PrivateAttr


class TradingTool(BaseTool, ABC):
    """
    Base class for all trading tools.

    Integrates with LangChain's tool system while providing
    trading-specific functionality like market context access.
    """

    # Tool metadata (required by LangChain)
    name: str
    description: str

    # Per-instance context (set before each decision)
    _context: dict = PrivateAttr(default_factory=dict)
    _storage: Any = PrivateAttr(default=None)

    class Config:
        arbitrary_types_allowed = True

    def set_context(self, context: dict) -> None:
        """Set the current trading context (market data, portfolio, etc.)."""
        self._context = context

    def set_storage(self, storage: Any) -> None:
        """Set storage for database access."""
        self._storage = storage

    @abstractmethod
    def _run(self, **kwargs) -> str:
        """Synchronous execution - required by LangChain."""
        pass

    async def _arun(self, **kwargs) -> str:
        """Async execution - defaults to sync."""
        return self._run(**kwargs)
