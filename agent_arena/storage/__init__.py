"""Storage module with backend selection."""

from __future__ import annotations

import os
from typing import Union, TYPE_CHECKING

from .sqlite import SQLiteStorage
from .archive import ArchiveService

if TYPE_CHECKING:
    from .postgres import PostgresStorage


def get_storage(
    backend: str = None,
    **kwargs,
) -> Union[SQLiteStorage, "PostgresStorage"]:
    """
    Get storage backend based on configuration.

    Args:
        backend: 'sqlite' or 'postgres'. Defaults to DATABASE_BACKEND env var or 'sqlite'.
        **kwargs: Additional arguments passed to storage constructor.

    Returns:
        Storage instance (SQLiteStorage or PostgresStorage).

    Example:
        # Default SQLite
        storage = get_storage()

        # Explicit Postgres
        storage = get_storage("postgres")

        # From environment
        # DATABASE_BACKEND=postgres
        # DATABASE_URL=postgresql://user:pass@host:5432/db
        storage = get_storage()
    """
    backend = backend or os.getenv("DATABASE_BACKEND", "sqlite")

    if backend == "postgres":
        # Lazy import to avoid dependency if not used
        from .postgres import PostgresStorage

        connection_string = kwargs.get("connection_string") or os.getenv("DATABASE_URL")
        if not connection_string:
            raise ValueError(
                "DATABASE_URL environment variable required for postgres backend. "
                "Set DATABASE_URL=postgresql://user:pass@host:5432/dbname"
            )
        return PostgresStorage(connection_string)

    # Default to SQLite
    db_path = kwargs.get("db_path", "data/arena.db")
    return SQLiteStorage(db_path)


__all__ = ["SQLiteStorage", "ArchiveService", "get_storage"]
