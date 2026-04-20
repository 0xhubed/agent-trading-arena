"""Shared time utilities."""

from __future__ import annotations

from datetime import datetime, timezone


def utc_iso(dt: datetime) -> str:
    """Format datetime as ISO string with Z suffix for JavaScript."""
    return dt.isoformat().replace("+00:00", "Z")


def utc_now_iso() -> str:
    """Return current UTC time as ISO string with Z suffix for JavaScript."""
    return utc_iso(datetime.now(timezone.utc))
