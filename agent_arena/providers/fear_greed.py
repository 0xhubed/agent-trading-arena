"""Shared Fear & Greed Index service with TTL cache."""

from __future__ import annotations

import logging
import time

import httpx

logger = logging.getLogger(__name__)

FEAR_GREED_URL = "https://api.alternative.me/fng/"
_TTL = 600  # 10 minutes (index updates daily, this is plenty)

# Module-level singleton cache
_cache: dict | None = None
_cache_ts: float = 0


async def get_fear_greed() -> dict | None:
    """Fetch Fear & Greed Index with TTL cache.

    Returns a dict with ``value`` (int 0-100) and ``classification``
    (str like "Fear", "Extreme Greed", etc.), or *None* on failure
    when no cached value is available.
    """
    global _cache, _cache_ts  # noqa: PLW0603

    now = time.monotonic()
    if _cache and (now - _cache_ts) < _TTL:
        return _cache

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(FEAR_GREED_URL, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data.get("data"):
                current = data["data"][0]
                _cache = {
                    "value": int(current["value"]),
                    "classification": current["value_classification"],
                    "timestamp": current["timestamp"],
                }
                _cache_ts = now
                return _cache
    except Exception as e:
        logger.warning("Fear & Greed fetch failed: %s", e)

    return _cache  # stale cache better than nothing
