"""
Per-place Redis cache — caches extracted lead data by Google Maps place URL.

Why this is different from result_cache.py
------------------------------------------
result_cache.py caches the *full result list* for a (keyword, city) search.
place_cache.py caches a single *place extraction* keyed by the place URL.

Use case: the same business may appear in multiple searches
(e.g. "psicólogo" and "psicóloga" in the same city).  With place-level
caching the second search reuses the already-extracted payload and skips
the ~800 ms Playwright navigation entirely.

Key:   alvify:place:{sha1(normalised_url)[:16]}
Value: JSON-serialised lead dict + metadata
TTL:   7 days (default) — configurable via PLACE_CACHE_TTL env var

Usage in scraper::

    from core.place_cache import PlaceCache

    cache = PlaceCache(redis_client)   # one per worker process

    async def _extract_with_cache(url: str, pool) -> dict | None:
        if cached := await cache.get(url):
            return cached          # cache HIT — no Playwright needed
        lead = await _extract_place(url, pool)
        if lead:
            await cache.set(url, lead)
        return lead
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import unicodedata
from typing import Optional

logger = logging.getLogger(__name__)

_KEY_PREFIX = "alvify:place"
_DEFAULT_TTL = int(os.environ.get("PLACE_CACHE_TTL", str(7 * 24 * 3600)))  # 7 days
_ENABLED = os.environ.get("PLACE_CACHE_ENABLED", "true").lower() not in ("0", "false", "no")


def _url_key(url: str) -> str:
    """Return a stable Redis key for a given place URL."""
    normalised = url.strip().lower()
    h = hashlib.sha1(normalised.encode()).hexdigest()[:16]
    return f"{_KEY_PREFIX}:{h}"


class PlaceCache:
    """
    Redis-backed cache for individual place extraction results.

    Thread-safe for asyncio (all operations are awaitable and stateless
    except for the redis client reference).

    Args:
        redis: An aioredis client instance.  Pass None to operate in
               no-op mode (all gets return None, sets are silently skipped).
        ttl:   TTL in seconds (default: PLACE_CACHE_TTL env var or 7 days).
        enabled: If False, all operations are no-ops.  Useful for testing.
    """

    def __init__(self, redis=None, ttl: int = _DEFAULT_TTL, enabled: bool = _ENABLED) -> None:
        self._redis = redis
        self._ttl = ttl
        self._enabled = enabled and redis is not None

    # ── Public API ────────────────────────────────────────────────────────────

    async def get(self, url: str) -> Optional[dict]:
        """
        Return the cached lead dict for *url*, or None on miss/error.
        Never raises — cache errors are logged and swallowed.
        """
        if not self._enabled:
            return None
        key = _url_key(url)
        try:
            raw = await self._redis.get(key)
            if raw is None:
                return None
            data = json.loads(raw)
            logger.debug("place_cache HIT  %s", key)
            return data.get("lead")
        except Exception as exc:
            logger.debug("place_cache get error (%s): %s", key, exc)
            return None

    async def set(self, url: str, lead: dict) -> None:
        """
        Store a lead dict for *url* with the configured TTL.
        Never raises.
        """
        if not self._enabled:
            return
        key = _url_key(url)
        try:
            payload = json.dumps(
                {"lead": lead, "url": url, "cached_at": time.time()},
                default=str,
            )
            await self._redis.setex(key, self._ttl, payload)
            logger.debug("place_cache SET  %s  ttl=%ds", key, self._ttl)
        except Exception as exc:
            logger.debug("place_cache set error (%s): %s", key, exc)

    async def delete(self, url: str) -> None:
        """Invalidate the cached entry for *url*. Never raises."""
        if not self._enabled:
            return
        try:
            await self._redis.delete(_url_key(url))
        except Exception:
            pass

    @property
    def is_enabled(self) -> bool:
        return self._enabled
