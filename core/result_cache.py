"""
Search result cache — stores scraped leads by (keyword, city, state, country).

When the same search is repeated, results are served instantly from cache
instead of launching a new Playwright scrape.

Redis keys
----------
alvify:rc:{hash}          → JSON list[lead_dict]            TTL = RESULT_CACHE_TTL
alvify:rc:meta:{hash}     → JSON metadata dict              TTL = RESULT_CACHE_TTL
alvify:rc:idx             → Sorted Set {hash: epoch_ts}     no TTL (pruned lazily)

The Sorted Set score is the Unix timestamp of the last update. The refresh
task queries entries older than CACHE_REFRESH_INTERVAL to re-scrape them.
"""
from __future__ import annotations

import hashlib
import json
import time
import unicodedata
from datetime import datetime, timezone
from typing import Optional

RC_PREFIX = "alvify:rc"
RC_IDX_KEY = "alvify:rc:idx"


# ── Key helpers ───────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    """Lowercase, strip accents."""
    s = unicodedata.normalize("NFD", (s or "").lower().strip())
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


def result_cache_hash(keyword: str, city: str, state: str = "", country: str = "BR") -> str:
    """Stable 16-char hex key for a (keyword, city, state, country) tuple."""
    raw = f"{_norm(keyword)}|{_norm(city)}|{_norm(state)}|{_norm(country)}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def _rc_key(h: str) -> str:
    return f"{RC_PREFIX}:{h}"


def _meta_key(h: str) -> str:
    return f"{RC_PREFIX}:meta:{h}"


# ── Public API ────────────────────────────────────────────────────────────────

async def get_cached_results(
    redis,
    keyword: str,
    city: str,
    state: str = "",
    country: str = "BR",
) -> Optional[list[dict]]:
    """Return cached leads, or None on miss."""
    h = result_cache_hash(keyword, city, state, country)
    raw = await redis.get(_rc_key(h))
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


async def get_cache_meta(
    redis,
    keyword: str,
    city: str,
    state: str = "",
    country: str = "BR",
) -> Optional[dict]:
    """Return metadata for a cached entry (cached_at, count, etc.)."""
    h = result_cache_hash(keyword, city, state, country)
    raw = await redis.get(_meta_key(h))
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


async def store_result_cache(
    redis,
    keyword: str,
    city: str,
    state: str,
    country: str,
    leads: list[dict],
    max_results: int = 20,
    ttl: int = 604800,      # 7 days default; overridden by settings.result_cache_ttl
) -> None:
    """Persist leads + metadata and register in the refresh index."""
    if not leads:
        return

    h = result_cache_hash(keyword, city, state, country)
    now_ts = time.time()

    await redis.set(_rc_key(h), json.dumps(leads), ex=ttl)

    meta = {
        "hash": h,
        "keyword": keyword,
        "city": city,
        "state": state,
        "country": country,
        "max_results": max_results,
        "count": len(leads),
        "cached_at": datetime.now(timezone.utc).isoformat(),
        "ttl": ttl,
    }
    await redis.set(_meta_key(h), json.dumps(meta), ex=ttl)

    # Update sorted-set index so the refresh task can find this entry by age
    await redis.zadd(RC_IDX_KEY, {h: now_ts})


async def get_stale_entries(redis, older_than_seconds: int) -> list[dict]:
    """
    Return metadata for entries that haven't been refreshed in older_than_seconds.
    Capped at 50 entries per call to avoid burst re-scraping.
    """
    cutoff = time.time() - older_than_seconds
    hashes = await redis.zrangebyscore(RC_IDX_KEY, "-inf", cutoff, start=0, num=50)
    entries = []
    for h in (hashes or []):
        if isinstance(h, bytes):
            h = h.decode()
        raw = await redis.get(_meta_key(h))
        if raw:
            try:
                entries.append(json.loads(raw))
            except Exception:
                pass
    return entries


async def prune_stale_index(redis) -> int:
    """Remove index entries whose lead keys have already expired in Redis."""
    all_hashes = await redis.zrange(RC_IDX_KEY, 0, -1)
    expired = []
    for h in (all_hashes or []):
        if isinstance(h, bytes):
            h = h.decode()
        if not await redis.exists(_rc_key(h)):
            expired.append(h)
    if expired:
        await redis.zrem(RC_IDX_KEY, *expired)
    return len(expired)
