"""
Local in-memory deduplication for a single scraping job.

Prevents the worker from sending duplicate leads to the backend,
saving HTTP round-trips and unnecessary DB upserts.

Dedup keys (all checked independently):
  1. (normalized_name, normalized_city) — same business, same location
  2. normalized_phone                   — same business with a different name

Normalisation rules:
  - Strip whitespace, lowercase, remove diacritics
  - Phones: digits only, strip country-code prefix

Also includes GlobalDedup — a Redis-backed cross-job deduplication layer
with a 30-day TTL. Used to avoid re-submitting leads that were already
sent in a different job (different keyword, different schedule).

Usage::

    from core.dedup import LocalDedup, GlobalDedup

    dedup = LocalDedup()
    global_dedup = GlobalDedup(redis_client)  # or None if Redis unavailable

    for lead in scraped:
        if global_dedup and await global_dedup.is_duplicate(lead):
            continue            # already sent in a previous job
        if dedup.is_duplicate(lead):
            continue            # already seen in this job
        dedup.register(lead)
        if global_dedup:
            await global_dedup.register(lead)
        await submit(lead)
"""
from __future__ import annotations

import hashlib
import logging
import re
import unicodedata
from typing import Optional

logger = logging.getLogger("alvify.dedup")


def _normalize_text(s: Optional[str]) -> str:
    """Lowercase, strip whitespace, remove diacritical marks."""
    if not s:
        return ""
    s = unicodedata.normalize("NFD", s.lower().strip())
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


def _normalize_phone(phone: Optional[str]) -> str:
    """
    Extract digits only, strip leading country code (+55 or 55 for Brazil).
    Returns empty string if phone is falsy.
    """
    if not phone:
        return ""
    digits = re.sub(r"\D", "", phone)
    # Strip Brazilian country code if present and number is long enough
    if len(digits) >= 12 and digits.startswith("55"):
        digits = digits[2:]
    return digits


class LocalDedup:
    """
    Stateful per-job deduplication cache.

    Instantiate once per job and call `is_duplicate()` + `register()` for
    every lead before submitting to the API.  The instance can safely be
    reused across the progress_cb calls within the same job because it's
    single-threaded (asyncio).
    """

    def __init__(self) -> None:
        # Primary key: (normalized_name, normalized_city)
        self._name_city: set[tuple[str, str]] = set()
        # Secondary key: non-empty normalized phone
        self._phones: set[str] = set()

    # ── Public interface ──────────────────────────────────────────────────────

    def is_duplicate(self, lead: dict) -> bool:
        """Return True if this lead has already been seen in the current job."""
        name_city = self._name_city_key(lead)
        if name_city in self._name_city:
            return True

        phone = _normalize_phone(lead.get("phone"))
        if phone and phone in self._phones:
            return True

        return False

    def register(self, lead: dict) -> None:
        """Mark a lead as seen so future duplicates are detected."""
        self._name_city.add(self._name_city_key(lead))
        phone = _normalize_phone(lead.get("phone"))
        if phone:
            self._phones.add(phone)

    def check_and_register(self, lead: dict) -> bool:
        """
        Convenience method: returns True (skip) if duplicate, otherwise
        registers and returns False (proceed).
        """
        if self.is_duplicate(lead):
            return True
        self.register(lead)
        return False

    @property
    def seen_count(self) -> int:
        """Number of unique leads registered so far."""
        return len(self._name_city)

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _name_city_key(lead: dict) -> tuple[str, str]:
        return (
            _normalize_text(lead.get("name")),
            _normalize_text(lead.get("city")),
        )


# ── Global cross-job deduplication (Redis-backed) ────────────────────────────

class GlobalDedup:
    """
    Cross-job deduplication backed by Redis with a 30-day TTL.

    Prevents re-submitting leads that were already sent in a different job
    (e.g., same business found via different keywords or scheduled re-runs).

    Gracefully handles Redis failures — returns False (not duplicate) on errors
    so the pipeline continues without blocking.
    """

    KEY_PREFIX = "alvify:dedup:"
    TTL = 30 * 86400  # 30 days

    def __init__(self, redis_client) -> None:
        self._redis = redis_client

    @staticmethod
    def _hash_lead(lead: dict) -> str:
        """Compute a deterministic hash for dedup lookup."""
        name = _normalize_text(lead.get("name"))
        city = _normalize_text(lead.get("city"))
        phone = _normalize_phone(lead.get("phone"))
        raw = f"{name}|{city}|{phone}"
        return hashlib.sha1(raw.encode()).hexdigest()[:16]

    def _key(self, lead: dict) -> str:
        return f"{self.KEY_PREFIX}{self._hash_lead(lead)}"

    async def is_duplicate(self, lead: dict) -> bool:
        """Return True if this lead was already submitted in a previous job."""
        try:
            return bool(await self._redis.exists(self._key(lead)))
        except Exception as exc:
            logger.debug("global_dedup check failed: %s", exc)
            return False

    async def register(self, lead: dict) -> None:
        """Mark a lead as submitted (set with TTL)."""
        try:
            await self._redis.setex(self._key(lead), self.TTL, "1")
        except Exception as exc:
            logger.debug("global_dedup register failed: %s", exc)

    async def check_and_register(self, lead: dict) -> bool:
        """
        Returns True (skip) if duplicate globally, otherwise registers
        and returns False (proceed).
        """
        if await self.is_duplicate(lead):
            return True
        await self.register(lead)
        return False
