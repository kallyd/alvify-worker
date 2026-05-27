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

Usage::

    from core.dedup import LocalDedup

    dedup = LocalDedup()

    for lead in scraped:
        if dedup.is_duplicate(lead):
            continue            # already seen — skip submission
        dedup.register(lead)    # mark as seen
        await submit(lead)
"""
from __future__ import annotations

import re
import unicodedata
from typing import Optional


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
