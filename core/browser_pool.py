"""
Playwright browser pool — persistent page slots for maximum reuse.

Architecture
------------
Instead of creating and destroying a BrowserContext+Page for every place
URL (the previous approach), this module keeps a fixed pool of persistent
(BrowserContext, Page) *slots*.

Each slot stays alive across many page navigations:

  acquire slot → goto(place_url) → extract → release slot → goto("about:blank")

This eliminates ~200-400 ms of context creation overhead per extraction.
Cookies / session state accumulated within a context (e.g. "accept cookies"
clicks on Google Maps) are preserved, further reducing friction.

Slot recycling
--------------
To prevent gradual memory creep each slot is recycled (context closed +
new context opened) after SLOT_MAX_USES navigations.  The Chromium process
itself is recycled every BROWSER_MAX_USES slot recyclings.

Pool sizing
-----------
Default: 4 concurrent slots (configurable via env MAX_BROWSER_SLOTS).
Each slot ≈ 150-250 MB RAM.  Four slots ≈ 600 MB–1 GB on a typical VPS.

Lifecycle (called from worker main):
    await browser_pool.start()
    ...
    await browser_pool.stop()

Scraper usage:
    from core.browser_pool import browser_pool

    async with browser_pool.page() as page:
        await page.goto("https://maps.google.com/...")
        # page is a real Playwright Page, fully functional

Thread safety: pure asyncio — no OS threads.
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ── Tunable constants ─────────────────────────────────────────────────────────

_MAX_SLOTS: int = int(os.environ.get("MAX_BROWSER_SLOTS", "4"))
_SLOT_MAX_USES: int = 40   # recycle a slot's context after this many navigations
_BROWSER_MAX_USES: int = 160  # restart Chromium after this many total slot uses
_RESET_TIMEOUT_MS: int = 4_000  # max ms for about:blank reset navigation

_LAUNCH_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-blink-features=AutomationControlled",
    # Reduce unnecessary background activity
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-renderer-backgrounding",
    "--disable-sync",
    "--no-first-run",
    "--no-default-browser-check",
]

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

_INIT_SCRIPT = (
    "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
    "window.chrome={runtime:{}};"
)


# ── Slot ──────────────────────────────────────────────────────────────────────

@dataclass
class _Slot:
    """One (context, page) pair managed by the pool."""
    ctx: Any       # playwright BrowserContext
    page: Any      # playwright Page
    uses: int = 0  # navigation count since last recycling
    slot_id: int = 0

    def is_healthy(self) -> bool:
        """Quick, sync check — does not hit the browser process."""
        if self.page is None or self.ctx is None:
            return False
        try:
            return not self.page.is_closed()
        except Exception:
            return False

    def needs_recycle(self) -> bool:
        return self.uses >= _SLOT_MAX_USES


# ── helpers ───────────────────────────────────────────────────────────────────

async def _close_slot(slot: _Slot) -> None:
    """Silently close a slot's page and context."""
    for obj, name in [(slot.page, "page"), (slot.ctx, "context")]:
        if obj is None:
            continue
        try:
            await obj.close()
        except Exception as exc:
            logger.debug(
                "BrowserPool: error closing %s on slot %d: %s",
                name, slot.slot_id, exc,
            )


# ── BrowserPool ───────────────────────────────────────────────────────────────

class BrowserPool:
    """
    Pool of persistent Playwright (BrowserContext, Page) slots.

    Public interface (unchanged from previous version)::

        async with browser_pool.page() as page:
            await page.goto("...")
    """

    def __init__(self, max_slots: int = _MAX_SLOTS) -> None:
        self._max_slots = max_slots
        self._playwright: Any = None
        self._browser: Any = None
        # Total slot-recycles since last browser restart; used to age the browser.
        self._browser_uses: int = 0
        self._browser_lock = asyncio.Lock()
        self._available: Optional[asyncio.Queue] = None
        self.is_started = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Launch Chromium and pre-allocate page slots.

        Creates 1 slot immediately (so the first job can start fast),
        then spawns remaining slots in background.
        """
        self._available = asyncio.Queue(maxsize=self._max_slots)
        await self._launch_browser()
        # Create first slot immediately — unblocks the first job
        slot = await self._new_slot(0)
        await self._available.put(slot)
        self.is_started = True
        logger.info(
            "BrowserPool started — 1 slot ready, warming %d more in background",
            self._max_slots - 1,
        )
        # Warm remaining slots in background (non-blocking)
        if self._max_slots > 1:
            asyncio.create_task(self._warm_remaining_slots())

    async def _warm_remaining_slots(self) -> None:
        """Create remaining browser slots in background after start()."""
        for i in range(1, self._max_slots):
            try:
                slot = await self._new_slot(i)
                await self._available.put(slot)
            except Exception as exc:
                logger.warning("BrowserPool: failed to warm slot %d: %s", i, exc)
        logger.info("BrowserPool: all %d slots ready", self._max_slots)

    async def stop(self) -> None:
        """Drain the pool and close Chromium."""
        self.is_started = False
        if self._available:
            while not self._available.empty():
                try:
                    slot = self._available.get_nowait()
                    await _close_slot(slot)
                except asyncio.QueueEmpty:
                    break
        await self._close_browser()
        logger.info("BrowserPool stopped")

    # ── Browser-level operations ──────────────────────────────────────────────

    async def _launch_browser(self) -> None:
        """(Re)start the Chromium process."""
        from playwright.async_api import async_playwright

        await self._close_browser()
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(headless=True, args=_LAUNCH_ARGS)
        self._playwright = pw
        self._browser = browser
        self._browser_uses = 0
        logger.info("BrowserPool: Chromium launched")

    async def _close_browser(self) -> None:
        for obj, name in [
            (self._browser, "browser"),
            (self._playwright, "playwright"),
        ]:
            if obj is None:
                continue
            try:
                await (obj.close() if hasattr(obj, "close") else obj.stop())
            except Exception as exc:
                logger.debug("BrowserPool close %s: %s", name, exc)
        self._browser = None
        self._playwright = None

    async def _ensure_browser_healthy(self) -> None:
        """Restart the browser if it has died or been over-used. Caller holds _browser_lock."""
        dead = self._browser is None or not self._browser.is_connected()
        aged = self._browser_uses >= _BROWSER_MAX_USES
        if dead or aged:
            reason = "disconnected" if dead else f"recycled after {self._browser_uses} slot uses"
            logger.info("BrowserPool: restarting browser (%s)", reason)
            await self._launch_browser()

    # ── Slot-level operations ─────────────────────────────────────────────────

    async def _new_slot(self, slot_id: int = 0) -> _Slot:
        """Create a fresh (context, page) pair. Browser must already be live."""
        ctx = await self._browser.new_context(
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
            user_agent=random.choice(_USER_AGENTS),
            viewport={"width": 1366, "height": 900},
        )
        await ctx.add_init_script(_INIT_SCRIPT)
        page = await ctx.new_page()

        _BLOCKED_TYPES = {"image", "media", "font"}

        async def _block_heavy(route):
            if route.request.resource_type in _BLOCKED_TYPES:
                await route.abort()
            else:
                await route.continue_()

        await page.route("**/*", _block_heavy)

        self._browser_uses += 1
        return _Slot(ctx=ctx, page=page, uses=0, slot_id=slot_id)

    async def _recycle_slot(self, old_slot: _Slot) -> _Slot:
        """Replace an aged slot with a fresh context+page."""
        logger.debug(
            "BrowserPool: recycling slot %d after %d uses",
            old_slot.slot_id, old_slot.uses,
        )
        await _close_slot(old_slot)
        async with self._browser_lock:
            await self._ensure_browser_healthy()
            return await self._new_slot(old_slot.slot_id)

    async def _recover_slot(self, bad_slot: _Slot) -> _Slot:
        """Replace a crashed slot."""
        logger.warning(
            "BrowserPool: recovering crashed slot %d", bad_slot.slot_id
        )
        await _close_slot(bad_slot)
        async with self._browser_lock:
            await self._ensure_browser_healthy()
            return await self._new_slot(bad_slot.slot_id)

    async def _return_slot(self, slot: _Slot) -> None:
        """
        Reset page to about:blank and return the slot to the pool.
        Creates a replacement slot on failure so pool never shrinks.
        """
        try:
            if not slot.page.is_closed():
                await slot.page.goto(
                    "about:blank",
                    wait_until="commit",
                    timeout=_RESET_TIMEOUT_MS,
                )
        except Exception as exc:
            logger.debug(
                "BrowserPool: blank reset failed on slot %d (%s) — recovering",
                slot.slot_id, exc,
            )
            slot = await self._recover_slot(slot)

        if self._available:
            await self._available.put(slot)

    # ── Public interface ──────────────────────────────────────────────────────

    @asynccontextmanager
    async def page(self):
        """
        Acquire a persistent Page from the pool.

        The page is reset to about:blank and returned to the pool when the
        context manager exits, even on exception.
        """
        if not self.is_started or self._available is None:
            raise RuntimeError("BrowserPool.start() was not called")

        slot: _Slot = await self._available.get()

        if not slot.is_healthy():
            slot = await self._recover_slot(slot)
        if slot.needs_recycle():
            slot = await self._recycle_slot(slot)

        slot.uses += 1

        try:
            yield slot.page
        finally:
            await self._return_slot(slot)

    @asynccontextmanager
    async def context(self):
        """
        Backwards-compatibility shim — yields the slot's BrowserContext.

        Do NOT call ctx.close() from the caller; the pool manages lifetime.
        Prefer using pool.page() for new code.
        """
        if not self.is_started or self._available is None:
            raise RuntimeError("BrowserPool.start() was not called")

        slot: _Slot = await self._available.get()

        if not slot.is_healthy():
            slot = await self._recover_slot(slot)
        if slot.needs_recycle():
            slot = await self._recycle_slot(slot)

        slot.uses += 1

        try:
            yield slot.ctx
        finally:
            await self._return_slot(slot)


# ── Module singleton (imported by scraper and main) ──────────────────────────
browser_pool = BrowserPool(max_slots=_MAX_SLOTS)
