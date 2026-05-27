"""
Playwright browser pool.

Keeps one persistent Chromium process alive and hands out isolated
BrowserContexts via an asyncio.Semaphore so at most MAX_CONTEXTS
Playwright operations run simultaneously.

Lifecycle (called from app.main lifespan):
    await browser_pool.start()
    ...
    await browser_pool.stop()

Scraper usage:
    from app.core.browser_pool import browser_pool

    async with browser_pool.page() as page:
        await page.goto("https://...")

The pool automatically recycles the Chromium process after MAX_USES
context openings to prevent memory creep.
"""
from __future__ import annotations

import asyncio
import logging
import random
from contextlib import asynccontextmanager
from typing import Optional

logger = logging.getLogger(__name__)

_MAX_CONTEXTS = 4    # simultaneous contexts (each ~150-200 MB RAM)
_MAX_USES     = 80   # recycle Chromium after this many context opens

_LAUNCH_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-blink-features=AutomationControlled",
]

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]


class BrowserPool:
    """Manages one persistent Chromium browser with semaphore-limited contexts."""

    def __init__(self, max_contexts: int = _MAX_CONTEXTS) -> None:
        self._max_contexts = max_contexts
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._lock = asyncio.Lock()
        self._playwright = None
        self._browser = None
        self._use_count = 0
        self.is_started = False

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Launch Chromium and initialise the context semaphore."""
        self._semaphore = asyncio.Semaphore(self._max_contexts)
        await self._launch()
        self.is_started = True
        logger.info("BrowserPool started (max_contexts=%d)", self._max_contexts)

    async def stop(self) -> None:
        """Gracefully close Chromium and Playwright."""
        self.is_started = False
        for obj, name in [(self._browser, "browser"), (self._playwright, "playwright")]:
            if obj is None:
                continue
            try:
                await (obj.close() if hasattr(obj, "close") else obj.stop())
            except Exception as exc:
                logger.debug("BrowserPool stop %s: %s", name, exc)
        self._browser = None
        self._playwright = None
        logger.info("BrowserPool stopped")

    async def _launch(self) -> None:
        """(Re)launch Chromium. Must be called while holding _lock or during init."""
        from playwright.async_api import async_playwright

        # Tear down any existing instance first
        for obj in (self._browser, self._playwright):
            if obj is None:
                continue
            try:
                await (obj.close() if hasattr(obj, "close") else obj.stop())
            except Exception:
                pass

        pw = await async_playwright().start()
        browser = await pw.chromium.launch(headless=True, args=_LAUNCH_ARGS)
        self._playwright = pw
        self._browser = browser
        self._use_count = 0
        logger.info("BrowserPool: Chromium launched")

    async def _ensure_healthy(self) -> None:
        """Restart browser if disconnected or overused. Caller must hold _lock."""
        dead = self._browser is None or not self._browser.is_connected()
        aged = self._use_count >= _MAX_USES
        if dead or aged:
            reason = (
                "disconnected" if dead
                else f"recycled after {self._use_count} uses"
            )
            logger.info("BrowserPool: restarting (%s)", reason)
            await self._launch()

    # ── Context / page acquisition ────────────────────────────────────────

    @asynccontextmanager
    async def context(self):
        """
        Yield an isolated BrowserContext; release the semaphore slot on exit.
        The context is closed automatically — callers should not close it.
        """
        if not self.is_started or self._semaphore is None:
            raise RuntimeError("BrowserPool.start() was not called")

        await self._semaphore.acquire()
        ctx = None
        try:
            async with self._lock:
                await self._ensure_healthy()
                ctx = await self._browser.new_context(
                    locale="pt-BR",
                    timezone_id="America/Sao_Paulo",
                    user_agent=random.choice(_USER_AGENTS),
                    viewport={"width": 1366, "height": 900},
                )
                await ctx.add_init_script(
                    "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
                )
                self._use_count += 1
            yield ctx
        finally:
            if ctx is not None:
                try:
                    await ctx.close()
                except Exception:
                    pass
            self._semaphore.release()

    @asynccontextmanager
    async def page(self):
        """Yield a single Page from a fresh isolated context."""
        async with self.context() as ctx:
            pg = await ctx.new_page()
            try:
                yield pg
            finally:
                try:
                    await pg.close()
                except Exception:
                    pass


# ── Module singleton (imported by scraper and main) ──────────────────────────
browser_pool = BrowserPool(max_contexts=_MAX_CONTEXTS)
