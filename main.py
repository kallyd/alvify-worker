#!/usr/bin/env python3
"""
Alvify Remote Worker — standalone scraping process.

This worker registers itself with the Alvify API, long-polls for jobs,
runs the Playwright-based scraper locally, and reports leads + progress
back through the API. No direct database or Redis access is required.

Environment variables (see .env.example):
  API_URL            Base URL of the Alvify API  (e.g. http://api.example.com/api)
  WORKER_ID          UUID assigned to this worker in the admin panel
  WORKER_API_KEY     API key generated when the worker was registered
  MAX_CONCURRENCY    Maximum parallel jobs (default 2)
  LOG_LEVEL          Logging level (default INFO)
  VERSION            Worker version string (default 1.0.0)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import platform
import sys
import time
import uuid
from typing import Optional

import aiohttp

# ── Env / config ──────────────────────────────────────────────────────────────

API_URL: str = os.environ.get("API_URL", "http://localhost:18080/api").rstrip("/")
WORKER_ID: str = os.environ.get("WORKER_ID", "")
WORKER_API_KEY: str = os.environ.get("WORKER_API_KEY", "")
MAX_CONCURRENCY: int = int(os.environ.get("MAX_CONCURRENCY", "2"))
LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO").upper()
VERSION: str = os.environ.get("VERSION", "1.0.0")

POLL_URL = f"{API_URL}/internal/workers/jobs/poll"
HEARTBEAT_URL = f"{API_URL}/internal/workers/heartbeat"
HEALTH_BIND_PORT = int(os.environ.get("HEALTH_PORT", "8001"))
CACHE_REFRESH_QUEUE = "alvify:queue:cache_refresh"
REDIS_URL: str = os.environ.get("REDIS_URL", "")

# Add this file's directory to sys.path so 'core' package is always importable
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("alvify.worker")


def _worker_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {WORKER_API_KEY}",
        "X-Worker-ID": WORKER_ID,
        "Content-Type": "application/json",
        "User-Agent": f"alvify-worker/{VERSION}",
    }


def _resource_usage() -> tuple[float, float]:
    """Return (cpu_percent, ram_percent) using psutil if available."""
    try:
        import psutil
        cpu = psutil.cpu_percent(interval=None)
        ram = psutil.virtual_memory().percent
        return cpu, ram
    except Exception:
        return 0.0, 0.0


# ── Health endpoint (tiny HTTP server) ────────────────────────────────────────

async def _health_server() -> None:
    """Minimal HTTP server so the API's /workers/{id}/test can ping us."""
    from aiohttp import web

    async def _health(_: web.Request) -> web.Response:
        return web.Response(text='{"ok":true}', content_type="application/json")

    web_app = web.Application()
    web_app.router.add_get("/health", _health)
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HEALTH_BIND_PORT)
    await site.start()
    logger.info("Health endpoint: http://0.0.0.0:%d/health", HEALTH_BIND_PORT)
    # Run forever
    while True:
        await asyncio.sleep(3600)


# ── Heartbeat ─────────────────────────────────────────────────────────────────

_active_jobs = 0


async def _heartbeat_loop(session: aiohttp.ClientSession) -> None:
    while True:
        await asyncio.sleep(15)
        cpu, ram = _resource_usage()
        payload = {
            "cpu_usage": cpu,
            "ram_usage": ram,
            "active_jobs": _active_jobs,
            "version": VERSION,
        }
        try:
            async with session.post(
                HEARTBEAT_URL,
                headers=_worker_headers(),
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status not in (200, 204):
                    logger.warning("Heartbeat responded %d", resp.status)
        except Exception as exc:
            logger.warning("Heartbeat failed: %s", exc)


# ── Scraping ──────────────────────────────────────────────────────────────────

async def _process_job(session: aiohttp.ClientSession, job: dict) -> None:
    global _active_jobs

    job_id = job["job_id"]
    search_id = job.get("search_id")
    keyword = job.get("keyword", "")
    city = job.get("city", "")
    state = job.get("state", "")
    country = job.get("country", "BR")
    max_results = int(job.get("max_results") or 20)

    logger.info("Processing job %s — %s %s/%s (max %d)", job_id, keyword, city, state, max_results)
    _active_jobs += 1
    start_ts = time.monotonic()
    leads_sent = 0
    new_leads = 0

    try:
        from core.scraper import scrape_leads
        from core.browser_pool import browser_pool

        if not browser_pool.is_started:
            await browser_pool.start()

        # ── Build progress callback ────────────────────────────────────────
        async def _progress_cb(n: int, lead_dict: dict) -> None:
            nonlocal leads_sent, new_leads
            pct = min(99, int(n / max_results * 100)) if max_results > 0 else 0

            # Submit the lead
            try:
                async with session.post(
                    f"{API_URL}/internal/workers/jobs/{job_id}/lead",
                    headers=_worker_headers(),
                    json=lead_dict,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as r:
                    if r.status == 201:
                        data = await r.json()
                        leads_sent += 1
                        if data.get("is_new"):
                            new_leads += 1
                    else:
                        logger.warning("Lead submission %d failed: %s", r.status, await r.text())
            except Exception as exc:
                logger.warning("Lead submit error: %s", exc)

            # Send progress update
            try:
                async with session.post(
                    f"{API_URL}/internal/workers/jobs/{job_id}/progress",
                    headers=_worker_headers(),
                    json={"pct": pct, "n": n, "msg": f"{n} leads coletados"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as _:
                    pass
            except Exception:
                pass

        # ── Run scraper ───────────────────────────────────────────────────
        # scrape_leads is a regular async function that fires progress_cb
        # for each lead as it is extracted, then returns the full list.
        await scrape_leads(
            keyword=keyword,
            city=city,
            state=state,
            max_results=max_results,
            country=country,
            pool=browser_pool,
            progress_cb=_progress_cb,
        )

        # ── Mark complete ──────────────────────────────────────────────────
        async with session.post(
            f"{API_URL}/internal/workers/jobs/{job_id}/complete",
            headers=_worker_headers(),
            json={"count": leads_sent, "new_count": new_leads},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                logger.warning("Complete endpoint returned %d", resp.status)

        elapsed = time.monotonic() - start_ts
        logger.info("Job %s done — %d leads in %.1fs", job_id, leads_sent, elapsed)

    except Exception as exc:
        logger.error("Job %s failed: %s", job_id, exc, exc_info=True)
        try:
            async with session.post(
                f"{API_URL}/internal/workers/jobs/{job_id}/error",
                headers=_worker_headers(),
                json={"msg": str(exc)[:400]},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as _:
                pass
        except Exception:
            pass
    finally:
        _active_jobs -= 1


# ── Cache refresh (background, no user-visible job) ──────────────────────────

async def _refresh_cache_entry(session: aiohttp.ClientSession, params: dict) -> None:
    """Re-scrape a stale cache entry and POST the updated leads to the API."""
    global _active_jobs

    keyword     = params.get("keyword", "")
    city        = params.get("city", "")
    state       = params.get("state", "") or None
    country     = params.get("country", "BR") or "BR"
    max_results = int(params.get("max_results") or 20)

    if not keyword or not city:
        return

    logger.info("Cache refresh: re-scraping '%s' in %s", keyword, city)
    _active_jobs += 1

    try:
        from core.scraper import scrape_leads
        from core.browser_pool import browser_pool
        from core.result_cache import store_result_cache

        if not browser_pool.is_started:
            await browser_pool.start()

        scraped: list[dict] = []

        async def _cb(n: int, lead: dict) -> None:
            scraped.append(lead)

        await scrape_leads(
            keyword=keyword, city=city, state=state,
            max_results=max_results, country=country,
            progress_cb=_cb, pool=browser_pool,
        )

        if scraped and REDIS_URL:
            import redis.asyncio as aioredis
            _r = aioredis.from_url(REDIS_URL)
            await store_result_cache(
                _r, keyword, city, state or "", country,
                scraped, max_results,
            )
            await _r.aclose()
            logger.info("Cache refresh: updated '%s %s' (%d leads)", keyword, city, len(scraped))

    except Exception as exc:
        logger.warning("Cache refresh failed for '%s %s': %s", keyword, city, exc)
    finally:
        _active_jobs -= 1


# ── Poll loop ─────────────────────────────────────────────────────────────────

async def _poll_loop(session: aiohttp.ClientSession) -> None:
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
    logger.info("Polling for jobs (concurrency=%d)…", MAX_CONCURRENCY)

    while True:
        # Avoid hammering the API if we are at capacity
        if _active_jobs >= MAX_CONCURRENCY:
            await asyncio.sleep(2)
            continue

        # ── Check cache-refresh queue (via Redis directly if REDIS_URL set) ──
        if REDIS_URL:
            try:
                import redis.asyncio as aioredis
                _r = aioredis.from_url(REDIS_URL)
                raw = await _r.lpop(CACHE_REFRESH_QUEUE)
                await _r.aclose()
                if raw:
                    params = json.loads(raw)
                    asyncio.create_task(_refresh_cache_entry(session, params))
            except Exception:
                pass

        try:
            async with session.get(
                POLL_URL,
                headers=_worker_headers(),
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 204:
                    # No job available — loop immediately (server already blocked 25s)
                    continue
                if resp.status != 200:
                    logger.warning("Poll returned %d — backing off 5s", resp.status)
                    await asyncio.sleep(5)
                    continue

                job = await resp.json()
                if not job or not job.get("job_id"):
                    await asyncio.sleep(1)
                    continue

                async def _run(j=job):
                    async with semaphore:
                        await _process_job(session, j)

                asyncio.create_task(_run())

        except asyncio.TimeoutError:
            # Expected for long-poll — just retry
            continue
        except Exception as exc:
            logger.error("Poll error: %s", exc, exc_info=True)
            await asyncio.sleep(10)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    if not WORKER_ID or not WORKER_API_KEY:
        logger.error("WORKER_ID and WORKER_API_KEY must be set")
        sys.exit(1)

    logger.info(
        "Alvify Worker v%s | id=%s | api=%s | concurrency=%d",
        VERSION, WORKER_ID, API_URL, MAX_CONCURRENCY,
    )

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENCY * 4)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Send an initial heartbeat so the API marks us online immediately
        try:
            cpu, ram = _resource_usage()
            async with session.post(
                HEARTBEAT_URL,
                headers=_worker_headers(),
                json={"cpu_usage": cpu, "ram_usage": ram, "active_jobs": 0, "version": VERSION},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    logger.info("Initial heartbeat OK")
                else:
                    logger.warning("Initial heartbeat returned %d — check credentials", resp.status)
        except Exception as exc:
            logger.warning("Initial heartbeat failed: %s — continuing anyway", exc)

        await asyncio.gather(
            _heartbeat_loop(session),
            _poll_loop(session),
            _health_server(),
        )


if __name__ == "__main__":
    asyncio.run(main())
