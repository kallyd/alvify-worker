#!/usr/bin/env python3
"""
Alvify Remote Worker — standalone scraping process.

This worker registers itself with the Alvify API, long-polls for jobs,
runs the Playwright-based scraper locally, and reports leads + progress
back through the API. No direct database or Redis access is required
(Redis is optional — used only for place cache and result cache refresh).

Performance improvements in this version
-----------------------------------------
1. Batch lead submission  — buffers up to BATCH_SIZE leads and flushes
   every BATCH_TIMEOUT seconds or when the buffer is full.  Reduces HTTP
   round-trips from N → ceil(N/50).

2. Local deduplication    — drops duplicate leads (same name+city or phone)
   before they hit the batch buffer, saving redundant API calls.

3. Place-level Redis cache — optional; caches each extracted place for 7
   days so repeated searches reuse payloads without Playwright.

4. Structured metrics      — logs scraped/sent/new/deduped/cache-hits after
   each job.

5. Abstracted queue client — the poll mechanism is wrapped behind
   QueueClient so it can be swapped to Redis Streams later without
   changing this file.

6. Granular UX messages    — progress events carry human-readable stage
   labels ("Encontrando empresas", "Extraindo contatos", etc.) so the
   frontend can show meaningful status to the user.

Environment variables
---------------------
  API_URL            Base URL of the Alvify API
  WORKER_ID          UUID assigned to this worker
  WORKER_API_KEY     API key for this worker
  MAX_CONCURRENCY    Max parallel jobs (default 2)
  LOG_LEVEL          INFO / DEBUG / WARNING (default INFO)
  VERSION            Version string (default 1.0.0)
  REDIS_URL          Optional; enables place cache + cache refresh
  PLACE_CACHE_ENABLED  0/false to disable place cache (default: enabled)
  BATCH_SIZE         Leads per batch flush (default 50)
  BATCH_TIMEOUT      Seconds before auto-flush (default 3)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
import uuid
from typing import Optional

import aiohttp

# ── Env / config ──────────────────────────────────────────────────────────────

API_URL:        str = os.environ.get("API_URL", "http://localhost:18080/api").rstrip("/")
WORKER_ID:      str = os.environ.get("WORKER_ID", "")
WORKER_API_KEY: str = os.environ.get("WORKER_API_KEY", "")
MAX_CONCURRENCY: int = int(os.environ.get("MAX_CONCURRENCY", "5"))  # tuned for 6-core VPS
LOG_LEVEL:      str = os.environ.get("LOG_LEVEL", "INFO").upper()
VERSION:        str = os.environ.get("VERSION", "1.0.0")
REDIS_URL:      str = os.environ.get("REDIS_URL", "")
HEALTH_BIND_PORT: int = int(os.environ.get("HEALTH_PORT", "8001"))
CACHE_REFRESH_QUEUE = "alvify:queue:cache_refresh"

# Batch flush settings
BATCH_SIZE:    int   = int(os.environ.get("BATCH_SIZE", "50"))
BATCH_TIMEOUT: float = float(os.environ.get("BATCH_TIMEOUT", "3"))

# Enrichment pipeline
ENRICHMENT_ENABLED: bool  = os.environ.get("ENRICHMENT_ENABLED", "true").lower() not in ("0", "false", "no")
ENRICHMENT_TIMEOUT: float = float(os.environ.get("ENRICHMENT_TIMEOUT", "15"))

# API endpoints
POLL_URL      = f"{API_URL}/internal/workers/jobs/poll"
HEARTBEAT_URL = f"{API_URL}/internal/workers/heartbeat"
BATCH_URL     = f"{API_URL}/internal/workers/jobs/{{job_id}}/leads/batch"
SINGLE_URL    = f"{API_URL}/internal/workers/jobs/{{job_id}}/lead"

# Add this file's directory to sys.path so 'core' package is importable
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("alvify.worker")


# ── Auth headers ──────────────────────────────────────────────────────────────

def _worker_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {WORKER_API_KEY}",
        "X-Worker-ID": WORKER_ID,
        "Content-Type": "application/json",
        "User-Agent": f"alvify-worker/{VERSION}",
    }


# ── Resource usage ────────────────────────────────────────────────────────────

def _resource_usage() -> tuple[float, float]:
    """Return (cpu_percent, ram_percent) via psutil if available."""
    try:
        import psutil
        cpu = psutil.cpu_percent(interval=None)
        ram = psutil.virtual_memory().percent
        return cpu, ram
    except Exception:
        return 0.0, 0.0


# ── Health endpoint ────────────────────────────────────────────────────────────

async def _health_server() -> None:
    """Minimal HTTP server for /health ping from the API."""
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
    while True:
        await asyncio.sleep(3600)


# ── Heartbeat ─────────────────────────────────────────────────────────────────

_active_jobs = 0


async def _heartbeat_loop(session: aiohttp.ClientSession) -> None:
    while True:
        await asyncio.sleep(15)
        cpu, ram = _resource_usage()
        try:
            async with session.post(
                HEARTBEAT_URL,
                headers=_worker_headers(),
                json={
                    "cpu_usage": cpu,
                    "ram_usage": ram,
                    "active_jobs": _active_jobs,
                    "version": VERSION,
                },
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status not in (200, 204):
                    logger.warning("Heartbeat responded %d", resp.status)
        except Exception as exc:
            logger.warning("Heartbeat failed: %s", exc)


# ── Stage messages (Melhoria 7 — UX realtime) ─────────────────────────────────

def _stage_msg(n: int, max_results: int, phase: str = "", name: str = "") -> str:
    """
    Return a human-readable progress message for the current scraping phase.

    phase values set by the scraper orchestration below:
      "init"       — starting browser
      "maps_load"  — loading Google Maps feed
      "finding"    — scrolling feed, collecting place URLs
      "extracting" — extracting individual place details
      "saving"     — batch flushing to backend
      "done"       — completed
    """
    if phase == "init":
        return "Iniciando navegador…"
    if phase == "maps_load":
        return "Carregando Google Maps…"
    if phase == "finding":
        if n == 0:
            return "Encontrando empresas…"
        return f"Encontrando empresas… ({n} locais encontrados)"
    if phase == "extracting":
        pct = int(n / max_results * 100) if max_results > 0 else 0
        brief = (name[:28] + "…") if len(name) > 28 else name
        if brief:
            return f"Extraindo contatos: {brief} ({n}/{max_results})"
        return f"Extraindo contatos… ({n}/{max_results})"
    if phase == "saving":
        return f"Salvando resultados… ({n} leads)"
    if phase == "done":
        return f"Concluído — {n} leads coletados"
    # Fallback: infer from progress percentage
    pct = int(n / max_results * 100) if max_results > 0 else 0
    if pct < 10:
        return "Encontrando empresas…"
    if pct < 85:
        return f"Extraindo contatos… ({n}/{max_results})"
    return f"Coletando locais… ({n} encontrados)"


# ── Batch flusher ─────────────────────────────────────────────────────────────

async def _flush_batch(
    session: aiohttp.ClientSession,
    job_id: str,
    batch: list[dict],
    metrics,  # JobMetrics
) -> tuple[int, int]:
    """
    Submit a batch of leads to the backend.

    Tries the batch endpoint first; falls back to the single-lead endpoint
    if the batch endpoint fails (e.g. on first deploy before the new backend
    is live).

    Returns (count_sent, count_new).
    """
    if not batch:
        return 0, 0

    logger.info(
        "job=%s _flush_batch called with %d leads",
        job_id, len(batch),
    )

    t0 = time.monotonic()

    # ── Try batch endpoint first ──────────────────────────────────────────────
    try:
        logger.info(
            "job=%s trying batch endpoint: %s",
            job_id, BATCH_URL.format(job_id=job_id),
        )
        async with session.post(
            BATCH_URL.format(job_id=job_id),
            headers=_worker_headers(),
            json=batch,
            timeout=aiohttp.ClientTimeout(total=60),
        ) as resp:
            logger.info(
                "job=%s batch endpoint response: status=%d",
                job_id, resp.status,
            )
            if resp.status == 200:
                data = await resp.json()
                sent     = data.get("count", 0)
                new_c    = data.get("new_count", 0)
                elapsed  = int((time.monotonic() - t0) * 1000)
                metrics.batch_sent(len(batch), elapsed)
                metrics.lead_sent(sent)
                metrics.lead_new(new_c)
                logger.debug(
                    "batch flush job=%s size=%d sent=%d new=%d ms=%d",
                    job_id, len(batch), sent, new_c, elapsed,
                )
                return sent, new_c
            else:
                body = await resp.text()
                logger.warning(
                    "batch endpoint returned %d — falling back to single: %s",
                    resp.status, body[:200],
                )
    except Exception as exc:
        logger.warning("batch flush error (%s) — falling back to single", exc)

    # ── Fallback: submit each lead individually ───────────────────────────────
    sent = new_c = 0
    for lead in batch:
        try:
            async with session.post(
                SINGLE_URL.format(job_id=job_id),
                headers=_worker_headers(),
                json=lead,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status == 201:
                    d = await r.json()
                    sent += 1
                    if d.get("is_new"):
                        new_c += 1
                    metrics.lead_sent()
                    if d.get("is_new"):
                        metrics.lead_new()
                else:
                    metrics.record_error()
        except Exception as exc:
            logger.warning("Single lead submit error: %s", exc)
            metrics.record_error()
    return sent, new_c


# ── Job processor ─────────────────────────────────────────────────────────────

async def _process_job(session: aiohttp.ClientSession, job: dict) -> None:
    global _active_jobs

    job_id      = job["job_id"]
    search_id   = job.get("search_id")
    keyword     = job.get("keyword", "")
    city        = job.get("city", "")
    state       = job.get("state", "")
    country     = job.get("country", "BR")
    max_results = int(job.get("max_results") or 20)

    logger.info(
        "job_start job=%s keyword=%r city=%r state=%r max=%d",
        job_id, keyword, city, state, max_results,
    )
    _active_jobs += 1
    start_ts = time.monotonic()

    # ── Per-job objects ───────────────────────────────────────────────────────
    from core.dedup import LocalDedup
    from core.metrics import JobMetrics

    dedup   = LocalDedup()
    metrics = JobMetrics(job_id=job_id)
    metrics.start()

    # Batch buffer: accumulated leads waiting to be flushed
    _batch: list[dict] = []
    _last_flush_ts = time.monotonic()
    leads_sent = 0
    new_leads  = 0

    # ── Optional: place cache (requires REDIS_URL) ────────────────────────────
    _place_cache = None
    if REDIS_URL:
        try:
            import redis.asyncio as aioredis
            from core.place_cache import PlaceCache
            _redis = aioredis.from_url(REDIS_URL, socket_connect_timeout=2)
            _place_cache = PlaceCache(_redis)
            logger.debug("job=%s place_cache enabled", job_id)
        except Exception as exc:
            logger.debug("job=%s place_cache init failed: %s", job_id, exc)

    async def _maybe_flush(force: bool = False) -> None:
        """Flush the batch buffer if full or timed out."""
        nonlocal leads_sent, new_leads, _batch, _last_flush_ts

        should_flush = (
            force
            or len(_batch) >= BATCH_SIZE
            or (len(_batch) > 0 and time.monotonic() - _last_flush_ts >= BATCH_TIMEOUT)
        )
        
        # DEBUG: log flush decisions
        if len(_batch) > 0:
            logger.debug(
                "job=%s _maybe_flush called: batch_size=%d force=%s should_flush=%s",
                job_id, len(_batch), force, should_flush,
            )
        
        if not should_flush or not _batch:
            return

        to_send = _batch[:]
        _batch.clear()
        _last_flush_ts = time.monotonic()

        logger.info(
            "job=%s flushing batch of %d leads",
            job_id, len(to_send),
        )

        # Fire-and-forget progress (non-blocking, returns immediately)
        await _send_progress(leads_sent, _stage_msg(leads_sent, max_results, phase="saving"))

        logger.debug("job=%s BEFORE _flush_batch", job_id)
        try:
            sent, new_c = await _flush_batch(session, job_id, to_send, metrics)
        except Exception as flush_exc:
            logger.error("job=%s _flush_batch EXCEPTION: %s", job_id, flush_exc, exc_info=True)
            sent, new_c = 0, 0
        logger.debug("job=%s AFTER _flush_batch sent=%d new=%d", job_id, sent, new_c)
        
        leads_sent += sent
        new_leads  += new_c
        
        logger.info(
            "job=%s batch flushed: sent=%d new=%d",
            job_id, sent, new_c,
        )

    async def _send_progress(n: int, msg: str) -> None:
        """Truly fire-and-forget — never blocks the flush path."""
        pct = min(99, int(n / max_results * 100)) if max_results > 0 else 0
        async def _do():
            try:
                async with session.post(
                    f"{API_URL}/internal/workers/jobs/{job_id}/progress",
                    headers=_worker_headers(),
                    json={"pct": pct, "n": n, "msg": msg},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as _:
                    pass
            except Exception:
                pass
        asyncio.create_task(_do())

    async def _progress_cb(n: int, lead_dict: dict) -> None:
        """
        Called by the scraper after each lead is extracted.

        1. Skip duplicates (local dedup)
        2. Run enrichment pipeline (website, email, socials, technologies)
        3. Add to batch buffer
        4. Flush if batch is full or timeout elapsed
        5. Send progress update to API
        """
        nonlocal leads_sent, new_leads

        metrics.lead_scraped()

        # DEBUG: log first 3 leads to diagnose dedup issue
        if metrics.leads_scraped <= 3:
            logger.info(
                "job=%s lead_extracted n=%d name=%r city=%r phone=%r",
                job_id, n, lead_dict.get("name"), lead_dict.get("city"), lead_dict.get("phone"),
            )

        # ── Local dedup ───────────────────────────────────────────────────────
        if dedup.check_and_register(lead_dict):
            metrics.lead_deduped()
            logger.info(
                "job=%s dedup_skip n=%d name=%r city=%r phone=%r",
                job_id, n, lead_dict.get("name"), lead_dict.get("city"), lead_dict.get("phone"),
            )
            return

        # ── Enrichment pipeline (best-effort, never blocks ingest) ────────────
        if ENRICHMENT_ENABLED and lead_dict.get("website"):
            try:
                from core.enrichment import enrich_lead
                await enrich_lead(lead_dict, session=session, timeout=ENRICHMENT_TIMEOUT)
                metrics.lead_enriched(
                    emails=len(lead_dict.get("emails") or []),
                    socials=len(lead_dict.get("socials") or {}),
                    technologies=len(lead_dict.get("technologies") or []),
                )
            except Exception as enrich_exc:
                logger.debug("enrichment failed for %s: %s", lead_dict.get("website"), enrich_exc)

        _batch.append(lead_dict)
        
        logger.debug(
            "job=%s lead_added_to_batch n=%d batch_size=%d name=%r",
            job_id, n, len(_batch), lead_dict.get("name"),
        )

        # Flush if we hit the batch size limit or the timeout
        await _maybe_flush()

        # Progress message (fire-and-forget, returns immediately)
        await _send_progress(n, _stage_msg(n, max_results, phase="extracting", name=lead_dict.get("name", "")))

    try:
        from core.scraper import scrape_leads
        from core.browser_pool import browser_pool

        if not browser_pool.is_started:
            await browser_pool.start()

        # Announce init
        await _send_progress(0, _stage_msg(0, max_results, phase="init"))

        # ── Run scraper ───────────────────────────────────────────────────────
        # scrape_leads calls _progress_cb for each extracted lead.
        # It accepts an optional place_cache argument for per-URL caching.
        scrape_kwargs: dict = dict(
            keyword=keyword,
            city=city,
            state=state,
            max_results=max_results,
            country=country,
            pool=browser_pool,
            progress_cb=_progress_cb,
            metrics=metrics,
        )
        if _place_cache is not None:
            scrape_kwargs["place_cache"] = _place_cache

        await scrape_leads(**scrape_kwargs)

        # ── Final flush of any remaining buffered leads ───────────────────────
        logger.info(
            "job=%s scraping complete, final batch size=%d",
            job_id, len(_batch),
        )
        
        if _batch:
            await _maybe_flush(force=True)

        # ── Mark job complete ─────────────────────────────────────────────────
        await _send_progress(leads_sent, _stage_msg(leads_sent, max_results, phase="done"))

        async with session.post(
            f"{API_URL}/internal/workers/jobs/{job_id}/complete",
            headers=_worker_headers(),
            json={"count": leads_sent, "new_count": new_leads},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                logger.warning("complete endpoint returned %d", resp.status)

        elapsed = time.monotonic() - start_ts
        metrics.log_summary(final=True)
        logger.info(
            "job_done job=%s leads_sent=%d new=%d deduped=%d elapsed=%.1fs",
            job_id, leads_sent, new_leads, dedup.seen_count - leads_sent, elapsed,
        )

    except Exception as exc:
        logger.error("job_error job=%s error=%s", job_id, exc, exc_info=True)
        metrics.record_error()
        metrics.log_summary(final=True)
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
        # Close the per-job Redis connection if we opened one
        if _place_cache is not None and hasattr(_place_cache, "_redis"):
            try:
                await _place_cache._redis.aclose()
            except Exception:
                pass


# ── Cache refresh (background, no user-visible job) ──────────────────────────

async def _refresh_cache_entry(session: aiohttp.ClientSession, params: dict) -> None:
    """Re-scrape a stale result-cache entry and update Redis."""
    global _active_jobs

    keyword     = params.get("keyword", "")
    city        = params.get("city", "")
    state       = params.get("state") or None
    country     = params.get("country", "BR") or "BR"
    max_results = int(params.get("max_results") or 20)

    if not keyword or not city:
        return

    logger.info("cache_refresh_start keyword=%r city=%r", keyword, city)
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
            await store_result_cache(_r, keyword, city, state or "", country, scraped, max_results)
            await _r.aclose()
            logger.info(
                "cache_refresh_done keyword=%r city=%r leads=%d",
                keyword, city, len(scraped),
            )
    except Exception as exc:
        logger.warning("cache_refresh_error keyword=%r city=%r error=%s", keyword, city, exc)
    finally:
        _active_jobs -= 1


# ── Poll loop ─────────────────────────────────────────────────────────────────

async def _poll_loop(session: aiohttp.ClientSession) -> None:
    from core.queue import make_queue_client

    queue = make_queue_client(POLL_URL, _worker_headers)
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    logger.info("poll_start concurrency=%d batch_size=%d batch_timeout=%.1fs",
                MAX_CONCURRENCY, BATCH_SIZE, BATCH_TIMEOUT)

    while True:
        # Back-pressure: wait if we are already at capacity
        if _active_jobs >= MAX_CONCURRENCY:
            await asyncio.sleep(2)
            continue

        # ── Optional: consume cache-refresh queue (direct Redis) ──────────────
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

        # ── Poll for the next job ─────────────────────────────────────────────
        job = await queue.poll(session)
        if job is None:
            # No job available (204 or timeout) — immediately re-poll.
            continue

        await queue.ack(job.get("job_id", ""), session)

        async def _run(j=job):
            async with semaphore:
                await _process_job(session, j)

        asyncio.create_task(_run())


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    if not WORKER_ID or not WORKER_API_KEY:
        logger.error("WORKER_ID and WORKER_API_KEY must be set")
        sys.exit(1)

    logger.info(
        "worker_start version=%s id=%s api=%s concurrency=%d",
        VERSION, WORKER_ID, API_URL, MAX_CONCURRENCY,
    )

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENCY * 6)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Initial heartbeat
        try:
            cpu, ram = _resource_usage()
            async with session.post(
                HEARTBEAT_URL,
                headers=_worker_headers(),
                json={"cpu_usage": cpu, "ram_usage": ram, "active_jobs": 0, "version": VERSION},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    logger.info("heartbeat_ok")
                else:
                    logger.warning("heartbeat_warn status=%d", resp.status)
        except Exception as exc:
            logger.warning("heartbeat_fail error=%s — continuing", exc)

        await asyncio.gather(
            _heartbeat_loop(session),
            _poll_loop(session),
            _health_server(),
        )


if __name__ == "__main__":
    asyncio.run(main())
