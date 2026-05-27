"""
Google Maps business scraper using Playwright (v2 — two-phase parallel).

Phase 1 — Feed scroll (1 pool page):
  Open search URL, scroll the results feed, collect all place URLs (fast).

Phase 2 — Parallel detail extraction (pool pages):
  Navigate to each place URL directly in an isolated context.
  BrowserPool.semaphore limits concurrency; asyncio.gather runs all tasks.
  Each extracted lead triggers progress_cb(n, lead_dict) immediately so the
  caller (worker.py) can persist + stream it to Redis without waiting for
  the full batch.

Fallback (no pool): sequential single-browser extraction (v1 behaviour).
"""
from __future__ import annotations

import asyncio
import logging
import random as _random
import re
import unicodedata
from typing import Callable, Awaitable, Optional
from urllib.parse import quote_plus

try:
    import phonenumbers as _phonenumbers
    _PHONENUMBERS_AVAILABLE = True
except ImportError:
    _PHONENUMBERS_AVAILABLE = False

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

_MAPS_URL = "https://www.google.com/maps/search/{query}"


def _normalize_phone(raw: str, country: str = "BR") -> str | None:
    """Return E.164 formatted phone or the raw string if parsing fails."""
    if not raw:
        return None
    if _PHONENUMBERS_AVAILABLE:
        try:
            parsed = _phonenumbers.parse(raw, country.upper())
            if _phonenumbers.is_valid_number(parsed):
                return _phonenumbers.format_number(
                    parsed, _phonenumbers.PhoneNumberFormat.E164
                )
        except Exception:
            pass
    return raw or None

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

_SEL_PHOTO = [
    "div.RZ66Rb img",
    "div[jsaction*='heroHeaderImage'] img",
    "button[jsaction*='heroHeaderImage'] img",
    "img[src*='googleusercontent']",
    "img[src*='ggpht']",
]

_BR_STATES = frozenset({
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
    "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO",
})

_SCROLL_PX         = 3000    # pixels per scroll
_SCROLL_PAUSE_S    = 0.8     # seconds between scrolls
_MAX_STUCK_SCROLLS = 3       # stop after N scrolls with no new cards
_RETRY_ATTEMPTS    = 3       # retries per place card (sequential fallback)
_DETAIL_TIMEOUT    = 4_000   # ms waiting for h1 after navigating to place
_NAV_TIMEOUT       = 25_000  # ms for page.goto in phase-2


def _norm_city(s: str) -> str:
    """Lowercase + strip accents for fuzzy city comparison."""
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode().lower().strip()


def _city_matches(lead_city: str, target_city: str) -> bool:
    """True when the lead city is reasonably close to the requested city."""
    if not lead_city or not target_city:
        return True  # no info — keep the lead
    n_lead   = _norm_city(lead_city)
    n_target = _norm_city(target_city)
    # exact or substring match (handles "São Paulo" vs "Sao Paulo", abbrevs, etc.)
    return n_target in n_lead or n_lead in n_target


# ── Helpers ───────────────────────────────────────────────────────────────────

def _random_ua() -> str:
    return _random.choice(_USER_AGENTS)


async def _aceitar_cookies(page) -> None:
    for text in ["Aceitar tudo", "Accept all"]:
        try:
            btn = page.get_by_role("button", name=re.compile(text, re.IGNORECASE))
            if await btn.count():
                await btn.first.click()
                await asyncio.sleep(1)
                return
        except Exception:
            pass


async def _detectar_captcha(page) -> bool:
    try:
        content = (await page.content()).lower()
        return any(s in content for s in [
            "detected unusual traffic",
            "unusual traffic from your computer",
            "recaptcha",
            "rc-anchor",
        ])
    except Exception:
        return False


async def _texto(page, seletores: list[str], timeout: int = 2000) -> str:
    """Return inner text of the first matching selector, or empty string."""
    for sel in seletores:
        try:
            el = page.locator(sel).first
            await el.wait_for(timeout=timeout)
            txt = await el.text_content()
            if txt and txt.strip():
                return txt.strip()
        except Exception:
            continue
    return ""


async def _href(page, seletores: list[str], timeout: int = 2000) -> str:
    """Return href of the first matching selector (skip google.com links)."""
    for sel in seletores:
        try:
            el = page.locator(sel).first
            await el.wait_for(timeout=timeout)
            href = await el.get_attribute("href") or ""
            if href and not href.startswith("https://www.google"):
                return href.strip()
        except Exception:
            continue
    return ""


# ── Detail extractor ─────────────────────────────────────────────────────────

async def _extrair_detalhe(
    page,
    keyword: str,
    city: str,
    state: Optional[str],
    country: str = "BR",
) -> Optional[dict]:
    """
    Parse the currently-open Google Maps place panel.
    Selectors match coleta_maps.py extrair_detalhe() with photo support.
    """
    nome = await _texto(page, [
        "h1.DUwDvf",
        "h1[jstcache]",
        'div[role="main"] h1',
    ])
    if not nome:
        return None

    # .Io6YTe = the text-content span inside Google Maps info buttons
    endereco = await _texto(page, [
        'button[data-item-id="address"] .Io6YTe',
        '[data-item-id="address"]',
        'button[aria-label*="ndereço"]',
    ])

    telefone_raw = await _texto(page, [
        'button[data-item-id^="phone:tel"] .Io6YTe',
        'button[aria-label*="elefone"] .Io6YTe',
        'button[data-item-id^="phone:"] .Io6YTe',
        '[data-tooltip="Copiar número de telefone"]',
    ])
    if not telefone_raw:
        try:
            el = page.locator('button[aria-label*="hone"]').first
            lbl = await el.get_attribute("aria-label") or ""
            if lbl:
                telefone_raw = lbl.split(":")[-1].strip()
        except Exception:
            pass
    telefone = re.sub(r"[^\d\s()\-+]", "", telefone_raw).strip()[:20] if telefone_raw else ""

    site_href = await _href(page, [
        'a[data-item-id="authority"]',
        'a[href*="http"][aria-label*="ite"]',
    ])
    site: Optional[str] = None
    if site_href:
        site = re.sub(r"^https?://", "", site_href).rstrip("/").split("?")[0] or None

    instagram: Optional[str] = None
    try:
        painel = page.locator('div[role="main"]').first
        links  = painel.locator('a[href*="instagram.com"]')
        count  = await links.count()
        for idx in range(count):
            href = await links.nth(idx).get_attribute("href") or ""
            if "instagram.com" in href and href.rstrip("/") != "https://www.instagram.com":
                instagram = href.split("?")[0].rstrip("/")
                break
    except Exception:
        pass

    # Rating — coleta_maps uses aria-hidden="true" inside div.F7nice
    avaliacao_raw = await _texto(page, [
        'div.F7nice span[aria-hidden="true"]',
        'div.F7nice span.MW4etd',
        'span.ceNzKf',
        'div.fontDisplayLarge',
    ])
    rating: Optional[float] = None
    if avaliacao_raw:
        m = re.search(r"[\d,]+", avaliacao_raw)
        if m:
            try:
                rating = float(m.group().replace(",", "."))
                if rating > 5:
                    rating = None
            except ValueError:
                pass

    # Review count — read aria-label directly.
    # Selectors match both PT ("avaliação"/"avaliações") and EN ("review"/"reviews").
    review_count = 0
    _review_btn_sels = [
        'button[aria-label*="avalia"]',   # PT: avaliação / avaliações
        'button[aria-label*="eview"]',    # EN: review / reviews
        'a[aria-label*="avalia"]',
        'a[aria-label*="eview"]',
    ]
    for _sel in _review_btn_sels:
        try:
            _el = page.locator(_sel).first
            _label = await _el.get_attribute("aria-label", timeout=2000)
            if _label:
                # Anchor to the count word so we don't pick up the rating ("4,5").
                # PT: "1.250 avaliações" — EN: "1,250 reviews"
                _m = (
                    re.search(r"([\d.]+)\s*avaliaç", _label.lower()) or
                    re.search(r"([\d,]+)\s*review",  _label.lower())
                )
                if _m:
                    _cleaned = re.sub(r"[^\d]", "", _m.group(1))
                    if _cleaned.isdigit():
                        review_count = int(_cleaned)
                        break
        except Exception:
            continue
    # Fallback: iterate aria-hidden spans inside div.F7nice.
    # Rating span contains a comma ("4,5"); count span never does ("1.250" / "250").
    if review_count == 0:
        try:
            _spans = page.locator('div.F7nice span[aria-hidden="true"]')
            _n = await _spans.count()
            for _i in range(_n):
                _txt = (await _spans.nth(_i).text_content(timeout=800) or "").strip()
                if not _txt or "," in _txt:   # skip empty or decimal (rating)
                    continue
                _cleaned = re.sub(r"[^\d]", "", _txt)
                if _cleaned.isdigit() and int(_cleaned) > 0:
                    review_count = int(_cleaned)
                    break
        except Exception:
            pass

    categoria = await _texto(page, [
        "button.DkEaL",
        'button[jsaction*="category"]',
        "span.YhemCb",
        "div.skqShb button",
    ]) or keyword.title()

    horario: Optional[str] = None
    try:
        el  = page.locator('[data-item-id="oh"] .Io6YTe, .o0Svhf, .ZDu9vd span').first
        txt = await el.text_content(timeout=1500)
        if txt:
            horario = txt.strip().split("\n")[0][:60] or None
    except Exception:
        pass

    photo_url: Optional[str] = None
    for sel in _SEL_PHOTO:
        try:
            els   = page.locator(sel)
            count = await els.count()
            for idx in range(count):
                el      = els.nth(idx)
                in_feed = await el.evaluate('el => !!el.closest(\'[role="feed"]\')')
                if in_feed:
                    continue
                src = (await el.get_attribute("src") or "").strip()
                if src and ("googleusercontent" in src or "ggpht" in src) and len(src) > 30:
                    base      = re.sub(r"=w\d+.*$", "", src)
                    photo_url = base + "=w400-h300-k-no" if base != src else src
                    break
        except Exception:
            pass
        if photo_url:
            break

    # City / state from address (store real location, not just the search param)
    parsed_city  = city
    parsed_state = state or ("SP" if country == "BR" else "")
    # _city_confirmed: True when we verified the city from the address (or no address exists).
    # False means the address contains text but we couldn't confirm the target city — the
    # caller should treat these leads with extra scepticism.
    _city_confirmed: bool = not bool(endereco)  # True when there is no address at all
    neighborhood: Optional[str] = None
    if endereco and country == "BR":
        addr_matches = re.findall(
            r"[,\-\u2013]\s*([A-Za-z\u00c0-\u00ff][A-Za-z\u00c0-\u00ff\s\'\-\.]{1,38}?)\s*[,\-\u2013]\s*([A-Z]{2})\b",
            endereco,
        )
        valid = [
            (c.strip(), s)
            for c, s in addr_matches
            if s in _BR_STATES and 2 <= len(c.strip()) <= 40
        ]
        if valid:
            parsed_city, parsed_state = valid[-1]
            _city_confirmed = True
        else:
            # Regex didn't find a "City - ST" pattern; fall back to checking whether
            # the target city name appears anywhere in the raw address text.
            _addr_norm = unicodedata.normalize("NFD", endereco).encode("ascii", "ignore").decode().lower()
            _city_confirmed = bool(_addr_norm) and _norm_city(city) in _addr_norm
        m = re.search(r"-\s*([^,\-]+)(?:,|\s*-)", endereco)
        if m:
            neighborhood = m.group(1).strip()
    elif endereco:
        m = re.search(r"-\s*([^,\-]+)(?:,|\s*-)", endereco)
        if m:
            neighborhood = m.group(1).strip()

    has_website   = bool(site)
    has_phone     = bool(telefone)
    has_instagram = bool(instagram)

    base = 50
    if rating:
        base += int(min(rating * 6, 30))
    if not has_website:
        base = min(99, base + 12)
    if not has_phone:
        base = min(99, base + 5)
    if not has_instagram:
        base = min(99, base + 5)
    if review_count < 20:
        base = min(99, base + 5)
    score = min(99, base)

    if has_website and has_instagram:
        digital_status = "excellent"
    elif has_website or has_instagram:
        digital_status = "good"
    elif has_phone:
        digital_status = "poor"
    else:
        digital_status = "none"

    tags: list[str] = []
    if not has_website:
        tags.append("sem site")
    if not has_phone:
        tags.append("sem telefone")
    if not has_instagram:
        tags.append("sem instagram")
    if review_count < 20:
        tags.append("poucas avaliações")
    if score >= 80:
        tags.append("alto potencial")

    return dict(
        name=nome,
        category=categoria,
        address=endereco,
        neighborhood=neighborhood,
        city=parsed_city,
        state=parsed_state,
        phone=_normalize_phone(telefone, country) if telefone else None,
        website=site,
        instagram=instagram,
        rating=rating,
        review_count=review_count,
        score=score,
        digital_status=digital_status,
        tags=tags,
        hours=horario,
        photo_url=photo_url,
        status="new",
        source="google_maps",
        # Internal flag consumed by scrape_leads() — not persisted to the DB.
        # True when the city was verified from the address (or no address exists).
        _city_confirmed=_city_confirmed,
    )


# ── Phase 1: collect place URLs from the Maps feed ────────────────────────────

async def _collect_place_urls(
    query: str,
    search_url: str,
    max_results: int,
    pool=None,
) -> list[str]:
    """Scroll the Maps search feed and return up to max_results place URLs."""

    async def _run(page) -> list[str]:
        logger.info("Scraper phase-1: loading feed for '%s'", query)
        await page.goto(search_url, wait_until="domcontentloaded", timeout=30_000)
        await asyncio.sleep(3)

        if await _detectar_captcha(page):
            logger.warning("Scraper phase-1: captcha detected — aborting")
            return []

        await _aceitar_cookies(page)

        feed_sel = 'div[role="feed"]'
        try:
            await page.wait_for_selector(feed_sel, timeout=10_000)
        except Exception:
            logger.warning("Scraper phase-1: feed not found")
            return []

        prev_count = 0
        stuck      = 0
        max_scrolls = max(5, max_results // 5 + 4)
        for _ in range(max_scrolls):
            await page.eval_on_selector(feed_sel, f"el=>el.scrollBy(0,{_SCROLL_PX})")
            await asyncio.sleep(_SCROLL_PAUSE_S)
            curr = await page.locator(f'{feed_sel} a[href*="/maps/place/"]').count()
            if curr >= max_results:
                break
            if curr == prev_count:
                stuck += 1
                if stuck >= _MAX_STUCK_SCROLLS:
                    break
            else:
                stuck = 0
            prev_count = curr

        links_loc = page.locator(f'{feed_sel} a[href*="/maps/place/"]')
        total = min(await links_loc.count(), max_results)
        hrefs: list[str] = []
        for i in range(total):
            try:
                href = await links_loc.nth(i).get_attribute("href")
                if href and "/maps/place/" in href:
                    hrefs.append(href)
            except Exception:
                pass

        logger.info("Scraper phase-1: collected %d URLs for '%s'", len(hrefs), query)
        return hrefs

    if pool is not None and pool.is_started:
        async with pool.page() as page:
            return await _run(page)
    else:
        from playwright.async_api import async_playwright
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=_LAUNCH_ARGS)
            ctx = await browser.new_context(
                locale="pt-BR", timezone_id="America/Sao_Paulo",
                user_agent=_random_ua(), viewport={"width": 1366, "height": 900},
            )
            await ctx.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
            )
            page = await ctx.new_page()
            try:
                return await _run(page)
            finally:
                await browser.close()


# ── Phase 2: navigate directly to a place URL and extract detail ──────────────

async def _extract_place(
    place_url: str,
    keyword: str,
    city: str,
    state: Optional[str],
    country: str,
    pool=None,
) -> Optional[dict]:
    """Navigate directly to a place URL and extract all detail fields."""

    async def _run(page) -> Optional[dict]:
        try:
            await page.goto(place_url, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT)
            try:
                await page.wait_for_selector(
                    'h1.DUwDvf, h1[jstcache], div[role="main"] h1',
                    timeout=_DETAIL_TIMEOUT,
                )
            except Exception:
                pass
            await asyncio.sleep(0.5)
            if await _detectar_captcha(page):
                logger.warning("Scraper phase-2: captcha detected")
                return None
            return await _extrair_detalhe(page, keyword, city, state, country)
        except Exception as exc:
            logger.debug("Scraper phase-2 error for %s: %s", place_url[:80], exc)
            return None

    if pool is not None and pool.is_started:
        async with pool.page() as page:
            return await _run(page)
    else:
        from playwright.async_api import async_playwright
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=_LAUNCH_ARGS)
            ctx = await browser.new_context(
                locale="pt-BR", timezone_id="America/Sao_Paulo",
                user_agent=_random_ua(), viewport={"width": 1366, "height": 900},
            )
            await ctx.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
            )
            page = await ctx.new_page()
            try:
                return await _run(page)
            finally:
                await browser.close()


# ── Public entry-point ────────────────────────────────────────────────────────

async def scrape_leads(
    keyword: str,
    city: str,
    state: Optional[str],
    max_results: int = 20,
    country: str = "BR",
    progress_cb: Optional[Callable[[int, dict], Awaitable[None]]] = None,
    pool=None,
) -> list[dict]:
    """
    Scrape Google Maps for businesses matching *keyword* in *city*.

    Two-phase:
      Phase 1 — scroll the Maps feed to collect place URLs.
      Phase 2 — extract details for each URL in parallel (pool) or
                sequentially (fallback).

    progress_cb(n, lead_dict) fires after each lead is extracted so the
    caller can persist and stream the lead immediately.
    """
    localidade = f"{city} {state}" if state else city
    query      = f"{keyword} {localidade}"
    url        = _MAPS_URL.format(query=quote_plus(query))
    leads: list[dict] = []

    # Phase 1 ─────────────────────────────────────────────────────────────────
    place_urls = await _collect_place_urls(query, url, max_results, pool)
    if not place_urls:
        logger.info("Scraper: no URLs collected for '%s'", query)
        return leads

    logger.info("Scraper: starting phase-2 for %d places", len(place_urls))

    # Phase 2 ─────────────────────────────────────────────────────────────────
    if pool is not None and pool.is_started:
        # Parallel — BrowserPool semaphore limits concurrency automatically
        found_counter = {"n": 0}
        counter_lock  = asyncio.Lock()

        async def _extract_one(place_url: str) -> None:
            detail = await _extract_place(place_url, keyword, city, state, country, pool)
            if detail is None:
                return
            # Strip internal flag before any downstream use
            city_confirmed = detail.pop("_city_confirmed", True)
            if not _city_matches(detail.get("city", ""), city):
                logger.debug(
                    "Scraper: skipping '%s' — city '%s' ≠ target '%s'",
                    detail.get("name", ""), detail.get("city", ""), city,
                )
                return
            # If the address didn't confirm the target city, drop the lead
            # (address mentions a place but the target city name is absent).
            if not city_confirmed:
                logger.debug(
                    "Scraper: skipping '%s' — city not confirmed in address '%s'",
                    detail.get("name", ""), detail.get("address", ""),
                )
                return
            async with counter_lock:
                leads.append(detail)
                found_counter["n"] += 1
                n = found_counter["n"]
            if progress_cb:
                try:
                    await progress_cb(n, detail)
                except Exception as cb_exc:
                    logger.debug("progress_cb error: %s", cb_exc)

        tasks = [asyncio.create_task(_extract_one(pu)) for pu in place_urls]
        await asyncio.gather(*tasks, return_exceptions=True)
        # Sequential fallback (no pool)
        for place_url in place_urls:
            if len(leads) >= max_results:
                break
            detail = await _extract_place(place_url, keyword, city, state, country, pool=None)
            if detail is None:
                continue
            city_confirmed = detail.pop("_city_confirmed", True)
            city_ok = _city_matches(detail.get("city", ""), city)
            if not city_ok:
                logger.debug(
                    "Scraper: skipping '%s' — city '%s' ≠ target '%s'",
                    detail.get("name", ""), detail.get("city", ""), city,
                )
                continue
            if not city_confirmed:
                logger.debug(
                    "Scraper: skipping '%s' — city not confirmed in address '%s'",
                    detail.get("name", ""), detail.get("address", ""),
                )
                continue
            leads.append(detail)
            if progress_cb:
                try:
                    await progress_cb(len(leads), detail)
                except Exception as cb_exc:
                    logger.debug("progress_cb error: %s", cb_exc)

    logger.info("Scraper finished: %d leads for '%s' in %s", len(leads), keyword, city)
    return leads
