"""
Instagram profile discovery via Google Search.

Finds Instagram profiles for businesses by searching Google with:
  site:instagram.com "business name" "city"

Uses the existing Playwright browser pool — no Instagram scraping directly,
which avoids their aggressive anti-bot measures.

IMPORTANT: Google rate-limits aggressively from datacenter IPs. This module
works best with:
- Residential proxies (set PROXY_URL env var)
- Generous delays between searches (default 5s)
- Low volume per session (max ~20-30 searches before rotating)

The module is designed as an OPTIONAL enrichment step — failures are silent
and the pipeline continues without Instagram data.

Usage::

    from core.instagram_discovery import discover_instagram

    ig_url = await discover_instagram(
        name="Restaurante Fogão a Lenha",
        city="Francisco Alves",
        pool=browser_pool,
    )
    # Returns: "https://www.instagram.com/fogaoalenha_fa" or None
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Optional
from urllib.parse import quote_plus, unquote

logger = logging.getLogger("alvify.instagram_discovery")

# Enable/disable via env var — DISABLED by default on datacenter IPs.
# Enable when you have residential proxies configured or run from a non-DC IP.
INSTAGRAM_DISCOVERY_ENABLED = os.environ.get(
    "INSTAGRAM_DISCOVERY_ENABLED", "false"
).lower() in ("1", "true", "yes")

# Delay between searches (seconds) — be respectful to avoid blocks
SEARCH_DELAY = float(os.environ.get("INSTAGRAM_SEARCH_DELAY", "5"))

# Handles to ignore (generic/platform pages)
_BLACKLIST_HANDLES = {
    "instagram", "explore", "p", "reel", "reels", "stories",
    "accounts", "directory", "about", "legal", "privacy",
    "terms", "help", "developer", "press", "meta", "creators",
}

_INSTAGRAM_URL_RE = re.compile(
    r"https?://(?:www\.)?instagram\.com/([a-zA-Z0-9_.]+)/?",
)

_HANDLE_IN_TITLE_RE = re.compile(r"@([a-zA-Z0-9_.]{2,30})")


def _is_valid_handle(handle: str) -> bool:
    """Check if an Instagram handle looks like a real business profile."""
    handle_lower = handle.lower().rstrip("/")
    if handle_lower in _BLACKLIST_HANDLES:
        return False
    if len(handle_lower) < 2 or len(handle_lower) > 30:
        return False
    if handle_lower.startswith(("explore", "p/", "reel")):
        return False
    return True


def _build_search_query(name: str, city: str) -> str:
    """Build a Google search query to find the Instagram profile."""
    clean_name = re.sub(
        r"\s*[-–]\s*(ME|EPP|LTDA|EIRELI|S/A|SA)$", "", name, flags=re.IGNORECASE
    )
    clean_name = clean_name.strip()
    return f'site:instagram.com "{clean_name}" "{city}"'


async def discover_instagram(
    *,
    name: str,
    city: str,
    pool=None,
    timeout: float = 15,
) -> Optional[str]:
    """
    Search Google for a business's Instagram profile.

    Returns the Instagram URL if found with high confidence, or None.
    """
    if not INSTAGRAM_DISCOVERY_ENABLED:
        return None

    if not name or not city:
        return None

    query = _build_search_query(name, city)
    search_url = f"https://duckduckgo.com/html/?q={quote_plus(query)}"

    async def _run(page) -> Optional[str]:
        try:
            await page.goto(search_url, wait_until="domcontentloaded", timeout=int(timeout * 1000))
            await asyncio.sleep(1.5)

            content = await page.content()

            # Strategy 1: Extract @ handle from result titles
            # DuckDuckGo shows: "Cristianini's - Francisco Alves (@cristianinis.burgers)"
            titles = page.locator("a.result__a, h2.result__title a")
            title_count = await titles.count()

            for i in range(min(title_count, 8)):
                try:
                    title_text = await titles.nth(i).text_content() or ""
                    # Look for (@handle) in title
                    handle_match = _HANDLE_IN_TITLE_RE.search(title_text)
                    if handle_match:
                        handle = handle_match.group(1).rstrip(".")
                        if _is_valid_handle(handle):
                            return f"https://www.instagram.com/{handle}"

                    # Also check the href
                    href = await titles.nth(i).get_attribute("href") or ""
                    if "instagram.com" in href:
                        ig_match = _INSTAGRAM_URL_RE.search(href)
                        if ig_match:
                            handle = ig_match.group(1).rstrip("/")
                            if _is_valid_handle(handle):
                                return f"https://www.instagram.com/{handle}"
                except Exception:
                    continue

            # Strategy 2: Look for instagram.com in result URLs/snippets
            result_links = page.locator("a.result__url, span.result__url")
            link_count = await result_links.count()
            for i in range(min(link_count, 8)):
                try:
                    link_text = await result_links.nth(i).text_content() or ""
                    if "instagram.com" in link_text:
                        m = re.search(r"instagram\.com/([a-zA-Z0-9_.]+)", link_text)
                        if m:
                            handle = m.group(1).rstrip(".")
                            if _is_valid_handle(handle):
                                return f"https://www.instagram.com/{handle}"
                except Exception:
                    continue

            # Strategy 3: Regex fallback on full page content
            all_matches = _INSTAGRAM_URL_RE.findall(content)
            seen: set[str] = set()
            for handle in all_matches:
                handle = handle.rstrip("/").lower()
                if handle in seen:
                    continue
                seen.add(handle)
                if _is_valid_handle(handle):
                    return f"https://www.instagram.com/{handle}"

            return None

        except Exception as exc:
            logger.debug("instagram_discovery: error for %r: %s", name, exc)
            return None

    if pool is not None and pool.is_started:
        async with pool.page() as page:
            return await _run(page)
    else:
        from playwright.async_api import async_playwright
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            ctx = await browser.new_context(
                locale="pt-BR",
                timezone_id="America/Sao_Paulo",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            )
            await ctx.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
            )
            page = await ctx.new_page()
            try:
                return await _run(page)
            finally:
                await browser.close()


async def discover_instagram_batch(
    leads: list[dict],
    pool=None,
    delay: float = None,
    max_searches: int = 20,
) -> int:
    """
    Discover Instagram profiles for a batch of leads that don't have one.

    Modifies leads in-place, adding 'instagram' and updating 'socials'.
    Returns the number of profiles discovered.

    Stops after max_searches to avoid rate-limiting, or if Google blocks.
    """
    if not INSTAGRAM_DISCOVERY_ENABLED:
        return 0

    if delay is None:
        delay = SEARCH_DELAY

    discovered = 0
    searches = 0

    for lead in leads:
        if searches >= max_searches:
            logger.info("instagram_discovery: hit max_searches limit (%d)", max_searches)
            break

        # Skip leads that already have Instagram
        if lead.get("instagram"):
            continue

        name = lead.get("name", "")
        city = lead.get("city", "")

        if not name or not city:
            continue

        ig_url = await discover_instagram(name=name, city=city, pool=pool)
        searches += 1

        if ig_url:
            lead["instagram"] = ig_url
            socials = lead.get("socials") or {}
            socials["instagram"] = ig_url
            lead["socials"] = socials
            discovered += 1
            logger.info("instagram_discovery: found @%s for %r",
                       ig_url.split("/")[-1], name)
        elif ig_url is None and searches > 1:
            # Check if we got blocked (ig_url is None could mean no result OR block)
            # If we had 3 consecutive misses after initially finding some, might be blocked
            pass

        # Respectful delay between searches
        await asyncio.sleep(delay)

    logger.info(
        "instagram_discovery: batch done — %d found in %d searches",
        discovered, searches,
    )
    return discovered


# ── LLM-based Instagram Discovery (via OpenRouter) ───────────────────────────

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = os.environ.get(
    "OPENROUTER_MODEL", "google/gemini-flash-1.5"
)


async def discover_instagram_llm(
    leads: list[dict],
    api_key: str = None,
    model: str = None,
    batch_size: int = 10,
) -> int:
    """
    Use an LLM (via OpenRouter) to discover Instagram handles for leads.

    The LLM is asked to find the Instagram @ for each business based on
    its name and city. Works from any IP (no search engine needed).

    Modifies leads in-place. Returns number of profiles discovered.
    """
    import aiohttp
    import json

    _api_key = api_key or OPENROUTER_API_KEY
    _model = model or OPENROUTER_MODEL

    if not _api_key:
        logger.debug("instagram_llm: no OPENROUTER_API_KEY configured")
        return 0

    # Filter leads that need Instagram
    needs_ig = [
        (i, lead) for i, lead in enumerate(leads)
        if not lead.get("instagram") and lead.get("name") and lead.get("city")
    ]

    if not needs_ig:
        return 0

    discovered = 0

    # Process in batches
    for batch_start in range(0, len(needs_ig), batch_size):
        batch = needs_ig[batch_start:batch_start + batch_size]

        # Build prompt
        lines = []
        for idx, (_, lead) in enumerate(batch, 1):
            name = lead.get("name", "")
            city = lead.get("city", "")
            state = lead.get("state", "")
            lines.append(f"{idx}. {name} - {city}, {state}")

        prompt = f"""Para cada negócio abaixo, informe o @ do Instagram. Se não souber, diga null.
Responda SOMENTE com JSON puro, sem explicações.

{chr(10).join(lines)}

JSON (use os números como chave):"""

        headers = {
            "Authorization": f"Bearer {_api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": _model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
            "max_tokens": 2000,
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status != 200:
                        error = await resp.text()
                        logger.warning("instagram_llm: API error %d: %s", resp.status, error[:200])
                        continue

                    data = await resp.json()
                    content = data["choices"][0]["message"]["content"]

                    # Parse JSON from response (handle markdown code blocks)
                    content = content.strip()
                    if content.startswith("```"):
                        content = re.sub(r"^```\w*\n?", "", content)
                        content = re.sub(r"\n?```$", "", content)

                    results = json.loads(content)

                    for idx, (lead_idx, lead) in enumerate(batch, 1):
                        # Try numeric key first, then name key
                        handle = results.get(str(idx))
                        if not handle:
                            handle = results.get(lead.get("name", ""))
                        if handle and handle != "null" and str(handle).lower() != "none":
                            # Clean handle
                            handle = str(handle).lstrip("@").strip()
                            if _is_valid_handle(handle):
                                ig_url = f"https://www.instagram.com/{handle}"
                                lead["instagram"] = ig_url
                                socials = lead.get("socials") or {}
                                socials["instagram"] = ig_url
                                lead["socials"] = socials
                                discovered += 1
                                logger.info(
                                    "instagram_llm: found @%s for %r",
                                    handle, lead.get("name"),
                                )

        except json.JSONDecodeError as e:
            logger.warning("instagram_llm: failed to parse response: %s", e)
        except Exception as exc:
            logger.warning("instagram_llm: error: %s", exc)

    logger.info("instagram_llm: discovered %d profiles for %d leads", discovered, len(needs_ig))
    return discovered

    async def _run(page) -> Optional[str]:
        try:
            await page.goto(search_url, wait_until="domcontentloaded", timeout=int(timeout * 1000))

            # Wait for results to load
            await asyncio.sleep(1.5)

            # Check for CAPTCHA
            content = await page.content()
            if "unusual traffic" in content.lower() or "captcha" in content.lower():
                logger.debug("instagram_discovery: captcha detected for %r", name)
                return None

            # Strategy 1: Extract @ handle from search result titles
            # Google shows titles like: "Business Name (@handle) • Instagram"
            titles = page.locator("h3")
            title_count = await titles.count()

            for i in range(min(title_count, 5)):
                try:
                    title_text = await titles.nth(i).text_content() or ""
                    # Look for (@handle) pattern in title
                    handle_match = re.search(r"@([a-zA-Z0-9_.]+)", title_text)
                    if handle_match:
                        handle = handle_match.group(1).rstrip(".")
                        if _is_valid_handle(handle):
                            url = f"https://www.instagram.com/{handle}"
                            logger.debug(
                                "instagram_discovery: found @%s for %r (from title)",
                                handle, name,
                            )
                            return url
                except Exception:
                    continue

            # Strategy 2: Look for instagram.com links in result snippets/cite elements
            cite_els = page.locator("cite, span.VuuXrf")
            cite_count = await cite_els.count()
            for i in range(min(cite_count, 10)):
                try:
                    cite_text = await cite_els.nth(i).text_content() or ""
                    if "instagram.com" in cite_text:
                        match = _INSTAGRAM_URL_RE.search(cite_text)
                        if match:
                            handle = match.group(1).rstrip("/")
                            if _is_valid_handle(handle):
                                return f"https://www.instagram.com/{handle}"
                except Exception:
                    continue

            # Strategy 3: Search full page content for instagram.com/handle patterns
            all_matches = _INSTAGRAM_URL_RE.findall(content)
            for handle in all_matches:
                handle = handle.rstrip("/")
                if _is_valid_handle(handle) and handle.lower() != "accounts":
                    return f"https://www.instagram.com/{handle}"

            logger.debug("instagram_discovery: no profile found for %r in %r", name, city)
            return None

        except Exception as exc:
            logger.debug("instagram_discovery: error for %r: %s", name, exc)
            return None

    if pool is not None and pool.is_started:
        async with pool.page() as page:
            return await _run(page)
    else:
        from playwright.async_api import async_playwright
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            ctx = await browser.new_context(
                locale="pt-BR",
                timezone_id="America/Sao_Paulo",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            )
            await ctx.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
            )
            page = await ctx.new_page()
            try:
                return await _run(page)
            finally:
                await browser.close()


async def discover_instagram_batch(
    leads: list[dict],
    pool=None,
    delay: float = 2.0,
) -> int:
    """
    Discover Instagram profiles for a batch of leads that don't have one.

    Modifies leads in-place, adding 'instagram' and updating 'socials'.
    Returns the number of profiles discovered.

    Includes a delay between searches to avoid Google rate-limiting.
    """
    discovered = 0

    for lead in leads:
        # Skip leads that already have Instagram
        if lead.get("instagram"):
            continue

        name = lead.get("name", "")
        city = lead.get("city", "")

        if not name or not city:
            continue

        ig_url = await discover_instagram(name=name, city=city, pool=pool)

        if ig_url:
            lead["instagram"] = ig_url
            socials = lead.get("socials") or {}
            socials["instagram"] = ig_url
            lead["socials"] = socials
            discovered += 1
            logger.info("instagram_discovery: found @%s for %r",
                       ig_url.split("/")[-1], name)

        # Respectful delay between searches
        await asyncio.sleep(delay)

    return discovered
