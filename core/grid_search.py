"""
Grid search module — divides a city into geographic quadrants for broader
coverage on Google Maps.

Google Maps typically returns 60-100 results per search. By dividing the
search area into a grid (e.g. 3x3 = 9 sub-searches), we can cover 3-5x
more businesses in large cities.

The grid is activated automatically when max_results > GRID_SEARCH_THRESHOLD
(default 60). Each quadrant search uses a viewport-specific URL
(@lat,lng,zoom) to focus on that area.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Callable, Optional

logger = logging.getLogger("alvify.grid_search")

# Default threshold: use grid search when max_results exceeds this value.
GRID_SEARCH_THRESHOLD = int(os.environ.get("GRID_SEARCH_THRESHOLD", "60"))

# ── City coordinates database (major Brazilian cities) ────────────────────────
# Center lat/lng and approximate radius in degrees for grid generation.
# For cities not in this list, we fall back to standard (non-grid) scraping.

_CITY_COORDS: dict[str, dict[str, float]] = {
    # São Paulo state
    "sao paulo": {"lat": -23.5505, "lng": -46.6333, "radius": 0.15},
    "campinas": {"lat": -22.9099, "lng": -47.0626, "radius": 0.08},
    "guarulhos": {"lat": -23.4538, "lng": -46.5333, "radius": 0.06},
    "santos": {"lat": -23.9608, "lng": -46.3336, "radius": 0.05},
    "sorocaba": {"lat": -23.5015, "lng": -47.4526, "radius": 0.06},
    "ribeirao preto": {"lat": -21.1704, "lng": -47.8103, "radius": 0.06},
    "sao jose dos campos": {"lat": -23.1896, "lng": -45.8840, "radius": 0.06},
    "osasco": {"lat": -23.5325, "lng": -46.7917, "radius": 0.04},
    "santo andre": {"lat": -23.6737, "lng": -46.5432, "radius": 0.04},
    "sao bernardo do campo": {"lat": -23.6914, "lng": -46.5646, "radius": 0.05},
    # Rio de Janeiro
    "rio de janeiro": {"lat": -22.9068, "lng": -43.1729, "radius": 0.12},
    "niteroi": {"lat": -22.8833, "lng": -43.1036, "radius": 0.05},
    # Minas Gerais
    "belo horizonte": {"lat": -19.9167, "lng": -43.9345, "radius": 0.10},
    "uberlandia": {"lat": -18.9186, "lng": -48.2772, "radius": 0.06},
    "juiz de fora": {"lat": -21.7642, "lng": -43.3503, "radius": 0.05},
    # Paraná
    "curitiba": {"lat": -25.4284, "lng": -49.2733, "radius": 0.08},
    "londrina": {"lat": -23.3045, "lng": -51.1696, "radius": 0.06},
    "maringa": {"lat": -23.4205, "lng": -51.9333, "radius": 0.05},
    # Santa Catarina
    "florianopolis": {"lat": -27.5954, "lng": -48.5480, "radius": 0.08},
    "joinville": {"lat": -26.3045, "lng": -48.8487, "radius": 0.05},
    # Rio Grande do Sul
    "porto alegre": {"lat": -30.0346, "lng": -51.2177, "radius": 0.08},
    "caxias do sul": {"lat": -29.1681, "lng": -51.1794, "radius": 0.05},
    # Nordeste
    "salvador": {"lat": -12.9714, "lng": -38.5124, "radius": 0.10},
    "fortaleza": {"lat": -3.7172, "lng": -38.5433, "radius": 0.08},
    "recife": {"lat": -8.0476, "lng": -34.8770, "radius": 0.07},
    "natal": {"lat": -5.7945, "lng": -35.2110, "radius": 0.06},
    "joao pessoa": {"lat": -7.1195, "lng": -34.8450, "radius": 0.05},
    "maceio": {"lat": -9.6658, "lng": -35.7353, "radius": 0.05},
    "sao luis": {"lat": -2.5297, "lng": -44.2825, "radius": 0.06},
    "teresina": {"lat": -5.0892, "lng": -42.8019, "radius": 0.05},
    "aracaju": {"lat": -10.9095, "lng": -37.0748, "radius": 0.04},
    # Centro-Oeste
    "brasilia": {"lat": -15.7975, "lng": -47.8919, "radius": 0.12},
    "goiania": {"lat": -16.6869, "lng": -49.2648, "radius": 0.08},
    "campo grande": {"lat": -20.4697, "lng": -54.6201, "radius": 0.06},
    "cuiaba": {"lat": -15.6014, "lng": -56.0979, "radius": 0.06},
    # Norte
    "manaus": {"lat": -3.1190, "lng": -60.0217, "radius": 0.08},
    "belem": {"lat": -1.4558, "lng": -48.5024, "radius": 0.07},
}


def _normalize_city_key(city: str) -> str:
    """Normalize city name for coordinate lookup."""
    import unicodedata
    s = unicodedata.normalize("NFD", city.lower().strip())
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


def get_city_coords(city: str) -> Optional[dict[str, float]]:
    """Get coordinates for a city, or None if unknown."""
    key = _normalize_city_key(city)
    return _CITY_COORDS.get(key)


def generate_grid_points(
    center_lat: float,
    center_lng: float,
    radius: float,
    grid_size: int = 3,
) -> list[tuple[float, float]]:
    """
    Generate a grid of lat/lng points centered around the given coordinates.

    Returns a list of (lat, lng) tuples forming a grid_size x grid_size grid.
    """
    points: list[tuple[float, float]] = []
    step = (2 * radius) / grid_size

    start_lat = center_lat - radius + step / 2
    start_lng = center_lng - radius + step / 2

    for row in range(grid_size):
        for col in range(grid_size):
            lat = start_lat + row * step
            lng = start_lng + col * step
            points.append((lat, lng))

    return points


def build_grid_url(keyword: str, lat: float, lng: float, zoom: int = 14) -> str:
    """Build a Google Maps search URL focused on a specific viewport."""
    from urllib.parse import quote_plus
    query = quote_plus(keyword)
    return f"https://www.google.com/maps/search/{query}/@{lat},{lng},{zoom}z"


async def grid_scrape(
    *,
    keyword: str,
    city: str,
    state: str,
    max_results: int,
    country: str = "BR",
    progress_cb: Callable,
    pool: Any,
    metrics: Any = None,
    place_cache: Any = None,
    grid_size: int = 3,
) -> None:
    """
    Perform a grid-based search that divides the city into quadrants.

    Uses the standard scraper's _collect_place_urls and _extract_place
    for each quadrant, deduplicating across all sub-searches.

    This function is called by main.py when max_results > GRID_SEARCH_THRESHOLD
    and the city has known coordinates.
    """
    from core.scraper import _collect_place_urls, _extract_place, _city_matches

    coords = get_city_coords(city)
    if not coords:
        logger.warning("grid_scrape: no coordinates for city=%r, falling back to standard", city)
        # Fallback: single standard search
        from core.scraper import scrape_leads
        await scrape_leads(
            keyword=keyword, city=city, state=state,
            max_results=max_results, country=country,
            progress_cb=progress_cb, pool=pool,
            metrics=metrics, place_cache=place_cache,
        )
        return

    points = generate_grid_points(
        coords["lat"], coords["lng"], coords["radius"], grid_size
    )

    # Determine results per quadrant
    results_per_quadrant = max(10, max_results // len(points) + 5)

    logger.info(
        "grid_scrape: keyword=%r city=%r grid=%dx%d quadrants=%d per_quadrant=%d",
        keyword, city, grid_size, grid_size, len(points), results_per_quadrant,
    )

    # Collect all place URLs from all quadrants
    all_places: list[dict] = []
    seen_urls: set[str] = set()

    for idx, (lat, lng) in enumerate(points):
        search_url = build_grid_url(f"{keyword} {city}", lat, lng)
        try:
            places = await _collect_place_urls(
                query=f"{keyword} {city}",
                search_url=search_url,
                max_results=results_per_quadrant,
                pool=pool,
            )
            # Deduplicate by URL
            for place in places:
                url = place.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_places.append(place)

            logger.debug(
                "grid_scrape: quadrant %d/%d at (%.4f,%.4f) found %d places (total unique: %d)",
                idx + 1, len(points), lat, lng, len(places), len(all_places),
            )
        except Exception as exc:
            logger.warning("grid_scrape: quadrant %d failed: %s", idx + 1, exc)
            continue

        # Stop early if we have enough
        if len(all_places) >= max_results * 1.3:
            break

    logger.info("grid_scrape: total unique places collected: %d", len(all_places))

    # Phase 2: Extract details for each place (with concurrency via pool)
    collected = 0
    for place in all_places:
        if collected >= max_results:
            break

        url = place.get("url", "")
        if not url:
            continue

        # Check place cache
        if place_cache:
            try:
                cached = await place_cache.get(url)
                if cached:
                    # Validate city match
                    if _city_matches(cached.get("city", ""), city):
                        collected += 1
                        await progress_cb(collected, cached)
                        continue
            except Exception:
                pass

        # Extract full details
        try:
            lead = await _extract_place(
                place_url=url,
                keyword=keyword,
                city=city,
                state=state,
                country=country,
                pool=pool,
            )
            if lead is None:
                continue

            # City validation
            if not lead.pop("_city_confirmed", True):
                if not _city_matches(lead.get("city", ""), city):
                    continue

            # Cache the result
            if place_cache:
                try:
                    await place_cache.set(url, lead)
                except Exception:
                    pass

            # Backfill missing fields from feed card metadata
            if not lead.get("category") and place.get("category"):
                lead["category"] = place["category"]

            collected += 1
            await progress_cb(collected, lead)

        except Exception as exc:
            logger.debug("grid_scrape: extraction failed for %s: %s", url, exc)
            continue

    logger.info("grid_scrape: done, emitted %d leads", collected)
