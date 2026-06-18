"""
Website enrichment pipeline.

Extracts emails, social media links, and technologies from a lead's website.
Uses aiohttp (no browser) for lightweight, fast extraction. Designed to be
best-effort — failures are silent and never block the main scraping pipeline.
"""
from __future__ import annotations

import logging
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

import aiohttp

logger = logging.getLogger("alvify.enrichment")

# ── Email extraction ──────────────────────────────────────────────────────────

_EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
)

_EMAIL_BLACKLIST = {
    "example.com", "exemplo.com", "test.com", "email.com",
    "sentry.io", "wixpress.com", "googleapis.com",
    "w3.org", "schema.org", "wordpress.org",
}


def _is_valid_email(email: str) -> bool:
    """Basic email validation — filters junk and known false positives."""
    email = email.lower().strip()
    if len(email) > 254 or len(email) < 5:
        return False
    domain = email.split("@", 1)[1] if "@" in email else ""
    if domain in _EMAIL_BLACKLIST:
        return False
    # Skip image/asset-looking emails
    if any(ext in email for ext in (".png", ".jpg", ".gif", ".svg", ".css", ".js")):
        return False
    return True


# ── Social media extraction ───────────────────────────────────────────────────

_SOCIAL_PATTERNS = {
    "instagram": re.compile(r"https?://(?:www\.)?instagram\.com/([a-zA-Z0-9_.]+)/?"),
    "facebook": re.compile(r"https?://(?:www\.)?facebook\.com/([a-zA-Z0-9_.]+)/?"),
    "linkedin": re.compile(r"https?://(?:www\.)?linkedin\.com/(?:company|in)/([a-zA-Z0-9_.\-]+)/?"),
    "twitter": re.compile(r"https?://(?:www\.)?(?:twitter|x)\.com/([a-zA-Z0-9_]+)/?"),
    "youtube": re.compile(r"https?://(?:www\.)?youtube\.com/(?:@|channel/|c/)([a-zA-Z0-9_.\-]+)/?"),
    "tiktok": re.compile(r"https?://(?:www\.)?tiktok\.com/@([a-zA-Z0-9_.]+)/?"),
}

_SOCIAL_BLACKLIST_HANDLES = {
    "share", "sharer", "intent", "login", "signup", "help",
    "about", "legal", "privacy", "terms", "policies",
}


# ── Technology detection ──────────────────────────────────────────────────────

_TECH_SIGNATURES = [
    # (name, pattern in HTML)
    ("WordPress", r"wp-content|wp-includes|wordpress"),
    ("Shopify", r"cdn\.shopify\.com|shopify\.com/s/"),
    ("Wix", r"static\.wixstatic\.com|wix\.com"),
    ("Squarespace", r"squarespace\.com|static1\.squarespace"),
    ("Webflow", r"webflow\.com|assets\.website-files\.com"),
    ("React", r"react(?:\.production|DOM|dom)"),
    ("Next.js", r"_next/static|__NEXT_DATA__"),
    ("Vue.js", r"vue(?:\.runtime|\.global|\.esm)"),
    ("Angular", r"angular(?:\.min)?\.js|ng-version"),
    ("jQuery", r"jquery(?:\.min)?\.js"),
    ("Bootstrap", r"bootstrap(?:\.min)?\.(?:css|js)"),
    ("Tailwind", r"tailwindcss|tailwind\."),
    ("Google Analytics", r"google-analytics\.com|gtag/js|googletagmanager"),
    ("Google Tag Manager", r"googletagmanager\.com/gtm"),
    ("Facebook Pixel", r"connect\.facebook\.net/.*fbevents|fbq\("),
    ("Hotjar", r"hotjar\.com|hj\("),
    ("RD Station", r"rdstation\.com|d335luupugsy2\.cloudfront"),
    ("HubSpot", r"hubspot\.com|hs-scripts\.com"),
    ("Mailchimp", r"mailchimp\.com|mc\.us\d+\.list-manage"),
    ("WhatsApp Widget", r"wa\.me|api\.whatsapp\.com|whatsapp"),
]


# ── Main enrichment function ──────────────────────────────────────────────────

async def enrich_lead(
    lead: dict,
    session: aiohttp.ClientSession,
    timeout: float = 15,
) -> None:
    """
    Enrich a lead dict in-place by scraping its website.

    Adds:
      - lead["emails"]: list[str]
      - lead["socials"]: dict[str, str]  (platform -> URL)
      - lead["technologies"]: list[str]

    Best-effort: any failure is logged at debug level and skipped.
    """
    website = lead.get("website", "")
    if not website:
        return

    # Ensure we have a full URL.
    if not website.startswith("http"):
        url = f"https://{website}"
    else:
        url = website

    html = await _fetch_html(session, url, timeout)
    if not html:
        return

    # Extract data
    lead["emails"] = _extract_emails(html)
    lead["socials"] = _extract_socials(html, url)
    lead["technologies"] = _extract_technologies(html)

    # Store HTML temporarily for digital diagnosis (removed before submission)
    lead["_enrichment_html"] = html

    logger.debug(
        "enriched %s: emails=%d socials=%d techs=%d",
        website,
        len(lead["emails"]),
        len(lead["socials"]),
        len(lead["technologies"]),
    )


async def _fetch_html(
    session: aiohttp.ClientSession,
    url: str,
    timeout: float,
) -> Optional[str]:
    """Fetch HTML content from a URL. Returns None on failure."""
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        }
        async with session.get(
            url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout),
            allow_redirects=True,
            max_redirects=5,
        ) as resp:
            if resp.status != 200:
                return None
            content_type = resp.headers.get("Content-Type", "")
            if "text/html" not in content_type and "application/xhtml" not in content_type:
                return None
            # Limit to 1MB to avoid memory issues
            body = await resp.read()
            if len(body) > 1_048_576:
                body = body[:1_048_576]
            # Decode with fallback
            for encoding in ("utf-8", "latin-1", "iso-8859-1"):
                try:
                    return body.decode(encoding)
                except (UnicodeDecodeError, LookupError):
                    continue
            return body.decode("utf-8", errors="replace")
    except Exception as exc:
        logger.debug("fetch failed for %s: %s", url, exc)
        return None


def _extract_emails(html: str) -> list[str]:
    """Extract unique valid emails from HTML content."""
    raw = _EMAIL_RE.findall(html)
    seen: set[str] = set()
    emails: list[str] = []
    for email in raw:
        email_lower = email.lower()
        if email_lower not in seen and _is_valid_email(email_lower):
            seen.add(email_lower)
            emails.append(email_lower)
    return emails[:10]  # Cap at 10


def _extract_socials(html: str, base_url: str) -> dict[str, str]:
    """Extract social media links from HTML."""
    socials: dict[str, str] = {}
    for platform, pattern in _SOCIAL_PATTERNS.items():
        matches = pattern.findall(html)
        for handle in matches:
            handle_lower = handle.lower().rstrip("/")
            if handle_lower in _SOCIAL_BLACKLIST_HANDLES:
                continue
            if len(handle_lower) < 2:
                continue
            # Build full URL
            match = pattern.search(html)
            if match:
                socials[platform] = match.group(0)
            break  # Take first valid match per platform
    return socials


def _extract_technologies(html: str) -> list[str]:
    """Detect technologies used by the website."""
    html_lower = html.lower()
    detected: list[str] = []
    for name, pattern in _TECH_SIGNATURES:
        if re.search(pattern, html_lower):
            detected.append(name)
    return detected


# ── Digital Diagnosis ─────────────────────────────────────────────────────────

_COPYRIGHT_YEAR_RE = re.compile(r"©\s*(\d{4})|copyright\s*(\d{4})", re.IGNORECASE)
_VIEWPORT_RE = re.compile(r'<meta[^>]*name=["\']viewport["\']', re.IGNORECASE)
_META_DESC_RE = re.compile(r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\']+)', re.IGNORECASE)
_OG_TAG_RE = re.compile(r'<meta[^>]*property=["\']og:', re.IGNORECASE)
_H1_RE = re.compile(r"<h1[^>]*>(.+?)</h1>", re.IGNORECASE | re.DOTALL)
_TITLE_RE = re.compile(r"<title[^>]*>(.+?)</title>", re.IGNORECASE | re.DOTALL)
_CANONICAL_RE = re.compile(r'<link[^>]*rel=["\']canonical["\']', re.IGNORECASE)
_STRUCTURED_DATA_RE = re.compile(r'application/ld\+json|itemtype=["\']https?://schema\.org', re.IGNORECASE)


def compute_digital_diagnosis(lead: dict, html: str | None = None) -> dict:
    """
    Compute a comprehensive digital diagnosis for a lead.

    Analyzes the lead's digital presence and returns a diagnosis dict with:
    - needs_* booleans for each service category
    - suggested_services list
    - digital_maturity_score (0-100)
    - issues list (specific problems found)

    Can work with or without HTML (gracefully degrades without it).
    """
    diagnosis = {
        "needs_website": False,
        "needs_seo": False,
        "needs_social_media": False,
        "needs_paid_traffic": False,
        "needs_redesign": False,
        "needs_automation": False,
        "needs_google_business": False,
        "digital_maturity_score": 0,
        "issues": [],
        "suggested_services": [],
    }

    has_website = bool(lead.get("website"))
    has_instagram = bool(lead.get("instagram") or (lead.get("socials") or {}).get("instagram"))
    has_facebook = bool((lead.get("socials") or {}).get("facebook"))
    has_any_social = has_instagram or has_facebook or bool(lead.get("socials"))
    technologies = lead.get("technologies") or []
    rating = lead.get("rating")
    review_count = lead.get("review_count") or 0

    # ── No website at all ────────────────────────────────────────────────────
    if not has_website:
        diagnosis["needs_website"] = True
        diagnosis["issues"].append("Empresa não possui website")
        diagnosis["suggested_services"].append("criação de site")
    else:
        # Website-specific analysis (requires HTML)
        if html:
            _analyze_website(html, lead, diagnosis, technologies)

    # ── Social media analysis ────────────────────────────────────────────────
    if not has_any_social:
        diagnosis["needs_social_media"] = True
        diagnosis["issues"].append("Nenhuma rede social encontrada")
        diagnosis["suggested_services"].append("gestão de redes sociais")
    elif not has_instagram:
        diagnosis["needs_social_media"] = True
        diagnosis["issues"].append("Sem presença no Instagram")
        diagnosis["suggested_services"].append("gestão de Instagram")

    # ── Google Business analysis ─────────────────────────────────────────────
    if rating is None and review_count == 0:
        diagnosis["needs_google_business"] = True
        diagnosis["issues"].append("Perfil do Google Meu Negócio não otimizado ou inexistente")
        diagnosis["suggested_services"].append("otimização Google Meu Negócio")
    elif review_count < 10:
        diagnosis["issues"].append(f"Apenas {review_count} avaliações no Google")
        diagnosis["needs_google_business"] = True

    # ── Paid traffic analysis ────────────────────────────────────────────────
    has_tracking = any(t in technologies for t in [
        "Google Analytics", "Google Tag Manager", "Facebook Pixel"
    ])
    if has_website and not has_tracking:
        diagnosis["needs_paid_traffic"] = True
        diagnosis["issues"].append("Sem ferramentas de tracking/analytics")
        diagnosis["suggested_services"].append("configuração de tráfego pago")

    # ── Automation analysis ──────────────────────────────────────────────────
    has_automation = any(t in technologies for t in [
        "RD Station", "HubSpot", "Mailchimp"
    ])
    if has_website and not has_automation:
        diagnosis["needs_automation"] = True
        diagnosis["issues"].append("Sem ferramenta de automação/email marketing")
        diagnosis["suggested_services"].append("automação de marketing")

    # ── Compute digital maturity score (0-100) ───────────────────────────────
    maturity = 0
    if has_website:
        maturity += 25
    if has_any_social:
        maturity += 15
    if has_instagram:
        maturity += 10
    if has_tracking:
        maturity += 15
    if has_automation:
        maturity += 10
    if review_count >= 20:
        maturity += 10
    elif review_count >= 5:
        maturity += 5
    if rating and rating >= 4.0:
        maturity += 5
    if not diagnosis["needs_seo"]:
        maturity += 5
    if not diagnosis["needs_redesign"]:
        maturity += 5

    diagnosis["digital_maturity_score"] = min(100, maturity)

    # Deduplicate suggested services
    diagnosis["suggested_services"] = list(dict.fromkeys(diagnosis["suggested_services"]))

    return diagnosis


def _analyze_website(html: str, lead: dict, diagnosis: dict, technologies: list) -> None:
    """Analyze website HTML for SEO, mobile, freshness issues."""
    html_lower = html.lower()
    url = lead.get("website", "")

    # ── HTTPS check ──────────────────────────────────────────────────────────
    if url and not url.startswith("https"):
        diagnosis["issues"].append("Site sem HTTPS (inseguro)")
        diagnosis["needs_redesign"] = True

    # ── SEO checks ───────────────────────────────────────────────────────────
    seo_issues = []

    # Meta description
    has_meta_desc = bool(_META_DESC_RE.search(html))
    if not has_meta_desc:
        seo_issues.append("Sem meta description")

    # Title tag
    title_match = _TITLE_RE.search(html)
    if not title_match or len(title_match.group(1).strip()) < 10:
        seo_issues.append("Title tag ausente ou muito curta")

    # H1 tag
    if not _H1_RE.search(html):
        seo_issues.append("Sem tag H1")

    # OG tags (Open Graph for social sharing)
    if not _OG_TAG_RE.search(html):
        seo_issues.append("Sem Open Graph tags (compartilhamento social)")

    # Canonical URL
    if not _CANONICAL_RE.search(html):
        seo_issues.append("Sem canonical URL")

    # Structured data
    if not _STRUCTURED_DATA_RE.search(html):
        seo_issues.append("Sem dados estruturados (Schema.org)")

    if len(seo_issues) >= 2:
        diagnosis["needs_seo"] = True
        diagnosis["issues"].extend(seo_issues)
        diagnosis["suggested_services"].append("otimização SEO")

    # ── Mobile-friendliness ──────────────────────────────────────────────────
    has_viewport = bool(_VIEWPORT_RE.search(html))
    if not has_viewport:
        diagnosis["needs_redesign"] = True
        diagnosis["issues"].append("Site não responsivo (sem viewport meta tag)")
        diagnosis["suggested_services"].append("redesign responsivo")

    # ── Site freshness (copyright year) ──────────────────────────────────────
    copyright_matches = _COPYRIGHT_YEAR_RE.findall(html)
    if copyright_matches:
        # Get the most recent year found
        years = [int(y) for pair in copyright_matches for y in pair if y]
        if years:
            latest_year = max(years)
            import datetime
            current_year = datetime.datetime.now().year
            if current_year - latest_year >= 3:
                diagnosis["needs_redesign"] = True
                diagnosis["issues"].append(f"Site desatualizado (copyright {latest_year})")
                diagnosis["suggested_services"].append("redesign/atualização do site")

    # ── Platform-specific insights ───────────────────────────────────────────
    if "WordPress" in technologies:
        # Check for very old WordPress indicators
        if re.search(r"wp-includes/js/jquery/jquery\.js\?ver=[12]\.", html_lower):
            diagnosis["needs_redesign"] = True
            diagnosis["issues"].append("WordPress muito desatualizado (risco de segurança)")

    if "Wix" in technologies or "Squarespace" in technologies:
        # These platforms limit SEO control
        diagnosis["issues"].append(f"Plataforma {technologies[0]} limita controle de SEO")


def enrich_lead_with_diagnosis(lead: dict) -> None:
    """
    Apply digital diagnosis to a lead and set service-need tags.

    This should be called AFTER enrich_lead() so that technologies and
    socials are already populated. Works on the lead dict in-place.

    Adds:
      - lead["digital_diagnosis"]: full diagnosis dict
      - lead["suggested_services"]: list of service recommendations
      - lead["digital_maturity_score"]: 0-100 maturity score
      - Updates lead["tags"] with service-need tags
    """
    # Use stored HTML if available, otherwise work without it
    html = lead.pop("_enrichment_html", None)

    diagnosis = compute_digital_diagnosis(lead, html)

    lead["digital_diagnosis"] = diagnosis
    lead["suggested_services"] = diagnosis["suggested_services"]
    lead["digital_maturity_score"] = diagnosis["digital_maturity_score"]

    # Add service-need tags
    tags = lead.get("tags") or []
    tag_map = {
        "needs_website": "precisa de site",
        "needs_seo": "precisa de SEO",
        "needs_social_media": "precisa de social media",
        "needs_paid_traffic": "precisa de tráfego pago",
        "needs_redesign": "precisa de redesign",
        "needs_automation": "precisa de automação",
        "needs_google_business": "precisa de Google Meu Negócio",
    }
    for key, tag in tag_map.items():
        if diagnosis.get(key) and tag not in tags:
            tags.append(tag)

    # Maturity label
    maturity = diagnosis["digital_maturity_score"]
    if maturity <= 20:
        maturity_label = "maturidade digital: crítica"
    elif maturity <= 40:
        maturity_label = "maturidade digital: baixa"
    elif maturity <= 60:
        maturity_label = "maturidade digital: média"
    elif maturity <= 80:
        maturity_label = "maturidade digital: boa"
    else:
        maturity_label = "maturidade digital: alta"

    if maturity_label not in tags:
        tags.append(maturity_label)

    lead["tags"] = tags
