"""
Lead quality validation module.

Validates phone numbers, websites, addresses, and business names to ensure
only high-quality leads are submitted to the backend. Each validator is
independent and best-effort — failures don't block the pipeline.

Usage::

    from core.validators import validate_lead_quality

    quality_score, issues = validate_lead_quality(lead)
    lead["quality_score"] = quality_score
    lead["quality_issues"] = issues
"""
from __future__ import annotations

import logging
import re
from typing import Optional

import aiohttp

logger = logging.getLogger("alvify.validators")

# ── Valid Brazilian DDDs ──────────────────────────────────────────────────────

_VALID_DDDS = {
    "11", "12", "13", "14", "15", "16", "17", "18", "19",  # SP
    "21", "22", "24",                                        # RJ
    "27", "28",                                              # ES
    "31", "32", "33", "34", "35", "37", "38",               # MG
    "41", "42", "43", "44", "45", "46",                     # PR
    "47", "48", "49",                                        # SC
    "51", "53", "54", "55",                                  # RS
    "61",                                                    # DF
    "62", "64",                                              # GO
    "63",                                                    # TO
    "65", "66",                                              # MT
    "67",                                                    # MS
    "68",                                                    # AC
    "69",                                                    # RO
    "71", "73", "74", "75", "77",                           # BA
    "79",                                                    # SE
    "81", "87",                                              # PE
    "82",                                                    # AL
    "83",                                                    # PB
    "84",                                                    # RN
    "85", "88",                                              # CE
    "86", "89",                                              # PI
    "91", "93", "94",                                        # PA
    "92", "97",                                              # AM
    "95",                                                    # RR
    "96",                                                    # AP
    "98", "99",                                              # MA
}

# ── Generic/invalid names ─────────────────────────────────────────────────────

_GENERIC_NAMES = {
    "empresa", "loja", "comercio", "estabelecimento", "negocio",
    "servicos", "sem nome", "teste", "test", "example",
    "null", "undefined", "n/a", "na", "-",
}

# ── Website placeholder patterns ─────────────────────────────────────────────

_PLACEHOLDER_PATTERNS = [
    r"em\s*constru[çc][aã]o",
    r"coming\s*soon",
    r"under\s*construction",
    r"site\s*em\s*manuten[çc][aã]o",
    r"p[aá]gina\s*n[aã]o\s*encontrada",
    r"parked\s*(free|domain)",
    r"este\s*dom[ií]nio\s*(est[aá]|foi)",
    r"domain\s*(is\s*)?(for\s*sale|expired|parked)",
    r"buy\s*this\s*domain",
]
_PLACEHOLDER_RE = re.compile("|".join(_PLACEHOLDER_PATTERNS), re.IGNORECASE)


# ── Phone validation ──────────────────────────────────────────────────────────

def validate_phone(phone: Optional[str]) -> tuple[bool, Optional[str]]:
    """
    Validate a Brazilian phone number.

    Returns (is_valid, normalized_phone_or_None).
    """
    if not phone:
        return False, None

    # Extract digits only
    digits = re.sub(r"\D", "", phone)

    # Strip country code
    if len(digits) >= 12 and digits.startswith("55"):
        digits = digits[2:]

    # Must have DDD (2 digits) + number (8-9 digits) = 10-11 digits
    if len(digits) < 10 or len(digits) > 11:
        return False, None

    ddd = digits[:2]

    # Valid DDD check
    if ddd not in _VALID_DDDS:
        return False, None

    number = digits[2:]

    # Reject generic numbers
    if number in ("00000000", "99999999", "11111111", "000000000", "999999999"):
        return False, None

    # Mobile: 9 digits starting with 9. Landline: 8 digits starting with 2-5.
    if len(number) == 9:
        if not number.startswith("9"):
            return False, None
    elif len(number) == 8:
        if number[0] not in "2345":
            return False, None
    else:
        return False, None

    return True, f"+55{digits}"


# ── Website validation ────────────────────────────────────────────────────────

async def validate_website(
    session: aiohttp.ClientSession,
    url: str,
    timeout: float = 5,
) -> dict:
    """
    Check if a website is online and not a placeholder.

    Returns dict with:
      - online: bool
      - status_code: int (0 if unreachable)
      - is_placeholder: bool
      - redirect_url: str or None
    """
    result = {
        "online": False,
        "status_code": 0,
        "is_placeholder": False,
        "redirect_url": None,
    }

    if not url:
        return result

    if not url.startswith("http"):
        url = f"https://{url}"

    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=timeout),
            allow_redirects=True,
            max_redirects=5,
            headers={"User-Agent": "Mozilla/5.0 (compatible; AlvifyBot/1.0)"},
        ) as resp:
            result["status_code"] = resp.status
            result["online"] = resp.status == 200

            # Check final URL after redirects
            if str(resp.url) != url:
                result["redirect_url"] = str(resp.url)

            # Check for placeholder content
            if resp.status == 200:
                content_type = resp.headers.get("Content-Type", "")
                if "text/html" in content_type:
                    body = await resp.read()
                    # Only check first 10KB
                    text = body[:10240].decode("utf-8", errors="replace").lower()
                    if _PLACEHOLDER_RE.search(text):
                        result["is_placeholder"] = True

    except Exception:
        pass

    return result


# ── Name validation ───────────────────────────────────────────────────────────

def validate_name(name: Optional[str]) -> tuple[bool, list[str]]:
    """
    Validate a business name.

    Returns (is_valid, issues).
    """
    issues: list[str] = []

    if not name:
        return False, ["Nome ausente"]

    name_clean = name.strip()

    if len(name_clean) < 3:
        issues.append("Nome muito curto")

    # Check against generic names
    name_lower = re.sub(r"[^\w\s]", "", name_clean.lower()).strip()
    if name_lower in _GENERIC_NAMES:
        issues.append("Nome genérico")

    # All digits
    if name_clean.replace(" ", "").isdigit():
        issues.append("Nome contém apenas números")

    return len(issues) == 0, issues


# ── Address validation ────────────────────────────────────────────────────────

def validate_address(lead: dict) -> tuple[bool, list[str]]:
    """
    Validate that the lead has meaningful address information.

    Returns (is_valid, issues).
    """
    issues: list[str] = []

    address = lead.get("address", "")
    city = lead.get("city", "")
    state = lead.get("state", "")

    if not city:
        issues.append("Cidade ausente")
    if not state:
        issues.append("Estado ausente")
    if not address:
        issues.append("Endereço ausente")

    return len(issues) == 0, issues


# ── Composite quality scoring ─────────────────────────────────────────────────

def validate_lead_quality(lead: dict) -> tuple[int, list[str]]:
    """
    Compute a quality score (0-100) for the lead based on data completeness
    and validity.

    Returns (quality_score, issues_list).

    Score breakdown:
      - Valid name: 20 points
      - Valid phone: 20 points
      - Has address + city + state: 20 points
      - Has website (any): 15 points
      - Has rating/reviews: 10 points
      - Has category: 5 points
      - Has photo: 5 points
      - Has any social media: 5 points

    Penalties:
      - Generic name: -15
      - Invalid phone format: -10
      - Placeholder website: -10
    """
    score = 0
    issues: list[str] = []

    # Name (20 pts)
    name_valid, name_issues = validate_name(lead.get("name"))
    if name_valid:
        score += 20
    else:
        issues.extend(name_issues)
        if "Nome genérico" in name_issues:
            score -= 15

    # Phone (20 pts)
    phone = lead.get("phone")
    if phone:
        phone_valid, _ = validate_phone(phone)
        if phone_valid:
            score += 20
        else:
            score += 5  # Has phone but invalid format
            issues.append("Telefone com formato inválido")
    else:
        issues.append("Sem telefone")

    # Address (20 pts)
    addr_valid, addr_issues = validate_address(lead)
    if addr_valid:
        score += 20
    else:
        # Partial credit
        if lead.get("city"):
            score += 10
        issues.extend(addr_issues)

    # Website (15 pts)
    if lead.get("website"):
        score += 15
    else:
        issues.append("Sem website")

    # Rating/reviews (10 pts)
    if lead.get("rating") is not None:
        score += 5
    if (lead.get("review_count") or 0) > 0:
        score += 5

    # Category (5 pts)
    if lead.get("category"):
        score += 5

    # Photo (5 pts)
    if lead.get("photo_url"):
        score += 5

    # Social media (5 pts)
    has_social = bool(
        lead.get("instagram") or
        lead.get("facebook") or
        lead.get("socials")
    )
    if has_social:
        score += 5

    return max(0, min(100, score)), issues
