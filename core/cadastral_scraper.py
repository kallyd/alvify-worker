"""
Cadastral scraper — fetches leads from the Alvify CNPJ API (api-db).

Unlike the Google Maps scraper, this module does NOT use a browser. It queries
the 68M-company PostgreSQL database via REST API, applying filters like CNAE,
city, state, porte, and date range.

Supports two modes:
  - "cadastral": standard filtered search
  - "empresas_novas": filters by recent data_abertura (last 7 days by default)
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional

import aiohttp

from core.cnpj_client import CnpjApiClient

logger = logging.getLogger("alvify.cadastral_scraper")

# ── Porte labels ──────────────────────────────────────────────────────────────

_PORTE_LABELS = {
    0: "N/A",
    1: "ME",
    3: "EPP",
    5: "Demais",
}


def _compute_cadastral_score(empresa: dict) -> tuple[int, str, list[str]]:
    """
    Compute a prospecting score for a cadastral lead.

    Since we have no digital presence data, the score is based on:
    - Porte (EPP > ME > Demais)
    - Capital social
    - Has email / phone
    """
    base = 60  # Higher base than Maps — cadastral leads are pre-filtered

    capital = float(empresa.get("capital_social") or 0)
    porte = int(empresa.get("porte_empresa") or 0)
    has_email = bool(empresa.get("email"))
    has_phone = bool(empresa.get("ddd_telefone1"))

    # Porte scoring
    if porte == 3:    # EPP
        base += 10
    elif porte == 5:  # Grande
        base += 5
    elif porte == 1:  # ME
        base += 7

    # Capital scoring
    if capital >= 500_000:
        base += 8
    elif capital >= 100_000:
        base += 5
    elif capital >= 50_000:
        base += 3

    # Contact availability
    if not has_email:
        base += 5  # No email = more opportunity for prospecting
    if not has_phone:
        base += 3

    tags: list[str] = ["cadastral"]
    if not has_email:
        tags.append("sem email")
    if not has_phone:
        tags.append("sem telefone")
    if porte == 1:
        tags.append("MEI/ME")
    elif porte == 3:
        tags.append("EPP")
    if capital >= 100_000:
        tags.append("capital alto")

    digital_status = "none"  # No digital data available from cadastral source

    return min(base, 99), digital_status, tags


def _empresa_to_lead(empresa: dict) -> dict[str, Any]:
    """Convert an api-db empresa dict to the standard lead format."""
    score, digital_status, tags = _compute_cadastral_score(empresa)

    nome = empresa.get("nome_fantasia") or empresa.get("razao_social") or ""
    logradouro = empresa.get("logradouro", "")
    numero = empresa.get("numero", "")
    tipo_logr = empresa.get("tipo_logradouro", "")

    address_parts = []
    if tipo_logr:
        address_parts.append(tipo_logr)
    if logradouro:
        address_parts.append(logradouro)
    if numero:
        address_parts.append(numero)

    return {
        "name": nome.strip(),
        "category": empresa.get("cnae_fiscal_descricao", ""),
        "address": " ".join(address_parts),
        "neighborhood": empresa.get("bairro", ""),
        "city": empresa.get("municipio", ""),
        "state": empresa.get("uf", ""),
        "phone": empresa.get("ddd_telefone1", "") or None,
        "website": None,
        "instagram": None,
        "rating": None,
        "review_count": 0,
        "score": score,
        "digital_status": digital_status,
        "tags": tags,
        "hours": None,
        "photo_url": None,
        "status": "new",
        "source": "cnpj_database",
        # Extra cadastral fields
        "cnpj": empresa.get("cnpj", ""),
        "razao_social": empresa.get("razao_social", ""),
        "capital_social": float(empresa.get("capital_social") or 0),
        "porte_empresa": int(empresa.get("porte_empresa") or 0),
        "data_abertura": empresa.get("data_abertura", ""),
        "opcao_mei": empresa.get("opcao_mei", False),
        "opcao_simples": empresa.get("opcao_simples", False),
        "email": empresa.get("email", "") or None,
    }


async def scrape_leads(
    *,
    keyword: str,
    city: str,
    state: str,
    max_results: int = 20,
    country: str = "BR",
    progress_cb: Callable,
    cnpj_client: CnpjApiClient,
    metrics: Any = None,
    session: aiohttp.ClientSession,
    # Optional extra filters
    data_abertura_de: str = "",
    data_abertura_ate: str = "",
    porte: int = 0,
    mei: Optional[bool] = None,
    simples: Optional[bool] = None,
    ddd: str = "",
    tem_email: Optional[bool] = None,
    tem_telefone: Optional[bool] = None,
    **kwargs: Any,
) -> None:
    """
    Fetch leads from the CNPJ database (api-db) and emit them via progress_cb.

    The keyword is interpreted as:
    - A CNAE code if it's exactly 7 digits (e.g., "5611201")
    - A text search (q parameter) otherwise (e.g., "restaurante")

    Paginates via cursor until max_results are collected.
    """
    # Determine if keyword is a CNAE code or free-text search
    is_cnae = keyword.isdigit() and len(keyword) == 7

    # Build base filters
    filters: dict[str, Any] = {
        "cidade": city,
        "situacao": 2,  # Only active companies
        "limit": min(max_results, 200),  # API max is 200 per page
    }

    if state:
        filters["uf"] = state
    if is_cnae:
        filters["cnae"] = keyword
    elif keyword:
        filters["q"] = keyword

    # Optional filters
    if data_abertura_de:
        filters["data_abertura_de"] = data_abertura_de
    if data_abertura_ate:
        filters["data_abertura_ate"] = data_abertura_ate
    if porte > 0:
        filters["porte"] = porte
    if mei is not None:
        filters["mei"] = mei
    if simples is not None:
        filters["simples"] = simples
    if ddd:
        filters["ddd"] = ddd
    if tem_email is not None:
        filters["tem_email"] = tem_email
    if tem_telefone is not None:
        filters["tem_telefone"] = tem_telefone

    logger.info(
        "cadastral_start keyword=%r city=%r state=%r max=%d is_cnae=%s",
        keyword, city, state, max_results, is_cnae,
    )

    collected = 0
    cursor = 0
    page = 0

    while collected < max_results:
        if cursor > 0:
            filters["cursor"] = cursor

        # Adjust limit for the last page
        remaining = max_results - collected
        filters["limit"] = min(remaining, 200)

        empresas = await cnpj_client.search(session, **filters)

        if not empresas:
            logger.info("cadastral_end no more results at page=%d collected=%d", page, collected)
            break

        for empresa in empresas:
            lead = _empresa_to_lead(empresa)

            # Skip leads without a name
            if not lead["name"]:
                continue

            collected += 1
            await progress_cb(collected, lead)

            if collected >= max_results:
                break

        # Advance cursor for next page
        last_id = empresas[-1].get("id", 0)
        if last_id:
            cursor = int(last_id)
        else:
            break

        page += 1

        # Safety: don't paginate infinitely
        if page > 100:
            logger.warning("cadastral_safety_break after 100 pages")
            break

    logger.info("cadastral_done keyword=%r city=%r collected=%d", keyword, city, collected)
