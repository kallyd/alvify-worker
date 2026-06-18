"""
Client for the Alvify CNPJ API (api-db).

Provides async methods to query the 68M-company database:
- search: paginated list with filters
- bulk_cnpj: fetch multiple companies by CNPJ
- count: total matching records
- get_by_cnpj: single company lookup
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import aiohttp

logger = logging.getLogger("alvify.cnpj_client")

# Default timeout for api-db calls (seconds).
_DEFAULT_TIMEOUT = 10


class CnpjApiClient:
    """HTTP client for the Alvify CNPJ database API."""

    def __init__(self, base_url: str, api_key: str = "", timeout: float = _DEFAULT_TIMEOUT):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = aiohttp.ClientTimeout(total=timeout)

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Accept": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    async def search(
        self,
        session: aiohttp.ClientSession,
        *,
        cnpj: str = "",
        razao_social: str = "",
        nome_fantasia: str = "",
        cidade: str = "",
        uf: str = "",
        cnae: str = "",
        situacao: int = 0,
        bairro: str = "",
        cep: str = "",
        porte: int = 0,
        mei: Optional[bool] = None,
        simples: Optional[bool] = None,
        data_abertura_de: str = "",
        data_abertura_ate: str = "",
        capital_min: float = 0,
        capital_max: float = 0,
        tem_email: Optional[bool] = None,
        tem_telefone: Optional[bool] = None,
        ddd: str = "",
        q: str = "",
        limit: int = 30,
        cursor: int = 0,
    ) -> list[dict[str, Any]]:
        """
        GET /v1/empresas with filters. Returns list of empresa dicts.
        """
        params: dict[str, str] = {}
        if cnpj:
            params["cnpj"] = cnpj
        if razao_social:
            params["razao_social"] = razao_social
        if nome_fantasia:
            params["nome_fantasia"] = nome_fantasia
        if cidade:
            params["cidade"] = cidade
        if uf:
            params["uf"] = uf
        if cnae:
            params["cnae"] = cnae
        if situacao > 0:
            params["situacao"] = str(situacao)
        if bairro:
            params["bairro"] = bairro
        if cep:
            params["cep"] = cep
        if porte > 0:
            params["porte"] = str(porte)
        if mei is not None:
            params["mei"] = "true" if mei else "false"
        if simples is not None:
            params["simples"] = "true" if simples else "false"
        if data_abertura_de:
            params["data_abertura_de"] = data_abertura_de
        if data_abertura_ate:
            params["data_abertura_ate"] = data_abertura_ate
        if capital_min > 0:
            params["capital_min"] = str(capital_min)
        if capital_max > 0:
            params["capital_max"] = str(capital_max)
        if tem_email is not None:
            params["tem_email"] = "true" if tem_email else "false"
        if tem_telefone is not None:
            params["tem_telefone"] = "true" if tem_telefone else "false"
        if ddd:
            params["ddd"] = ddd
        if q:
            params["q"] = q
        if limit != 30:
            params["limit"] = str(limit)
        if cursor > 0:
            params["cursor"] = str(cursor)

        try:
            async with session.get(
                f"{self.base_url}/v1/empresas",
                headers=self._headers(),
                params=params,
                timeout=self.timeout,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", [])
                logger.debug("cnpj_api search returned %d", resp.status)
                return []
        except Exception as exc:
            logger.debug("cnpj_api search error: %s", exc)
            return []

    async def bulk_cnpj(
        self,
        session: aiohttp.ClientSession,
        cnpjs: list[str],
    ) -> list[dict[str, Any]]:
        """
        POST /v1/empresas/bulk — fetch up to 50 companies by CNPJ.
        """
        if not cnpjs:
            return []

        try:
            async with session.post(
                f"{self.base_url}/v1/empresas/bulk",
                headers=self._headers(),
                json={"cnpjs": cnpjs[:50]},
                timeout=self.timeout,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", [])
                logger.debug("cnpj_api bulk returned %d", resp.status)
                return []
        except Exception as exc:
            logger.debug("cnpj_api bulk error: %s", exc)
            return []

    async def count(
        self,
        session: aiohttp.ClientSession,
        **filters: Any,
    ) -> int:
        """
        GET /v1/empresas/count — returns total matching records.
        Accepts same filter kwargs as search().
        """
        params: dict[str, str] = {}
        for key, val in filters.items():
            if val is None or val == "" or val == 0:
                continue
            if isinstance(val, bool):
                params[key] = "true" if val else "false"
            else:
                params[key] = str(val)

        try:
            async with session.get(
                f"{self.base_url}/v1/empresas/count",
                headers=self._headers(),
                params=params,
                timeout=self.timeout,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("count", 0)
                return 0
        except Exception as exc:
            logger.debug("cnpj_api count error: %s", exc)
            return 0

    async def get_by_cnpj(
        self,
        session: aiohttp.ClientSession,
        cnpj: str,
    ) -> Optional[dict[str, Any]]:
        """
        GET /v1/empresas/:cnpj — returns single empresa or None.
        """
        try:
            async with session.get(
                f"{self.base_url}/v1/empresas/{cnpj}",
                headers=self._headers(),
                timeout=self.timeout,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data")
                return None
        except Exception as exc:
            logger.debug("cnpj_api get_by_cnpj error: %s", exc)
            return None
