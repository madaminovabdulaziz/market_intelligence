"""Shared scraper infrastructure: HTTP client, retry, logging, DB helpers."""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any

import asyncpg
import httpx
from loguru import logger

from config import config

# Legal form suffixes to strip when normalising company names
_LEGAL_FORMS = re.compile(
    r"\b(OOO|MCHJ|МЧЖ|ООО|ОАО|АО|АЖ|AJ|QK|QMJ|ХК|XK|GmbH|LLC|ЯТТ|YaTT|ЧП|XP)\b",
    re.IGNORECASE,
)
_EXTRA_SPACES = re.compile(r"\s+")
_QUOTES = re.compile(r'[«»""\'"]')


def clean_company_name(raw: str) -> str:
    """Normalise a company name: strip legal forms, quotes, extra spaces."""
    name = _QUOTES.sub("", raw)
    name = _LEGAL_FORMS.sub("", name)
    name = _EXTRA_SPACES.sub(" ", name).strip()
    return name.upper()


def extract_region(text: str) -> str | None:
    """Try to extract an Uzbekistan region name from free text."""
    if not text:
        return None
    text_lower = text.lower()
    for region in config.regions:
        if region.lower() in text_lower:
            return region
    return None


class BaseScraper:
    """Base class providing HTTP, retry, scrape-log, and company-upsert helpers."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool
        self.client = httpx.AsyncClient(
            timeout=config.http_timeout,
            headers={"User-Agent": config.user_agent},
            follow_redirects=True,
        )

    async def close(self) -> None:
        await self.client.aclose()

    # ── HTTP with retry ──────────────────────────────────────

    async def http_get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        retries: int = config.max_retries,
    ) -> httpx.Response:
        return await self._request("GET", url, params=params, headers=headers, retries=retries)

    async def http_post(
        self,
        url: str,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        retries: int = config.max_retries,
    ) -> httpx.Response:
        return await self._request("POST", url, json_body=json_body, headers=headers, retries=retries)

    async def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        retries: int = 3,
    ) -> httpx.Response:
        delay = 1.0
        for attempt in range(1, retries + 1):
            try:
                resp = await self.client.request(
                    method, url, params=params, json=json_body, headers=headers,
                )
                if resp.status_code in (429, 500, 502, 503):
                    raise httpx.HTTPStatusError(
                        f"HTTP {resp.status_code}", request=resp.request, response=resp,
                    )
                resp.raise_for_status()
                return resp
            except (httpx.HTTPStatusError, httpx.ConnectError, httpx.ReadTimeout) as exc:
                if attempt == retries:
                    raise
                logger.warning("Attempt {}/{} failed for {}: {}. Retrying in {}s …",
                               attempt, retries, url, exc, delay)
                await asyncio.sleep(delay)
                delay *= 2
        raise RuntimeError("unreachable")

    # ── Scrape log management ────────────────────────────────

    async def start_scrape_log(self, source: str) -> int:
        row = await self.pool.fetchrow(
            "INSERT INTO scrape_logs (source) VALUES ($1) RETURNING id",
            source,
        )
        log_id: int = row["id"]
        logger.info("Scrape log #{} started for '{}'", log_id, source)
        return log_id

    async def update_scrape_log(self, log_id: int, **kwargs: Any) -> None:
        sets = []
        vals: list[Any] = []
        i = 1
        for key, val in kwargs.items():
            if key == "details":
                sets.append(f"details = ${i}::jsonb")
                vals.append(json.dumps(val) if not isinstance(val, str) else val)
            else:
                sets.append(f"{key} = ${i}")
                vals.append(val)
            i += 1
        vals.append(log_id)
        await self.pool.execute(
            f"UPDATE scrape_logs SET {', '.join(sets)} WHERE id = ${i}",
            *vals,
        )

    async def finish_scrape_log(
        self, log_id: int, status: str = "completed", error: str | None = None,
    ) -> None:
        await self.pool.execute(
            """UPDATE scrape_logs
               SET finished_at = NOW(), status = $1, error_message = $2
               WHERE id = $3""",
            status, error, log_id,
        )
        logger.info("Scrape log #{} finished with status '{}'", log_id, status)

    # ── Company upsert ───────────────────────────────────────

    async def upsert_company(
        self,
        stir: str,
        raw_name: str,
        source: str,
        region: str | None = None,
    ) -> None:
        """Insert company or update its raw_names array if it already exists."""
        canonical = clean_company_name(raw_name)
        await self.pool.execute(
            """INSERT INTO companies (stir, canonical_name, raw_names, region, source)
               VALUES ($1, $2, jsonb_build_array($3::text), $4, $5)
               ON CONFLICT (stir) DO UPDATE SET
                   raw_names = CASE
                       WHEN NOT companies.raw_names @> jsonb_build_array($3::text)
                       THEN companies.raw_names || jsonb_build_array($3::text)
                       ELSE companies.raw_names
                   END,
                   region = COALESCE(companies.region, EXCLUDED.region),
                   updated_at = NOW()""",
            stir, canonical, raw_name, region, source,
        )
