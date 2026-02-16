"""Scraper for reyting.mc.uz — Ministry of Construction company ratings."""

from __future__ import annotations

import asyncio
import json
from datetime import date
from decimal import Decimal
from typing import Any

import asyncpg
from loguru import logger

from config import config
from scrapers.base import BaseScraper, clean_company_name

# API base and headers
API_BASE = "https://japi-reyting.mc.uz/api"
API_HEADERS: dict[str, str] = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://reyting.mc.uz",
    "Referer": "https://reyting.mc.uz/",
}

# Category type mapping for the agency groupings in the detail response
AGENCY_TO_CATEGORY: dict[str, str] = {
    "mehnat": "qualified_specialists",
    "soliq": "financial_performance",
    "inspeksiya": "quality_of_work",
    "tajriba": "work_experience",
    "texnika": "technical_base",
    "raqobat": "competitiveness",
}


class ReytingScraper(BaseScraper):
    """Fetches company ratings from reyting.mc.uz API."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        super().__init__(pool)
        # Override headers to include reyting-specific ones
        self.client.headers.update(API_HEADERS)

    # ── Listing: paginate all companies ──────────────────────

    async def scrape_listing(
        self,
        types: list[int] | None = None,
        per_page: int = 100,
    ) -> dict[str, int]:
        """Scrape the full company listing for given types."""
        if types is None:
            types = [0, 2]  # general construction + roads/bridges

        log_id = await self.start_scrape_log("reyting_listing")
        stats = {"found": 0, "inserted": 0, "updated": 0, "failed": 0}

        try:
            for type_id in types:
                logger.info("Scraping reyting listing type={}", type_id)

                # Get total count
                resp = await self.http_get(
                    f"{API_BASE}/v2/category/all",
                    params={"type": type_id, "page": 1, "perPage": 1},
                )
                data = resp.json()
                total = data["data"]["total"]
                total_pages = (total + per_page - 1) // per_page
                logger.info("Type {}: {} companies, {} pages", type_id, total, total_pages)

                for page in range(1, total_pages + 1):
                    try:
                        resp = await self.http_get(
                            f"{API_BASE}/v2/category/all",
                            params={"type": type_id, "page": page, "perPage": per_page},
                        )
                        page_data = resp.json()
                        companies = page_data["data"]["data"]

                        for company in companies:
                            stats["found"] += 1
                            try:
                                await self._store_listing_company(company, type_id)
                                stats["inserted"] += 1
                            except Exception as exc:
                                logger.error("Failed to store {}: {}", company.get("inn"), exc)
                                stats["failed"] += 1

                    except Exception as exc:
                        logger.error("Failed page {}/type {}: {}", page, type_id, exc)
                        stats["failed"] += 1

                    if page % 10 == 0:
                        logger.info("Type {} progress: page {}/{} | stored={}",
                                    type_id, page, total_pages, stats["inserted"])

                    await asyncio.sleep(config.reyting_request_delay)

            await self.update_scrape_log(
                log_id,
                records_found=stats["found"],
                records_inserted=stats["inserted"],
                records_failed=stats["failed"],
            )
            await self.finish_scrape_log(log_id, "completed")
            logger.info("Reyting listing scrape complete: {}", stats)
            return stats

        except Exception as exc:
            await self.finish_scrape_log(log_id, "failed", str(exc))
            raise

    async def _store_listing_company(self, company: dict[str, Any], type_id: int) -> None:
        """Store a company from the listing API."""
        inn = str(company["inn"]).strip()
        if not inn or len(inn) > 9:
            return

        name = company.get("name", "")
        rating = company.get("rating", "")
        score = company.get("sumbal")
        region = company.get("viloyat_name", "")

        canonical = clean_company_name(name)
        score_decimal = Decimal(str(score)) if score is not None else None

        await self.pool.execute(
            """INSERT INTO companies (stir, canonical_name, raw_names, region,
                   rating_letter, rating_score, rating_fetched_at, source)
               VALUES ($1, $2, jsonb_build_array($3::text), $4, $5, $6, NOW(), 'reyting')
               ON CONFLICT (stir) DO UPDATE SET
                   canonical_name = CASE
                       WHEN LENGTH(EXCLUDED.canonical_name) > LENGTH(companies.canonical_name)
                       THEN EXCLUDED.canonical_name
                       ELSE companies.canonical_name
                   END,
                   raw_names = CASE
                       WHEN NOT companies.raw_names @> jsonb_build_array($3::text)
                       THEN companies.raw_names || jsonb_build_array($3::text)
                       ELSE companies.raw_names
                   END,
                   region = COALESCE(EXCLUDED.region, companies.region),
                   rating_letter = COALESCE(EXCLUDED.rating_letter, companies.rating_letter),
                   rating_score = COALESCE(EXCLUDED.rating_score, companies.rating_score),
                   rating_fetched_at = NOW(),
                   source = CASE
                       WHEN companies.source = 'etender' THEN 'both'
                       ELSE COALESCE(companies.source, 'reyting')
                   END,
                   updated_at = NOW()""",
            inn, canonical, name, region, rating, score_decimal,
        )

    # ── Detail: fetch full rating breakdown ──────────────────

    async def scrape_details(
        self,
        stirs: list[str] | None = None,
        limit: int = 200,
        type_id: int = 0,
    ) -> dict[str, int]:
        """Fetch detailed rating breakdown for top companies."""
        log_id = await self.start_scrape_log("reyting_detail")

        try:
            if stirs is None:
                rows = await self.pool.fetch(
                    """SELECT stir FROM companies
                       WHERE rating_score IS NOT NULL
                       ORDER BY rating_score DESC
                       LIMIT $1""",
                    limit,
                )
                stirs = [r["stir"] for r in rows]
                logger.info("Selected top {} companies by rating score", len(stirs))

            stats = {"found": len(stirs), "inserted": 0, "failed": 0}
            sem = asyncio.Semaphore(config.reyting_concurrency)

            async def fetch_one(stir: str) -> None:
                async with sem:
                    try:
                        resp = await self.http_get(
                            f"{API_BASE}/v2/category/get/{stir}",
                            params={"type": type_id},
                        )
                        data = resp.json()
                        if data.get("success") and data.get("data"):
                            await self._store_detail(stir, data["data"], type_id)
                            stats["inserted"] += 1
                        else:
                            logger.debug("No detail data for STIR {}", stir)
                            stats["failed"] += 1
                    except Exception as exc:
                        logger.error("Detail fetch failed for {}: {}", stir, exc)
                        stats["failed"] += 1
                    await asyncio.sleep(config.reyting_request_delay)

            # Process in batches to show progress
            for i in range(0, len(stirs), 20):
                batch = stirs[i:i + 20]
                await asyncio.gather(*[fetch_one(s) for s in batch])
                logger.info("Detail progress: {}/{} | ok={} fail={}",
                            min(i + 20, len(stirs)), len(stirs),
                            stats["inserted"], stats["failed"])

            await self.update_scrape_log(
                log_id,
                records_found=stats["found"],
                records_inserted=stats["inserted"],
                records_failed=stats["failed"],
            )
            await self.finish_scrape_log(log_id, "completed")
            logger.info("Reyting detail scrape complete: {}", stats)
            return stats

        except Exception as exc:
            await self.finish_scrape_log(log_id, "failed", str(exc))
            raise

    async def _store_detail(self, stir: str, data: dict[str, Any], type_id: int) -> None:
        """Parse and store the detailed rating breakdown from /category/get."""
        ballar = data.get("ballar", {})
        today = date.today()

        # Store JSONB snapshot
        await self.pool.execute(
            """INSERT INTO company_rating_snapshots
                   (company_stir, rating_date, categories_json, indicators_json)
               VALUES ($1, $2, $3::jsonb, $4::jsonb)
               ON CONFLICT (company_stir, rating_date) DO UPDATE SET
                   categories_json = EXCLUDED.categories_json,
                   indicators_json = EXCLUDED.indicators_json,
                   scraped_at = NOW()""",
            stir, today,
            json.dumps(ballar, ensure_ascii=False, default=str),
            json.dumps(data, ensure_ascii=False, default=str),
        )

        # Parse each agency group and store EAV indicators
        total_employees = 0
        total_specialists = 0

        for agency_key, agency_data in ballar.items():
            if not isinstance(agency_data, dict):
                continue
            indicators = agency_data.get("data", [])
            if not isinstance(indicators, list):
                continue

            category_code = AGENCY_TO_CATEGORY.get(agency_key, "competitiveness")

            for indicator in indicators:
                if not isinstance(indicator, dict):
                    continue

                name = indicator.get("nomi_ru") or indicator.get("nomi_uz") or indicator.get("nomi", "")
                if not name:
                    continue

                earned = self._parse_decimal(indicator.get("ball"))
                max_pts = self._parse_decimal(indicator.get("max_ball"))
                raw_value = indicator.get("qiymat", "")
                key = indicator.get("key", "")

                criterion_id = await self._ensure_criterion(
                    name=name,
                    code=key,
                    category_code=category_code,
                    max_points=max_pts,
                    source_agency=indicator.get("masul_ru", ""),
                )
                if criterion_id is None:
                    continue

                await self.pool.execute(
                    """INSERT INTO company_ratings
                           (company_stir, criterion_id, raw_value, earned_points, max_points, rating_date)
                       VALUES ($1, $2, $3, $4, $5, $6)
                       ON CONFLICT (company_stir, criterion_id, rating_date) DO UPDATE SET
                           raw_value = EXCLUDED.raw_value,
                           earned_points = EXCLUDED.earned_points,
                           max_points = EXCLUDED.max_points,
                           scraped_at = NOW()""",
                    stir, criterion_id,
                    str(raw_value) if raw_value is not None else None,
                    earned, max_pts, today,
                )

                # Track employee/specialist counts from specific indicators
                if key == "mehnat_total_workers":
                    try:
                        total_employees = int(float(raw_value))
                    except (ValueError, TypeError):
                        pass
                elif key == "mehnat_engineers":
                    try:
                        total_specialists = int(float(raw_value))
                    except (ValueError, TypeError):
                        pass

        # Update company with employee counts
        if total_employees > 0 or total_specialists > 0:
            await self.pool.execute(
                """UPDATE companies SET
                       employee_count = COALESCE($2, employee_count),
                       specialist_count = COALESCE($3, specialist_count),
                       updated_at = NOW()
                   WHERE stir = $1""",
                stir,
                total_employees if total_employees > 0 else None,
                total_specialists if total_specialists > 0 else None,
            )

    def _parse_decimal(self, val: Any) -> Decimal | None:
        if val is None:
            return None
        try:
            return Decimal(str(val))
        except Exception:
            return None

    async def _ensure_criterion(
        self,
        name: str,
        code: str,
        category_code: str,
        max_points: Decimal | None,
        source_agency: str = "",
    ) -> int | None:
        """Get or create a rating_criteria row."""
        if not code:
            code = name.lower().strip().replace(" ", "_")[:100]

        row = await self.pool.fetchrow(
            "SELECT id FROM rating_criteria WHERE code = $1", code,
        )
        if row:
            return row["id"]

        cat_row = await self.pool.fetchrow(
            "SELECT id FROM rating_categories WHERE code = $1", category_code,
        )
        category_id = cat_row["id"] if cat_row else 6

        row = await self.pool.fetchrow(
            """INSERT INTO rating_criteria (category_id, code, name_uz, name_ru, source_agency, max_points)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT (code) DO UPDATE SET
                   name_ru = COALESCE(EXCLUDED.name_ru, rating_criteria.name_ru)
               RETURNING id""",
            category_id, code, name, name, source_agency, max_points,
        )
        return row["id"] if row else None

    # ── Combined entry point ─────────────────────────────────

    async def scrape_companies(
        self,
        stirs: list[str] | None = None,
        limit: int = 200,
    ) -> dict[str, int]:
        """Full scrape: listing + details for top companies."""
        # Phase 1: Scrape full listing (type=0 and type=2)
        logger.info("=== Phase 1: Scraping company listings ===")
        listing_stats = await self.scrape_listing(types=[0, 2])

        # Phase 2: Fetch detailed breakdowns for top companies by score
        logger.info("=== Phase 2: Fetching detailed ratings ===")
        detail_stats = await self.scrape_details(stirs=stirs, limit=limit, type_id=0)

        combined = {
            "listing_found": listing_stats["found"],
            "listing_stored": listing_stats["inserted"],
            "details_fetched": detail_stats["inserted"],
            "failed": listing_stats["failed"] + detail_stats["failed"],
        }
        logger.info("Combined reyting scrape: {}", combined)
        return combined
