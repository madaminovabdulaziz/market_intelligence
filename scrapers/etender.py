"""Scraper for etender.uzex.uz DealsList API."""

from __future__ import annotations

import asyncio
import json
from datetime import date as Date
from decimal import Decimal
from typing import Any

import asyncpg
from loguru import logger

from config import config
from scrapers.base import BaseScraper, extract_region

# Page size: how many rows per request
PAGE_SIZE = 20


class ETenderScraper(BaseScraper):
    """Paginates the DealsList API, filters construction deals, stores results."""

    API_URL = config.etender_api_url
    CONSTRUCTION_KEYWORDS: list[str] = config.construction_keywords
    NON_CONSTRUCTION_KEYWORDS: list[str] = config.non_construction_keywords

    # Required headers — API needs Origin/Referer from the SPA
    API_HEADERS: dict[str, str] = {
        "Origin": "https://etender.uzex.uz",
        "Referer": "https://etender.uzex.uz/deals-list",
    }

    def __init__(self, pool: asyncpg.Pool) -> None:
        super().__init__(pool)
        self._total_count: int = 0

    # ── API discovery ────────────────────────────────────────

    async def discover_format(self) -> dict[str, Any]:
        """Probe first batch to confirm total_count and field names."""
        resp = await self.http_post(
            self.API_URL,
            json_body={"From": 1, "To": PAGE_SIZE, "currencyId": None, "System_Id": 0},
            headers=self.API_HEADERS,
        )
        items = resp.json()

        if not isinstance(items, list) or not items:
            logger.error("Unexpected response: {}", str(items)[:500])
            return {"total_count": 0, "fields": []}

        self._total_count = items[0].get("total_count", 0)
        fields = list(items[0].keys())
        logger.info("ETender API: total_count={}, page_size={}, fields={}",
                     self._total_count, PAGE_SIZE, fields)
        logger.info("Sample deal: {}", json.dumps(items[0], ensure_ascii=False, indent=2)[:1000])
        return {"total_count": self._total_count, "fields": fields}

    # ── Filtering ────────────────────────────────────────────

    def is_construction_deal(self, deal: dict[str, Any]) -> bool:
        """Two-tier construction deal filter.

        Tier 1: category_name contains construction keywords AND no
                non-construction keywords → accept.  If both match
                (e.g., "питание в дошкольных" matches "школ" + "питан"),
                the non-construction keyword wins — it's more specific.
        Tier 2: customer_name or provider_name matches, but category_name
                is NOT obviously non-construction → accept.
        """
        category = str(deal.get("category_name") or "").lower()
        is_non_construction = any(
            nkw in category for nkw in self.NON_CONSTRUCTION_KEYWORDS
        )

        # Tier 1: direct match on the deal's own description
        if any(kw in category for kw in self.CONSTRUCTION_KEYWORDS):
            # If category also matches a non-construction keyword,
            # the deal is ambiguous — reject it.
            return not is_non_construction

        # Tier 2: secondary signals from party names
        secondary = " ".join([
            str(deal.get("customer_name") or ""),
            str(deal.get("provider_name") or ""),
        ]).lower()

        if any(kw in secondary for kw in self.CONSTRUCTION_KEYWORDS):
            return not is_non_construction

        return False

    # ── Main scraping loop ───────────────────────────────────

    async def scrape_all(
        self,
        start_page: int = 1,
        max_pages: int | None = None,
    ) -> dict[str, int]:
        """Scrape all pages, filter construction deals, store in DB.

        Pagination uses row ranges: page 1 = From:1 To:20, page 2 = From:21 To:40, etc.
        """
        log_id = await self.start_scrape_log("etender")

        try:
            # Discovery (get total_count)
            if self._total_count == 0:
                await self.discover_format()

            total_pages = (
                (self._total_count + PAGE_SIZE - 1) // PAGE_SIZE
            ) if self._total_count else 10000

            if max_pages:
                total_pages = min(total_pages, start_page + max_pages - 1)

            logger.info("Scraping pages {} to {} ({} total records)",
                        start_page, total_pages, self._total_count)

            stats = {"found": 0, "inserted": 0, "updated": 0, "skipped": 0, "failed": 0}
            page = start_page
            empty_streak = 0

            while page <= total_pages:
                # Fetch a batch of pages concurrently
                batch_size = min(config.etender_concurrency, total_pages - page + 1)
                tasks = [
                    self._fetch_page(p) for p in range(page, page + batch_size)
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                all_empty = True
                for i, result in enumerate(results):
                    current_page = page + i
                    if isinstance(result, Exception):
                        logger.error("Page {} failed: {}", current_page, result)
                        stats["failed"] += 1
                        continue

                    items = result
                    if not items:
                        continue

                    all_empty = False
                    for deal in items:
                        stats["found"] += 1
                        if self.is_construction_deal(deal):
                            try:
                                inserted = await self._store_deal(deal)
                                if inserted:
                                    stats["inserted"] += 1
                                else:
                                    stats["updated"] += 1
                            except Exception as exc:
                                logger.error("Failed to store deal {}: {}",
                                             deal.get("deal_id"), exc)
                                stats["failed"] += 1
                        else:
                            stats["skipped"] += 1

                if all_empty:
                    empty_streak += 1
                    if empty_streak >= 3:
                        logger.info("3 consecutive empty batches — stopping")
                        break
                else:
                    empty_streak = 0

                page += batch_size

                # Progress log every 50 pages
                if (page - start_page) % 50 < batch_size:
                    logger.info(
                        "Progress: page {}/{} | found={} construction={} skipped={}",
                        page - 1, total_pages,
                        stats["found"], stats["inserted"], stats["skipped"],
                    )
                    await self.update_scrape_log(
                        log_id,
                        records_found=stats["found"],
                        records_inserted=stats["inserted"],
                        records_skipped=stats["skipped"],
                        last_page_scraped=page - 1,
                    )

                await asyncio.sleep(config.etender_batch_delay)

            await self.update_scrape_log(
                log_id,
                records_found=stats["found"],
                records_inserted=stats["inserted"],
                records_updated=stats["updated"],
                records_skipped=stats["skipped"],
                records_failed=stats["failed"],
            )
            await self.finish_scrape_log(log_id, "completed")
            logger.info("ETender scrape complete: {}", stats)
            return stats

        except Exception as exc:
            await self.finish_scrape_log(log_id, "failed", str(exc))
            raise

    async def _fetch_page(self, page: int) -> list[dict[str, Any]]:
        """Fetch a single page using From/To row ranges."""
        from_row = (page - 1) * PAGE_SIZE + 1
        to_row = page * PAGE_SIZE
        resp = await self.http_post(
            self.API_URL,
            json_body={
                "From": from_row,
                "To": to_row,
                "currencyId": None,
                "System_Id": 0,
            },
            headers=self.API_HEADERS,
        )
        data = resp.json()
        return data if isinstance(data, list) else []

    async def _store_deal(self, deal: dict[str, Any]) -> bool:
        """Upsert a construction deal. Returns True if new insert, False if update."""
        deal_id = deal.get("deal_id")
        if deal_id is None:
            return False

        provider_inn = str(deal.get("provider_inn") or "").strip()
        provider_name = deal.get("provider_name") or ""
        customer_name = deal.get("customer_name") or ""
        category_name = deal.get("category_name") or ""

        # Extract region from customer/category text
        region = extract_region(customer_name) or extract_region(category_name)

        # Ensure company exists before inserting tender (FK constraint)
        # Skip foreign companies with non-standard STIRs (Uzbek STIR is 9 digits)
        if provider_inn and len(provider_inn) > 9:
            logger.debug("Skipping non-standard STIR: {}", provider_inn)
            provider_inn = ""

        if provider_inn:
            try:
                await self.upsert_company(
                    stir=provider_inn,
                    raw_name=provider_name,
                    source="etender",
                    region=region,
                )
            except Exception as exc:
                logger.error("upsert_company failed for STIR={} name={}: {}", provider_inn, provider_name, exc)
                raise

        # Parse deal_date: "2026-02-14T14:46:45" → datetime.date
        deal_date: Date | None = None
        deal_date_raw = deal.get("deal_date")
        if deal_date_raw:
            try:
                deal_date = Date.fromisoformat(str(deal_date_raw).split("T")[0])
            except ValueError:
                pass

        start_cost = Decimal(str(deal["start_cost"])) if deal.get("start_cost") is not None else Decimal(0)
        deal_cost = Decimal(str(deal["deal_cost"])) if deal.get("deal_cost") is not None else Decimal(0)
        logger.debug("Storing deal_id={} start_cost={!r} deal_cost={!r} provider_inn={!r}",
                      deal_id, start_cost, deal_cost, provider_inn)
        participants = int(deal["participants_count"]) if deal.get("participants_count") is not None else 0

        result = await self.pool.fetchrow(
            """INSERT INTO tender_results
                   (deal_id, start_cost, deal_cost, customer_name,
                    provider_stir, provider_name, deal_date,
                    deal_description, participants_count, region, raw_data)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
               ON CONFLICT (deal_id) DO UPDATE SET
                   start_cost = EXCLUDED.start_cost,
                   deal_cost = EXCLUDED.deal_cost,
                   customer_name = EXCLUDED.customer_name,
                   provider_name = EXCLUDED.provider_name,
                   deal_date = EXCLUDED.deal_date,
                   deal_description = EXCLUDED.deal_description,
                   participants_count = EXCLUDED.participants_count,
                   region = EXCLUDED.region,
                   raw_data = EXCLUDED.raw_data
               RETURNING (xmax = 0) AS is_insert""",
            int(deal_id),
            start_cost,
            deal_cost,
            customer_name,
            provider_inn or None,
            provider_name,
            deal_date,
            category_name,
            participants,
            region,
            json.dumps(deal, ensure_ascii=False, default=str),
        )
        return result["is_insert"] if result else False
