"""Post-scrape enrichment: aggregate tender stats into the companies table."""

from __future__ import annotations

import asyncpg
from loguru import logger


class EnrichmentPipeline:
    """Computes derived statistics on the companies table after scraping."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def run(self, lookback_months: int = 12) -> dict[str, int]:
        """Execute all enrichment steps. Returns counts of updated rows."""
        logger.info("Starting enrichment pipeline (lookback={}m)", lookback_months)
        results: dict[str, int] = {}

        results["tender_stats"] = await self.aggregate_tender_stats(lookback_months)
        results["regions"] = await self.extract_active_regions()
        results["source"] = await self.update_company_source()

        logger.info("Enrichment complete: {}", results)
        return results

    async def aggregate_tender_stats(self, lookback_months: int = 12) -> int:
        """Aggregate tender wins, value, discount into companies table."""
        result = await self.pool.execute(
            """
            UPDATE companies c SET
                total_wins           = agg.win_count,
                total_contract_value = agg.total_value,
                avg_discount_pct     = agg.avg_discount,
                first_tender_date    = agg.first_date,
                last_tender_date     = agg.last_date,
                updated_at           = NOW()
            FROM (
                SELECT
                    provider_stir,
                    COUNT(*)                         AS win_count,
                    COALESCE(SUM(deal_cost), 0)      AS total_value,
                    ROUND(AVG(
                        CASE WHEN start_cost > 0
                             THEN (start_cost - deal_cost) / start_cost * 100
                             ELSE 0
                        END
                    ), 2)                            AS avg_discount,
                    MIN(deal_date)                   AS first_date,
                    MAX(deal_date)                   AS last_date
                FROM tender_results
                WHERE provider_stir IS NOT NULL
                  AND deal_date >= CURRENT_DATE - make_interval(months => $1)
                GROUP BY provider_stir
            ) agg
            WHERE c.stir = agg.provider_stir
            """,
            lookback_months,
        )
        count = int(result.split()[-1]) if result else 0
        logger.info("Updated tender stats for {} companies", count)
        return count

    async def extract_active_regions(self) -> int:
        """Collect distinct regions from tenders into companies.active_regions."""
        result = await self.pool.execute(
            """
            UPDATE companies c SET
                active_regions = agg.regions,
                updated_at     = NOW()
            FROM (
                SELECT
                    provider_stir,
                    COALESCE(
                        jsonb_agg(DISTINCT region) FILTER (WHERE region IS NOT NULL),
                        '[]'::jsonb
                    ) AS regions
                FROM tender_results
                WHERE provider_stir IS NOT NULL
                GROUP BY provider_stir
            ) agg
            WHERE c.stir = agg.provider_stir
            """
        )
        count = int(result.split()[-1]) if result else 0
        logger.info("Updated active regions for {} companies", count)
        return count

    async def update_company_source(self) -> int:
        """Set source='both' for companies that appear in both data sources."""
        result = await self.pool.execute(
            """
            UPDATE companies SET
                source = 'both',
                updated_at = NOW()
            WHERE rating_fetched_at IS NOT NULL
              AND total_wins > 0
              AND source != 'both'
            """
        )
        count = int(result.split()[-1]) if result else 0
        logger.info("Marked {} companies with source='both'", count)
        return count
