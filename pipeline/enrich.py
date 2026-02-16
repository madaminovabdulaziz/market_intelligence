"""Post-scrape enrichment: classify companies, aggregate tender stats."""

from __future__ import annotations

import json

import asyncpg
from loguru import logger

from config import config


class EnrichmentPipeline:
    """Computes derived statistics on the companies table after scraping."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def run(self, lookback_months: int = 12) -> dict[str, int]:
        """Execute all enrichment steps. Returns counts of updated rows."""
        logger.info("Starting enrichment pipeline (lookback={}m)", lookback_months)
        results: dict[str, int] = {}

        # Step 1: Classify company types BEFORE aggregation
        results["classified"] = await self.classify_company_types()

        # Step 2: Fill missing regions on tender_results
        results["regions_filled"] = await self.fill_missing_regions()

        # Step 3: Aggregate tender stats
        results["tender_stats"] = await self.aggregate_tender_stats(lookback_months)
        results["regions"] = await self.extract_active_regions()
        results["source"] = await self.update_company_source()

        # Step 4: Verification
        await self.verify_classification()

        logger.info("Enrichment complete: {}", results)
        return results

    # ── Layer 1: Company-type classification ──────────────────

    async def classify_company_types(self) -> int:
        """Classify companies as contractor, consultant, laboratory, assessor, etc.

        Three-pass approach:
        1. Match negative keywords → set to consultant/laboratory/assessor
        2. Has rating_score (on reyting.mc.uz = construction registry) → 'contractor'
        3. Remaining with tender wins → 'unknown'

        Idempotent: safe to re-run.
        """
        keywords = config.non_contractor_keywords
        total_classified = 0

        # ── Pass 1: Negative keyword matching ──
        # Build a single SQL expression that checks canonical_name + all raw_names
        # against all negative keywords.
        #
        # We classify into sub-types based on which keyword matched:
        keyword_to_type = {
            # Laboratory
            "laboratoriya": "laboratory", "лаборатор": "laboratory",
            "laboratory": "laboratory", "испытат": "laboratory",
            "sinov": "laboratory",
            # Assessor / Evaluation
            "baholash": "assessor", "baho": "assessor",
            "оценк": "assessor", "оценоч": "assessor",
            "assessment": "assessor", "evaluation": "assessor",
            # Consultant
            "konsalting": "consultant", "консалтинг": "consultant",
            "consulting": "consultant", "консультац": "consultant",
            "ekspertiza": "consultant", "ekspert": "consultant",
            "экспертиз": "consultant",
            "expertise": "consultant", "аудит": "consultant", "audit": "consultant",
        }

        # First, do a bulk update for each non-contractor type
        for company_type in ("laboratory", "assessor", "consultant"):
            type_kws = [kw for kw, ct in keyword_to_type.items() if ct == company_type]
            if not type_kws:
                continue

            # Build ILIKE conditions: canonical_name or any raw_name variant
            conditions = []
            params = []
            for i, kw in enumerate(type_kws, start=1):
                conditions.append(f"(c.canonical_name ILIKE $%d OR c.raw_names::text ILIKE $%d)" % (i, i))
                params.append(f"%{kw}%")

            where = " OR ".join(conditions)
            query = f"""
                UPDATE companies c SET
                    company_type = '{company_type}',
                    updated_at = NOW()
                WHERE ({where})
                  AND company_type != '{company_type}'
            """
            result = await self.pool.execute(query, *params)
            count = int(result.split()[-1]) if result else 0
            total_classified += count
            if count > 0:
                logger.info("Classified {} companies as '{}'", count, company_type)

        # Also catch remaining negative keywords not in keyword_to_type
        # (inspection, certification, metrology, etc.) → 'other'
        remaining_kws = [
            kw for kw in keywords
            if kw not in keyword_to_type
        ]
        if remaining_kws:
            conditions = []
            params = []
            for i, kw in enumerate(remaining_kws, start=1):
                conditions.append(f"(c.canonical_name ILIKE ${i} OR c.raw_names::text ILIKE ${i})")
                params.append(f"%{kw}%")

            where = " OR ".join(conditions)
            query = f"""
                UPDATE companies c SET
                    company_type = 'other',
                    updated_at = NOW()
                WHERE ({where})
                  AND company_type NOT IN ('consultant', 'laboratory', 'assessor', 'other')
            """
            result = await self.pool.execute(query, *params)
            count = int(result.split()[-1]) if result else 0
            total_classified += count
            if count > 0:
                logger.info("Classified {} companies as 'other' (non-contractor)", count)

        # ── Pass 2: Companies on reyting.mc.uz with no negative match → 'contractor'
        result = await self.pool.execute("""
            UPDATE companies SET
                company_type = 'contractor',
                updated_at = NOW()
            WHERE rating_score IS NOT NULL
              AND company_type NOT IN ('consultant', 'laboratory', 'assessor', 'other')
              AND company_type != 'contractor'
        """)
        count = int(result.split()[-1]) if result else 0
        total_classified += count
        logger.info("Classified {} rated companies as 'contractor'", count)

        # ── Pass 3: Remaining with tender wins → stay 'unknown'
        unknown_count = await self.pool.fetchval("""
            SELECT COUNT(*) FROM companies
            WHERE company_type = 'unknown' AND total_wins > 0
        """)
        if unknown_count:
            logger.info("{} unrated companies with tender wins remain as 'unknown'", unknown_count)

        logger.info("Classification complete: {} companies updated", total_classified)
        return total_classified

    async def verify_classification(self) -> None:
        """Log verification stats after classification."""
        # Distribution by type
        rows = await self.pool.fetch("""
            SELECT company_type, COUNT(*) as cnt
            FROM companies
            GROUP BY company_type
            ORDER BY cnt DESC
        """)
        logger.info("=== Company type distribution ===")
        for r in rows:
            logger.info("  {:15s} {:,}", r["company_type"], r["cnt"])

        # Top 10 non-contractor companies by wins (eyeball check)
        rows = await self.pool.fetch("""
            SELECT canonical_name, stir, company_type, total_wins, total_contract_value
            FROM companies
            WHERE company_type IN ('consultant', 'laboratory', 'assessor', 'other')
              AND total_wins > 0
            ORDER BY total_wins DESC
            LIMIT 10
        """)
        if rows:
            logger.info("=== Top non-contractors by tender wins (sanity check) ===")
            for r in rows:
                logger.info("  [{:11s}] {:40s} STIR={} wins={} value={:,.0f}",
                            r["company_type"], r["canonical_name"],
                            r["stir"], r["total_wins"],
                            float(r["total_contract_value"] or 0))

        # High-impact reclassifications (>50 wins)
        rows = await self.pool.fetch("""
            SELECT canonical_name, stir, company_type, total_wins
            FROM companies
            WHERE company_type IN ('consultant', 'laboratory', 'assessor', 'other')
              AND total_wins > 50
            ORDER BY total_wins DESC
        """)
        if rows:
            logger.info("=== HIGH-IMPACT: Non-contractors with >50 wins ===")
            for r in rows:
                logger.info("  [{:11s}] {:40s} wins={}", r["company_type"],
                            r["canonical_name"], r["total_wins"])

    # ── Region enrichment ─────────────────────────────────────

    async def fill_missing_regions(self) -> int:
        """Fill NULL regions on tender_results using multiple fallback layers.

        Layer 1: Normalize existing region values (Latin→Cyrillic canonical).
        Layer 2: Provider company's region (biggest win: ~60% of NULLs).
        Layer 3: Text search customer_name + deal_description for region/district patterns.

        Returns total rows updated.
        """
        total = 0

        # ── Layer 1: Normalize existing region values ──
        norm_map = config.region_normalization
        for variant, canonical in norm_map.items():
            result = await self.pool.execute(
                "UPDATE tender_results SET region = $1 WHERE region = $2",
                canonical, variant,
            )
            count = int(result.split()[-1]) if result else 0
            total += count
        # Also normalize on companies table
        for variant, canonical in norm_map.items():
            await self.pool.execute(
                "UPDATE companies SET region = $1 WHERE region = $2",
                canonical, variant,
            )
        logger.info("Layer 1 (normalize): {} tender region values canonicalized", total)

        # ── Layer 2: Fill from provider company's region ──
        result = await self.pool.execute("""
            UPDATE tender_results t SET region = c.region
            FROM companies c
            WHERE t.provider_stir = c.stir
              AND t.region IS NULL
              AND c.region IS NOT NULL
        """)
        layer2 = int(result.split()[-1]) if result else 0
        total += layer2
        logger.info("Layer 2 (provider region): {} tenders filled", layer2)

        # ── Layer 3: Text extraction from customer_name + deal_description ──
        # Check region names, Russian oblast names, and district/city names
        all_patterns: dict[str, str] = {}
        # Direct region names
        for region in config.regions:
            canonical = norm_map.get(region, region)
            all_patterns[region] = canonical
        # Russian/district variants
        for pattern, canonical in {**norm_map, **config.district_to_region}.items():
            all_patterns[pattern] = canonical

        layer3 = 0
        for pattern, canonical in all_patterns.items():
            result = await self.pool.execute(
                """
                UPDATE tender_results SET region = $1
                WHERE region IS NULL
                  AND (customer_name ILIKE $2 OR deal_description ILIKE $2)
                """,
                canonical, f"%{pattern}%",
            )
            count = int(result.split()[-1]) if result else 0
            layer3 += count
        total += layer3
        logger.info("Layer 3 (text extraction): {} tenders filled", layer3)

        # Final stats
        null_count = await self.pool.fetchval(
            "SELECT COUNT(*) FROM tender_results WHERE region IS NULL"
        )
        total_count = await self.pool.fetchval(
            "SELECT COUNT(*) FROM tender_results"
        )
        logger.info("Region coverage: {}/{} ({:.1f}%), still NULL: {}",
                     total_count - null_count, total_count,
                     100 * (total_count - null_count) / total_count if total_count else 0,
                     null_count)
        return total

    # ── Tender aggregation ─────────────────────────────────────

    async def aggregate_tender_stats(self, lookback_months: int = 12) -> int:
        """Aggregate tender wins, value, discount into companies table.

        First resets ALL companies to zero, then sets real values for
        companies with tenders in the lookback window.  This prevents
        stale data when companies fall out of the window between runs.
        """
        # Step 1: Reset all companies to zero so stale values don't persist
        await self.pool.execute("""
            UPDATE companies SET
                total_wins           = 0,
                total_contract_value = 0,
                avg_discount_pct     = NULL,
                first_tender_date    = NULL,
                last_tender_date     = NULL,
                updated_at           = NOW()
            WHERE total_wins > 0 OR total_contract_value > 0
        """)

        # Step 2: Set real values from tender_results
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
        logger.info("Updated tender stats for {} companies (lookback={}m)", count, lookback_months)
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
