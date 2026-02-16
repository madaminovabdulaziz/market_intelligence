"""Top companies, market overview, and UET positioning queries."""

from __future__ import annotations

from typing import Any

import asyncpg
import pandas as pd
from loguru import logger

from config import config

# Shared SQL fragment for excluding non-contractor companies from rankings.
# Deals still count toward market volume — only company-level rankings filter.
_CONTRACTOR_FILTER = (
    "c.company_type NOT IN ("
    + ", ".join(f"'{t}'" for t in config.excluded_company_types)
    + ")"
)


async def get_top_companies(
    pool: asyncpg.Pool,
    limit: int = 15,
    lookback_months: int = 12,
) -> pd.DataFrame:
    """Top N companies ranked by tender wins in the lookback period."""
    rows = await pool.fetch(
        f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY c.total_wins DESC)  AS "№",
            c.canonical_name   AS "Компания",
            c.stir             AS "СТИР",
            c.region           AS "Регион",
            c.rating_letter    AS "Рейтинг",
            c.rating_score     AS "Балл",
            c.total_wins       AS "Побед",
            c.total_contract_value AS "Объём (UZS)",
            c.avg_discount_pct AS "Ср. скидка %",
            c.employee_count   AS "Сотрудники"
        FROM companies c
        WHERE c.total_wins > 0
          AND {_CONTRACTOR_FILTER}
        ORDER BY c.total_wins DESC
        LIMIT $1
        """,
        limit,
    )
    df = pd.DataFrame([dict(r) for r in rows])
    logger.info("Top {} companies fetched ({} rows)", limit, len(df))
    return df


async def get_market_overview(
    pool: asyncpg.Pool,
    lookback_months: int = 12,
) -> dict[str, Any]:
    """Market summary metrics, regional distribution, and monthly trends.

    Note: Market volume metrics include ALL deals (including non-contractors),
    because the deals themselves are real construction tenders. The filter
    only applies to company-level rankings.
    """
    result: dict[str, Any] = {}

    # Overall metrics (no company filter — total market size)
    row = await pool.fetchrow(
        """
        SELECT
            COUNT(*)                        AS total_tenders,
            COUNT(DISTINCT provider_stir)   AS unique_winners,
            COALESCE(SUM(deal_cost), 0)     AS total_volume,
            COALESCE(AVG(deal_cost), 0)     AS avg_contract,
            ROUND(AVG(
                CASE WHEN start_cost > 0
                     THEN (start_cost - deal_cost) / start_cost * 100
                END
            ), 2)                           AS avg_discount,
            ROUND(AVG(participants_count)::numeric, 1) AS avg_participants
        FROM tender_results
        WHERE deal_date >= CURRENT_DATE - make_interval(months => $1)
        """,
        lookback_months,
    )
    result["summary"] = dict(row) if row else {}

    # Regional distribution (no company filter)
    rows = await pool.fetch(
        """
        SELECT
            COALESCE(region, 'Не определён') AS "Регион",
            COUNT(*)                          AS "Тендеров",
            COALESCE(SUM(deal_cost), 0)       AS "Объём (UZS)",
            ROUND(AVG(
                CASE WHEN start_cost > 0
                     THEN (start_cost - deal_cost) / start_cost * 100
                END
            ), 2)                             AS "Ср. скидка %"
        FROM tender_results
        WHERE deal_date >= CURRENT_DATE - make_interval(months => $1)
        GROUP BY region
        ORDER BY SUM(deal_cost) DESC
        """,
        lookback_months,
    )
    result["by_region"] = pd.DataFrame([dict(r) for r in rows])

    # Monthly trend (no company filter)
    rows = await pool.fetch(
        """
        SELECT
            TO_CHAR(DATE_TRUNC('month', deal_date), 'YYYY-MM') AS "Месяц",
            COUNT(*)                      AS "Тендеров",
            COALESCE(SUM(deal_cost), 0)   AS "Объём (UZS)"
        FROM tender_results
        WHERE deal_date >= CURRENT_DATE - make_interval(months => $1)
        GROUP BY DATE_TRUNC('month', deal_date)
        ORDER BY DATE_TRUNC('month', deal_date)
        """,
        lookback_months,
    )
    result["monthly_trend"] = pd.DataFrame([dict(r) for r in rows])

    # Top 10 customers (no company filter)
    rows = await pool.fetch(
        """
        SELECT
            customer_name                 AS "Заказчик",
            COUNT(*)                      AS "Тендеров",
            COALESCE(SUM(deal_cost), 0)   AS "Объём (UZS)"
        FROM tender_results
        WHERE deal_date >= CURRENT_DATE - make_interval(months => $1)
        GROUP BY customer_name
        ORDER BY SUM(deal_cost) DESC
        LIMIT 10
        """,
        lookback_months,
    )
    result["top_customers"] = pd.DataFrame([dict(r) for r in rows])

    logger.info("Market overview fetched")
    return result


async def get_company_position(
    pool: asyncpg.Pool,
    stir: str,
) -> pd.DataFrame:
    """Show where a company ranks among all contractors.

    Excludes non-contractor companies (labs, assessors, consultants)
    from the ranking pool so positions are meaningful.
    """
    rows = await pool.fetch(
        f"""
        WITH ranked AS (
            SELECT
                stir,
                canonical_name,
                region,
                rating_letter,
                total_wins,
                total_contract_value,
                rating_score,
                avg_discount_pct,
                employee_count,
                RANK() OVER (ORDER BY total_wins DESC)                     AS rank_wins,
                RANK() OVER (ORDER BY total_contract_value DESC)           AS rank_volume,
                RANK() OVER (ORDER BY rating_score DESC NULLS LAST)        AS rank_rating,
                COUNT(*) OVER ()                                           AS total_companies
            FROM companies c
            WHERE (total_wins > 0 OR rating_score IS NOT NULL)
              AND {_CONTRACTOR_FILTER}
        )
        SELECT
            canonical_name       AS "Компания",
            stir                 AS "СТИР",
            region               AS "Регион",
            rating_letter        AS "Рейтинг",
            rating_score         AS "Балл рейтинга",
            rank_rating          AS "Место (рейтинг)",
            total_wins           AS "Побед",
            rank_wins            AS "Место (побед)",
            total_contract_value AS "Объём (UZS)",
            rank_volume          AS "Место (объём)",
            total_companies      AS "Всего компаний"
        FROM ranked
        WHERE stir = $1
           OR rank_wins <= 10
        ORDER BY rank_wins
        """,
        stir,
    )
    df = pd.DataFrame([dict(r) for r in rows])
    logger.info("Position report for STIR {}: {} rows", stir, len(df))
    return df


async def find_company_by_name(
    pool: asyncpg.Pool,
    search: str,
    limit: int = 10,
) -> pd.DataFrame:
    """Fuzzy search for companies by name. Shows company_type for transparency."""
    rows = await pool.fetch(
        """
        SELECT stir, canonical_name, company_type, total_wins,
               total_contract_value, rating_letter
        FROM companies
        WHERE canonical_name ILIKE $1
           OR raw_names::text ILIKE $1
        ORDER BY total_wins DESC
        LIMIT $2
        """,
        f"%{search}%",
        limit,
    )
    return pd.DataFrame([dict(r) for r in rows])
