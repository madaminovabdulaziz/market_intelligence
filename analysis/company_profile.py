"""Single company deep-dive profile."""

from __future__ import annotations

from typing import Any

import asyncpg
import pandas as pd
from loguru import logger


async def get_company_profile(
    pool: asyncpg.Pool,
    stir: str,
) -> dict[str, Any]:
    """Full profile card for a company: basic info, top contracts, ratings, monthly activity."""
    result: dict[str, Any] = {}

    # Basic info
    row = await pool.fetchrow(
        """
        SELECT
            canonical_name, stir, region, rating_letter, rating_score,
            total_wins, total_contract_value, avg_discount_pct,
            employee_count, specialist_count, first_tender_date, last_tender_date,
            active_regions, source
        FROM companies
        WHERE stir = $1
        """,
        stir,
    )
    if not row:
        logger.warning("Company not found: STIR={}", stir)
        return result
    result["info"] = dict(row)

    # Top 10 largest contracts
    rows = await pool.fetch(
        """
        SELECT
            deal_date            AS "Дата",
            customer_name        AS "Заказчик",
            deal_description     AS "Описание",
            start_cost           AS "Начальная цена (UZS)",
            deal_cost            AS "Цена контракта (UZS)",
            discount_pct         AS "Скидка %",
            participants_count   AS "Участники"
        FROM tender_results
        WHERE provider_stir = $1
        ORDER BY deal_cost DESC
        LIMIT 10
        """,
        stir,
    )
    result["top_contracts"] = pd.DataFrame([dict(r) for r in rows])

    # Rating breakdown by category
    rows = await pool.fetch(
        """
        SELECT
            rc.name_ru                          AS "Категория",
            ROUND(SUM(cr.earned_points), 2)     AS "Баллы",
            ROUND(SUM(cr.max_points), 2)        AS "Макс. баллы",
            CASE WHEN SUM(cr.max_points) > 0
                 THEN ROUND(SUM(cr.earned_points) / SUM(cr.max_points) * 100, 1)
                 ELSE 0
            END                                 AS "Процент %"
        FROM company_ratings cr
        JOIN rating_criteria rk ON cr.criterion_id = rk.id
        JOIN rating_categories rc ON rk.category_id = rc.id
        WHERE cr.company_stir = $1
        GROUP BY rc.id, rc.name_ru, rc.display_order
        ORDER BY rc.display_order
        """,
        stir,
    )
    result["rating_breakdown"] = pd.DataFrame([dict(r) for r in rows])

    # Detailed indicators
    rows = await pool.fetch(
        """
        SELECT
            rc.name_ru          AS "Категория",
            rk.name_uz          AS "Показатель",
            cr.raw_value        AS "Значение",
            cr.earned_points    AS "Баллы",
            cr.max_points       AS "Макс."
        FROM company_ratings cr
        JOIN rating_criteria rk ON cr.criterion_id = rk.id
        JOIN rating_categories rc ON rk.category_id = rc.id
        WHERE cr.company_stir = $1
        ORDER BY rc.display_order, rk.display_order
        """,
        stir,
    )
    result["indicators"] = pd.DataFrame([dict(r) for r in rows])

    # Monthly tender activity (last 12 months)
    rows = await pool.fetch(
        """
        SELECT
            TO_CHAR(DATE_TRUNC('month', deal_date), 'YYYY-MM') AS "Месяц",
            COUNT(*)                      AS "Кол-во тендеров",
            COALESCE(SUM(deal_cost), 0)   AS "Объём (UZS)"
        FROM tender_results
        WHERE provider_stir = $1
          AND deal_date >= CURRENT_DATE - INTERVAL '12 months'
        GROUP BY DATE_TRUNC('month', deal_date)
        ORDER BY DATE_TRUNC('month', deal_date)
        """,
        stir,
    )
    result["monthly_activity"] = pd.DataFrame([dict(r) for r in rows])

    # Customers this company works with most
    rows = await pool.fetch(
        """
        SELECT
            customer_name       AS "Заказчик",
            COUNT(*)            AS "Тендеров",
            SUM(deal_cost)      AS "Объём (UZS)"
        FROM tender_results
        WHERE provider_stir = $1
        GROUP BY customer_name
        ORDER BY COUNT(*) DESC
        LIMIT 10
        """,
        stir,
    )
    result["top_customers"] = pd.DataFrame([dict(r) for r in rows])

    logger.info("Profile fetched for {} ({})", result["info"]["canonical_name"], stir)
    return result
