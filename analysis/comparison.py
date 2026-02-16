"""Head-to-head comparison of 2-3 companies."""

from __future__ import annotations

from typing import Any

import asyncpg
import pandas as pd
from loguru import logger


async def compare_companies(
    pool: asyncpg.Pool,
    stirs: list[str],
) -> dict[str, Any]:
    """Side-by-side comparison of companies by STIRs."""
    if not stirs or len(stirs) < 2:
        logger.warning("Need at least 2 STIRs for comparison")
        return {}

    result: dict[str, Any] = {}

    # Summary metrics side by side
    rows = await pool.fetch(
        """
        SELECT
            canonical_name       AS "Компания",
            stir                 AS "СТИР",
            rating_letter        AS "Рейтинг",
            rating_score         AS "Балл рейтинга",
            total_wins           AS "Побед за 12 мес",
            total_contract_value AS "Объём контрактов (UZS)",
            avg_discount_pct     AS "Ср. скидка %",
            employee_count       AS "Сотрудники",
            specialist_count     AS "Специалисты",
            region               AS "Регион"
        FROM companies
        WHERE stir = ANY($1::varchar[])
        ORDER BY total_wins DESC
        """,
        stirs,
    )
    result["summary"] = pd.DataFrame([dict(r) for r in rows])

    # Rating categories comparison
    rows = await pool.fetch(
        """
        SELECT
            cr.company_stir                     AS "СТИР",
            c.canonical_name                    AS "Компания",
            rc.name_ru                          AS "Категория",
            ROUND(SUM(cr.earned_points), 2)     AS "Баллы",
            ROUND(SUM(cr.max_points), 2)        AS "Макс."
        FROM company_ratings cr
        JOIN rating_criteria rk ON cr.criterion_id = rk.id
        JOIN rating_categories rc ON rk.category_id = rc.id
        JOIN companies c ON cr.company_stir = c.stir
        WHERE cr.company_stir = ANY($1::varchar[])
        GROUP BY cr.company_stir, c.canonical_name, rc.id, rc.name_ru, rc.display_order
        ORDER BY rc.display_order, cr.company_stir
        """,
        stirs,
    )
    if rows:
        df = pd.DataFrame([dict(r) for r in rows])
        # Pivot to get companies as columns
        pivot = df.pivot_table(
            index="Категория",
            columns="Компания",
            values="Баллы",
            aggfunc="first",
        )
        result["rating_comparison"] = pivot
    else:
        result["rating_comparison"] = pd.DataFrame()

    # Common customers (customers served by more than one of the compared companies)
    rows = await pool.fetch(
        """
        SELECT
            customer_name                       AS "Заказчик",
            COUNT(DISTINCT provider_stir)        AS "Кол-во компаний",
            COUNT(*)                             AS "Всего тендеров",
            COALESCE(SUM(deal_cost), 0)          AS "Общий объём (UZS)"
        FROM tender_results
        WHERE provider_stir = ANY($1::varchar[])
        GROUP BY customer_name
        HAVING COUNT(DISTINCT provider_stir) > 1
        ORDER BY COUNT(*) DESC
        LIMIT 20
        """,
        stirs,
    )
    result["common_customers"] = pd.DataFrame([dict(r) for r in rows])

    # Tender activity comparison by month
    rows = await pool.fetch(
        """
        SELECT
            provider_stir                           AS "СТИР",
            c.canonical_name                        AS "Компания",
            TO_CHAR(DATE_TRUNC('month', t.deal_date), 'YYYY-MM') AS "Месяц",
            COUNT(*)                                AS "Тендеров",
            COALESCE(SUM(t.deal_cost), 0)           AS "Объём (UZS)"
        FROM tender_results t
        JOIN companies c ON t.provider_stir = c.stir
        WHERE t.provider_stir = ANY($1::varchar[])
          AND t.deal_date >= CURRENT_DATE - INTERVAL '12 months'
        GROUP BY t.provider_stir, c.canonical_name, DATE_TRUNC('month', t.deal_date)
        ORDER BY DATE_TRUNC('month', t.deal_date), t.provider_stir
        """,
        stirs,
    )
    result["monthly_comparison"] = pd.DataFrame([dict(r) for r in rows])

    logger.info("Comparison fetched for {} companies", len(stirs))
    return result
