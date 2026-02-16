"""Strategic market intelligence queries for UET Construction report."""

from __future__ import annotations

from typing import Any

import asyncpg
import pandas as pd
from loguru import logger


async def get_market_summary(pool: asyncpg.Pool) -> dict[str, Any]:
    """Overall construction tender market metrics."""
    row = await pool.fetchrow("""
        SELECT
            COUNT(*) AS total_tenders,
            COUNT(DISTINCT provider_stir) AS unique_winners,
            COALESCE(SUM(deal_cost), 0)::bigint AS total_volume,
            ROUND(AVG(deal_cost)::numeric, 0)::bigint AS avg_deal_size,
            ROUND(AVG(
                CASE WHEN start_cost > 0
                     THEN (start_cost - deal_cost) / start_cost * 100
                     ELSE 0
                END
            )::numeric, 2) AS avg_discount,
            ROUND(AVG(participants_count)::numeric, 1) AS avg_participants,
            MIN(deal_date) AS earliest_date,
            MAX(deal_date) AS latest_date
        FROM tender_results
    """)
    return dict(row) if row else {}


async def get_market_summary_12m(pool: asyncpg.Pool) -> dict[str, Any]:
    """Last 12 months construction tender market metrics."""
    row = await pool.fetchrow("""
        SELECT
            COUNT(*) AS total_tenders,
            COUNT(DISTINCT provider_stir) AS unique_winners,
            COALESCE(SUM(deal_cost), 0)::bigint AS total_volume,
            ROUND(AVG(deal_cost)::numeric, 0)::bigint AS avg_deal_size,
            ROUND(AVG(
                CASE WHEN start_cost > 0
                     THEN (start_cost - deal_cost) / start_cost * 100
                     ELSE 0
                END
            )::numeric, 2) AS avg_discount,
            ROUND(AVG(participants_count)::numeric, 1) AS avg_participants
        FROM tender_results
        WHERE deal_date >= CURRENT_DATE - INTERVAL '12 months'
    """)
    return dict(row) if row else {}


async def get_monthly_trend(pool: asyncpg.Pool) -> pd.DataFrame:
    """Monthly tender volume and count (last 12 months)."""
    rows = await pool.fetch("""
        SELECT
            TO_CHAR(DATE_TRUNC('month', deal_date), 'YYYY-MM') AS "Месяц",
            COUNT(*) AS "Тендеров",
            SUM(deal_cost)::bigint AS "Объём (UZS)"
        FROM tender_results
        WHERE deal_date >= CURRENT_DATE - INTERVAL '12 months'
        GROUP BY DATE_TRUNC('month', deal_date)
        ORDER BY DATE_TRUNC('month', deal_date)
    """)
    return pd.DataFrame([dict(r) for r in rows])


async def get_regional_distribution(pool: asyncpg.Pool) -> pd.DataFrame:
    """Tenders by region (last 12 months)."""
    rows = await pool.fetch("""
        SELECT
            COALESCE(region, 'Не определён') AS "Регион",
            COUNT(*) AS "Тендеров",
            SUM(deal_cost)::bigint AS "Объём (UZS)",
            ROUND(AVG(
                CASE WHEN start_cost > 0
                     THEN (start_cost - deal_cost) / start_cost * 100
                     ELSE 0
                END
            )::numeric, 2) AS "Ср. скидка %"
        FROM tender_results
        WHERE deal_date >= CURRENT_DATE - INTERVAL '12 months'
        GROUP BY region
        ORDER BY SUM(deal_cost) DESC
    """)
    return pd.DataFrame([dict(r) for r in rows])


async def get_top_customers(pool: asyncpg.Pool, limit: int = 10) -> pd.DataFrame:
    """Biggest construction buyers (last 12 months)."""
    rows = await pool.fetch("""
        SELECT
            customer_name AS "Заказчик",
            COUNT(*) AS "Тендеров",
            SUM(deal_cost)::bigint AS "Объём (UZS)",
            ROUND(AVG(
                CASE WHEN start_cost > 0
                     THEN (start_cost - deal_cost) / start_cost * 100
                     ELSE 0
                END
            )::numeric, 2) AS "Ср. скидка %"
        FROM tender_results
        WHERE deal_date >= CURRENT_DATE - INTERVAL '12 months'
        GROUP BY customer_name
        ORDER BY SUM(deal_cost) DESC
        LIMIT $1
    """, limit)
    return pd.DataFrame([dict(r) for r in rows])


async def get_tashkent_customers(pool: asyncpg.Pool, limit: int = 10) -> pd.DataFrame:
    """Biggest construction buyers in Tashkent (last 12 months)."""
    rows = await pool.fetch("""
        SELECT
            customer_name AS "Заказчик",
            COUNT(*) AS "Тендеров",
            SUM(deal_cost)::bigint AS "Объём (UZS)",
            ROUND(AVG(
                CASE WHEN start_cost > 0
                     THEN (start_cost - deal_cost) / start_cost * 100
                     ELSE 0
                END
            )::numeric, 2) AS "Ср. скидка %"
        FROM tender_results
        WHERE deal_date >= CURRENT_DATE - INTERVAL '12 months'
          AND (region ILIKE '%toshkent%' OR region ILIKE '%тошкент%' OR region ILIKE '%ташкент%')
        GROUP BY customer_name
        ORDER BY SUM(deal_cost) DESC
        LIMIT $1
    """, limit)
    return pd.DataFrame([dict(r) for r in rows])


# ── UET-specific queries ──────────────────────────────────


async def get_uet_profile(pool: asyncpg.Pool, stir: str) -> dict[str, Any]:
    """Full UET company data."""
    row = await pool.fetchrow("""
        SELECT
            canonical_name, stir, region, rating_letter, rating_score,
            total_wins, total_contract_value, avg_discount_pct,
            employee_count, specialist_count, source
        FROM companies WHERE stir = $1
    """, stir)
    return dict(row) if row else {}


async def get_uet_rating_percentile(pool: asyncpg.Pool, score: float) -> dict[str, Any]:
    """How UET ranks among all rated companies."""
    row = await pool.fetchrow("""
        SELECT
            (SELECT COUNT(*) FROM companies WHERE rating_score IS NOT NULL) AS total_rated,
            (SELECT COUNT(*) FROM companies WHERE rating_score IS NOT NULL AND rating_score >= $1) AS at_or_above,
            (SELECT COUNT(*) FROM companies WHERE rating_score IS NOT NULL AND rating_score > $1) AS strictly_above
    """, score)
    return dict(row) if row else {}


async def get_rating_distribution(pool: asyncpg.Pool) -> pd.DataFrame:
    """How many companies per rating letter."""
    rows = await pool.fetch("""
        SELECT
            rating_letter AS "Рейтинг",
            COUNT(*) AS "Компаний",
            ROUND(AVG(rating_score)::numeric, 2) AS "Ср. балл",
            MIN(rating_score) AS "Мин.",
            MAX(rating_score) AS "Макс."
        FROM companies
        WHERE rating_letter IS NOT NULL
        GROUP BY rating_letter
        ORDER BY AVG(rating_score) DESC
    """)
    return pd.DataFrame([dict(r) for r in rows])


async def get_uet_rating_breakdown(pool: asyncpg.Pool, stir: str) -> pd.DataFrame:
    """UET's earned vs max points by category."""
    rows = await pool.fetch("""
        SELECT
            rc.name_ru AS "Категория",
            ROUND(SUM(cr.earned_points)::numeric, 2) AS "UET баллы",
            ROUND(SUM(cr.max_points)::numeric, 2) AS "Макс. баллы",
            ROUND(SUM(cr.earned_points) / NULLIF(SUM(cr.max_points), 0) * 100, 1) AS "UET %"
        FROM company_ratings cr
        JOIN rating_criteria rk ON cr.criterion_id = rk.id
        JOIN rating_categories rc ON rk.category_id = rc.id
        WHERE cr.company_stir = $1
        GROUP BY rc.id, rc.name_ru, rc.display_order
        ORDER BY rc.display_order
    """, stir)
    return pd.DataFrame([dict(r) for r in rows])


async def get_top50_benchmark(pool: asyncpg.Pool) -> pd.DataFrame:
    """Average scores by category for top 50 rated companies (benchmark)."""
    rows = await pool.fetch("""
        SELECT
            rc.name_ru AS "Категория",
            ROUND(AVG(cat_sum.earned)::numeric, 2) AS "Топ-50 ср. баллы",
            ROUND(AVG(cat_sum.max_pts)::numeric, 2) AS "Макс. баллы",
            ROUND(AVG(cat_sum.earned / NULLIF(cat_sum.max_pts, 0) * 100)::numeric, 1) AS "Топ-50 %"
        FROM (
            SELECT cr.company_stir, rk.category_id,
                   SUM(cr.earned_points) AS earned, SUM(cr.max_points) AS max_pts
            FROM company_ratings cr
            JOIN rating_criteria rk ON cr.criterion_id = rk.id
            WHERE cr.company_stir IN (
                SELECT stir FROM companies ORDER BY rating_score DESC NULLS LAST LIMIT 50
            )
            GROUP BY cr.company_stir, rk.category_id
        ) cat_sum
        JOIN rating_categories rc ON cat_sum.category_id = rc.id
        GROUP BY rc.id, rc.name_ru, rc.display_order
        ORDER BY rc.display_order
    """)
    return pd.DataFrame([dict(r) for r in rows])


async def get_uet_rating_detail(pool: asyncpg.Pool, stir: str) -> pd.DataFrame:
    """All individual indicators for UET."""
    rows = await pool.fetch("""
        SELECT
            rc.name_ru AS "Категория",
            rk.name_uz AS "Показатель",
            cr.raw_value AS "Значение",
            cr.earned_points AS "Баллы",
            cr.max_points AS "Макс."
        FROM company_ratings cr
        JOIN rating_criteria rk ON cr.criterion_id = rk.id
        JOIN rating_categories rc ON rk.category_id = rc.id
        WHERE cr.company_stir = $1
        ORDER BY rc.display_order, cr.max_points DESC NULLS LAST
    """, stir)
    return pd.DataFrame([dict(r) for r in rows])


# ── Competitor analysis ───────────────────────────────────


async def get_tashkent_competitors(pool: asyncpg.Pool, limit: int = 15) -> pd.DataFrame:
    """Top companies active in Tashkent tenders."""
    rows = await pool.fetch("""
        SELECT
            c.canonical_name AS "Компания",
            c.stir AS "СТИР",
            c.rating_letter AS "Рейтинг",
            c.rating_score AS "Балл",
            COUNT(t.deal_id) AS "Побед (Ташкент)",
            SUM(t.deal_cost)::bigint AS "Объём (UZS)",
            c.employee_count AS "Сотрудники",
            ROUND(AVG(
                CASE WHEN t.start_cost > 0
                     THEN (t.start_cost - t.deal_cost) / t.start_cost * 100
                     ELSE 0
                END
            )::numeric, 2) AS "Ср. скидка %"
        FROM companies c
        JOIN tender_results t ON c.stir = t.provider_stir
        WHERE t.region ILIKE '%toshkent%' OR t.region ILIKE '%тошкент%'
              OR t.region ILIKE '%ташкент%'
        GROUP BY c.stir, c.canonical_name, c.rating_letter, c.rating_score,
                 c.employee_count
        ORDER BY COUNT(t.deal_id) DESC
        LIMIT $1
    """, limit)
    return pd.DataFrame([dict(r) for r in rows])


async def get_top_companies_overall(pool: asyncpg.Pool, limit: int = 15) -> pd.DataFrame:
    """Top N companies nationally by tender wins (last 12 months)."""
    rows = await pool.fetch("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY c.total_wins DESC) AS "№",
            c.canonical_name AS "Компания",
            c.stir AS "СТИР",
            c.region AS "Регион",
            c.rating_letter AS "Рейтинг",
            c.rating_score AS "Балл",
            c.total_wins AS "Побед",
            c.total_contract_value::bigint AS "Объём (UZS)",
            c.avg_discount_pct AS "Ср. скидка %",
            c.employee_count AS "Сотрудники"
        FROM companies c
        WHERE c.total_wins > 0
        ORDER BY c.total_wins DESC
        LIMIT $1
    """, limit)
    return pd.DataFrame([dict(r) for r in rows])


async def get_peer_comparison(
    pool: asyncpg.Pool,
    uet_stir: str,
    stirs: list[str],
) -> pd.DataFrame:
    """Side-by-side comparison of UET vs specific competitors."""
    all_stirs = [uet_stir] + [s for s in stirs if s != uet_stir]
    rows = await pool.fetch("""
        SELECT
            c.canonical_name AS "Компания",
            c.stir AS "СТИР",
            c.region AS "Регион",
            c.rating_letter AS "Рейтинг",
            c.rating_score AS "Балл рейтинга",
            c.total_wins AS "Побед (тендеры)",
            c.total_contract_value::bigint AS "Объём контрактов (UZS)",
            c.avg_discount_pct AS "Ср. скидка %",
            c.employee_count AS "Сотрудники",
            c.specialist_count AS "Специалисты"
        FROM companies c
        WHERE c.stir = ANY($1)
        ORDER BY array_position($1, c.stir)
    """, all_stirs)
    return pd.DataFrame([dict(r) for r in rows])


async def get_peer_rating_comparison(
    pool: asyncpg.Pool,
    stirs: list[str],
) -> pd.DataFrame:
    """Rating category comparison across companies."""
    rows = await pool.fetch("""
        SELECT
            c.canonical_name AS company,
            rc.name_ru AS category,
            ROUND(SUM(cr.earned_points)::numeric, 2) AS earned,
            ROUND(SUM(cr.max_points)::numeric, 2) AS max_pts
        FROM company_ratings cr
        JOIN rating_criteria rk ON cr.criterion_id = rk.id
        JOIN rating_categories rc ON rk.category_id = rc.id
        JOIN companies c ON cr.company_stir = c.stir
        WHERE cr.company_stir = ANY($1)
        GROUP BY c.canonical_name, rc.id, rc.name_ru, rc.display_order
        ORDER BY rc.display_order, c.canonical_name
    """, stirs)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    pivot = df.pivot_table(
        index="category", columns="company", values="earned", aggfunc="sum"
    )
    return pivot


# ── Opportunities ─────────────────────────────────────────


async def get_big_tashkent_tenders(pool: asyncpg.Pool, limit: int = 15) -> pd.DataFrame:
    """Biggest recent Tashkent tenders — opportunities UET missed."""
    rows = await pool.fetch("""
        SELECT
            t.deal_date AS "Дата",
            t.customer_name AS "Заказчик",
            t.deal_description AS "Описание",
            t.start_cost::bigint AS "Нач. цена (UZS)",
            t.deal_cost::bigint AS "Цена контракта (UZS)",
            t.provider_name AS "Победитель",
            c.rating_letter AS "Рейтинг победителя",
            t.participants_count AS "Участники"
        FROM tender_results t
        LEFT JOIN companies c ON t.provider_stir = c.stir
        WHERE (t.region ILIKE '%toshkent%' OR t.region ILIKE '%тошкент%'
               OR t.region ILIKE '%ташкент%')
          AND t.deal_date >= CURRENT_DATE - INTERVAL '6 months'
          AND t.deal_cost > 500000000
        ORDER BY t.deal_cost DESC
        LIMIT $1
    """, limit)
    df = pd.DataFrame([dict(r) for r in rows])
    if not df.empty and "Описание" in df.columns:
        df["Описание"] = df["Описание"].str[:120]
    return df


async def get_high_rated_without_etender(pool: asyncpg.Pool) -> dict[str, Any]:
    """How many high-rated companies participate in etender."""
    rows = await pool.fetch("""
        SELECT
            rating_letter AS letter,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE total_wins > 0) AS with_etender
        FROM companies
        WHERE rating_letter IN ('A', 'BBB', 'BB', 'B', 'CCC', 'CC')
        GROUP BY rating_letter
        ORDER BY MIN(rating_score) DESC
    """)
    return {r["letter"]: {"total": r["total"], "with_etender": r["with_etender"]} for r in rows}


async def get_uet_competitiveness_gaps(pool: asyncpg.Pool, stir: str) -> pd.DataFrame:
    """Specific competitiveness indicators where UET scores 0 but could improve."""
    rows = await pool.fetch("""
        SELECT
            rk.name_uz AS "Показатель",
            cr.raw_value AS "Текущее значение",
            cr.earned_points AS "Текущие баллы",
            cr.max_points AS "Макс. баллы",
            cr.max_points - COALESCE(cr.earned_points, 0) AS "Потенциал роста"
        FROM company_ratings cr
        JOIN rating_criteria rk ON cr.criterion_id = rk.id
        JOIN rating_categories rc ON rk.category_id = rc.id
        WHERE cr.company_stir = $1
          AND rc.code = 'competitiveness'
          AND cr.max_points > 0
        ORDER BY (cr.max_points - COALESCE(cr.earned_points, 0)) DESC
    """, stir)
    df = pd.DataFrame([dict(r) for r in rows])
    # Truncate long indicator names in Python (safe for multi-byte)
    if not df.empty and "Показатель" in df.columns:
        df["Показатель"] = df["Показатель"].str[:80]
    return df
