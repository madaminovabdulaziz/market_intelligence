"""CLI entry point for the Construction Market Intelligence Platform."""

from __future__ import annotations

import asyncio
import sys
from typing import Optional

import typer
from loguru import logger

# Configure loguru: remove default, add colored stderr handler
logger.remove()
logger.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}")

app = typer.Typer(
    name="market-intel",
    help="Construction Market Intelligence Platform — Uzbekistan",
)


def _run(coro):
    """Run an async coroutine from sync context."""
    return asyncio.run(coro)


# ── Commands ─────────────────────────────────────────────────


@app.command()
def init_db():
    """Create database tables (runs schema.sql via psql)."""
    import subprocess
    from config import config

    schema_path = "db/schema.sql"
    seed_path = "db/seed.sql"

    env = {
        "PGPASSWORD": config.db_password,
        "PGHOST": config.db_host,
        "PGPORT": str(config.db_port),
        "PGUSER": config.db_user,
        "PGDATABASE": config.db_name,
    }

    for path, label in [(schema_path, "schema"), (seed_path, "seed")]:
        logger.info("Running {} …", label)
        result = subprocess.run(
            ["psql", "-f", path],
            capture_output=True, text=True, env={**dict(__import__("os").environ), **env},
        )
        if result.returncode != 0:
            logger.error("psql {} failed:\n{}", label, result.stderr)
            raise typer.Exit(1)
        logger.info("{} applied successfully", label)


@app.command()
def discover():
    """Probe APIs to discover response formats. Run this first."""
    async def _discover():
        from db.connection import close_pool, get_pool
        from scrapers.etender import ETenderScraper
        from scrapers.reyting import ReytingScraper

        pool = await get_pool()

        logger.info("=== Discovering ETender API ===")
        et = ETenderScraper(pool)
        try:
            info = await et.discover_format()
            typer.echo(f"\nETender: total_count={info['total_count']}, "
                       f"fields={info['fields']}")
        finally:
            await et.close()

        logger.info("\n=== Discovering Reyting API ===")
        rt = ReytingScraper(pool)
        try:
            resp = await rt.http_get(
                "https://japi-reyting.mc.uz/api/v2/category/all",
                params={"type": 0, "page": 1, "perPage": 1},
            )
            data = resp.json()
            total = data.get("data", {}).get("total", 0)
            typer.echo(f"\nReyting API: working, {total} companies in type=0")
        finally:
            await rt.close()

        await close_pool()

    _run(_discover())


@app.command()
def scrape(
    source: str = typer.Argument(
        ..., help="'etender', 'reyting', or 'all'",
    ),
    max_pages: Optional[int] = typer.Option(
        None, help="Limit pages for etender (for testing)",
    ),
    stir_limit: int = typer.Option(
        200, help="Max companies to scrape from reyting",
    ),
):
    """Run scrapers. Use 'all' for both sources."""
    async def _scrape():
        from db.connection import close_pool, get_pool
        from scrapers.etender import ETenderScraper
        from scrapers.reyting import ReytingScraper

        pool = await get_pool()

        if source in ("etender", "all"):
            logger.info("=== Scraping ETender ===")
            et = ETenderScraper(pool)
            try:
                stats = await et.scrape_all(max_pages=max_pages)
                typer.echo(f"\nETender: {stats}")
            finally:
                await et.close()

        if source in ("reyting", "all"):
            logger.info("=== Scraping Reyting ===")
            rt = ReytingScraper(pool)
            try:
                stats = await rt.scrape_companies(limit=stir_limit)
                typer.echo(f"\nReyting: {stats}")
            finally:
                await rt.close()

        await close_pool()

    _run(_scrape())


@app.command()
def enrich(
    months: int = typer.Option(12, help="Lookback period in months"),
):
    """Run enrichment pipeline to aggregate statistics."""
    async def _enrich():
        from db.connection import close_pool, get_pool
        from pipeline.enrich import EnrichmentPipeline

        pool = await get_pool()
        pipeline = EnrichmentPipeline(pool)
        results = await pipeline.run(lookback_months=months)
        typer.echo(f"\nEnrichment: {results}")
        await close_pool()

    _run(_enrich())


@app.command()
def classify():
    """Classify companies into types (contractor, consultant, laboratory, etc.).

    Runs the classification step only, without full enrichment.
    Useful for re-classifying after keyword list changes.
    """
    async def _classify():
        from db.connection import close_pool, get_pool
        from pipeline.enrich import EnrichmentPipeline

        pool = await get_pool()
        pipeline = EnrichmentPipeline(pool)
        count = await pipeline.classify_company_types()
        await pipeline.verify_classification()
        typer.echo(f"\nClassification complete: {count} companies updated")
        await close_pool()

    _run(_classify())


@app.command()
def analyze(
    report: str = typer.Argument(
        ..., help="'top15', 'profile', 'compare', 'market', 'position', 'search'",
    ),
    stir: Optional[list[str]] = typer.Option(
        None, help="STIR(s) for profile/compare/position",
    ),
    search: Optional[str] = typer.Option(
        None, help="Search term for 'search' report",
    ),
):
    """Run analysis queries and print results."""
    async def _analyze():
        from db.connection import close_pool, get_pool

        from analysis.company_profile import get_company_profile
        from analysis.comparison import compare_companies
        from analysis.rankings import (
            find_company_by_name,
            get_company_position,
            get_market_overview,
            get_top_companies,
        )

        pool = await get_pool()

        if report == "top15":
            df = await get_top_companies(pool)
            typer.echo(df.to_string(index=False))

        elif report == "market":
            overview = await get_market_overview(pool)
            typer.echo("\n=== Сводка ===")
            for k, v in overview.get("summary", {}).items():
                typer.echo(f"  {k}: {v}")
            typer.echo("\n=== По регионам ===")
            typer.echo(overview.get("by_region", "Нет данных").to_string(index=False))
            typer.echo("\n=== Ежемесячный тренд ===")
            typer.echo(overview.get("monthly_trend", "Нет данных").to_string(index=False))

        elif report == "profile":
            if not stir:
                typer.echo("Укажите --stir")
                raise typer.Exit(1)
            profile = await get_company_profile(pool, stir[0])
            info = profile.get("info", {})
            typer.echo(f"\n=== {info.get('canonical_name', 'N/A')} ({stir[0]}) ===")
            for k, v in info.items():
                typer.echo(f"  {k}: {v}")
            typer.echo("\n--- Крупнейшие контракты ---")
            typer.echo(profile.get("top_contracts", "Нет данных").to_string(index=False))
            typer.echo("\n--- Рейтинг ---")
            typer.echo(profile.get("rating_breakdown", "Нет данных").to_string(index=False))

        elif report == "compare":
            if not stir or len(stir) < 2:
                typer.echo("Укажите минимум 2 СТИР через --stir")
                raise typer.Exit(1)
            data = await compare_companies(pool, stir)
            typer.echo("\n=== Сводка ===")
            typer.echo(data.get("summary", "Нет данных").to_string(index=False))
            typer.echo("\n=== Рейтинг по категориям ===")
            typer.echo(str(data.get("rating_comparison", "Нет данных")))
            typer.echo("\n=== Общие заказчики ===")
            typer.echo(data.get("common_customers", "Нет данных").to_string(index=False))

        elif report == "position":
            if not stir:
                typer.echo("Укажите --stir")
                raise typer.Exit(1)
            df = await get_company_position(pool, stir[0])
            typer.echo(df.to_string(index=False))

        elif report == "search":
            term = search or (stir[0] if stir else "")
            if not term:
                typer.echo("Укажите --search или --stir")
                raise typer.Exit(1)
            df = await find_company_by_name(pool, term)
            typer.echo(df.to_string(index=False))

        else:
            typer.echo(f"Неизвестный отчёт: {report}")
            raise typer.Exit(1)

        await close_pool()

    _run(_analyze())


@app.command()
def export(
    output: str = typer.Option("report.xlsx", help="Output file path"),
    uet_stir: str = typer.Option("310382944", help="UET's STIR for profile/positioning"),
    compare: Optional[list[str]] = typer.Option(None, help="STIRs for comparison sheet"),
):
    """Generate strategic intelligence Excel report for UET."""
    async def _export():
        from db.connection import close_pool, get_pool
        from export.to_excel import ExcelReportGenerator

        pool = await get_pool()
        gen = ExcelReportGenerator(pool)
        path = await gen.generate_full_report(
            output_path=output,
            uet_stir=uet_stir,
            compare_stirs=compare,
        )
        typer.echo(f"\nReport saved: {path}")
        await close_pool()

    _run(_export())


@app.command()
def run_all(
    max_pages: Optional[int] = typer.Option(None, help="Limit etender pages"),
    stir_limit: int = typer.Option(200, help="Max companies for reyting"),
    output: str = typer.Option("report.xlsx", help="Output file path"),
    uet_stir: Optional[str] = typer.Option(None, help="UET's STIR"),
    compare: Optional[list[str]] = typer.Option(None, help="STIRs for comparison"),
):
    """Full pipeline: discover → scrape → enrich → export."""
    async def _run_all():
        from db.connection import close_pool, get_pool
        from export.to_excel import ExcelReportGenerator
        from pipeline.enrich import EnrichmentPipeline
        from scrapers.etender import ETenderScraper
        from scrapers.reyting import ReytingScraper

        pool = await get_pool()

        # Step 1: Discover
        logger.info("=== Step 1/5: API Discovery ===")
        et = ETenderScraper(pool)
        await et.discover_format()

        # Step 2: Scrape ETender
        logger.info("=== Step 2/5: Scraping ETender ===")
        try:
            et_stats = await et.scrape_all(max_pages=max_pages)
            typer.echo(f"ETender: {et_stats}")
        finally:
            await et.close()

        # Step 3: Scrape Reyting
        logger.info("=== Step 3/5: Scraping Reyting ===")
        rt = ReytingScraper(pool)
        try:
            rt_stats = await rt.scrape_companies(limit=stir_limit)
            typer.echo(f"Reyting: {rt_stats}")
        finally:
            await rt.close()

        # Step 4: Enrich
        logger.info("=== Step 4/5: Enrichment ===")
        pipeline = EnrichmentPipeline(pool)
        enrich_result = await pipeline.run()
        typer.echo(f"Enrichment: {enrich_result}")

        # Step 5: Export
        logger.info("=== Step 5/5: Excel Export ===")
        gen = ExcelReportGenerator(pool)
        path = await gen.generate_full_report(
            output_path=output,
            uet_stir=uet_stir,
            compare_stirs=compare,
        )
        typer.echo(f"\nDone! Report saved: {path}")

        await close_pool()

    _run(_run_all())


if __name__ == "__main__":
    app()
