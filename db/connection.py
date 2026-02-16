"""Asyncpg connection pool management."""

from __future__ import annotations

import asyncpg
from loguru import logger

from config import config

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    """Return (and lazily create) the shared connection pool."""
    global _pool
    if _pool is None:
        logger.info("Creating asyncpg pool → {}", config.dsn)
        _pool = await asyncpg.create_pool(
            dsn=config.dsn,
            min_size=2,
            max_size=10,
        )
    return _pool


async def close_pool() -> None:
    """Gracefully close the connection pool."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Connection pool closed")
