"""Async connection pool factory."""
from __future__ import annotations

from psycopg_pool import AsyncConnectionPool
from inandout.config.tool import DatabaseConfig


def _parse_lifetime_seconds(s: str) -> float:
    from inandout.config._duration import parse_duration
    return parse_duration(s)


async def create_pool(config: DatabaseConfig) -> AsyncConnectionPool:
    """Create and open an async psycopg connection pool."""
    pool = AsyncConnectionPool(
        conninfo=config.dsn,
        min_size=config.max_idle_conns,
        max_size=config.max_open_conns,
        max_lifetime=_parse_lifetime_seconds(config.conn_max_lifetime),
        open=False,
    )
    await pool.open()
    return pool
