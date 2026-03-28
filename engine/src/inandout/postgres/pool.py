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


async def create_read_pool(config: DatabaseConfig) -> AsyncConnectionPool | None:
    """Create and open a pool pointing at the read replica, or None if not configured."""
    if not config.read_replica_dsn:
        return None
    pool = AsyncConnectionPool(
        conninfo=config.read_replica_dsn,
        min_size=config.max_idle_conns,
        max_size=config.max_open_conns,
        max_lifetime=_parse_lifetime_seconds(config.conn_max_lifetime),
        open=False,
    )
    await pool.open()
    return pool
