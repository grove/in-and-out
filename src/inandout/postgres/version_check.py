"""Schema version check at daemon startup (B7).

Queries the alembic_version table to confirm the deployed database schema
matches what the tool expects. Raises SchemaVersionMismatch on mismatch.
"""
from __future__ import annotations

import structlog
from psycopg_pool import AsyncConnectionPool

logger = structlog.get_logger(__name__)

# Current number of migration files (001 – 018)
SCHEMA_VERSION: int = 18


class SchemaVersionMismatch(Exception):
    """Raised when the database schema version does not match the expected version."""

    def __init__(self, current: str | None, expected: int) -> None:
        self.current = current
        self.expected = expected
        super().__init__(
            f"Database schema version {current!r} does not match tool expected version "
            f"{expected}. Run 'inandout db upgrade' first."
        )


async def check_schema_version(
    pool: AsyncConnectionPool,
    expected_version: int = SCHEMA_VERSION,
) -> None:
    """Query alembic_version; raise SchemaVersionMismatch if mismatch.

    Parameters
    ----------
    pool:
        The connection pool to use.
    expected_version:
        The migration revision number the tool was built against.
        Defaults to SCHEMA_VERSION.

    Raises
    ------
    SchemaVersionMismatch
        If the current version doesn't match *expected_version*.
    RuntimeError
        If the alembic_version table cannot be read (migrations not run).
    """
    try:
        async with pool.connection() as conn:
            row = await (await conn.execute(
                "SELECT version_num FROM alembic_version LIMIT 1"
            )).fetchone()
    except Exception as exc:
        raise RuntimeError(
            f"Could not read alembic_version table: {exc}. "
            "Run 'inandout db upgrade' first."
        ) from exc

    current_raw: str | None = row[0] if row else None

    # alembic version_num has format "018_20260323" — extract numeric prefix
    current_num: int | None = None
    if current_raw is not None:
        try:
            current_num = int(current_raw.split("_")[0])
        except (ValueError, IndexError):
            pass

    if current_num != expected_version:
        raise SchemaVersionMismatch(current=current_raw, expected=expected_version)

    logger.debug("schema_version_ok", version=current_raw)
