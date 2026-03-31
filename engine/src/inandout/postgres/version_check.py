"""Schema version check at daemon startup (B7).

Queries the alembic_version table to confirm the deployed database schema
matches what the tool expects. When the schema-manager hasn't run yet,
retries with exponential backoff instead of hard-failing, giving the
schema-manager time to complete its initial reconcile.
"""
from __future__ import annotations

import asyncio

import structlog
from psycopg_pool import AsyncConnectionPool

logger = structlog.get_logger(__name__)

# Current number of migration files (001 – 020)
SCHEMA_VERSION: int = 25

# Retry settings for waiting on schema-manager
_MAX_RETRIES: int = 30          # 30 retries × ~2-8s = up to ~3 min
_INITIAL_BACKOFF: float = 2.0   # seconds
_MAX_BACKOFF: float = 8.0       # seconds


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

    If the alembic_version table doesn't exist yet (schema-manager hasn't
    run), retries with exponential backoff up to ~3 minutes before giving up.

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
        If the alembic_version table cannot be read after all retries.
    """
    backoff = _INITIAL_BACKOFF
    last_exc: Exception | None = None

    for attempt in range(_MAX_RETRIES + 1):
        try:
            async with pool.connection() as conn:
                row = await (await conn.execute(
                    "SELECT version_num FROM alembic_version LIMIT 1"
                )).fetchone()
        except Exception as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES:
                logger.info(
                    "waiting_for_schema_manager",
                    attempt=attempt + 1,
                    max_retries=_MAX_RETRIES,
                    backoff_secs=backoff,
                    error=str(exc),
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, _MAX_BACKOFF)
                continue
            raise RuntimeError(
                f"Could not read alembic_version table after {_MAX_RETRIES} retries: {exc}. "
                "Ensure the schema-manager is running and has completed its initial reconcile."
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
            if attempt < _MAX_RETRIES:
                logger.info(
                    "schema_version_pending",
                    current=current_raw,
                    expected=expected_version,
                    attempt=attempt + 1,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, _MAX_BACKOFF)
                continue
            raise SchemaVersionMismatch(current=current_raw, expected=expected_version)

        logger.debug("schema_version_ok", version=current_raw)
        return
