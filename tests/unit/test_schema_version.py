"""Unit tests for schema version check at startup (B7)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from inandout.postgres.version_check import (
    check_schema_version,
    SchemaVersionMismatch,
    SCHEMA_VERSION,
)


@pytest.mark.asyncio
async def test_version_match_no_exception() -> None:
    """When alembic_version matches expected, no exception should be raised."""
    conn = AsyncMock()
    row_result = AsyncMock()
    row_result.fetchone = AsyncMock(return_value=(f"018_20260323",))
    conn.execute = AsyncMock(return_value=row_result)

    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)

    # Should not raise
    await check_schema_version(pool, expected_version=18)


@pytest.mark.asyncio
async def test_version_mismatch_raises_schema_version_mismatch() -> None:
    """When alembic_version doesn't match, SchemaVersionMismatch should be raised."""
    conn = AsyncMock()
    row_result = AsyncMock()
    # Old version
    row_result.fetchone = AsyncMock(return_value=("010_20260323",))
    conn.execute = AsyncMock(return_value=row_result)

    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)

    with pytest.raises(SchemaVersionMismatch) as exc_info:
        await check_schema_version(pool, expected_version=18)

    assert "010_20260323" in str(exc_info.value)
    assert "18" in str(exc_info.value)
    assert "inandout db upgrade" in str(exc_info.value)


@pytest.mark.asyncio
async def test_missing_alembic_table_raises_runtime_error() -> None:
    """When the alembic_version table is missing, a RuntimeError with migration hint."""
    conn = AsyncMock()
    conn.execute = AsyncMock(
        side_effect=Exception("relation \"alembic_version\" does not exist")
    )

    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)

    with pytest.raises(RuntimeError) as exc_info:
        await check_schema_version(pool, expected_version=18)

    assert "inandout db upgrade" in str(exc_info.value)


def test_schema_version_constant() -> None:
    """SCHEMA_VERSION constant should be set to 23."""
    assert SCHEMA_VERSION == 23


def test_schema_version_mismatch_message_helpful() -> None:
    """SchemaVersionMismatch message should be clear and actionable."""
    exc = SchemaVersionMismatch(current="005_20260323", expected=20)
    msg = str(exc)
    assert "005_20260323" in msg
    assert "20" in msg
    assert "inandout db upgrade" in msg
