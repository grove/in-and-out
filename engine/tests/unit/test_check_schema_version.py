"""Unit tests for check_schema_version in version_check.py."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from inandout.postgres.version_check import (
    SCHEMA_VERSION,
    SchemaVersionMismatch,
    check_schema_version,
)


def _make_pool(version_num: str | None) -> MagicMock:
    """Build a minimal mock AsyncConnectionPool."""
    pool = MagicMock()
    conn = AsyncMock()
    cursor = AsyncMock()
    cursor.fetchone = AsyncMock(
        return_value=(version_num,) if version_num is not None else None
    )
    conn.execute = AsyncMock(return_value=cursor)

    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)
    pool.connection = MagicMock(return_value=cm)
    return pool


async def test_matching_version_does_not_raise():
    version_str = f"{SCHEMA_VERSION:03d}_20260323"
    pool = _make_pool(version_str)
    await check_schema_version(pool, expected_version=SCHEMA_VERSION)


async def test_mismatch_raises_schema_version_mismatch():
    pool = _make_pool("001_20260323")
    with pytest.raises(SchemaVersionMismatch) as exc_info:
        await check_schema_version(pool, expected_version=SCHEMA_VERSION)
    assert exc_info.value.expected == SCHEMA_VERSION


async def test_mismatch_exception_has_current():
    pool = _make_pool("001_20260323")
    with pytest.raises(SchemaVersionMismatch) as exc_info:
        await check_schema_version(pool, expected_version=SCHEMA_VERSION)
    assert exc_info.value.current == "001_20260323"


async def test_none_row_raises_schema_version_mismatch():
    pool = _make_pool(None)
    with pytest.raises(SchemaVersionMismatch):
        await check_schema_version(pool, expected_version=SCHEMA_VERSION)


async def test_db_exception_raises_runtime_error():
    pool = MagicMock()
    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(side_effect=Exception("connection refused"))
    cm.__aexit__ = AsyncMock(return_value=False)
    pool.connection = MagicMock(return_value=cm)
    with pytest.raises(RuntimeError, match="alembic_version"):
        await check_schema_version(pool, expected_version=SCHEMA_VERSION)


async def test_custom_expected_version():
    pool = _make_pool("010_20260323")
    await check_schema_version(pool, expected_version=10)


async def test_version_zero_is_mismatch_for_schema_version():
    pool = _make_pool("000_20260323")
    with pytest.raises(SchemaVersionMismatch):
        await check_schema_version(pool, expected_version=SCHEMA_VERSION)


async def test_unparseable_version_string_raises():
    pool = _make_pool("corrupt_version")
    with pytest.raises(SchemaVersionMismatch):
        await check_schema_version(pool, expected_version=SCHEMA_VERSION)

