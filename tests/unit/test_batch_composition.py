"""Unit tests for T2 #33: batch_max_bytes composition limit in WritebackConfig / _fetch_delta_rows."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# Config field tests
# ---------------------------------------------------------------------------

def _make_writeback_cfg(**kwargs):
    from inandout.config.writeback import (
        ConflictResolution,
        OperationConfig,
        OperationsConfig,
        ProtectionLevel,
        UpdateOperationConfig,
        WritebackConfig,
    )
    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/contacts/${external_id}"),
        insert=OperationConfig(method="POST", path="/contacts"),
        update=UpdateOperationConfig(method="PATCH", path="/contacts/${external_id}"),
    )
    return WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert", "update"],
        operations=ops,
        **kwargs,
    )


def test_batch_max_bytes_default_is_none():
    cfg = _make_writeback_cfg()
    assert cfg.batch_max_bytes is None


def test_batch_max_age_secs_default_is_none():
    cfg = _make_writeback_cfg()
    assert cfg.batch_max_age_secs is None


def test_batch_max_bytes_accepts_positive_int():
    cfg = _make_writeback_cfg(batch_max_bytes=65536)
    assert cfg.batch_max_bytes == 65536


def test_batch_max_age_secs_accepts_positive_float():
    cfg = _make_writeback_cfg(batch_max_age_secs=5.0)
    assert cfg.batch_max_age_secs == 5.0


def test_batch_max_bytes_rejects_zero():
    import pydantic
    with pytest.raises((pydantic.ValidationError, ValueError)):
        _make_writeback_cfg(batch_max_bytes=0)


def test_batch_max_age_secs_accepts_zero():
    # ge=0.0 — zero is allowed
    cfg = _make_writeback_cfg(batch_max_age_secs=0.0)
    assert cfg.batch_max_age_secs == 0.0


# ---------------------------------------------------------------------------
# _fetch_delta_rows enforcement
# ---------------------------------------------------------------------------

def _make_engine():
    from inandout.writeback.engine import WritebackEngine
    engine = WritebackEngine.__new__(WritebackEngine)
    return engine


def _make_result(connector="c", datatype="d", delta_table="t"):
    from inandout.writeback.engine import WritebackResult
    return WritebackResult(connector=connector, datatype=datatype, delta_table=delta_table)


def _make_pool_mock(rows_as_dicts: list[dict]):
    """Build a psycopg-style pool mock that yields rows_as_dicts from execute().fetchall()."""
    if not rows_as_dicts:
        col_names = []
        raw_rows = []
    else:
        col_names = list(rows_as_dicts[0].keys())
        raw_rows = [tuple(r[c] for c in col_names) for r in rows_as_dicts]

    mock_cursor = MagicMock()
    mock_cursor.description = [(c,) for c in col_names] if col_names else None
    mock_cursor.fetchall = AsyncMock(return_value=raw_rows)

    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    mock_pool = MagicMock()
    mock_pool.connection = MagicMock()
    mock_pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)
    return mock_pool


@pytest.mark.asyncio
async def test_fetch_delta_rows_no_limit_returns_all():
    """Without batch_max_bytes, all fetched rows are returned unchanged."""
    engine = _make_engine()
    rows = [{"_ext_id": "1", "name": "Alice"}, {"_ext_id": "2", "name": "Bob"}]
    engine._pool = _make_pool_mock(rows)

    result = _make_result()
    log = MagicMock()
    fetched = await engine._fetch_delta_rows(
        "delta_table", log, result, batch_size=50, batch_max_bytes=None
    )
    assert fetched == rows


@pytest.mark.asyncio
async def test_fetch_delta_rows_batch_max_bytes_trims_rows():
    """With batch_max_bytes set to exactly row1 size, only row1 is returned."""
    import orjson
    engine = _make_engine()

    row1 = {"_ext_id": "1", "_action": "insert", "name": "Alice"}
    row2 = {"_ext_id": "2", "_action": "insert", "name": "Bob"}
    rows = [row1, row2]
    engine._pool = _make_pool_mock(rows)

    # Compute the payload bytes for row1 only (strips _ prefix keys)
    row1_payload = {k: v for k, v in row1.items() if not k.startswith("_")}
    row1_bytes = len(orjson.dumps(row1_payload))

    result = _make_result()
    log = MagicMock()
    # Limit to exactly row1_bytes forces only row1 to be included
    fetched = await engine._fetch_delta_rows(
        "delta_table", log, result, batch_size=50, batch_max_bytes=row1_bytes
    )
    assert fetched == [row1]


@pytest.mark.asyncio
async def test_fetch_delta_rows_batch_max_bytes_exactly_two_rows():
    """When batch_max_bytes is exactly big enough for both rows, returns both."""
    import orjson
    engine = _make_engine()

    row1 = {"_ext_id": "1", "name": "Alice"}
    row2 = {"_ext_id": "2", "name": "Bob"}
    rows = [row1, row2]
    engine._pool = _make_pool_mock(rows)

    total_bytes = sum(
        len(orjson.dumps({k: v for k, v in r.items() if not k.startswith("_")}))
        for r in rows
    )

    result = _make_result()
    log = MagicMock()
    fetched = await engine._fetch_delta_rows(
        "delta_table", log, result, batch_size=50, batch_max_bytes=total_bytes
    )
    assert fetched == rows


@pytest.mark.asyncio
async def test_fetch_delta_rows_empty_db_returns_empty():
    """When the DB returns no rows, _fetch_delta_rows returns empty list."""
    engine = _make_engine()
    engine._pool = _make_pool_mock([])

    result = _make_result()
    log = MagicMock()
    fetched = await engine._fetch_delta_rows(
        "delta_table", log, result, batch_size=50, batch_max_bytes=1000
    )
    assert not fetched
