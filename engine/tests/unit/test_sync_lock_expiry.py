"""Unit tests for inout_ops_sync_lock stale-lock expiry (migration 024)."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ingestion_cfg() -> "IngestionConfig":
    cfg = MagicMock()
    cfg.source_mode = "polling"
    cfg.cdc = None
    cfg.list = MagicMock()
    cfg.list.incremental = None
    cfg.history_mode = "none"
    return cfg


def _make_connector(name: str = "testconn") -> "ConnectorConfig":
    cfg = MagicMock()
    cfg.name = name
    cfg.version = "1.0.0"
    cfg.datatypes = {}
    return cfg


def _build_pool(conn: AsyncMock) -> MagicMock:
    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool


def _make_recording_conn(for_update_row=("row-id",)):
    """Return (conn, sql_list) where sql_list accumulates all SQL executed."""
    sql_list: list[str] = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql)
        cur = AsyncMock()
        if "FOR UPDATE SKIP LOCKED" in sql:
            cur.fetchone = AsyncMock(return_value=for_update_row)
        else:
            cur.fetchone = AsyncMock(return_value=None)
        cur.rowcount = 0
        return cur

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(side_effect=_execute)
    conn.commit = AsyncMock()
    return conn, sql_list


def _make_read_conn():
    async def _execute(sql: str, params=None):
        cur = AsyncMock()
        cur.fetchone = AsyncMock(return_value=None)
        cur.rowcount = 0
        return cur

    rconn = AsyncMock()
    rconn.__aenter__ = AsyncMock(return_value=rconn)
    rconn.__aexit__ = AsyncMock(return_value=None)
    rconn.execute = AsyncMock(side_effect=_execute)
    rconn.commit = AsyncMock()
    return rconn


# ---------------------------------------------------------------------------
# Test: stale lock UPDATE is emitted before SELECT FOR UPDATE
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_stale_lock_update_precedes_acquire():
    """run_sync should UPDATE locked_until=NULL for expired rows before SELECT FOR UPDATE SKIP LOCKED."""
    from inandout.ingestion.engine import IngestionEngine

    conn, sql_list = _make_recording_conn(for_update_row=("row-id",))
    pool = _build_pool(conn)
    engine = IngestionEngine(pool)
    engine._read_pool = _build_pool(_make_read_conn())

    with patch.object(engine, "_do_sync", new=AsyncMock()):
        await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    stale_update_indices = [
        i for i, s in enumerate(sql_list)
        if "locked_until IS NOT NULL" in s and "locked_until < NOW()" in s
    ]
    acquire_indices = [
        i for i, s in enumerate(sql_list)
        if "FOR UPDATE SKIP LOCKED" in s
    ]

    assert stale_update_indices, "Expected stale-lock UPDATE to be issued"
    assert acquire_indices, "Expected SELECT FOR UPDATE SKIP LOCKED to be issued"
    assert stale_update_indices[-1] < acquire_indices[0], (
        "Stale-lock UPDATE must precede SELECT FOR UPDATE SKIP LOCKED"
    )


# ---------------------------------------------------------------------------
# Test: locked_until stamp is set after successful acquire
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_locked_until_stamped_after_acquire():
    """run_sync should stamp locked_until=NOW()+1h after acquiring the lock."""
    from inandout.ingestion.engine import IngestionEngine

    conn, sql_list = _make_recording_conn(for_update_row=("row-id",))
    pool = _build_pool(conn)
    engine = IngestionEngine(pool)
    engine._read_pool = _build_pool(_make_read_conn())

    with patch.object(engine, "_do_sync", new=AsyncMock()):
        await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    stamp_sqls = [
        s for s in sql_list
        if "locked_until" in s
        and "INTERVAL '1 hour'" in s
        and "UPDATE" in s
        and "locked_until IS NOT NULL" not in s  # exclude the expiry-clear UPDATE
    ]
    assert stamp_sqls, "Expected UPDATE to set locked_until = NOW() + INTERVAL '1 hour' after acquire"


# ---------------------------------------------------------------------------
# Test: lock skipped when FOR UPDATE returns no row
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_sync_skipped_when_lock_already_held():
    """run_sync should return status='skipped' when another instance holds the lock."""
    from inandout.ingestion.engine import IngestionEngine

    conn, _ = _make_recording_conn(for_update_row=None)  # no row → lock held elsewhere
    pool = _build_pool(conn)
    engine = IngestionEngine(pool)
    engine._read_pool = _build_pool(_make_read_conn())

    result = await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())
    assert result.status == "skipped"
