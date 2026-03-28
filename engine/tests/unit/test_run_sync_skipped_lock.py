"""Unit tests for run_sync lock-skipped path.

When FOR UPDATE SKIP LOCKED returns None (another instance holds the lock),
run_sync must:
- Return result with status == "skipped".
- NOT call _do_sync.
- Issue UPDATE inout_ops_sync_run SET status='skipped'.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.ingestion.engine import IngestionEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_connector(name: str = "hubspot") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    cfg.version = "1.0.0"
    cfg.datatypes = {}
    cfg.api_version = "v1"
    return cfg


def _make_ingestion_cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.source_mode = "polling"
    cfg.cdc = None
    cfg.history_mode = "none"
    cfg.list = MagicMock()
    cfg.list.incremental = None
    cfg.schedule = MagicMock()
    cfg.schedule.cron = None
    cfg.schedule.interval = None
    return cfg


def _make_pool_lock_not_acquired() -> MagicMock:
    """Pool whose connection returns None for FOR UPDATE SKIP LOCKED (lock held)."""
    sql_list: list[str] = []
    params_list: list = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql.strip())
        params_list.append(params)
        cur = AsyncMock()
        if "FOR UPDATE SKIP LOCKED" in sql:
            cur.fetchone = AsyncMock(return_value=None)  # lock not acquired
        else:
            cur.fetchone = AsyncMock(return_value=None)
            cur.rowcount = 1
        return cur

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(side_effect=_execute)
    conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    pool._sql_list = sql_list  # expose for assertions
    return pool, conn, sql_list


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_run_sync_returns_skipped_when_lock_not_acquired():
    """When FOR UPDATE SKIP LOCKED returns None, result.status must be 'skipped'."""
    pool, _, _ = _make_pool_lock_not_acquired()
    engine = IngestionEngine(pool)
    engine._read_pool = pool

    do_sync_mock = AsyncMock()

    with (
        patch("inandout.ingestion.engine.get_watermark", return_value=None),
        patch.object(engine, "_do_sync", do_sync_mock),
    ):
        result = await engine.run_sync(
            _make_connector(), "contacts", _make_ingestion_cfg()
        )

    assert result.status == "skipped"


@pytest.mark.anyio
async def test_run_sync_does_not_call_do_sync_when_skipped():
    """_do_sync must NOT be called when the lock is not acquired."""
    pool, _, _ = _make_pool_lock_not_acquired()
    engine = IngestionEngine(pool)
    engine._read_pool = pool

    do_sync_mock = AsyncMock()

    with (
        patch("inandout.ingestion.engine.get_watermark", return_value=None),
        patch.object(engine, "_do_sync", do_sync_mock),
    ):
        await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    do_sync_mock.assert_not_called()


@pytest.mark.anyio
async def test_run_sync_updates_sync_run_to_skipped():
    """The sync_run record must be updated to status='skipped'."""
    pool, conn, sql_list = _make_pool_lock_not_acquired()
    engine = IngestionEngine(pool)
    engine._read_pool = pool

    with (
        patch("inandout.ingestion.engine.get_watermark", return_value=None),
        patch.object(engine, "_do_sync", new=AsyncMock()),
    ):
        await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    skipped_updates = [
        s for s in sql_list
        if "status='skipped'" in s or "status = 'skipped'" in s
    ]
    assert skipped_updates, (
        f"Expected UPDATE setting status='skipped', got sqls: {sql_list}"
    )
