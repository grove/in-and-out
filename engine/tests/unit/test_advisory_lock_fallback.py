"""Unit tests for the advisory-lock fallback path in run_sync (engine.py).

When the inout_ops_sync_lock table doesn't exist, the SELECT FOR UPDATE SKIP LOCKED
raises, and run_sync must:
1. Fall through to SELECT pg_try_advisory_lock(%s).
2. Complete successfully when the advisory lock is acquired (returns True).
3. Call SELECT pg_advisory_unlock(%s) in the finally block.
4. Return status='skipped' when pg_try_advisory_lock returns False (lock held).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ingestion_cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.source_mode = "polling"
    cfg.cdc = None
    cfg.list = MagicMock()
    cfg.list.incremental = None
    cfg.history_mode = "none"
    return cfg


def _make_connector(name: str = "testconn") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    cfg.version = "1.0.0"
    cfg.datatypes = {}
    return cfg


def _make_read_conn() -> AsyncMock:
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


def _make_fallback_conn(advisory_lock_result: bool) -> tuple[AsyncMock, list[str]]:
    """
    Return a connection where SELECT FOR UPDATE SKIP LOCKED raises (table missing),
    and pg_try_advisory_lock returns *advisory_lock_result*.
    """
    sql_list: list[str] = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql)
        cur = AsyncMock()

        if "FOR UPDATE SKIP LOCKED" in sql:
            raise Exception("relation inout_ops_sync_lock does not exist")
        elif "pg_try_advisory_lock" in sql:
            cur.fetchone = AsyncMock(return_value=(advisory_lock_result,))
        elif "pg_advisory_unlock" in sql:
            cur.fetchone = AsyncMock(return_value=(True,))
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


def _build_pool(conn: AsyncMock) -> MagicMock:
    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool


# ---------------------------------------------------------------------------
# Advisory lock acquired → sync completes, unlock called
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_advisory_lock_acquired_sync_completes_and_unlock_called():
    """When FOR UPDATE raises and pg_try_advisory_lock returns True, sync completes."""
    from inandout.ingestion.engine import IngestionEngine

    conn, sql_list = _make_fallback_conn(advisory_lock_result=True)
    engine = IngestionEngine(_build_pool(conn))
    engine._read_pool = _build_pool(_make_read_conn())

    with patch.object(engine, "_do_sync", new=AsyncMock()):
        result = await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    assert result.status not in ("skipped", "running"), (
        f"Expected sync to complete, got status={result.status}"
    )

    advisory_lock_sqls = [s for s in sql_list if "pg_try_advisory_lock" in s]
    assert advisory_lock_sqls, "Expected pg_try_advisory_lock to be called"

    unlock_sqls = [s for s in sql_list if "pg_advisory_unlock" in s]
    assert unlock_sqls, "Expected pg_advisory_unlock to be called in the finally block"


# ---------------------------------------------------------------------------
# Advisory lock not acquired → status='skipped'
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_advisory_lock_not_acquired_returns_skipped():
    """When FOR UPDATE raises and pg_try_advisory_lock returns False, status='skipped'."""
    from inandout.ingestion.engine import IngestionEngine

    conn, sql_list = _make_fallback_conn(advisory_lock_result=False)
    engine = IngestionEngine(_build_pool(conn))
    engine._read_pool = _build_pool(_make_read_conn())

    do_sync_called = []

    async def _fake_do_sync(*args, **kwargs):
        do_sync_called.append(True)

    with patch.object(engine, "_do_sync", side_effect=_fake_do_sync):
        result = await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    assert result.status == "skipped", f"Expected 'skipped', got '{result.status}'"
    assert not do_sync_called, "_do_sync must not be called when lock is not acquired"


# ---------------------------------------------------------------------------
# Advisory lock acquired: unlock is called even if _do_sync raises
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_advisory_unlock_called_even_when_do_sync_raises():
    """pg_advisory_unlock must be called in the finally block even if _do_sync raises."""
    from inandout.ingestion.engine import IngestionEngine

    conn, sql_list = _make_fallback_conn(advisory_lock_result=True)
    engine = IngestionEngine(_build_pool(conn))
    engine._read_pool = _build_pool(_make_read_conn())

    async def _raising_sync(*args, **kwargs):
        raise RuntimeError("synthetic _do_sync failure")

    with patch.object(engine, "_do_sync", side_effect=_raising_sync):
        result = await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    unlock_sqls = [s for s in sql_list if "pg_advisory_unlock" in s]
    assert unlock_sqls, "pg_advisory_unlock must be called even when _do_sync raises"
    assert result.status == "failed"
