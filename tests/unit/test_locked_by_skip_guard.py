"""Unit test — skipped-when-lock-held must NOT issue the locked_by UPDATE.

When SELECT FOR UPDATE SKIP LOCKED returns no row (lock held by another
worker), run_sync returns status='skipped' and must not issue the
UPDATE ... SET locked_until / locked_by SQL.
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


def _make_conn_lock_held() -> tuple[AsyncMock, list[str]]:
    """Return a connection where FOR UPDATE SKIP LOCKED returns no row (lock held)."""
    sql_list: list[str] = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql)
        cur = AsyncMock()
        if "FOR UPDATE SKIP LOCKED" in sql:
            cur.fetchone = AsyncMock(return_value=None)  # no row → lock held
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


def _build_pool(conn: AsyncMock) -> MagicMock:
    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool


# ---------------------------------------------------------------------------
# locked_by UPDATE not issued when lock is held
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_locked_by_not_stamped_when_lock_skipped():
    """
    When FOR UPDATE SKIP LOCKED returns no row, the run is skipped and the
    UPDATE ... SET locked_until / locked_by must NOT be issued.
    """
    from inandout.ingestion.engine import IngestionEngine

    conn, sql_list = _make_conn_lock_held()
    engine = IngestionEngine(_build_pool(conn))
    engine._read_pool = _build_pool(_make_read_conn())

    result = await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    assert result.status == "skipped"

    lock_stamp_sqls = [
        s for s in sql_list
        if "locked_by" in s and "locked_until" in s and "INTERVAL '1 hour'" in s
    ]
    assert not lock_stamp_sqls, (
        "UPDATE ... locked_by/locked_until must NOT be issued when the lock is skipped; "
        f"found: {lock_stamp_sqls}"
    )


@pytest.mark.anyio
async def test_locked_by_stamped_when_lock_acquired():
    """
    Positive control: when FOR UPDATE SKIP LOCKED returns a row, the
    locked_by / locked_until UPDATE IS issued.
    """
    from inandout.ingestion.engine import IngestionEngine

    async def _execute(sql: str, params=None):
        cur = AsyncMock()
        if "FOR UPDATE SKIP LOCKED" in sql:
            cur.fetchone = AsyncMock(return_value=("row-id",))
        else:
            cur.fetchone = AsyncMock(return_value=None)
        cur.rowcount = 0
        return cur

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    sql_list: list[str] = []

    async def _tracking(sql: str, params=None):
        sql_list.append(sql)
        return await _execute(sql, params)

    conn.execute = AsyncMock(side_effect=_tracking)
    conn.commit = AsyncMock()

    engine = IngestionEngine(_build_pool(conn))
    engine._read_pool = _build_pool(_make_read_conn())

    with patch.object(engine, "_do_sync", new=AsyncMock()):
        result = await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    assert result.status not in ("skipped", "running")

    lock_stamp_sqls = [
        s for s in sql_list
        if "locked_by" in s and "INTERVAL '1 hour'" in s
    ]
    assert lock_stamp_sqls, (
        "UPDATE ... locked_by/locked_until MUST be issued when lock is acquired"
    )
