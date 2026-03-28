"""Unit tests for the _lock_heartbeat inner coroutine and locked_by identity format.

Covers:
- _lock_heartbeat issues UPDATE inout_ops_sync_lock SET locked_until on a separate
  pool connection after the configured interval elapses.
- _lock_heartbeat is cancelled (run_sync completes cleanly) when _do_sync returns.
- The locked_by value stamped in run_sync matches socket.gethostname():os.getpid().
"""
from __future__ import annotations

import os
import socket
from unittest.mock import AsyncMock, MagicMock, patch

import anyio
import pytest


# ---------------------------------------------------------------------------
# Helpers (self-contained to keep each test file readable)
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


def _make_recording_conn_with_params(
    for_update_row: tuple | None = ("row-id",),
) -> tuple[AsyncMock, list[str], list[list]]:
    """Return (conn, sql_list, params_list) that capture every execute call."""
    sql_list: list[str] = []
    params_list: list[list] = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql)
        params_list.append(list(params) if params else [])
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
    return conn, sql_list, params_list


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


def _heartbeat_sqls(sql_list: list[str]) -> list[str]:
    """Return SQL entries that are heartbeat UPDATEs (set locked_until, not locked_by)."""
    return [
        s for s in sql_list
        if "UPDATE inout_ops_sync_lock" in s
        and "locked_until = NOW() + INTERVAL '1 hour'" in s
        and "locked_by" not in s
    ]


# ---------------------------------------------------------------------------
# Heartbeat fires after the configured interval
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_lock_heartbeat_issues_update_after_interval():
    """
    _lock_heartbeat must issue UPDATE inout_ops_sync_lock SET locked_until via a
    fresh pool connection once the configured interval has elapsed.
    """
    from inandout.ingestion import engine as engine_mod
    from inandout.ingestion.engine import IngestionEngine

    conn, sql_list, _ = _make_recording_conn_with_params()
    pool = _build_pool(conn)
    engine = IngestionEngine(pool)
    engine._read_pool = _build_pool(_make_read_conn())

    orig_interval = engine_mod._LOCK_HEARTBEAT_INTERVAL_SECS
    engine_mod._LOCK_HEARTBEAT_INTERVAL_SECS = 0.0  # fire immediately

    async def _sync_with_yield(*args, **kwargs):
        # Yield control so the heartbeat task (sleeping 0 s) can execute its UPDATE.
        await anyio.sleep(0)
        await anyio.sleep(0)

    try:
        with patch.object(engine, "_do_sync", side_effect=_sync_with_yield):
            await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())
    finally:
        engine_mod._LOCK_HEARTBEAT_INTERVAL_SECS = orig_interval

    hb = _heartbeat_sqls(sql_list)
    assert hb, (
        "Expected _lock_heartbeat to issue "
        "UPDATE inout_ops_sync_lock SET locked_until after the interval"
    )


# ---------------------------------------------------------------------------
# Heartbeat is cancelled when _do_sync returns
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_lock_heartbeat_cancelled_when_do_sync_returns():
    """
    After _do_sync finishes, _hb_scope.cancel() must stop the heartbeat loop.
    The number of heartbeat UPDATEs must not exceed the number of times _do_sync
    yielded control; and run_sync must complete without hanging.
    """
    from inandout.ingestion import engine as engine_mod
    from inandout.ingestion.engine import IngestionEngine

    conn, sql_list, _ = _make_recording_conn_with_params()
    pool = _build_pool(conn)
    engine = IngestionEngine(pool)
    engine._read_pool = _build_pool(_make_read_conn())

    orig_interval = engine_mod._LOCK_HEARTBEAT_INTERVAL_SECS
    engine_mod._LOCK_HEARTBEAT_INTERVAL_SECS = 0.0

    yields_done = 0

    async def _controlled_sync(*args, **kwargs):
        nonlocal yields_done
        await anyio.sleep(0)
        yields_done += 1
        await anyio.sleep(0)
        yields_done += 1

    try:
        with patch.object(engine, "_do_sync", side_effect=_controlled_sync):
            await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())
    finally:
        engine_mod._LOCK_HEARTBEAT_INTERVAL_SECS = orig_interval

    # If cancellation is broken, run_sync would hang above (test would time-out).
    # Additionally, the heartbeat can fire at most as many times as _do_sync yielded.
    hb_count = len(_heartbeat_sqls(sql_list))
    assert hb_count <= yields_done, (
        f"Heartbeat fired {hb_count} time(s) but _do_sync only yielded {yields_done} time(s); "
        "suggests _hb_scope.cancel() was not called promptly"
    )


# ---------------------------------------------------------------------------
# locked_by identity format
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_locked_by_stamped_as_hostname_colon_pid():
    """
    The locked_by value written to inout_ops_sync_lock must equal
    socket.gethostname():<os.getpid()> exactly.
    """
    from inandout.ingestion.engine import IngestionEngine

    conn, sql_list, params_list = _make_recording_conn_with_params()
    pool = _build_pool(conn)
    engine = IngestionEngine(pool)
    engine._read_pool = _build_pool(_make_read_conn())

    with patch.object(engine, "_do_sync", new=AsyncMock()):
        await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    expected_locked_by = f"{socket.gethostname()}:{os.getpid()}"

    # The lock-stamp UPDATE sets locked_by as its first parameter:
    #   UPDATE inout_ops_sync_lock
    #   SET locked_until = NOW() + INTERVAL '1 hour', locked_by = %s
    #   WHERE connector = %s AND datatype = %s
    # (excluded: the stale-lock expiry UPDATE which contains "locked_until IS NOT NULL")
    lock_stamp_entries = [
        (s, p)
        for s, p in zip(sql_list, params_list)
        if "locked_by" in s
        and "INTERVAL '1 hour'" in s
        and "locked_until IS NOT NULL" not in s
    ]

    assert lock_stamp_entries, "Expected a locked_by value to be stamped in run_sync"
    actual_locked_by = lock_stamp_entries[0][1][0]  # first param = locked_by value
    assert actual_locked_by == expected_locked_by, (
        f"locked_by format mismatch: expected '{expected_locked_by}', got '{actual_locked_by}'"
    )
