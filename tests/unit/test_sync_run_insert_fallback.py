"""Unit tests for run_sync INSERT-into-sync_run retry fallback path.

When the primary INSERT (with high_water_mark_before) raises, a simpler
INSERT without that column is attempted as a fallback.
Verifies both the fallback INSERT is issued and run_sync still completes.
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


def _make_conn_primary_insert_fails(
    for_update_row: tuple | None = ("row-id",),
) -> tuple[AsyncMock, list[str]]:
    """
    Return a connection where the first INSERT INTO inout_ops_sync_run
    (with high_water_mark_before) raises, simulating an older schema.
    All subsequent INSERT/UPDATE calls succeed.
    """
    sql_list: list[str] = []
    first_sync_run_insert = [False]

    async def _execute(sql: str, params=None):
        sql_list.append(sql)
        cur = AsyncMock()

        if "INSERT INTO inout_ops_sync_run" in sql and "high_water_mark_before" in sql:
            if not first_sync_run_insert[0]:
                first_sync_run_insert[0] = True
                raise Exception("column high_water_mark_before does not exist")

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


def _build_pool(conn: AsyncMock) -> MagicMock:
    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool


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


# ---------------------------------------------------------------------------
# Fallback INSERT is issued when primary INSERT raises
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_fallback_insert_issued_when_primary_insert_raises():
    """
    When INSERT with high_water_mark_before raises, the simpler fallback INSERT
    (without that column) must be issued and run_sync must still complete.
    """
    from inandout.ingestion.engine import IngestionEngine

    conn, sql_list = _make_conn_primary_insert_fails()
    engine = IngestionEngine(_build_pool(conn))
    engine._read_pool = _build_pool(_make_read_conn())

    with patch.object(engine, "_do_sync", new=AsyncMock()):
        result = await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    # Fallback INSERT: no high_water_mark_before column
    fallback_inserts = [
        s for s in sql_list
        if "INSERT INTO inout_ops_sync_run" in s
        and "high_water_mark_before" not in s
    ]
    assert fallback_inserts, (
        "Expected fallback INSERT INTO inout_ops_sync_run without high_water_mark_before"
    )
    assert result.status not in ("running",), (
        f"run_sync should complete even after fallback INSERT; got status={result.status}"
    )


@pytest.mark.anyio
async def test_primary_insert_used_when_schema_is_current():
    """When the primary INSERT succeeds, the fallback must NOT be issued."""
    from inandout.ingestion.engine import IngestionEngine

    # Normal conn: all SQLs succeed
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

    async def _tracking_execute(sql: str, params=None):
        sql_list.append(sql)
        return await _execute(sql, params)

    conn.execute = AsyncMock(side_effect=_tracking_execute)
    conn.commit = AsyncMock()

    engine = IngestionEngine(_build_pool(conn))
    engine._read_pool = _build_pool(_make_read_conn())

    with patch.object(engine, "_do_sync", new=AsyncMock()):
        await engine.run_sync(_make_connector(), "contacts", _make_ingestion_cfg())

    primary_inserts = [
        s for s in sql_list
        if "INSERT INTO inout_ops_sync_run" in s and "high_water_mark_before" in s
    ]
    fallback_inserts = [
        s for s in sql_list
        if "INSERT INTO inout_ops_sync_run" in s and "high_water_mark_before" not in s
    ]
    assert primary_inserts, "Expected primary INSERT to be used with current schema"
    assert not fallback_inserts, (
        "Fallback INSERT must not be issued when primary INSERT succeeds"
    )
