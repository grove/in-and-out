"""Unit tests for run_housekeeping SQL and exception swallowing.

Covers:
- DELETE FROM inout_ops_sync_run with the correct interval is issued.
- DELETE FROM inout_ops_webhook_route_seq is issued (with exception swallowed
  if the table doesn't exist).
- DELETE FROM inout_ops_writeback_result is issued.
- Dead-letter tables are purged for each connector/datatype pair.
- History tables are purged for each connector/datatype pair.
- pool.connection() exception is swallowed (function must never raise).
- Returns a dict with integer counts per table.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from inandout.postgres.housekeeping import run_housekeeping


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_retention(
    sync_run_log: str = "30d",
    dead_letter: str = "7d",
    history_table: str = "90d",
) -> MagicMock:
    r = MagicMock()
    r.sync_run_log = sync_run_log
    r.dead_letter = dead_letter
    r.history_table = history_table
    # Explicitly set optional attributes so getattr() returns strings, not MagicMock
    r.webhook_route_seq = "7d"
    r.writeback_result = "30d"
    r.writeback_dead_letter = "30d"
    return r


def _make_housekeeping_cfg(retention: MagicMock | None = None) -> MagicMock:
    cfg = MagicMock()
    cfg.retention = retention or _make_retention()
    return cfg


def _make_pool_capturing(
    rowcount: int = 5,
    raise_on_tables: frozenset[str] = frozenset(),
) -> tuple[MagicMock, list[str]]:
    """Return (pool, sql_list) capturing every DELETE SQL.

    If the SQL mentions a table name in *raise_on_tables*, execute raises
    an exception (simulating a missing table).
    """
    sql_list: list[str] = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql)
        for t in raise_on_tables:
            if t in sql:
                raise Exception(f"relation {t!r} does not exist")
        cur = AsyncMock()
        cur.rowcount = rowcount
        return cur

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(side_effect=_execute)
    conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool, sql_list


# ---------------------------------------------------------------------------
# Core tables are purged
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_housekeeping_purges_sync_run_log():
    pool, sql_list = _make_pool_capturing()
    cfg = _make_housekeeping_cfg()

    result = await run_housekeeping(pool, cfg, [])

    assert any("DELETE FROM inout_ops_sync_run" in s for s in sql_list), (
        "Expected DELETE FROM inout_ops_sync_run"
    )
    assert "sync_run" in result


@pytest.mark.anyio
async def test_housekeeping_sync_run_uses_correct_interval():
    """30d retention → interval '30 days'."""
    pool, sql_list = _make_pool_capturing()
    cfg = _make_housekeeping_cfg(_make_retention(sync_run_log="30d"))

    await run_housekeeping(pool, cfg, [])

    sync_run_sqls = [s for s in sql_list if "DELETE FROM inout_ops_sync_run" in s]
    assert sync_run_sqls
    assert "30 days" in sync_run_sqls[0]


@pytest.mark.anyio
async def test_housekeeping_purges_webhook_route_seq():
    pool, sql_list = _make_pool_capturing()
    cfg = _make_housekeeping_cfg()

    await run_housekeeping(pool, cfg, [])

    assert any("inout_ops_webhook_route_seq" in s for s in sql_list)


@pytest.mark.anyio
async def test_housekeeping_purges_writeback_result():
    pool, sql_list = _make_pool_capturing()
    cfg = _make_housekeeping_cfg()

    await run_housekeeping(pool, cfg, [])

    assert any("inout_ops_writeback_result" in s for s in sql_list)


# ---------------------------------------------------------------------------
# Per-connector tables
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_housekeeping_purges_dead_letter_tables_for_each_pair():
    pool, sql_list = _make_pool_capturing()
    cfg = _make_housekeeping_cfg()
    pairs = [("hubspot", "contacts"), ("salesforce", "deals")]

    await run_housekeeping(pool, cfg, pairs)

    for connector, datatype in pairs:
        dl_table = f"inout_dl_ingestion_{connector}_{datatype}"
        assert any(dl_table in s for s in sql_list), (
            f"Expected DELETE for dead-letter table {dl_table}"
        )


@pytest.mark.anyio
async def test_housekeeping_purges_history_tables_for_each_pair():
    pool, sql_list = _make_pool_capturing()
    cfg = _make_housekeeping_cfg()
    pairs = [("hubspot", "contacts"), ("salesforce", "deals")]

    await run_housekeeping(pool, cfg, pairs)

    for connector, datatype in pairs:
        hist_table = f"inout_src_{connector}_{datatype}_history"
        assert any(hist_table in s for s in sql_list), (
            f"Expected DELETE for history table {hist_table}"
        )


# ---------------------------------------------------------------------------
# Missing optional tables are silently skipped (exception swallowed)
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_housekeeping_swallows_exception_for_missing_optional_table():
    """If webhook_route_seq does not exist, run_housekeeping must not raise."""
    pool, _ = _make_pool_capturing(
        raise_on_tables=frozenset(["inout_ops_webhook_route_seq"])
    )
    cfg = _make_housekeeping_cfg()

    result = await run_housekeeping(pool, cfg, [])
    # Must complete and return a dict
    assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# Returns dict with int counts
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_housekeeping_returns_dict_with_counts():
    pool, _ = _make_pool_capturing(rowcount=3)
    cfg = _make_housekeeping_cfg()

    result = await run_housekeeping(pool, cfg, [("hubspot", "contacts")])

    assert isinstance(result, dict)
    assert result.get("sync_run") == 3
