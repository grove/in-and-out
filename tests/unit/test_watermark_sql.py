"""Unit tests for get_watermark and set_watermark SQL helpers.

Covers:
- get_watermark issues SELECT with correct connector/datatype params.
- get_watermark returns row[0] when a row is found.
- get_watermark returns None when no row is found.
- set_watermark issues INSERT ... ON CONFLICT DO UPDATE with correct params
  when called with a connection directly.
- set_watermark acquires a pool connection and commits when called with a pool.
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from inandout.postgres.watermark import get_watermark, set_watermark


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_conn(fetchone_return=None) -> tuple[AsyncMock, list[str], list[list]]:
    sql_list: list[str] = []
    params_list: list[list] = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql)
        params_list.append(list(params) if params else [])
        cur = AsyncMock()
        cur.fetchone = AsyncMock(return_value=fetchone_return)
        return cur

    conn = AsyncMock()
    conn.execute = AsyncMock(side_effect=_execute)
    conn.commit = AsyncMock()
    return conn, sql_list, params_list


def _build_pool(conn: AsyncMock) -> MagicMock:
    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool


# ---------------------------------------------------------------------------
# get_watermark
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_get_watermark_issues_select_with_correct_params():
    conn, sql_list, params_list = _make_conn(fetchone_return=None)
    await get_watermark(conn, "hubspot", "contacts")
    assert any("SELECT watermark_value FROM inout_ops_watermark" in s for s in sql_list)
    assert params_list[0] == ["hubspot", "contacts"]


@pytest.mark.anyio
async def test_get_watermark_returns_value_when_row_found():
    conn, _, _ = _make_conn(fetchone_return=("2026-01-01T00:00:00Z",))
    result = await get_watermark(conn, "hubspot", "contacts")
    assert result == "2026-01-01T00:00:00Z"


@pytest.mark.anyio
async def test_get_watermark_returns_none_when_no_row():
    conn, _, _ = _make_conn(fetchone_return=None)
    result = await get_watermark(conn, "hubspot", "contacts")
    assert result is None


# ---------------------------------------------------------------------------
# set_watermark with a connection
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_set_watermark_via_conn_issues_upsert_with_correct_params():
    conn, sql_list, params_list = _make_conn()
    run_id = uuid.uuid4()
    await set_watermark(conn, "hubspot", "contacts", "cursor", "page-42", run_id)

    upsert_sqls = [s for s in sql_list if "INSERT INTO inout_ops_watermark" in s]
    assert upsert_sqls, "Expected INSERT INTO inout_ops_watermark"
    assert all("ON CONFLICT" in s for s in upsert_sqls)

    p = params_list[0]
    assert p[0] == "hubspot"
    assert p[1] == "contacts"
    assert p[2] == "cursor"
    assert p[3] == "page-42"
    assert p[4] == run_id


@pytest.mark.anyio
async def test_set_watermark_via_pool_commits():
    conn, sql_list, params_list = _make_conn()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)

    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)

    # Remove the 'execute' attribute so the function treats it as a pool
    # (set_watermark checks hasattr(conn_or_pool, "execute"))
    pool_only = MagicMock(spec=[])  # no execute attr → treated as pool
    pool_only.connection = MagicMock(return_value=conn)

    run_id = uuid.uuid4()
    await set_watermark(pool_only, "salesforce", "deals", "timestamp", "2026-03-24", run_id)

    upsert_sqls = [s for s in sql_list if "INSERT INTO inout_ops_watermark" in s]
    assert upsert_sqls, "Expected INSERT INTO inout_ops_watermark via pool path"
    conn.commit.assert_called()
