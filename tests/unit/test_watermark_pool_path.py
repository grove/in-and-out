"""Unit tests for set_watermark pool path.

When conn_or_pool has no `execute` attribute (i.e. it's a pool),
set_watermark must acquire a connection via pool.connection() and call
execute on that acquired connection.

Distinct from test_watermark_idempotency.py, which tests the direct-conn
path. This file focuses on the pool branch.
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from inandout.postgres.watermark import set_watermark


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_pool() -> tuple[MagicMock, AsyncMock, list[str]]:
    """Return (pool, acquired_conn, sql_list)."""
    sql_list: list[str] = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql.strip())
        return AsyncMock()

    acquired_conn = AsyncMock()
    acquired_conn.execute = AsyncMock(side_effect=_execute)
    acquired_conn.commit = AsyncMock()
    acquired_conn.__aenter__ = AsyncMock(return_value=acquired_conn)
    acquired_conn.__aexit__ = AsyncMock(return_value=None)

    pool = MagicMock()
    # Must NOT have 'execute' attribute so set_watermark takes the pool branch
    del pool.execute
    pool.connection = MagicMock(return_value=acquired_conn)

    return pool, acquired_conn, sql_list


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_set_watermark_pool_path_calls_execute_on_acquired_conn():
    """Pool path must call execute on the acquired connection."""
    pool, acquired_conn, sql_list = _make_pool()
    run_id = uuid.uuid4()

    await set_watermark(pool, "hubspot", "contacts", "cursor", "page-5", run_id)

    assert acquired_conn.execute.called, "execute must be called on the acquired connection"


@pytest.mark.anyio
async def test_set_watermark_pool_path_commits_acquired_conn():
    """Pool path must commit the acquired connection."""
    pool, acquired_conn, _ = _make_pool()
    run_id = uuid.uuid4()

    await set_watermark(pool, "hubspot", "contacts", "cursor", "page-5", run_id)

    acquired_conn.commit.assert_called_once()


@pytest.mark.anyio
async def test_set_watermark_pool_path_issues_upsert_sql():
    """Pool path must issue INSERT ... ON CONFLICT DO UPDATE."""
    pool, _, sql_list = _make_pool()
    run_id = uuid.uuid4()

    await set_watermark(pool, "hubspot", "contacts", "cursor", "page-5", run_id)

    assert any("ON CONFLICT" in s for s in sql_list), (
        f"Expected ON CONFLICT upsert, got: {sql_list}"
    )


@pytest.mark.anyio
async def test_set_watermark_pool_path_includes_correct_params():
    """Watermark value and connector/datatype must appear in the execute params."""
    sql_list: list[str] = []
    param_list: list[list] = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql.strip())
        param_list.append(list(params) if params else [])
        return AsyncMock()

    acquired_conn = AsyncMock()
    acquired_conn.execute = AsyncMock(side_effect=_execute)
    acquired_conn.commit = AsyncMock()
    acquired_conn.__aenter__ = AsyncMock(return_value=acquired_conn)
    acquired_conn.__aexit__ = AsyncMock(return_value=None)

    pool = MagicMock()
    del pool.execute
    pool.connection = MagicMock(return_value=acquired_conn)

    run_id = uuid.uuid4()
    await set_watermark(pool, "salesforce", "leads", "timestamp", "2026-01-01", run_id)

    assert param_list, "execute must be called with params"
    p = param_list[0]
    assert p[0] == "salesforce"
    assert p[1] == "leads"
    assert p[3] == "2026-01-01"
    assert p[4] == run_id


@pytest.mark.anyio
async def test_set_watermark_direct_conn_path_does_not_commit():
    """Direct connection path must NOT call commit (caller owns the transaction)."""
    sql_list: list[str] = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql.strip())
        return AsyncMock()

    conn = AsyncMock()
    conn.execute = AsyncMock(side_effect=_execute)
    conn.commit = AsyncMock()

    run_id = uuid.uuid4()
    await set_watermark(conn, "hubspot", "contacts", "cursor", "page-1", run_id)

    conn.commit.assert_not_called()
