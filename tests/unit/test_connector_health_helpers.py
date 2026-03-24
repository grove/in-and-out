"""Unit tests for _mark_connector_unavailable and _mark_connector_healthy in daemon.py.

Covers:
- _mark_connector_unavailable issues INSERT ... status='unavailable' with correct
  connector, datatype, and reason params.
- _mark_connector_healthy issues INSERT ... status='healthy' with correct params.
- Both helpers swallow exceptions (pool errors must not propagate).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from inandout.ingestion.daemon import (
    _mark_connector_healthy,
    _mark_connector_unavailable,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pool_capturing() -> tuple[MagicMock, list[str], list[list]]:
    """Return (pool, sql_list, params_list) that captures every execute call."""
    sql_list: list[str] = []
    params_list: list[list] = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql)
        params_list.append(list(params) if params else [])
        return AsyncMock()

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(side_effect=_execute)
    conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool, sql_list, params_list


# ---------------------------------------------------------------------------
# _mark_connector_unavailable
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_mark_unavailable_issues_insert_with_unavailable_status():
    """INSERT with status='unavailable' and correct connector, datatype, reason."""
    pool, sql_list, params_list = _make_pool_capturing()

    await _mark_connector_unavailable(pool, "hubspot", "contacts", "timeout after 5 failures")

    inserts = [s for s in sql_list if "INSERT INTO inout_ops_connector_health" in s]
    assert inserts, "Expected INSERT INTO inout_ops_connector_health"
    assert all("unavailable" in s for s in inserts)
    assert all("ON CONFLICT" in s for s in inserts)

    # Params: [connector, datatype, reason]
    assert params_list[0][0] == "hubspot"
    assert params_list[0][1] == "contacts"
    assert params_list[0][2] == "timeout after 5 failures"


@pytest.mark.anyio
async def test_mark_unavailable_swallows_pool_exception():
    """Exceptions from pool.connection() must not propagate."""
    pool = MagicMock()
    pool.connection.side_effect = RuntimeError("db gone")

    # Should not raise
    await _mark_connector_unavailable(pool, "hubspot", "contacts", "db gone")


@pytest.mark.anyio
async def test_mark_unavailable_swallows_execute_exception():
    """Exceptions from conn.execute() must not propagate."""
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(side_effect=Exception("table missing"))
    conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)

    await _mark_connector_unavailable(pool, "hubspot", "contacts", "table missing")


# ---------------------------------------------------------------------------
# _mark_connector_healthy
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_mark_healthy_issues_insert_with_healthy_status():
    """INSERT with status='healthy' and correct connector, datatype."""
    pool, sql_list, params_list = _make_pool_capturing()

    await _mark_connector_healthy(pool, "salesforce", "deals")

    inserts = [s for s in sql_list if "INSERT INTO inout_ops_connector_health" in s]
    assert inserts, "Expected INSERT INTO inout_ops_connector_health"
    assert all("healthy" in s for s in inserts)
    assert all("ON CONFLICT" in s for s in inserts)

    # Params: [connector, datatype, ...]
    assert params_list[0][0] == "salesforce"
    assert params_list[0][1] == "deals"


@pytest.mark.anyio
async def test_mark_healthy_sets_marked_unhealthy_at_to_null():
    """The INSERT/UPDATE for healthy must clear marked_unhealthy_at (set to NULL)."""
    pool, sql_list, _ = _make_pool_capturing()

    await _mark_connector_healthy(pool, "hubspot", "contacts")

    inserts = [s for s in sql_list if "INSERT INTO inout_ops_connector_health" in s]
    assert inserts
    # NULL must appear for marked_unhealthy_at and reason columns
    assert any("NULL" in s for s in inserts), (
        "healthy INSERT should set marked_unhealthy_at and reason to NULL"
    )


@pytest.mark.anyio
async def test_mark_healthy_swallows_pool_exception():
    """Exceptions from pool.connection() must not propagate."""
    pool = MagicMock()
    pool.connection.side_effect = RuntimeError("db gone")

    await _mark_connector_healthy(pool, "hubspot", "contacts")
