"""Unit tests for get_watermark SQL and return value.

Covers:
- SELECT query includes correct WHERE clause (connector and datatype).
- Returns None when fetchone returns None.
- Returns the watermark_value string when a row exists.
- Works correctly with different connector/datatype combinations.
"""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from inandout.postgres.watermark import get_watermark


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_conn(fetchone_return) -> tuple[AsyncMock, list[str], list[list]]:
    sql_list: list[str] = []
    params_list: list[list] = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql.strip())
        params_list.append(list(params) if params else [])
        cur = AsyncMock()
        cur.fetchone = AsyncMock(return_value=fetchone_return)
        return cur

    conn = AsyncMock()
    conn.execute = AsyncMock(side_effect=_execute)
    return conn, sql_list, params_list


# ---------------------------------------------------------------------------
# Return value tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_get_watermark_returns_none_when_no_row():
    conn, _, _ = _make_conn(fetchone_return=None)
    result = await get_watermark(conn, "hubspot", "contacts")
    assert result is None


@pytest.mark.anyio
async def test_get_watermark_returns_value_when_row_exists():
    conn, _, _ = _make_conn(fetchone_return=("2026-01-15T12:00:00Z",))
    result = await get_watermark(conn, "hubspot", "contacts")
    assert result == "2026-01-15T12:00:00Z"


@pytest.mark.anyio
async def test_get_watermark_returns_string_type():
    conn, _, _ = _make_conn(fetchone_return=("some-cursor-value",))
    result = await get_watermark(conn, "hubspot", "contacts")
    assert isinstance(result, str)


# ---------------------------------------------------------------------------
# SQL correctness
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_get_watermark_selects_watermark_value_column():
    conn, sql_list, _ = _make_conn(fetchone_return=None)
    await get_watermark(conn, "hubspot", "contacts")
    assert any("watermark_value" in s for s in sql_list)


@pytest.mark.anyio
async def test_get_watermark_sql_targets_correct_table():
    conn, sql_list, _ = _make_conn(fetchone_return=None)
    await get_watermark(conn, "hubspot", "contacts")
    assert any("inout_ops_watermark" in s for s in sql_list)


@pytest.mark.anyio
async def test_get_watermark_sql_filters_by_connector_and_datatype():
    conn, sql_list, _ = _make_conn(fetchone_return=None)
    await get_watermark(conn, "hubspot", "contacts")
    assert any(
        "connector" in s and "datatype" in s
        for s in sql_list
    )


@pytest.mark.anyio
async def test_get_watermark_passes_connector_and_datatype_as_params():
    conn, _, params_list = _make_conn(fetchone_return=None)
    await get_watermark(conn, "salesforce", "opportunities")
    assert params_list
    p = params_list[0]
    assert "salesforce" in p
    assert "opportunities" in p


@pytest.mark.anyio
async def test_get_watermark_different_connectors_pass_correct_values():
    """Each call uses its own connector/datatype in the params."""
    for connector, datatype in [("hubspot", "contacts"), ("salesforce", "deals")]:
        conn, _, params_list = _make_conn(fetchone_return=None)
        await get_watermark(conn, connector, datatype)
        p = params_list[0]
        assert connector in p
        assert datatype in p
