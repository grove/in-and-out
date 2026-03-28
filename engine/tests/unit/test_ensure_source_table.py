"""Unit tests for ensure_source_table SQL output.

Verifies that ensure_source_table issues:
  1. A CREATE TABLE IF NOT EXISTS for the correct table name.
  2. An ALTER TABLE to add _lineage column.
"""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from inandout.postgres.schema import ensure_source_table


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_conn() -> tuple[AsyncMock, list[str]]:
    sql_list: list[str] = []

    async def _execute(sql: str, params=None):
        sql_list.append(sql.strip())
        return AsyncMock()

    conn = AsyncMock()
    conn.execute = AsyncMock(side_effect=_execute)
    return conn, sql_list


# ---------------------------------------------------------------------------
# Standard (non-shared) table
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_ensure_source_table_creates_correct_table_name():
    conn, sql_list = _make_conn()
    await ensure_source_table(conn, "hubspot", "contacts")
    create_sqls = [s for s in sql_list if "CREATE TABLE IF NOT EXISTS" in s]
    assert any("inout_src_hubspot_contacts" in s for s in create_sqls), (
        f"Expected CREATE TABLE for inout_src_hubspot_contacts, got:\n{create_sqls}"
    )


@pytest.mark.anyio
async def test_ensure_source_table_includes_external_id_column():
    conn, sql_list = _make_conn()
    await ensure_source_table(conn, "hubspot", "contacts")
    create_sql = next(s for s in sql_list if "CREATE TABLE IF NOT EXISTS" in s)
    assert "external_id" in create_sql


@pytest.mark.anyio
async def test_ensure_source_table_includes_data_and_raw_columns():
    conn, sql_list = _make_conn()
    await ensure_source_table(conn, "hubspot", "contacts")
    create_sql = next(s for s in sql_list if "CREATE TABLE IF NOT EXISTS" in s)
    assert "data" in create_sql
    assert "raw" in create_sql


@pytest.mark.anyio
async def test_ensure_source_table_alters_lineage_column():
    conn, sql_list = _make_conn()
    await ensure_source_table(conn, "hubspot", "contacts")
    alter_sqls = [s for s in sql_list if "ADD COLUMN IF NOT EXISTS" in s and "_lineage" in s]
    assert alter_sqls, "Expected ALTER TABLE ... ADD COLUMN IF NOT EXISTS _lineage"


@pytest.mark.anyio
async def test_ensure_source_table_no_connector_column_for_non_shared():
    """For standard (non-shared) tables, no _connector ALTER must be issued."""
    conn, sql_list = _make_conn()
    await ensure_source_table(conn, "hubspot", "contacts")
    connector_alters = [s for s in sql_list if "_connector" in s]
    assert not connector_alters, (
        f"Did not expect _connector ALTER for non-shared table, got: {connector_alters}"
    )


@pytest.mark.anyio
async def test_ensure_source_table_respects_namespace():
    """Non-public namespace must be reflected in the table name."""
    conn, sql_list = _make_conn()
    await ensure_source_table(conn, "salesforce", "deals", namespace="tenant_42")
    create_sqls = [s for s in sql_list if "CREATE TABLE IF NOT EXISTS" in s]
    assert any("tenant_42" in s for s in create_sqls), (
        f"Expected namespace in table name, got: {create_sqls}"
    )
