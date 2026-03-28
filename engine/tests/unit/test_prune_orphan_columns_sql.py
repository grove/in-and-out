"""Unit tests for prune_orphan_columns – SQL identifier safety."""
from __future__ import annotations

from unittest.mock import AsyncMock, call

import pytest
from psycopg import sql as pgsql

from inandout.postgres.schema_drift import prune_orphan_columns


def _make_conn() -> AsyncMock:
    conn = AsyncMock()
    conn.execute = AsyncMock()
    return conn


async def test_returns_count_of_dropped():
    conn = _make_conn()
    count = await prune_orphan_columns(conn, "my_table", ["col_a", "col_b"])
    assert count == 2


async def test_empty_list_returns_zero():
    conn = _make_conn()
    count = await prune_orphan_columns(conn, "my_table", [])
    assert count == 0
    conn.execute.assert_not_awaited()


async def test_single_column_bare_table():
    conn = _make_conn()
    await prune_orphan_columns(conn, "my_table", ["stale_col"])
    conn.execute.assert_awaited_once()
    stmt = conn.execute.await_args.args[0]
    # Expected: ALTER TABLE "my_table" DROP COLUMN IF EXISTS "stale_col"
    expected = pgsql.SQL("ALTER TABLE {} DROP COLUMN IF EXISTS {}").format(
        pgsql.Identifier("my_table"),
        pgsql.Identifier("stale_col"),
    )
    assert stmt.as_string(None) == expected.as_string(None)


async def test_schema_qualified_table_uses_two_part_identifier():
    conn = _make_conn()
    await prune_orphan_columns(conn, "tenant_1.my_table", ["old_col"])
    stmt = conn.execute.await_args.args[0]
    expected = pgsql.SQL("ALTER TABLE {} DROP COLUMN IF EXISTS {}").format(
        pgsql.Identifier("tenant_1", "my_table"),
        pgsql.Identifier("old_col"),
    )
    assert stmt.as_string(None) == expected.as_string(None)


async def test_bare_table_uses_single_part_identifier():
    conn = _make_conn()
    await prune_orphan_columns(conn, "plain_table", ["x"])
    stmt = conn.execute.await_args.args[0]
    expected = pgsql.SQL("ALTER TABLE {} DROP COLUMN IF EXISTS {}").format(
        pgsql.Identifier("plain_table"),
        pgsql.Identifier("x"),
    )
    assert stmt.as_string(None) == expected.as_string(None)


async def test_multiple_columns_all_executed():
    conn = _make_conn()
    cols = ["a", "b", "c"]
    count = await prune_orphan_columns(conn, "tbl", cols)
    assert count == 3
    assert conn.execute.await_count == 3


async def test_column_names_are_properly_quoted():
    """Columns with SQL keywords or mixed case are safely quoted."""
    conn = _make_conn()
    await prune_orphan_columns(conn, "tbl", ["select", "Order", "my col"])
    assert conn.execute.await_count == 3
    for i, col_name in enumerate(["select", "Order", "my col"]):
        stmt = conn.execute.await_args_list[i].args[0]
        rendered = stmt.as_string(None)
        assert f'"{col_name}"' in rendered


async def test_schema_qualified_renders_dot_notation():
    conn = _make_conn()
    await prune_orphan_columns(conn, "myschema.mytable", ["col"])
    stmt = conn.execute.await_args.args[0]
    rendered = stmt.as_string(None)
    assert '"myschema"."mytable"' in rendered
