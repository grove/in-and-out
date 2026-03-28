"""Unit tests for apply_schema_migrations SQL identifier safety."""
from __future__ import annotations

from unittest.mock import AsyncMock, call

import pytest
from psycopg import sql as pgsql

from inandout.postgres.schema_migration import apply_schema_migrations


def _make_conn() -> AsyncMock:
    conn = AsyncMock()
    conn.execute = AsyncMock()
    return conn


async def test_added_column_uses_identifier():
    """The column name must be passed through psycopg Identifier for safe quoting."""
    conn = _make_conn()
    diff = ["added column 'select' (TEXT)"]   # 'select' is a SQL keyword
    await apply_schema_migrations(conn, "tbl", diff)
    conn.execute.assert_awaited_once()
    stmt = conn.execute.await_args.args[0]
    # The composed SQL object should render with proper quoting
    rendered = stmt.as_string(None)
    assert '"select"' in rendered


async def test_removed_column_with_prune_uses_identifier():
    conn = _make_conn()
    diff = ["removed column 'order'"]   # SQL keyword
    await apply_schema_migrations(conn, "tbl", diff, prune=True)
    conn.execute.assert_awaited_once()
    stmt = conn.execute.await_args.args[0]
    rendered = stmt.as_string(None)
    assert '"order"' in rendered


async def test_added_column_table_uses_identifier():
    conn = _make_conn()
    diff = ["added column 'x' (TEXT)"]
    await apply_schema_migrations(conn, "my_table", diff)
    stmt = conn.execute.await_args.args[0]
    rendered = stmt.as_string(None)
    assert '"my_table"' in rendered


async def test_schema_qualified_table_identifier():
    conn = _make_conn()
    diff = ["added column 'x' (TEXT)"]
    await apply_schema_migrations(conn, "myschema.my_table", diff)
    stmt = conn.execute.await_args.args[0]
    rendered = stmt.as_string(None)
    assert '"myschema"."my_table"' in rendered


async def test_column_with_space_in_name():
    conn = _make_conn()
    diff = ["added column 'my col' (TEXT)"]
    await apply_schema_migrations(conn, "tbl", diff)
    stmt = conn.execute.await_args.args[0]
    rendered = stmt.as_string(None)
    assert '"my col"' in rendered


async def test_multiple_columns_each_quoted():
    conn = _make_conn()
    diff = ["added column 'from' (TEXT)", "added column 'where' (TEXT)"]
    await apply_schema_migrations(conn, "tbl", diff)
    assert conn.execute.await_count == 2
    for i, col_name in enumerate(["from", "where"]):
        stmt = conn.execute.await_args_list[i].args[0]
        rendered = stmt.as_string(None)
        assert f'"{col_name}"' in rendered


async def test_prune_drop_uses_add_if_not_exists_text():
    conn = _make_conn()
    diff = ["added column 'new_col' (TEXT)"]
    result = await apply_schema_migrations(conn, "tbl", diff)
    assert any("ADD COLUMN IF NOT EXISTS" in s for s in result)


async def test_prune_drop_result_uses_drop_if_exists_text():
    conn = _make_conn()
    diff = ["removed column 'stale'"]
    result = await apply_schema_migrations(conn, "tbl", diff, prune=True)
    assert any("DROP COLUMN IF EXISTS" in s for s in result)
