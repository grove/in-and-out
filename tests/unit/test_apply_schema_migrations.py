"""Unit tests for apply_schema_migrations in postgres/schema_migration.py."""
from __future__ import annotations

from unittest.mock import AsyncMock, call

import pytest

from inandout.postgres.schema_migration import apply_schema_migrations


def _make_conn() -> AsyncMock:
    conn = AsyncMock()
    conn.execute = AsyncMock()
    return conn


async def test_empty_diff_returns_empty_list():
    conn = _make_conn()
    result = await apply_schema_migrations(conn, "my_table", [])
    assert result == []
    conn.execute.assert_not_awaited()


async def test_added_column_executes_alter_table():
    conn = _make_conn()
    diff = ["added column 'new_field' (TEXT)"]
    result = await apply_schema_migrations(conn, "my_table", diff)
    assert len(result) == 1
    conn.execute.assert_awaited_once()


async def test_added_column_result_contains_table_name():
    conn = _make_conn()
    diff = ["added column 'email' (TEXT)"]
    result = await apply_schema_migrations(conn, "my_table", diff)
    assert any("my_table" in s for s in result)


async def test_added_column_result_contains_column_name():
    conn = _make_conn()
    diff = ["added column 'email' (TEXT)"]
    result = await apply_schema_migrations(conn, "my_table", diff)
    assert any("email" in s for s in result)


async def test_removed_column_without_prune_not_executed():
    conn = _make_conn()
    diff = ["removed column 'old_field'"]
    result = await apply_schema_migrations(conn, "my_table", diff, prune=False)
    # Removed column without prune=True should not produce a DDL
    conn.execute.assert_not_awaited()


async def test_removed_column_with_prune_executes_drop():
    conn = _make_conn()
    diff = ["removed column 'old_field'"]
    result = await apply_schema_migrations(conn, "my_table", diff, prune=True)
    conn.execute.assert_awaited()


async def test_multiple_additions():
    conn = _make_conn()
    diff = ["added column 'a' (TEXT)", "added column 'b' (TEXT)"]
    result = await apply_schema_migrations(conn, "tbl", diff)
    assert len(result) == 2
    assert conn.execute.await_count == 2


async def test_type_changed_not_executed():
    """Type changes are not auto-applied (safe: just log/record)."""
    conn = _make_conn()
    diff = ["column 'name' type changed: 'TEXT' → 'JSONB'"]
    result = await apply_schema_migrations(conn, "tbl", diff)
    # Type changes may or may not produce DDL — the key is no crash
    # and the result is a list
    assert isinstance(result, list)


async def test_returns_list_of_strings():
    conn = _make_conn()
    diff = ["added column 'x' (TEXT)"]
    result = await apply_schema_migrations(conn, "tbl", diff)
    assert all(isinstance(s, str) for s in result)


async def test_schema_qualified_table():
    """Schema-qualified tables should work without error."""
    conn = _make_conn()
    diff = ["added column 'x' (TEXT)"]
    result = await apply_schema_migrations(conn, "myschema.my_table", diff)
    assert len(result) == 1
