"""Unit tests for schema_migration (Step 65)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call

import pytest


async def test_added_column_triggers_add_ddl():
    """An 'added column' diff entry should execute ALTER TABLE ADD COLUMN."""
    from inandout.postgres.schema_migration import apply_schema_migrations

    mock_cursor = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    diff = ["added column 'email' (TEXT)"]
    executed = await apply_schema_migrations(mock_conn, "test_table", diff, prune=False)

    assert len(executed) == 1
    assert "ADD COLUMN" in executed[0]
    assert "email" in executed[0]
    mock_conn.execute.assert_called_once()


async def test_removed_column_with_prune_true_triggers_drop_ddl():
    """Removed column with prune=True should execute ALTER TABLE DROP COLUMN."""
    from inandout.postgres.schema_migration import apply_schema_migrations

    mock_cursor = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    diff = ["removed column 'legacy_field'"]
    executed = await apply_schema_migrations(mock_conn, "test_table", diff, prune=True)

    assert len(executed) == 1
    assert "DROP COLUMN" in executed[0]
    assert "legacy_field" in executed[0]
    mock_conn.execute.assert_called_once()


async def test_removed_column_with_prune_false_no_ddl():
    """Removed column with prune=False should NOT execute any DDL."""
    from inandout.postgres.schema_migration import apply_schema_migrations

    mock_conn = AsyncMock()

    diff = ["removed column 'legacy_field'"]
    executed = await apply_schema_migrations(mock_conn, "test_table", diff, prune=False)

    assert len(executed) == 0
    mock_conn.execute.assert_not_called()


async def test_type_changed_column_no_ddl_warning_only(caplog):
    """Type-changed column should not trigger DDL — just a warning log."""
    import logging
    from inandout.postgres.schema_migration import apply_schema_migrations

    mock_conn = AsyncMock()

    diff = ["column 'price' type changed: 'TEXT' → 'FLOAT'"]
    executed = await apply_schema_migrations(mock_conn, "test_table", diff, prune=False)

    assert len(executed) == 0
    mock_conn.execute.assert_not_called()


async def test_multiple_changes_mixed():
    """Mixed diff: add + remove (prune=True) + type change all handled correctly."""
    from inandout.postgres.schema_migration import apply_schema_migrations

    mock_cursor = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    diff = [
        "added column 'new_field' (TEXT)",
        "removed column 'old_field'",
        "column 'count' type changed: 'TEXT' → 'INTEGER'",
    ]
    executed = await apply_schema_migrations(mock_conn, "test_table", diff, prune=True)

    # Only add + drop should produce DDL; type change is a no-op
    assert len(executed) == 2
    ddl_str = " ".join(executed)
    assert "ADD COLUMN" in ddl_str
    assert "DROP COLUMN" in ddl_str
    assert mock_conn.execute.call_count == 2


async def test_qualified_table_name():
    """Schema-qualified table names should be handled correctly."""
    from inandout.postgres.schema_migration import apply_schema_migrations

    mock_cursor = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    diff = ["added column 'score' (FLOAT)"]
    executed = await apply_schema_migrations(mock_conn, "myschema.test_table", diff)

    assert len(executed) == 1
    mock_conn.execute.assert_called_once()
