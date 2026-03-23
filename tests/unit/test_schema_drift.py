"""Unit tests for schema drift detection and pruning."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from inandout.postgres.schema_drift import detect_schema_drift, prune_orphan_columns


# ---------------------------------------------------------------------------
# detect_schema_drift
# ---------------------------------------------------------------------------

async def test_detect_schema_drift_returns_orphan_columns():
    """detect_schema_drift should return columns present in DB but not in observed_keys."""
    # DB has columns: id, name, email, phone  (plus system columns _raw_hash etc.)
    db_columns = ["id", "name", "email", "phone", "_raw_hash", "_ingested_at"]
    observed_keys = {"id", "name", "email"}  # 'phone' is orphaned

    # Build a mock connection whose execute() returns a cursor with the DB columns.
    mock_cursor = AsyncMock()
    mock_cursor.fetchall = AsyncMock(
        return_value=[(col,) for col in db_columns]
    )

    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    orphans = await detect_schema_drift(mock_conn, "inout_src_hub_contacts", observed_keys)

    assert "phone" in orphans
    assert "id" not in orphans
    assert "name" not in orphans
    assert "email" not in orphans
    # System columns must be excluded
    assert "_raw_hash" not in orphans
    assert "_ingested_at" not in orphans


async def test_detect_schema_drift_no_orphans():
    db_columns = ["id", "name", "_raw_hash"]
    observed_keys = {"id", "name"}

    mock_cursor = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=[(col,) for col in db_columns])
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    orphans = await detect_schema_drift(mock_conn, "inout_src_hub_contacts", observed_keys)
    assert orphans == []


async def test_detect_schema_drift_parses_schema_from_table_name():
    """Ensure the schema.table notation is split correctly."""
    db_columns = ["id", "legacy_field"]
    observed_keys = {"id"}

    mock_cursor = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=[(col,) for col in db_columns])
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    orphans = await detect_schema_drift(
        mock_conn, "tenant_a.inout_src_hub_contacts", observed_keys
    )

    # Verify that the correct schema / table were passed to the query.
    call_args = mock_conn.execute.call_args
    params = call_args[0][1]  # positional args[1] is the parameter list
    assert params[0] == "tenant_a"
    assert params[1] == "inout_src_hub_contacts"
    assert "legacy_field" in orphans


# ---------------------------------------------------------------------------
# prune_orphan_columns
# ---------------------------------------------------------------------------

async def test_prune_orphan_columns_generates_alter_table():
    """prune_orphan_columns should issue ALTER TABLE ... DROP COLUMN for each orphan."""
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()

    count = await prune_orphan_columns(mock_conn, "inout_src_hub_contacts", ["phone", "legacy"])

    assert count == 2
    # execute should have been called twice (once per column)
    assert mock_conn.execute.call_count == 2


async def test_prune_orphan_columns_returns_zero_for_empty_list():
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()

    count = await prune_orphan_columns(mock_conn, "inout_src_hub_contacts", [])
    assert count == 0
    mock_conn.execute.assert_not_called()


async def test_prune_orphan_columns_with_namespace():
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()

    count = await prune_orphan_columns(
        mock_conn, "tenant_a.inout_src_hub_contacts", ["old_col"]
    )
    assert count == 1
    mock_conn.execute.assert_called_once()
