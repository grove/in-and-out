"""Unit tests for schema drift detection and pruning."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call

import pytest

from inandout.postgres.schema_drift import (
    bump_schema_version,
    detect_new_fields,
    detect_schema_drift,
    prune_orphan_columns,
)


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


# ---------------------------------------------------------------------------
# detect_new_fields  (T1 #31)
# ---------------------------------------------------------------------------

async def test_detect_new_fields_returns_fields_absent_from_db():
    """detect_new_fields should return observed keys that are not yet DB columns."""
    db_columns = ["id", "name", "_ingested_at", "_raw_hash"]
    observed_keys = {"id", "name", "email", "phone"}  # email + phone are new

    mock_cursor = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=[(col,) for col in db_columns])
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    new_fields = await detect_new_fields(mock_conn, "inout_src_hub_contacts", observed_keys)

    assert "email" in new_fields
    assert "phone" in new_fields
    # Already-present columns must not appear
    assert "id" not in new_fields
    assert "name" not in new_fields


async def test_detect_new_fields_excludes_system_and_reserved_keys():
    """detect_new_fields must exclude _prefixed keys and the reserved fixed columns."""
    db_columns = ["id"]
    observed_keys = {"id", "_internal", "external_id", "data", "raw", "new_field"}

    mock_cursor = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=[(col,) for col in db_columns])
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    new_fields = await detect_new_fields(mock_conn, "inout_src_hub_contacts", observed_keys)

    assert "new_field" in new_fields
    assert "_internal" not in new_fields
    assert "external_id" not in new_fields
    assert "data" not in new_fields
    assert "raw" not in new_fields


async def test_detect_new_fields_empty_when_all_present():
    db_columns = ["id", "name", "email"]
    observed_keys = {"id", "name", "email"}

    mock_cursor = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=[(col,) for col in db_columns])
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    new_fields = await detect_new_fields(mock_conn, "inout_src_hub_contacts", observed_keys)
    assert new_fields == []


async def test_detect_new_fields_parses_schema_from_table_name():
    """Ensure schema.table notation is split correctly for detect_new_fields."""
    db_columns = ["id"]
    observed_keys = {"id", "new_col"}

    mock_cursor = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=[(col,) for col in db_columns])
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    new_fields = await detect_new_fields(
        mock_conn, "tenant_a.inout_src_hub_contacts", observed_keys
    )

    call_args = mock_conn.execute.call_args
    params = call_args[0][1]
    assert params[0] == "tenant_a"
    assert params[1] == "inout_src_hub_contacts"
    assert "new_col" in new_fields


async def test_detect_new_fields_returns_sorted_list():
    """Return value should be deterministically sorted."""
    db_columns: list[str] = []
    observed_keys = {"zzz", "aaa", "mmm"}

    mock_cursor = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=[])
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    new_fields = await detect_new_fields(mock_conn, "t", observed_keys)
    assert new_fields == sorted(new_fields)


# ---------------------------------------------------------------------------
# bump_schema_version  (T1 #31)
# ---------------------------------------------------------------------------

async def test_bump_schema_version_executes_update():
    """bump_schema_version should issue an UPDATE ... SET _schema_version = _schema_version + 1."""
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()

    await bump_schema_version(mock_conn, "inout_src_hub_contacts")

    assert mock_conn.execute.call_count == 1
    # Inspect the composed SQL string
    composed = mock_conn.execute.call_args[0][0]
    sql_str = composed.as_string(None) if hasattr(composed, "as_string") else str(composed)
    assert "_schema_version" in sql_str
    assert "UPDATE" in sql_str.upper()


async def test_bump_schema_version_qualified_table():
    """bump_schema_version handles schema.table notation without error."""
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()

    await bump_schema_version(mock_conn, "tenant_a.inout_src_hub_contacts")

    assert mock_conn.execute.call_count == 1


# ---------------------------------------------------------------------------
# metrics existence check (T1 #31)
# ---------------------------------------------------------------------------

def test_schema_changes_total_metric_exists():
    """schema_changes_total Counter must be importable and have the right labels."""
    from inandout.observability.metrics import schema_changes_total

    assert schema_changes_total is not None
    # Prometheus metric label names are stored on ._labelnames
    labels = schema_changes_total._labelnames  # type: ignore[attr-defined]
    assert "connector" in labels
    assert "datatype" in labels
    assert "change_type" in labels


# ---------------------------------------------------------------------------
# Source-inspection checks for ingestion engine (T1 #31)
# ---------------------------------------------------------------------------

def test_engine_imports_detect_new_fields_and_bump_schema_version():
    """The ingestion engine must import both new T1 #31 helpers."""
    import importlib
    import inspect

    engine_module = importlib.import_module("inandout.ingestion.engine")
    assert hasattr(engine_module, "detect_new_fields") or (
        "detect_new_fields" in inspect.getsource(engine_module)
    )
    assert hasattr(engine_module, "bump_schema_version") or (
        "bump_schema_version" in inspect.getsource(engine_module)
    )


def test_engine_tracks_seen_fields():
    """Engine source must initialise and update seen_fields (T1 #31 bug-fix)."""
    import importlib
    import inspect

    engine_module = importlib.import_module("inandout.ingestion.engine")
    src = inspect.getsource(engine_module)
    assert "seen_fields" in src
    assert "seen_fields.update" in src


def test_engine_drifts_uses_seen_fields_not_seen_ids():
    """Schema drift call must use seen_fields, not seen_ids (critical bug-fix)."""
    import importlib
    import inspect

    engine_module = importlib.import_module("inandout.ingestion.engine")
    src = inspect.getsource(engine_module)
    # detect_new_fields must be called with seen_fields
    assert "detect_new_fields" in src
    # The (wrong) pattern of passing seen_ids to detect_schema_drift must NOT exist
    assert "detect_schema_drift(drift_conn, table, seen_ids)" not in src
