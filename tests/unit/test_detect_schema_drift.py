"""Unit tests for detect_schema_drift and prune_orphan_columns.

Covers:
- Added column (in DB, not in observed_keys) → returned as orphan.
- No drift when all DB columns match observed_keys.
- System columns (starting with _) are excluded from orphan list.
- Multiple orphans returned.
- prune_orphan_columns issues ALTER TABLE DROP COLUMN for each orphan.
- prune_orphan_columns returns count of dropped columns.
- Schema-qualified table names (namespace.table) split correctly.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call

import pytest

from inandout.postgres.schema_drift import detect_schema_drift, prune_orphan_columns


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_conn(columns: list[str]) -> AsyncMock:
    """Return a connection whose information_schema.columns query returns `columns`."""
    async def _execute(sql: str, params=None):
        cur = AsyncMock()
        cur.fetchall = AsyncMock(return_value=[(c,) for c in columns])
        return cur

    conn = AsyncMock()
    conn.execute = AsyncMock(side_effect=_execute)
    return conn


# ---------------------------------------------------------------------------
# detect_schema_drift
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_drift_detects_orphan_column():
    """Column in DB but not in observed_keys → returned as orphan."""
    conn = _make_conn(["external_id", "name", "old_field"])
    orphans = await detect_schema_drift(
        conn, "inout_src_hubspot_contacts", {"external_id", "name"}
    )
    assert "old_field" in orphans


@pytest.mark.anyio
async def test_drift_no_orphans_when_all_match():
    """When DB columns match observed_keys exactly, no orphans."""
    conn = _make_conn(["external_id", "name"])
    orphans = await detect_schema_drift(
        conn, "inout_src_hubspot_contacts", {"external_id", "name"}
    )
    assert orphans == []


@pytest.mark.anyio
async def test_drift_excludes_system_columns():
    """Columns starting with _ must NOT be reported as orphans."""
    conn = _make_conn(["external_id", "_ingested_at", "_raw_hash", "_deleted"])
    orphans = await detect_schema_drift(
        conn, "inout_src_hubspot_contacts", {"external_id"}
    )
    assert orphans == []


@pytest.mark.anyio
async def test_drift_returns_multiple_orphans():
    """Multiple orphan columns returned as list."""
    conn = _make_conn(["external_id", "stale_a", "stale_b", "_system"])
    orphans = await detect_schema_drift(
        conn, "inout_src_hubspot_contacts", {"external_id"}
    )
    assert set(orphans) == {"stale_a", "stale_b"}


@pytest.mark.anyio
async def test_drift_handles_schema_qualified_table():
    """Schema-qualified table name (namespace.table) is split correctly."""
    conn = _make_conn(["external_id"])
    orphans = await detect_schema_drift(
        conn,
        "tenant_42.inout_src_hubspot_contacts",
        {"external_id"},
    )
    # Should query with schema='tenant_42' and table='inout_src_hubspot_contacts'
    call_args = conn.execute.call_args
    params = call_args.args[1] if len(call_args.args) > 1 else call_args.kwargs.get("params", [])
    assert "tenant_42" in params


# ---------------------------------------------------------------------------
# prune_orphan_columns
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_prune_issues_alter_drop_per_column():
    """Each orphan column must result in one ALTER TABLE DROP COLUMN."""
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=AsyncMock())

    count = await prune_orphan_columns(
        conn, "inout_src_hubspot_contacts", ["old_a", "old_b"]
    )

    assert conn.execute.call_count == 2


@pytest.mark.anyio
async def test_prune_returns_count_of_dropped():
    """Return value must equal len(orphan_columns)."""
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=AsyncMock())

    count = await prune_orphan_columns(
        conn, "inout_src_hubspot_contacts", ["col1", "col2", "col3"]
    )

    assert count == 3


@pytest.mark.anyio
async def test_prune_returns_zero_for_empty_list():
    """No orphans → zero drops, execute not called."""
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=AsyncMock())

    count = await prune_orphan_columns(conn, "inout_src_hubspot_contacts", [])

    assert count == 0
    conn.execute.assert_not_called()
