"""Unit tests for sync run diff engine (Step 82)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.diff.engine import SyncRunDiff, compute_field_diff, diff_sync_runs


# ---------------------------------------------------------------------------
# compute_field_diff tests
# ---------------------------------------------------------------------------

def test_compute_field_diff_added_field():
    """Field present in after but not before → included in changed."""
    before = {"name": "Alice"}
    after = {"name": "Alice", "email": "alice@example.com"}
    changed = compute_field_diff(before, after)
    assert "email" in changed


def test_compute_field_diff_removed_field():
    """Field present in before but not after → included in changed."""
    before = {"name": "Alice", "email": "alice@example.com"}
    after = {"name": "Alice"}
    changed = compute_field_diff(before, after)
    assert "email" in changed


def test_compute_field_diff_changed_value():
    """Field with different value → included in changed."""
    before = {"name": "Alice", "email": "old@example.com"}
    after = {"name": "Alice", "email": "new@example.com"}
    changed = compute_field_diff(before, after)
    assert "email" in changed
    assert "name" not in changed


def test_compute_field_diff_no_changes():
    """Identical dicts → empty list."""
    before = {"name": "Alice", "score": 99}
    after = {"name": "Alice", "score": 99}
    changed = compute_field_diff(before, after)
    assert changed == []


def test_compute_field_diff_empty():
    """Both empty → empty list."""
    assert compute_field_diff({}, {}) == []


# ---------------------------------------------------------------------------
# diff_sync_runs tests — using mock pool
# ---------------------------------------------------------------------------

def _make_mock_pool(rows_a, rows_b):
    """Build a mock pool that returns rows_a for run_a and rows_b for run_b."""
    call_count = [0]

    async def _mock_fetchall():
        idx = call_count[0]
        call_count[0] += 1
        if idx == 0:
            return rows_a
        return rows_b

    mock_cursor = MagicMock()
    mock_cursor.fetchall = _mock_fetchall

    mock_conn_exec = AsyncMock(return_value=mock_cursor)
    mock_conn = MagicMock()
    mock_conn.execute = mock_conn_exec

    mock_pool_conn = MagicMock()
    mock_pool_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool_conn.__aexit__ = AsyncMock(return_value=None)

    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=mock_pool_conn)
    return mock_pool


@pytest.mark.anyio
async def test_diff_added_record():
    """Record in run_b only → appears in diff.added."""
    rows_a = [("ext-1", {"name": "Alice"})]
    rows_b = [("ext-1", {"name": "Alice"}), ("ext-2", {"name": "Bob"})]

    pool = _make_mock_pool(rows_a, rows_b)
    diff = await diff_sync_runs(pool, "myconn", "contacts", "run-a", "run-b")

    assert "ext-2" in diff.added
    assert diff.unchanged_count == 1
    assert len(diff.removed) == 0
    assert len(diff.changed) == 0


@pytest.mark.anyio
async def test_diff_removed_record():
    """Record in run_a only → appears in diff.removed."""
    rows_a = [("ext-1", {"name": "Alice"}), ("ext-2", {"name": "Bob"})]
    rows_b = [("ext-1", {"name": "Alice"})]

    pool = _make_mock_pool(rows_a, rows_b)
    diff = await diff_sync_runs(pool, "myconn", "contacts", "run-a", "run-b")

    assert "ext-2" in diff.removed
    assert diff.unchanged_count == 1
    assert len(diff.added) == 0


@pytest.mark.anyio
async def test_diff_changed_record():
    """Record in both runs with different raw → appears in diff.changed with fields_changed."""
    rows_a = [("ext-1", {"name": "Alice", "email": "old@example.com"})]
    rows_b = [("ext-1", {"name": "Alice", "email": "new@example.com"})]

    pool = _make_mock_pool(rows_a, rows_b)
    diff = await diff_sync_runs(pool, "myconn", "contacts", "run-a", "run-b")

    assert len(diff.changed) == 1
    changed_entry = diff.changed[0]
    assert changed_entry["external_id"] == "ext-1"
    assert "email" in changed_entry["fields_changed"]
    assert changed_entry["before"]["email"] == "old@example.com"
    assert changed_entry["after"]["email"] == "new@example.com"
    assert diff.unchanged_count == 0


@pytest.mark.anyio
async def test_diff_unchanged_record():
    """Record in both runs with identical raw → counted in unchanged_count."""
    rows_a = [("ext-1", {"name": "Alice"})]
    rows_b = [("ext-1", {"name": "Alice"})]

    pool = _make_mock_pool(rows_a, rows_b)
    diff = await diff_sync_runs(pool, "myconn", "contacts", "run-a", "run-b")

    assert diff.unchanged_count == 1
    assert len(diff.changed) == 0
    assert len(diff.added) == 0
    assert len(diff.removed) == 0


@pytest.mark.anyio
async def test_diff_empty_runs():
    """Both runs have no records → empty diff."""
    pool = _make_mock_pool([], [])
    diff = await diff_sync_runs(pool, "myconn", "contacts", "run-a", "run-b")

    assert diff.added == []
    assert diff.removed == []
    assert diff.changed == []
    assert diff.unchanged_count == 0


@pytest.mark.anyio
async def test_diff_run_ids_stored():
    """SyncRunDiff stores run_a and run_b IDs."""
    pool = _make_mock_pool([], [])
    diff = await diff_sync_runs(pool, "myconn", "contacts", "run-uuid-a", "run-uuid-b")

    assert diff.run_a == "run-uuid-a"
    assert diff.run_b == "run-uuid-b"
