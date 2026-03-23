"""Unit tests for CRDT merge helpers (T2 #6)."""
from __future__ import annotations

import pytest

from inandout.writeback.crdt import crdt_merge, gcounter_merge, lww_merge


# ---------------------------------------------------------------------------
# lww_merge
# ---------------------------------------------------------------------------

def test_lww_merge_remote_newer_returns_none():
    """lww_merge returns None (skip write) when remote timestamp is strictly greater."""
    local = {"name": "Alice", "_updated_at": "2026-01-01T10:00:00Z"}
    remote = {"name": "Bob", "_updated_at": "2026-01-02T10:00:00Z"}
    assert lww_merge(local, remote) is None


def test_lww_merge_local_newer_returns_local():
    """lww_merge returns local unchanged when local timestamp is greater."""
    local = {"name": "Alice", "_updated_at": "2026-01-02T10:00:00Z"}
    remote = {"name": "Bob", "_updated_at": "2026-01-01T10:00:00Z"}
    assert lww_merge(local, remote) == local


def test_lww_merge_equal_timestamps_returns_local():
    """lww_merge returns local when timestamps are equal (local wins on tie)."""
    ts = "2026-01-01T10:00:00Z"
    local = {"name": "Alice", "_updated_at": ts}
    remote = {"name": "Bob", "_updated_at": ts}
    assert lww_merge(local, remote) == local


def test_lww_merge_missing_ts_field_returns_local():
    """lww_merge cannot compare without ts_field — returns local (conservative)."""
    local = {"name": "Alice"}
    remote = {"name": "Bob"}
    assert lww_merge(local, remote) == local


def test_lww_merge_epoch_timestamps_numeric():
    """lww_merge handles Unix epoch float timestamps correctly (not string-compared)."""
    # "9" > "10" lexicographically but 9 < 10 numerically — float compare is correct
    local = {"count": 1, "_updated_at": 9}
    remote = {"count": 2, "_updated_at": 10}
    assert lww_merge(local, remote) is None  # remote is newer (10 > 9)


def test_lww_merge_epoch_timestamps_local_newer():
    local = {"count": 2, "_updated_at": 10}
    remote = {"count": 1, "_updated_at": 9}
    assert lww_merge(local, remote) == local


def test_lww_merge_custom_ts_field():
    """lww_merge respects a custom ts_field argument."""
    local = {"val": "x", "modified": "2026-02-01"}
    remote = {"val": "y", "modified": "2026-03-01"}
    assert lww_merge(local, remote, ts_field="modified") is None


# ---------------------------------------------------------------------------
# gcounter_merge
# ---------------------------------------------------------------------------

def test_gcounter_merge_delta_positive():
    """gcounter_merge sends positive delta for numeric counter fields."""
    local = {"views": 50, "likes": 10}
    remote = {"views": 30, "likes": 8}
    result = gcounter_merge(local, remote)
    assert result["views"] == 20  # delta = 50 - 30
    assert result["likes"] == 2   # delta = 10 - 8


def test_gcounter_merge_delta_zero_omitted():
    """gcounter_merge omits fields where local <= remote (no increment needed)."""
    local = {"views": 30}
    remote = {"views": 50}
    result = gcounter_merge(local, remote)
    assert "views" not in result


def test_gcounter_merge_non_numeric_forwarded_asis():
    """gcounter_merge forwards non-numeric fields unchanged (no delta)."""
    local = {"name": "Widget A", "status": "active"}
    remote = {"name": "Widget B", "status": "inactive"}
    result = gcounter_merge(local, remote)
    assert result["name"] == "Widget A"
    assert result["status"] == "active"


def test_gcounter_merge_private_fields_forwarded():
    """gcounter_merge forwards _-prefixed metadata fields as-is (not dropped)."""
    local = {"views": 5, "_updated_at": "2026-01-01", "_id": 42, "_etag": "abc123"}
    remote = {"views": 3, "_updated_at": "2025-12-01", "_id": 42, "_etag": "old"}
    result = gcounter_merge(local, remote)
    # Numeric counter gets delta
    assert result["views"] == 2
    # Metadata fields forwarded as-is
    assert result["_updated_at"] == "2026-01-01"
    assert result["_id"] == 42
    assert result["_etag"] == "abc123"


def test_gcounter_merge_private_field_previously_dropped():
    """Regression: _-prefixed fields must NOT be silently dropped."""
    local = {"_updated_at": "2026-03-01", "score": 100}
    remote = {"_updated_at": "2026-01-01", "score": 80}
    result = gcounter_merge(local, remote)
    assert "_updated_at" in result, "_updated_at must be present in the write payload"


def test_gcounter_merge_missing_remote_field():
    """gcounter_merge treats missing remote fields as 0 for numeric delta."""
    local = {"new_counter": 7}
    remote = {}
    result = gcounter_merge(local, remote)
    # remote_v is None, not a numeric — treated as non-numeric, forwarded as-is
    assert result["new_counter"] == 7


# ---------------------------------------------------------------------------
# crdt_merge dispatcher
# ---------------------------------------------------------------------------

def test_crdt_merge_lww_register_dispatch():
    local = {"x": 1, "_updated_at": "2026-01-02"}
    remote = {"x": 2, "_updated_at": "2026-01-01"}
    result = crdt_merge(local, remote, crdt_type="lww_register", ts_field="_updated_at")
    assert result == local


def test_crdt_merge_g_counter_dispatch():
    local = {"hits": 10}
    remote = {"hits": 7}
    result = crdt_merge(local, remote, crdt_type="g_counter")
    assert result is not None
    assert result["hits"] == 3


def test_crdt_merge_unknown_type_returns_local():
    local = {"a": 1}
    remote = {"a": 2}
    result = crdt_merge(local, remote, crdt_type="unknown_type")
    assert result == local
