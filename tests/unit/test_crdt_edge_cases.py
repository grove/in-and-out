"""Additional unit tests for lww_merge edge cases not covered in test_crdt.py."""
from __future__ import annotations

import pytest

from inandout.writeback.crdt import crdt_merge, gcounter_merge, lww_merge


# -- lww_merge edge cases --

def test_lww_merge_iso8601_strings_remote_newer():
    local = {"_updated_at": "2026-01-01T00:00:00Z", "val": "a"}
    remote = {"_updated_at": "2026-06-01T00:00:00Z", "val": "b"}
    assert lww_merge(local, remote) is None


def test_lww_merge_iso8601_strings_local_newer():
    local = {"_updated_at": "2026-06-01T00:00:00Z", "val": "a"}
    remote = {"_updated_at": "2026-01-01T00:00:00Z", "val": "b"}
    result = lww_merge(local, remote)
    assert result is local


def test_lww_merge_only_local_ts_returns_local():
    """Only local has timestamp — can't determine winner, return local."""
    local = {"_updated_at": "2026-01-01T00:00:00Z", "val": "a"}
    remote = {"val": "b"}
    result = lww_merge(local, remote)
    assert result is local


def test_lww_merge_only_remote_ts_returns_local():
    local = {"val": "a"}
    remote = {"_updated_at": "2026-01-01T00:00:00Z", "val": "b"}
    result = lww_merge(local, remote)
    assert result is local


# -- gcounter_merge edge cases --

def test_gcounter_local_less_than_remote_omitted():
    """When local value ≤ remote, the field should not appear in result."""
    local = {"views": 5}
    remote = {"views": 10}
    result = gcounter_merge(local, remote)
    assert "views" not in result


def test_gcounter_equal_values_omitted():
    local = {"clicks": 3}
    remote = {"clicks": 3}
    result = gcounter_merge(local, remote)
    assert "clicks" not in result


def test_gcounter_mixed_numeric_and_string():
    local = {"count": 5, "label": "foo"}
    remote = {"count": 3, "label": "bar"}
    result = gcounter_merge(local, remote)
    assert result["count"] == 2
    assert result["label"] == "foo"


def test_gcounter_float_local_larger():
    local = {"score": 1.5}
    remote = {"score": 0.5}
    result = gcounter_merge(local, remote)
    assert abs(result["score"] - 1.0) < 1e-9


# -- crdt_merge dispatch --

def test_crdt_merge_unknown_type_returns_local():
    local = {"k": "v"}
    remote = {"k": "old"}
    result = crdt_merge(local, remote, "unknown_crdt")
    assert result is local


def test_crdt_merge_lww_none_when_remote_newer():
    local = {"_updated_at": 100, "val": "a"}
    remote = {"_updated_at": 200, "val": "b"}
    assert crdt_merge(local, remote, "lww_register") is None


def test_crdt_merge_g_counter_returns_deltas():
    local = {"impressions": 10}
    remote = {"impressions": 7}
    result = crdt_merge(local, remote, "g_counter")
    assert result["impressions"] == 3
