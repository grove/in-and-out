"""Unit tests for _compute_field_diff in writeback/engine.py."""
from __future__ import annotations

import pytest

from inandout.writeback.engine import _compute_field_diff


def test_no_changes_returns_empty():
    last = {"a": 1, "b": "x"}
    sent = {"a": 1, "b": "x"}
    diff = _compute_field_diff(last, sent)
    assert diff["added"] == []
    assert diff["removed"] == []
    assert diff["changed"] == {}


def test_added_key_detected():
    last = {"a": 1}
    sent = {"a": 1, "b": 2}
    diff = _compute_field_diff(last, sent)
    assert "b" in diff["added"]


def test_removed_key_detected():
    last = {"a": 1, "b": 2}
    sent = {"a": 1}
    diff = _compute_field_diff(last, sent)
    assert "b" in diff["removed"]


def test_changed_value_detected():
    last = {"a": 1, "b": "old"}
    sent = {"a": 1, "b": "new"}
    diff = _compute_field_diff(last, sent)
    assert "b" in diff["changed"]
    assert diff["changed"]["b"]["from"] == "old"
    assert diff["changed"]["b"]["to"] == "new"


def test_changed_contains_from_and_to():
    last = {"x": 10}
    sent = {"x": 99}
    diff = _compute_field_diff(last, sent)
    assert diff["changed"]["x"] == {"from": 10, "to": 99}


def test_multiple_added():
    last = {}
    sent = {"a": 1, "b": 2, "c": 3}
    diff = _compute_field_diff(last, sent)
    assert set(diff["added"]) == {"a", "b", "c"}
    assert diff["removed"] == []


def test_multiple_removed():
    last = {"a": 1, "b": 2, "c": 3}
    sent = {}
    diff = _compute_field_diff(last, sent)
    assert set(diff["removed"]) == {"a", "b", "c"}
    assert diff["added"] == []


def test_mixed_add_remove_change():
    last = {"keep": 1, "remove": 2, "change": "old"}
    sent = {"keep": 1, "new_field": 9, "change": "new"}
    diff = _compute_field_diff(last, sent)
    assert "new_field" in diff["added"]
    assert "remove" in diff["removed"]
    assert "change" in diff["changed"]
    assert "keep" not in diff["changed"]


def test_result_keys_present():
    diff = _compute_field_diff({}, {})
    assert "added" in diff
    assert "removed" in diff
    assert "changed" in diff


def test_empty_both_sides():
    diff = _compute_field_diff({}, {})
    assert diff == {"added": [], "removed": [], "changed": {}}


def test_none_value_change():
    last = {"val": None}
    sent = {"val": "something"}
    diff = _compute_field_diff(last, sent)
    assert "val" in diff["changed"]
    assert diff["changed"]["val"]["from"] is None
