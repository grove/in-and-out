"""Unit tests for _resolve_dot_path in ingestion/webhook_lifecycle.py."""
from __future__ import annotations

import pytest

from inandout.ingestion.webhook_lifecycle import _resolve_dot_path


def test_simple_top_level_key():
    assert _resolve_dot_path({"id": "abc"}, "id") == "abc"


def test_nested_two_levels():
    obj = {"data": {"id": 42}}
    assert _resolve_dot_path(obj, "data.id") == 42


def test_nested_three_levels():
    obj = {"a": {"b": {"c": "deep"}}}
    assert _resolve_dot_path(obj, "a.b.c") == "deep"


def test_missing_top_level_key_returns_none():
    assert _resolve_dot_path({}, "missing") is None


def test_missing_nested_key_returns_none():
    obj = {"data": {"id": 1}}
    assert _resolve_dot_path(obj, "data.nope") is None


def test_non_dict_intermediate_returns_none():
    obj = {"items": [1, 2, 3]}
    assert _resolve_dot_path(obj, "items.0") is None


def test_none_value_returned():
    obj = {"key": None}
    assert _resolve_dot_path(obj, "key") is None


def test_integer_value_returned():
    obj = {"count": 99}
    assert _resolve_dot_path(obj, "count") == 99


def test_list_value_returned():
    obj = {"tags": ["a", "b"]}
    result = _resolve_dot_path(obj, "tags")
    assert result == ["a", "b"]


def test_empty_nested_dict():
    obj = {"meta": {}}
    assert _resolve_dot_path(obj, "meta.missing") is None


def test_path_traverses_through_empty_key():
    obj = {"a": {"": "val"}}
    # dot-split of "a." yields ["a", ""]  → key "" looked up in inner dict
    assert _resolve_dot_path(obj, "a.") == "val"


def test_value_is_false():
    obj = {"active": False}
    assert _resolve_dot_path(obj, "active") is False


def test_value_is_zero():
    obj = {"count": 0}
    assert _resolve_dot_path(obj, "count") == 0
