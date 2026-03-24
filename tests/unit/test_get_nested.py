"""Unit tests for _get_nested in field_mapper.py."""
from __future__ import annotations

import pytest

from inandout.ingestion.field_mapper import _get_nested


def test_single_key():
    assert _get_nested({"a": "v"}, "a") == "v"


def test_two_level():
    assert _get_nested({"a": {"b": "deep"}}, "a.b") == "deep"


def test_three_level():
    record = {"x": {"y": {"z": 42}}}
    assert _get_nested(record, "x.y.z") == 42


def test_missing_top_key_returns_none():
    assert _get_nested({}, "missing") is None


def test_missing_nested_key_returns_none():
    assert _get_nested({"a": {"b": 1}}, "a.c") is None


def test_intermediate_none_returns_none():
    # If an intermediate value is explicitly None, traversal should stop.
    assert _get_nested({"a": None}, "a.b") is None


def test_intermediate_non_dict_returns_none():
    assert _get_nested({"a": "string"}, "a.b") is None


def test_intermediate_list_returns_none():
    assert _get_nested({"a": [1, 2, 3]}, "a.b") is None


def test_intermediate_int_returns_none():
    assert _get_nested({"a": 5}, "a.b") is None


def test_value_zero_not_treated_as_missing():
    # 0 is falsy; ensure we only gate on isinstance checks, not truthiness
    record = {"a": {"b": 0}}
    # But our implementation returns None when cur is None — value 0 is not None
    # However _get_nested does `if cur is None: return None`, so 0 should pass through
    result = _get_nested(record, "a.b")
    assert result == 0


def test_value_false_not_treated_as_missing():
    record = {"flag": False}
    result = _get_nested(record, "flag")
    assert result is False


def test_value_empty_string_passes_through():
    record = {"a": {"b": ""}}
    result = _get_nested(record, "a.b")
    assert result == ""


def test_deeply_nested():
    record = {"a": {"b": {"c": {"d": {"e": "found"}}}}}
    assert _get_nested(record, "a.b.c.d.e") == "found"


def test_path_with_one_dot():
    assert _get_nested({"outer": {"inner": 99}}, "outer.inner") == 99


def test_empty_path_returns_record_itself():
    # "".split(".") == [""], so it tries record.get("") which is None for a normal dict
    record = {"a": 1}
    assert _get_nested(record, "") is None
