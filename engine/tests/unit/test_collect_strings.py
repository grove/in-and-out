"""Unit tests for _collect_strings helper in config/connector.py."""
from __future__ import annotations

from inandout.config.connector import _collect_strings


def test_empty_dict_returns_empty():
    assert _collect_strings({}) == []


def test_empty_list_returns_empty():
    assert _collect_strings([]) == []


def test_string_values_extracted():
    result = _collect_strings({"a": "hello", "b": "world"})
    assert "hello" in result
    assert "world" in result


def test_nested_dict_strings_extracted():
    result = _collect_strings({"a": {"b": "deep"}})
    assert "deep" in result


def test_list_strings_extracted():
    result = _collect_strings(["x", "y", "z"])
    assert "x" in result
    assert "y" in result
    assert "z" in result


def test_mixed_nested_structure():
    obj = {"key": ["v1", {"inner": "v2"}], "top": "v3"}
    result = _collect_strings(obj)
    assert "v1" in result
    assert "v2" in result
    assert "v3" in result


def test_non_string_values_ignored():
    obj = {"num": 42, "flag": True, "none": None, "text": "keep"}
    result = _collect_strings(obj)
    assert "keep" in result
    assert 42 not in result
    assert True not in result
    assert None not in result


def test_deeply_nested():
    obj = {"a": {"b": {"c": {"d": "leaf"}}}}
    result = _collect_strings(obj)
    assert "leaf" in result


def test_list_of_dicts():
    obj = [{"url": "https://a.com"}, {"url": "https://b.com"}]
    result = _collect_strings(obj)
    assert "https://a.com" in result
    assert "https://b.com" in result


def test_dict_keys_not_included():
    # Only values, not keys
    result = _collect_strings({"my_key": "my_value"})
    assert "my_value" in result
    assert "my_key" not in result


def test_empty_string_included():
    result = _collect_strings({"a": ""})
    assert "" in result


def test_returns_list():
    result = _collect_strings({"a": "x"})
    assert isinstance(result, list)


def test_duplicate_strings_appear_multiple_times():
    result = _collect_strings({"a": "x", "b": "x"})
    assert result.count("x") == 2
