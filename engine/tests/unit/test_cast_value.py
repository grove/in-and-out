"""Unit tests for _cast_value in field_mapper.py."""
from __future__ import annotations

import datetime

import pytest

from inandout.ingestion.field_mapper import _cast_value


def test_cast_str_from_int():
    assert _cast_value(42, "str") == "42"
    assert isinstance(_cast_value(42, "str"), str)


def test_cast_str_from_float():
    assert _cast_value(3.14, "str") == "3.14"


def test_cast_str_from_none():
    # str(None) == "None" — cast function is applied regardless
    assert _cast_value(None, "str") == "None"


def test_cast_int_from_string():
    assert _cast_value("5", "int") == 5
    assert isinstance(_cast_value("5", "int"), int)


def test_cast_int_from_float():
    assert _cast_value(3.9, "int") == 3


def test_cast_float_from_string():
    result = _cast_value("3.14", "float")
    assert abs(result - 3.14) < 1e-9
    assert isinstance(result, float)


def test_cast_bool_truthy():
    assert _cast_value(1, "bool") is True


def test_cast_bool_falsy():
    assert _cast_value(0, "bool") is False


def test_cast_bool_string():
    # bool("False") is True because non-empty string
    assert _cast_value("False", "bool") is True


def test_cast_datetime_from_iso_string():
    result = _cast_value("2026-01-15T10:30:00", "datetime")
    assert isinstance(result, datetime.datetime)
    assert result.year == 2026
    assert result.month == 1
    assert result.day == 15


def test_cast_datetime_preserves_tz():
    result = _cast_value("2026-06-01T12:00:00+02:00", "datetime")
    assert isinstance(result, datetime.datetime)
    assert result.utcoffset() is not None


def test_cast_date_from_iso_string():
    result = _cast_value("2026-03-22", "date")
    assert isinstance(result, datetime.date)
    assert result.year == 2026
    assert result.month == 3
    assert result.day == 22


def test_cast_unknown_key_returns_value_unchanged():
    sentinel = object()
    assert _cast_value(sentinel, "uuid") is sentinel


def test_cast_unknown_key_with_string():
    assert _cast_value("hello", "json") == "hello"


def test_cast_unknown_key_empty_string():
    assert _cast_value("x", "") == "x"


def test_cast_datetime_round_trip():
    dt = datetime.datetime(2026, 1, 1, 0, 0, 0)
    assert _cast_value(dt.isoformat(), "datetime") == dt


def test_cast_date_round_trip():
    d = datetime.date(2025, 12, 31)
    assert _cast_value(d.isoformat(), "date") == d
