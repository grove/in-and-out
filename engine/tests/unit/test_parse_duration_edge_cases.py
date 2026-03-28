"""Additional edge-case unit tests for parse_duration."""
from __future__ import annotations

import pytest

from inandout.config._duration import parse_duration


@pytest.mark.parametrize("s,expected", [
    ("1s", 1.0),
    ("1m", 60.0),
    ("1h", 3600.0),
    ("1d", 86400.0),
])
def test_unit_multipliers(s: str, expected: float):
    assert parse_duration(s) == expected


@pytest.mark.parametrize("s,expected", [
    ("2s", 2.0),
    ("2m", 120.0),
    ("2h", 7200.0),
    ("2d", 172800.0),
])
def test_integer_multiples(s: str, expected: float):
    assert parse_duration(s) == expected


def test_float_seconds_precision():
    result = parse_duration("0.001s")
    assert abs(result - 0.001) < 1e-12


def test_float_minutes():
    result = parse_duration("1.5m")
    assert abs(result - 90.0) < 1e-9


def test_float_hours():
    result = parse_duration("0.5h")
    assert abs(result - 1800.0) < 1e-9


def test_leading_space_accepted():
    assert parse_duration("  5s") == 5.0


def test_trailing_space_accepted():
    assert parse_duration("5s  ") == 5.0


def test_both_spaces_accepted():
    assert parse_duration("  10m  ") == 600.0


def test_zero_minutes():
    assert parse_duration("0m") == 0.0


def test_zero_hours():
    assert parse_duration("0h") == 0.0


def test_zero_days():
    assert parse_duration("0d") == 0.0


def test_large_days():
    assert parse_duration("365d") == 365 * 86400.0


def test_negative_number_raises():
    with pytest.raises(ValueError):
        parse_duration("-5s")


def test_plus_sign_raises():
    with pytest.raises(ValueError):
        parse_duration("+5s")


def test_uppercase_unit_raises():
    with pytest.raises(ValueError):
        parse_duration("5S")


def test_multiple_units_raises():
    with pytest.raises(ValueError):
        parse_duration("1h30m")


def test_space_between_number_and_unit():
    # The pattern allows whitespace between number and unit: r'(\d+(?:\.\d+)?)\s*([smhd])'
    assert parse_duration("5 s") == 5.0


def test_type_is_float():
    result = parse_duration("1s")
    assert isinstance(result, float)
