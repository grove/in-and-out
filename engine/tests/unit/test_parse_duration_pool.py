"""Unit tests for parse_duration and _parse_lifetime_seconds."""
from __future__ import annotations

import pytest

from inandout.config._duration import parse_duration
from inandout.postgres.pool import _parse_lifetime_seconds


# --- parse_duration ---

def test_seconds():
    assert parse_duration("30s") == 30.0


def test_minutes():
    assert parse_duration("5m") == 300.0


def test_hours():
    assert parse_duration("1h") == 3600.0


def test_days():
    assert parse_duration("1d") == 86400.0


def test_fractional_seconds():
    assert parse_duration("1.5s") == 1.5


def test_fractional_minutes():
    assert abs(parse_duration("0.5m") - 30.0) < 1e-9


def test_with_whitespace():
    assert parse_duration("  10s  ") == 10.0


def test_zero_seconds():
    assert parse_duration("0s") == 0.0


def test_large_value():
    assert parse_duration("365d") == 365 * 86400.0


def test_invalid_raises_value_error():
    with pytest.raises(ValueError):
        parse_duration("forever")


def test_no_unit_raises_value_error():
    with pytest.raises(ValueError):
        parse_duration("30")


def test_unknown_unit_raises_value_error():
    with pytest.raises(ValueError):
        parse_duration("30x")


def test_empty_string_raises_value_error():
    with pytest.raises(ValueError):
        parse_duration("")


# --- _parse_lifetime_seconds ---

def test_lifetime_seconds_delegates_to_parse_duration():
    assert _parse_lifetime_seconds("60s") == 60.0


def test_lifetime_minutes():
    assert _parse_lifetime_seconds("10m") == 600.0


def test_lifetime_hours():
    assert _parse_lifetime_seconds("2h") == 7200.0
