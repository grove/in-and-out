"""Unit tests for _to_pg_interval.

Covers:
- Whole-day durations (n*86400 secs) → "{n} days"
- Multi-day strings: "30d", "7d", "90d", "1d"
- Sub-day durations (< 86400 secs) → "0 days"
- Fractional days truncate (floor), not round
"""
from __future__ import annotations

import pytest

from inandout.postgres.housekeeping import _to_pg_interval


@pytest.mark.parametrize("s,expected", [
    ("1d",   "1 days"),
    ("7d",   "7 days"),
    ("30d",  "30 days"),
    ("90d",  "90 days"),
    ("365d", "365 days"),
])
def test_to_pg_interval_day_durations(s: str, expected: str):
    assert _to_pg_interval(s) == expected


@pytest.mark.parametrize("s", ["1s", "30s", "60s"])
def test_to_pg_interval_seconds_yields_zero_days(s: str):
    assert _to_pg_interval(s) == "0 days"


@pytest.mark.parametrize("s", ["1m", "5m", "59m"])
def test_to_pg_interval_minutes_yields_zero_days(s: str):
    assert _to_pg_interval(s) == "0 days"


def test_to_pg_interval_23h_yields_zero_days():
    assert _to_pg_interval("23h") == "0 days"


def test_to_pg_interval_24h_yields_one_day():
    assert _to_pg_interval("24h") == "1 days"


def test_to_pg_interval_48h_yields_two_days():
    assert _to_pg_interval("48h") == "2 days"


def test_to_pg_interval_fractional_days_truncates():
    """1.5d → int(1.5 * 86400 / 86400) == 1."""
    result = _to_pg_interval("1.5d")
    assert result == "1 days"


def test_to_pg_interval_returns_string():
    assert isinstance(_to_pg_interval("7d"), str)
