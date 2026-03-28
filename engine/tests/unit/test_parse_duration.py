"""parse_duration unit tests.

Covers:
- All four suffixes: s, m, h, d.
- Fractional values (e.g. "1.5h").
- Bare integers are invalid (no unit suffix → raises ValueError).
- Whitespace around the string is tolerated.
- Invalid strings raise ValueError.
"""
from __future__ import annotations

import pytest

from inandout.config._duration import parse_duration


# ---------------------------------------------------------------------------
# Supported suffixes
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("s,expected", [
    ("0s", 0.0),
    ("1s", 1.0),
    ("30s", 30.0),
    ("60s", 60.0),
    ("1m", 60.0),
    ("5m", 300.0),
    ("90m", 5400.0),
    ("1h", 3600.0),
    ("2h", 7200.0),
    ("24h", 86400.0),
    ("1d", 86400.0),
    ("7d", 604800.0),
    ("30d", 2592000.0),
])
def test_parse_duration_supported_suffixes(s: str, expected: float):
    assert parse_duration(s) == pytest.approx(expected)


# ---------------------------------------------------------------------------
# Fractional values
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("s,expected", [
    ("1.5h", 5400.0),
    ("0.5m", 30.0),
    ("2.5d", 216000.0),
    ("0.1s", 0.1),
])
def test_parse_duration_fractional_values(s: str, expected: float):
    assert parse_duration(s) == pytest.approx(expected)


# ---------------------------------------------------------------------------
# Whitespace tolerance
# ---------------------------------------------------------------------------

def test_parse_duration_strips_leading_trailing_whitespace():
    assert parse_duration("  30s  ") == pytest.approx(30.0)


def test_parse_duration_internal_whitespace_between_number_and_unit():
    # The regex allows optional whitespace between number and unit
    assert parse_duration("30 s") == pytest.approx(30.0)


# ---------------------------------------------------------------------------
# Invalid inputs raise ValueError
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("s", [
    "forever",
    "30",       # bare integer, no unit
    "30x",      # unknown suffix
    "",
    "abc",
    "-5s",      # negative not supported
    "1.2.3s",
])
def test_parse_duration_invalid_raises_value_error(s: str):
    with pytest.raises(ValueError):
        parse_duration(s)


def test_parse_duration_returns_float():
    assert isinstance(parse_duration("1h"), float)
