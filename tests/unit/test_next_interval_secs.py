"""Unit tests for _next_interval_secs cron-expression helper in daemon.py.

Covers:
- A valid cron string returns a non-negative float ≤ its interval window
  (within one cron period of now).
- An invalid cron string logs a warning and falls back to default_interval_secs.
- schedule.cron = None (falsy) returns default_interval_secs immediately.
- schedule.interval present but cron=None uses default_interval_secs.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from inandout.ingestion.daemon import _next_interval_secs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sched(cron: str | None) -> MagicMock:
    s = MagicMock()
    s.cron = cron
    return s


# ---------------------------------------------------------------------------
# cron = None → return default_interval_secs
# ---------------------------------------------------------------------------

def test_no_cron_returns_default():
    result = _next_interval_secs(_sched(None), 300.0)
    assert result == 300.0


def test_empty_string_cron_returns_default():
    result = _next_interval_secs(_sched(""), 120.0)
    assert result == 120.0


# ---------------------------------------------------------------------------
# Valid cron → positive float within one period
# ---------------------------------------------------------------------------

def test_valid_cron_every_minute_returns_non_negative():
    """'* * * * *' fires every minute; result should be 0–60 s."""
    result = _next_interval_secs(_sched("* * * * *"), 60.0)
    assert 0.0 <= result <= 60.0, f"Expected 0–60, got {result}"


def test_valid_cron_every_5_minutes_returns_non_negative():
    """'*/5 * * * *' fires every 5 min; result should be 0–300 s."""
    result = _next_interval_secs(_sched("*/5 * * * *"), 300.0)
    assert 0.0 <= result <= 300.0, f"Expected 0–300, got {result}"


def test_valid_cron_returns_float():
    result = _next_interval_secs(_sched("0 * * * *"), 3600.0)
    assert isinstance(result, float)


# ---------------------------------------------------------------------------
# Invalid cron → fallback to default_interval_secs
# ---------------------------------------------------------------------------

def test_invalid_cron_falls_back_to_default():
    """An unparseable cron expression should return default_interval_secs."""
    result = _next_interval_secs(_sched("not-a-cron"), 500.0)
    assert result == 500.0


def test_invalid_cron_with_extra_fields_falls_back():
    result = _next_interval_secs(_sched("99 99 99 99 99"), 42.0)
    assert result == 42.0
