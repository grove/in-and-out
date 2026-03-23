"""Unit tests for backfill / historical load mode."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from inandout.ingestion.backfill import (
    BackfillConfig,
    BackfillResult,
    _safe_table_name,
    split_into_windows,
)


# ---------------------------------------------------------------------------
# split_into_windows tests
# ---------------------------------------------------------------------------

def test_split_7_day_range_into_1d_windows():
    """A 7-day range with 1d window should produce exactly 7 windows."""
    from_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    to_dt = datetime(2024, 1, 8, tzinfo=timezone.utc)  # 7 days later
    windows = split_into_windows(from_dt, to_dt, "1d")
    assert len(windows) == 7


def test_split_windows_cover_full_range():
    """All windows combined should cover the entire date range without gaps."""
    from_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    to_dt = datetime(2024, 1, 8, tzinfo=timezone.utc)
    windows = split_into_windows(from_dt, to_dt, "1d")

    # First window starts at from_dt
    assert windows[0][0] == from_dt
    # Last window ends at to_dt
    assert windows[-1][1] == to_dt
    # No gaps: each window end equals next window start
    for i in range(len(windows) - 1):
        assert windows[i][1] == windows[i + 1][0]


def test_split_windows_no_overlap():
    """Windows should not overlap: end of window N == start of window N+1."""
    from_dt = datetime(2024, 3, 1, tzinfo=timezone.utc)
    to_dt = datetime(2024, 3, 15, tzinfo=timezone.utc)
    windows = split_into_windows(from_dt, to_dt, "1d")

    seen: set[datetime] = set()
    for start, end in windows:
        # Each start should only appear once
        assert start not in seen, f"Duplicate start: {start}"
        seen.add(start)


def test_split_windows_partial_last_window():
    """Last window can be shorter than the full window size."""
    from_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    to_dt = datetime(2024, 1, 3, 12, tzinfo=timezone.utc)  # 2.5 days
    windows = split_into_windows(from_dt, to_dt, "1d")

    assert len(windows) == 3  # 3 windows: day1, day2, half-day3
    assert windows[-1][1] == to_dt


def test_split_windows_hourly():
    """Test with hourly windows."""
    from_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    to_dt = datetime(2024, 1, 1, 6, tzinfo=timezone.utc)  # 6 hours
    windows = split_into_windows(from_dt, to_dt, "1h")
    assert len(windows) == 6


def test_split_windows_single_window():
    """When range is smaller than window size, should produce 1 window."""
    from_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    to_dt = datetime(2024, 1, 1, 12, tzinfo=timezone.utc)  # 12 hours
    windows = split_into_windows(from_dt, to_dt, "1d")
    assert len(windows) == 1
    assert windows[0] == (from_dt, to_dt)


# ---------------------------------------------------------------------------
# _safe_table_name tests
# ---------------------------------------------------------------------------

def test_safe_table_name_basic():
    """_safe_table_name should generate a name starting with _backfill_."""
    name = _safe_table_name("my-connector", "my-datatype", "20240101_120000")
    assert name.startswith("_backfill_")


def test_safe_table_name_no_special_chars():
    """_safe_table_name result should contain only alphanumeric and underscores."""
    name = _safe_table_name("my-connector", "my datatype", "2024-01-01T12:00:00")
    # Should not contain dashes, spaces, colons, etc.
    import re
    assert re.match(r"^[a-zA-Z0-9_]+$", name), f"Invalid chars in: {name}"


def test_safe_table_name_deterministic():
    """_safe_table_name should produce the same result for the same inputs."""
    name1 = _safe_table_name("connector", "datatype", "ts")
    name2 = _safe_table_name("connector", "datatype", "ts")
    assert name1 == name2


def test_safe_table_name_includes_connector_and_datatype():
    """_safe_table_name should include recognizable parts of the connector/datatype."""
    name = _safe_table_name("myconn", "mytype", "ts123")
    assert "myconn" in name
    assert "mytype" in name


# ---------------------------------------------------------------------------
# BackfillConfig tests
# ---------------------------------------------------------------------------

def test_backfill_config_creation():
    """BackfillConfig should be creatable with required fields."""
    cfg = BackfillConfig(
        connector_path=Path("/tmp/connector.yaml"),
        datatype="users",
        from_dt=datetime(2024, 1, 1, tzinfo=timezone.utc),
        to_dt=datetime(2024, 1, 31, tzinfo=timezone.utc),
    )
    assert cfg.datatype == "users"
    assert cfg.window == "1d"
    assert cfg.staging_table is None


def test_backfill_config_custom_window():
    """BackfillConfig should accept custom window sizes."""
    cfg = BackfillConfig(
        connector_path=Path("/tmp/connector.yaml"),
        datatype="orders",
        from_dt=datetime(2024, 1, 1, tzinfo=timezone.utc),
        to_dt=datetime(2024, 1, 7, tzinfo=timezone.utc),
        window="6h",
    )
    assert cfg.window == "6h"


# ---------------------------------------------------------------------------
# BackfillResult tests
# ---------------------------------------------------------------------------

def test_backfill_result_defaults():
    """BackfillResult should have sensible defaults."""
    result = BackfillResult()
    assert result.windows_processed == 0
    assert result.total_records == 0
    assert result.staging_table == ""
    assert result.promoted is False


def test_backfill_result_accumulation():
    """BackfillResult totals should accumulate correctly."""
    result = BackfillResult(staging_table="_backfill_test")
    result.windows_processed += 3
    result.total_records += 150
    result.total_records += 200
    assert result.windows_processed == 3
    assert result.total_records == 350


def test_window_boundaries_30_day_range():
    """30-day range with 1d windows should produce 30 windows."""
    from_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    to_dt = datetime(2024, 1, 31, tzinfo=timezone.utc)  # 30 days
    windows = split_into_windows(from_dt, to_dt, "1d")
    assert len(windows) == 30
    # Verify first window start and last window end
    assert windows[0][0] == from_dt
    assert windows[-1][1] == to_dt
