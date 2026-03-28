"""Unit tests for timestamp normalizer (A7)."""
from __future__ import annotations

import pytest

from inandout.ingestion.timestamp_normalizer import (
    normalize_timestamp,
    apply_timestamp_normalization,
)


def test_unix_seconds_to_utc_iso8601() -> None:
    # 2026-01-14 12:00:00 UTC = 1768392000
    result = normalize_timestamp(1768392000, "unix_seconds")
    assert result == "2026-01-14T12:00:00Z"


def test_unix_milliseconds_to_utc_iso8601() -> None:
    result = normalize_timestamp(1768392000000, "unix_millis")
    assert result == "2026-01-14T12:00:00Z"


def test_iso8601_with_offset_to_utc() -> None:
    result = normalize_timestamp("2026-01-14T14:00:00+02:00", "iso8601")
    assert result == "2026-01-14T12:00:00Z"


def test_iso8601_z_suffix() -> None:
    result = normalize_timestamp("2026-01-14T12:00:00Z", "iso8601")
    assert result == "2026-01-14T12:00:00Z"


def test_rfc2822_to_utc_iso8601() -> None:
    result = normalize_timestamp("Wed, 14 Jan 2026 12:00:00 +0000", "rfc2822")
    assert result == "2026-01-14T12:00:00Z"


def test_auto_mode_detects_unix_seconds() -> None:
    result = normalize_timestamp(1768392000, "auto")
    assert result == "2026-01-14T12:00:00Z"


def test_auto_mode_detects_unix_millis() -> None:
    # Large int → millis
    result = normalize_timestamp(1768392000000, "auto")
    assert result == "2026-01-14T12:00:00Z"


def test_auto_mode_detects_iso8601() -> None:
    result = normalize_timestamp("2026-01-14T12:00:00Z", "auto")
    assert result == "2026-01-14T12:00:00Z"


def test_auto_mode_detects_rfc2822() -> None:
    result = normalize_timestamp("Wed, 14 Jan 2026 12:00:00 +0000", "auto")
    assert result == "2026-01-14T12:00:00Z"


def test_invalid_value_returns_none() -> None:
    result = normalize_timestamp("not-a-timestamp", "iso8601")
    assert result is None


def test_none_value_returns_none() -> None:
    result = normalize_timestamp(None, "auto")
    assert result is None


def test_apply_normalization_overwrites_field() -> None:
    from inandout.config.connector import TimestampFieldConfig

    configs = [TimestampFieldConfig(field="created_at", format="unix_seconds")]
    record = {"id": "1", "created_at": 1768392000, "name": "Alice"}
    result = apply_timestamp_normalization(record, configs)
    assert result["created_at"] == "2026-01-14T12:00:00Z"
    assert result["name"] == "Alice"
    assert result["id"] == "1"


def test_apply_normalization_target_field() -> None:
    from inandout.config.connector import TimestampFieldConfig

    configs = [
        TimestampFieldConfig(
            field="ts_raw", format="unix_seconds", target_field="ts_normalized"
        )
    ]
    record = {"ts_raw": 1768392000, "name": "Bob"}
    result = apply_timestamp_normalization(record, configs)
    assert result["ts_normalized"] == "2026-01-14T12:00:00Z"
    assert result["ts_raw"] == 1768392000  # original unchanged


def test_apply_normalization_invalid_leaves_original() -> None:
    from inandout.config.connector import TimestampFieldConfig

    configs = [TimestampFieldConfig(field="bad_ts", format="iso8601")]
    record = {"bad_ts": "not-valid", "name": "Carol"}
    result = apply_timestamp_normalization(record, configs)
    assert result["bad_ts"] == "not-valid"  # original preserved


def test_apply_normalization_missing_field_skipped() -> None:
    from inandout.config.connector import TimestampFieldConfig

    configs = [TimestampFieldConfig(field="nonexistent", format="unix_seconds")]
    record = {"name": "Dave"}
    result = apply_timestamp_normalization(record, configs)
    assert result == {"name": "Dave"}
