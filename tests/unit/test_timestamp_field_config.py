"""Unit tests for TimestampFieldConfig."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.connector import TimestampFieldConfig


VALID_FORMATS = ["iso8601", "unix_seconds", "unix_millis", "rfc2822", "auto"]


def test_minimal_valid():
    cfg = TimestampFieldConfig(field="created_at")
    assert cfg.field == "created_at"


def test_format_default_auto():
    cfg = TimestampFieldConfig(field="created_at")
    assert cfg.format == "auto"


def test_target_field_default_none():
    cfg = TimestampFieldConfig(field="created_at")
    assert cfg.target_field is None


def test_format_iso8601():
    cfg = TimestampFieldConfig(field="ts", format="iso8601")
    assert cfg.format == "iso8601"


def test_format_unix_seconds():
    cfg = TimestampFieldConfig(field="ts", format="unix_seconds")
    assert cfg.format == "unix_seconds"


def test_format_unix_millis():
    cfg = TimestampFieldConfig(field="ts", format="unix_millis")
    assert cfg.format == "unix_millis"


def test_format_rfc2822():
    cfg = TimestampFieldConfig(field="ts", format="rfc2822")
    assert cfg.format == "rfc2822"


def test_format_auto():
    cfg = TimestampFieldConfig(field="ts", format="auto")
    assert cfg.format == "auto"


def test_invalid_format_raises():
    with pytest.raises(ValidationError):
        TimestampFieldConfig(field="ts", format="epoch")


def test_target_field_set():
    cfg = TimestampFieldConfig(field="created_at", target_field="created_at_normalized")
    assert cfg.target_field == "created_at_normalized"


def test_missing_field_raises():
    with pytest.raises(ValidationError):
        TimestampFieldConfig()


def test_extra_field_forbidden():
    with pytest.raises(ValidationError):
        TimestampFieldConfig(field="ts", unknown="bad")


def test_all_formats_valid():
    for fmt in VALID_FORMATS:
        cfg = TimestampFieldConfig(field="ts", format=fmt)
        assert cfg.format == fmt


def test_round_trip_json():
    cfg = TimestampFieldConfig(
        field="updated_at",
        format="unix_millis",
        target_field="updated_at_iso",
    )
    loaded = TimestampFieldConfig.model_validate_json(cfg.model_dump_json())
    assert loaded.field == "updated_at"
    assert loaded.format == "unix_millis"
    assert loaded.target_field == "updated_at_iso"
