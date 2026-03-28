"""Unit tests for IncrementalConfig and IncrementalCursorType."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.ingestion import IncrementalConfig, IncrementalCursorType


# --- IncrementalCursorType enum ---

def test_cursor_type_values_exist():
    # Just verify the enum is importable and has members
    members = list(IncrementalCursorType)
    assert len(members) > 0


def test_cursor_type_datetime_or_iso():
    # Common cursor types — at least one date/time variant exists
    values = [m.value for m in IncrementalCursorType]
    assert any("datetime" in v or "iso" in v or "timestamp" in v for v in values)


# --- IncrementalConfig ---

def test_enabled_default_true():
    cfg = IncrementalConfig()
    assert cfg.enabled is True


def test_cursor_field_default_none():
    cfg = IncrementalConfig()
    assert cfg.cursor_field is None


def test_cursor_type_default_none():
    cfg = IncrementalConfig()
    assert cfg.cursor_type is None


def test_request_filter_default_none():
    cfg = IncrementalConfig()
    assert cfg.request_filter is None


def test_cursor_window_default_none():
    cfg = IncrementalConfig()
    assert cfg.cursor_window is None


def test_enabled_false():
    cfg = IncrementalConfig(enabled=False)
    assert cfg.enabled is False


def test_cursor_field_set():
    cfg = IncrementalConfig(cursor_field="updated_at")
    assert cfg.cursor_field == "updated_at"


def test_cursor_window_set():
    cfg = IncrementalConfig(cursor_window="1d")
    assert cfg.cursor_window == "1d"


def test_extra_fields_allowed():
    # IncrementalConfig uses extra="allow"
    cfg = IncrementalConfig(custom_param="value")
    assert cfg.custom_param == "value"  # type: ignore[attr-defined]


def test_round_trip_json():
    cfg = IncrementalConfig(enabled=True, cursor_field="modified_at", cursor_window="7d")
    loaded = IncrementalConfig.model_validate_json(cfg.model_dump_json())
    assert loaded.cursor_field == "modified_at"
    assert loaded.cursor_window == "7d"
