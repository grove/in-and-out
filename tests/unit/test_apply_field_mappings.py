"""Unit tests for apply_field_mappings transformation DSL.

Covers:
- Rename: source field mapped to target name.
- Nested dot-notation source path.
- Drop (strict mode): unmapped fields excluded.
- Pass-through (non-strict, default): unmapped fields preserved.
- Default value used when source path is absent.
- Cast: str→int, str→float conversions.
- Bad cast falls back to default.
- Empty mappings returns record as-is.
"""
from __future__ import annotations

import pytest

from inandout.config.field_mapping import FieldMapping
from inandout.ingestion.field_mapper import apply_field_mappings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _m(source: str, target: str, cast=None, default=None) -> FieldMapping:
    return FieldMapping(source=source, target=target, cast=cast, default=default)


# ---------------------------------------------------------------------------
# Rename
# ---------------------------------------------------------------------------

def test_rename_simple_field():
    record = {"firstName": "Alice"}
    result = apply_field_mappings(record, [_m("firstName", "first_name")])
    assert result["first_name"] == "Alice"


def test_rename_preserves_unmapped_field_by_default():
    record = {"firstName": "Alice", "age": 30}
    result = apply_field_mappings(record, [_m("firstName", "first_name")])
    assert "age" in result


# ---------------------------------------------------------------------------
# Nested dot-notation source path
# ---------------------------------------------------------------------------

def test_nested_source_path():
    record = {"properties": {"email": "a@b.com"}}
    result = apply_field_mappings(
        record, [_m("properties.email", "email")]
    )
    assert result["email"] == "a@b.com"


def test_nested_source_path_missing_returns_default():
    record = {"properties": {}}
    result = apply_field_mappings(
        record, [_m("properties.email", "email", default="none@none.com")]
    )
    assert result["email"] == "none@none.com"


def test_deeply_nested_source_path():
    record = {"a": {"b": {"c": "deep-value"}}}
    result = apply_field_mappings(record, [_m("a.b.c", "flat_c")])
    assert result["flat_c"] == "deep-value"


# ---------------------------------------------------------------------------
# Strict mode (drop unmapped)
# ---------------------------------------------------------------------------

def test_strict_mode_drops_unmapped_fields():
    record = {"firstName": "Alice", "age": 30}
    result = apply_field_mappings(
        record, [_m("firstName", "first_name")], strict=True
    )
    assert "age" not in result
    assert "first_name" in result


def test_strict_mode_only_mapped_fields_present():
    record = {"a": 1, "b": 2, "c": 3}
    result = apply_field_mappings(record, [_m("a", "x")], strict=True)
    assert set(result.keys()) == {"x"}


# ---------------------------------------------------------------------------
# Default value
# ---------------------------------------------------------------------------

def test_default_used_when_source_missing():
    record = {}
    result = apply_field_mappings(
        record, [_m("email", "email", default="unknown@example.com")]
    )
    assert result["email"] == "unknown@example.com"


def test_default_none_when_source_missing_and_no_default():
    record = {}
    result = apply_field_mappings(record, [_m("email", "email")])
    assert result["email"] is None


# ---------------------------------------------------------------------------
# Cast
# ---------------------------------------------------------------------------

def test_cast_str_to_int():
    record = {"count": "42"}
    result = apply_field_mappings(record, [_m("count", "count", cast="int")])
    assert result["count"] == 42
    assert isinstance(result["count"], int)


def test_cast_str_to_float():
    record = {"score": "3.14"}
    result = apply_field_mappings(record, [_m("score", "score", cast="float")])
    assert result["score"] == pytest.approx(3.14)


def test_bad_cast_falls_back_to_default():
    record = {"count": "not-a-number"}
    result = apply_field_mappings(
        record, [_m("count", "count", cast="int", default=0)]
    )
    assert result["count"] == 0


# ---------------------------------------------------------------------------
# Empty mappings
# ---------------------------------------------------------------------------

def test_empty_mappings_returns_record_unchanged():
    record = {"a": 1, "b": 2}
    result = apply_field_mappings(record, [])
    assert result == record
