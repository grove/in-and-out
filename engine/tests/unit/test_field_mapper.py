"""Unit tests for the field mapping DSL."""
from __future__ import annotations

import datetime

import pytest

from inandout.config.field_mapping import FieldMapping
from inandout.ingestion.field_mapper import apply_field_mappings


# ---------------------------------------------------------------------------
# Empty mappings → pass-through
# ---------------------------------------------------------------------------

def test_empty_mappings_passthrough():
    record = {"id": "1", "name": "Alice", "email": "alice@example.com"}
    result = apply_field_mappings(record, [])
    assert result is record


def test_empty_mappings_preserves_all_fields():
    record = {"a": 1, "b": 2, "c": 3}
    result = apply_field_mappings(record, [])
    assert result == record


# ---------------------------------------------------------------------------
# Rename field
# ---------------------------------------------------------------------------

def test_rename_field():
    record = {"external_id": "abc123", "full_name": "Alice Smith"}
    mappings = [
        FieldMapping(source="full_name", target="name"),
    ]
    result = apply_field_mappings(record, mappings)
    assert result["name"] == "Alice Smith"
    # Unmapped field passes through
    assert result["external_id"] == "abc123"


def test_rename_multiple_fields():
    record = {"first": "Alice", "last": "Smith", "age": 30}
    mappings = [
        FieldMapping(source="first", target="given_name"),
        FieldMapping(source="last", target="family_name"),
    ]
    result = apply_field_mappings(record, mappings)
    assert result["given_name"] == "Alice"
    assert result["family_name"] == "Smith"
    assert result["age"] == 30  # passed through


# ---------------------------------------------------------------------------
# Dot-notation extraction
# ---------------------------------------------------------------------------

def test_dot_notation_single_level():
    record = {"properties": {"email": "alice@example.com", "phone": "555-1234"}}
    mappings = [FieldMapping(source="properties.email", target="email")]
    result = apply_field_mappings(record, mappings)
    assert result["email"] == "alice@example.com"


def test_dot_notation_deep_nesting():
    record = {"a": {"b": {"c": "deep_value"}}}
    mappings = [FieldMapping(source="a.b.c", target="flat")]
    result = apply_field_mappings(record, mappings)
    assert result["flat"] == "deep_value"


def test_dot_notation_missing_path_uses_default():
    record = {"properties": {"name": "Alice"}}
    mappings = [FieldMapping(source="properties.email", target="email", default="unknown@example.com")]
    result = apply_field_mappings(record, mappings)
    assert result["email"] == "unknown@example.com"


def test_dot_notation_partially_missing_path_uses_default():
    record = {"shallow": "value"}
    mappings = [FieldMapping(source="nested.deep.field", target="out", default="fallback")]
    result = apply_field_mappings(record, mappings)
    assert result["out"] == "fallback"


# ---------------------------------------------------------------------------
# Type casting
# ---------------------------------------------------------------------------

def test_cast_str_to_int():
    record = {"count": "42"}
    mappings = [FieldMapping(source="count", target="count_int", cast="int")]
    result = apply_field_mappings(record, mappings)
    assert result["count_int"] == 42
    assert isinstance(result["count_int"], int)


def test_cast_str_to_float():
    record = {"score": "3.14"}
    mappings = [FieldMapping(source="score", target="score_float", cast="float")]
    result = apply_field_mappings(record, mappings)
    assert abs(result["score_float"] - 3.14) < 0.001


def test_cast_int_to_str():
    record = {"id": 123}
    mappings = [FieldMapping(source="id", target="id_str", cast="str")]
    result = apply_field_mappings(record, mappings)
    assert result["id_str"] == "123"


def test_cast_str_to_datetime():
    record = {"created_at": "2024-01-15T10:30:00"}
    mappings = [FieldMapping(source="created_at", target="created_dt", cast="datetime")]
    result = apply_field_mappings(record, mappings)
    assert isinstance(result["created_dt"], datetime.datetime)
    assert result["created_dt"].year == 2024
    assert result["created_dt"].month == 1
    assert result["created_dt"].day == 15


def test_cast_str_to_date():
    record = {"date": "2024-06-01"}
    mappings = [FieldMapping(source="date", target="dt", cast="date")]
    result = apply_field_mappings(record, mappings)
    assert isinstance(result["dt"], datetime.date)
    assert result["dt"] == datetime.date(2024, 6, 1)


def test_cast_failure_uses_default():
    record = {"count": "not-a-number"}
    mappings = [FieldMapping(source="count", target="count_int", cast="int", default=0)]
    result = apply_field_mappings(record, mappings)
    assert result["count_int"] == 0


# ---------------------------------------------------------------------------
# Default value for missing path
# ---------------------------------------------------------------------------

def test_default_value_for_none_source():
    record = {"id": "1"}
    mappings = [FieldMapping(source="missing_field", target="out", default="default_val")]
    result = apply_field_mappings(record, mappings)
    assert result["out"] == "default_val"


def test_default_value_none_when_not_set():
    record = {"id": "1"}
    mappings = [FieldMapping(source="missing_field", target="out")]
    result = apply_field_mappings(record, mappings)
    assert result["out"] is None


# ---------------------------------------------------------------------------
# Strict mode: only mapped fields kept
# ---------------------------------------------------------------------------

def test_strict_mode_keeps_only_mapped_fields():
    record = {"id": "1", "name": "Alice", "secret": "do-not-keep"}
    mappings = [
        FieldMapping(source="id", target="id"),
        FieldMapping(source="name", target="full_name"),
    ]
    result = apply_field_mappings(record, mappings, strict=True)
    assert "id" in result
    assert "full_name" in result
    assert "secret" not in result
    assert "name" not in result


def test_non_strict_mode_passes_through_unmapped():
    record = {"id": "1", "name": "Alice", "extra": "keep-me"}
    mappings = [
        FieldMapping(source="name", target="full_name"),
    ]
    result = apply_field_mappings(record, mappings, strict=False)
    assert result["full_name"] == "Alice"
    assert result["extra"] == "keep-me"
    assert result["id"] == "1"
