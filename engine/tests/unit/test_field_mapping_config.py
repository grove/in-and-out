"""Unit tests for FieldMapping Pydantic model."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.field_mapping import FieldMapping


def test_minimal_valid():
    fm = FieldMapping(source="id", target="external_id")
    assert fm.source == "id"
    assert fm.target == "external_id"


def test_cast_none_by_default():
    fm = FieldMapping(source="x", target="y")
    assert fm.cast is None


def test_default_none_by_default():
    fm = FieldMapping(source="x", target="y")
    assert fm.default is None


def test_cast_str():
    fm = FieldMapping(source="x", target="y", cast="str")
    assert fm.cast == "str"


def test_cast_int():
    fm = FieldMapping(source="x", target="y", cast="int")
    assert fm.cast == "int"


def test_cast_float():
    fm = FieldMapping(source="x", target="y", cast="float")
    assert fm.cast == "float"


def test_cast_bool():
    fm = FieldMapping(source="x", target="y", cast="bool")
    assert fm.cast == "bool"


def test_cast_datetime():
    fm = FieldMapping(source="x", target="y", cast="datetime")
    assert fm.cast == "datetime"


def test_cast_date():
    fm = FieldMapping(source="x", target="y", cast="date")
    assert fm.cast == "date"


def test_invalid_cast_raises():
    with pytest.raises(ValidationError):
        FieldMapping(source="x", target="y", cast="uuid")


def test_extra_field_forbidden():
    with pytest.raises(ValidationError):
        FieldMapping(source="x", target="y", extra="nope")


def test_missing_source_raises():
    with pytest.raises(ValidationError):
        FieldMapping(target="y")


def test_missing_target_raises():
    with pytest.raises(ValidationError):
        FieldMapping(source="x")


def test_default_with_value():
    fm = FieldMapping(source="x", target="y", default="N/A")
    assert fm.default == "N/A"


def test_default_int():
    fm = FieldMapping(source="x", target="y", default=0)
    assert fm.default == 0


def test_dot_notation_source():
    fm = FieldMapping(source="properties.email", target="email")
    assert fm.source == "properties.email"


def test_round_trip_json():
    fm = FieldMapping(source="a.b", target="c", cast="str", default="x")
    loaded = FieldMapping.model_validate_json(fm.model_dump_json())
    assert loaded.source == "a.b"
    assert loaded.cast == "str"
    assert loaded.default == "x"
