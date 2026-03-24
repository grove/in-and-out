"""Unit tests for infer_schema_from_record."""
from __future__ import annotations

import pytest

from inandout.schema_registry.local import infer_schema_from_record
from inandout.schema_registry.types import ConnectorSchema


def test_returns_connector_schema():
    result = infer_schema_from_record("crm", "contacts", "1", {"id": "1"})
    assert isinstance(result, ConnectorSchema)


def test_connector_and_datatype_preserved():
    result = infer_schema_from_record("myconn", "mydtype", "2", {"x": 1})
    assert result.connector == "myconn"
    assert result.datatype == "mydtype"


def test_version_preserved():
    result = infer_schema_from_record("c", "d", "v42", {"x": 1})
    assert result.version == "v42"


def test_str_value_infers_text():
    result = infer_schema_from_record("c", "d", "1", {"name": "hello"})
    col = next(c for c in result.columns if c.name == "name")
    assert col.pg_type == "TEXT"


def test_int_value_infers_integer():
    result = infer_schema_from_record("c", "d", "1", {"count": 5})
    col = next(c for c in result.columns if c.name == "count")
    assert col.pg_type == "INTEGER"


def test_float_value_infers_float():
    result = infer_schema_from_record("c", "d", "1", {"score": 3.14})
    col = next(c for c in result.columns if c.name == "score")
    assert col.pg_type == "FLOAT"


def test_bool_value_infers_boolean():
    result = infer_schema_from_record("c", "d", "1", {"active": True})
    col = next(c for c in result.columns if c.name == "active")
    assert col.pg_type == "BOOLEAN"


def test_dict_value_infers_jsonb():
    result = infer_schema_from_record("c", "d", "1", {"meta": {"k": "v"}})
    col = next(c for c in result.columns if c.name == "meta")
    assert col.pg_type == "JSONB"


def test_list_value_infers_jsonb():
    result = infer_schema_from_record("c", "d", "1", {"tags": [1, 2, 3]})
    col = next(c for c in result.columns if c.name == "tags")
    assert col.pg_type == "JSONB"


def test_none_value_infers_text():
    result = infer_schema_from_record("c", "d", "1", {"unknown": None})
    col = next(c for c in result.columns if c.name == "unknown")
    assert col.pg_type == "TEXT"


def test_bool_before_int_check():
    """True is also an int, but bool check should come first."""
    result = infer_schema_from_record("c", "d", "1", {"flag": True})
    col = next(c for c in result.columns if c.name == "flag")
    assert col.pg_type == "BOOLEAN"


def test_all_columns_nullable_true_by_default():
    result = infer_schema_from_record("c", "d", "1", {"a": 1, "b": "x"})
    assert all(c.nullable for c in result.columns)


def test_column_count_matches_record():
    record = {"a": 1, "b": "x", "c": True, "d": 3.0, "e": {}}
    result = infer_schema_from_record("c", "d", "1", record)
    assert len(result.columns) == 5


def test_empty_record_produces_no_columns():
    result = infer_schema_from_record("c", "d", "1", {})
    assert result.columns == []
