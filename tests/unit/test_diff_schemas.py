"""Unit tests for LocalSchemaRegistry.diff_schemas."""
from __future__ import annotations

import pytest

from inandout.schema_registry.local import LocalSchemaRegistry
from inandout.schema_registry.types import ColumnSchema, ConnectorSchema


def _schema(connector: str, datatype: str, columns: list[ColumnSchema]) -> ConnectorSchema:
    return ConnectorSchema(connector=connector, datatype=datatype, version="1", columns=columns)


def _col(name: str, pg_type: str = "TEXT", nullable: bool = True) -> ColumnSchema:
    return ColumnSchema(name=name, pg_type=pg_type, nullable=nullable)


def test_no_changes_returns_empty_list():
    old = _schema("crm", "contacts", [_col("id"), _col("email")])
    new = _schema("crm", "contacts", [_col("id"), _col("email")])
    assert LocalSchemaRegistry.diff_schemas(old, new) == []


def test_added_column():
    old = _schema("crm", "contacts", [_col("id")])
    new = _schema("crm", "contacts", [_col("id"), _col("phone")])
    diffs = LocalSchemaRegistry.diff_schemas(old, new)
    assert len(diffs) == 1
    assert "added" in diffs[0]
    assert "phone" in diffs[0]


def test_added_column_includes_type():
    old = _schema("crm", "contacts", [_col("id")])
    new = _schema("crm", "contacts", [_col("id"), _col("score", "INTEGER")])
    diffs = LocalSchemaRegistry.diff_schemas(old, new)
    assert "INTEGER" in diffs[0]


def test_removed_column():
    old = _schema("crm", "contacts", [_col("id"), _col("old_field")])
    new = _schema("crm", "contacts", [_col("id")])
    diffs = LocalSchemaRegistry.diff_schemas(old, new)
    assert len(diffs) == 1
    assert "removed" in diffs[0]
    assert "old_field" in diffs[0]


def test_type_changed():
    old = _schema("crm", "contacts", [_col("data", "TEXT")])
    new = _schema("crm", "contacts", [_col("data", "JSONB")])
    diffs = LocalSchemaRegistry.diff_schemas(old, new)
    assert len(diffs) == 1
    assert "type changed" in diffs[0]
    assert "TEXT" in diffs[0]
    assert "JSONB" in diffs[0]


def test_nullable_changed():
    old = _schema("crm", "contacts", [_col("email", nullable=True)])
    new = _schema("crm", "contacts", [_col("email", nullable=False)])
    diffs = LocalSchemaRegistry.diff_schemas(old, new)
    assert len(diffs) == 1
    assert "nullable changed" in diffs[0]


def test_multiple_changes():
    old = _schema("crm", "contacts", [_col("id"), _col("old_col"), _col("name", "TEXT")])
    new = _schema("crm", "contacts", [_col("id"), _col("new_col"), _col("name", "JSONB")])
    diffs = LocalSchemaRegistry.diff_schemas(old, new)
    assert any("added" in d and "new_col" in d for d in diffs)
    assert any("removed" in d and "old_col" in d for d in diffs)
    assert any("type changed" in d and "name" in d for d in diffs)


def test_empty_both_returns_empty():
    old = _schema("crm", "contacts", [])
    new = _schema("crm", "contacts", [])
    assert LocalSchemaRegistry.diff_schemas(old, new) == []


def test_added_all_columns():
    old = _schema("crm", "contacts", [])
    new = _schema("crm", "contacts", [_col("a"), _col("b"), _col("c")])
    diffs = LocalSchemaRegistry.diff_schemas(old, new)
    assert len(diffs) == 3
    assert all("added" in d for d in diffs)


def test_removed_all_columns():
    old = _schema("crm", "contacts", [_col("a"), _col("b")])
    new = _schema("crm", "contacts", [])
    diffs = LocalSchemaRegistry.diff_schemas(old, new)
    assert len(diffs) == 2
    assert all("removed" in d for d in diffs)


def test_returns_list():
    old = _schema("crm", "contacts", [_col("id")])
    new = _schema("crm", "contacts", [_col("id")])
    result = LocalSchemaRegistry.diff_schemas(old, new)
    assert isinstance(result, list)
