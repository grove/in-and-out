"""Unit tests for the schema registry — Step 44."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from inandout.schema_registry.local import LocalSchemaRegistry, infer_schema_from_record
from inandout.schema_registry.types import ColumnSchema, ConnectorSchema


def _make_schema(
    connector: str = "my_connector",
    datatype: str = "contacts",
    version: str = "1.0.0",
    columns: list[ColumnSchema] | None = None,
) -> ConnectorSchema:
    if columns is None:
        columns = [
            ColumnSchema(name="id", pg_type="TEXT", nullable=False),
            ColumnSchema(name="name", pg_type="TEXT"),
            ColumnSchema(name="age", pg_type="INTEGER"),
        ]
    return ConnectorSchema(connector=connector, datatype=datatype, version=version, columns=columns)


# ---------------------------------------------------------------------------
# diff_schemas
# ---------------------------------------------------------------------------

def test_diff_schemas_added_column():
    old = _make_schema(columns=[ColumnSchema(name="id", pg_type="TEXT")])
    new = _make_schema(columns=[
        ColumnSchema(name="id", pg_type="TEXT"),
        ColumnSchema(name="email", pg_type="TEXT"),
    ])
    diffs = LocalSchemaRegistry.diff_schemas(old, new)
    assert len(diffs) == 1
    assert "added column 'email'" in diffs[0]


def test_diff_schemas_removed_column():
    old = _make_schema(columns=[
        ColumnSchema(name="id", pg_type="TEXT"),
        ColumnSchema(name="phone", pg_type="TEXT"),
    ])
    new = _make_schema(columns=[ColumnSchema(name="id", pg_type="TEXT")])
    diffs = LocalSchemaRegistry.diff_schemas(old, new)
    assert len(diffs) == 1
    assert "removed column 'phone'" in diffs[0]


def test_diff_schemas_type_changed():
    old = _make_schema(columns=[ColumnSchema(name="count", pg_type="TEXT")])
    new = _make_schema(columns=[ColumnSchema(name="count", pg_type="INTEGER")])
    diffs = LocalSchemaRegistry.diff_schemas(old, new)
    assert len(diffs) == 1
    assert "type changed" in diffs[0]
    assert "TEXT" in diffs[0]
    assert "INTEGER" in diffs[0]


def test_diff_schemas_no_changes():
    old = _make_schema()
    new = _make_schema()
    diffs = LocalSchemaRegistry.diff_schemas(old, new)
    assert diffs == []


def test_diff_schemas_multiple_changes():
    old = _make_schema(columns=[
        ColumnSchema(name="id", pg_type="TEXT"),
        ColumnSchema(name="removed_col", pg_type="TEXT"),
    ])
    new = _make_schema(columns=[
        ColumnSchema(name="id", pg_type="UUID"),
        ColumnSchema(name="new_col", pg_type="INTEGER"),
    ])
    diffs = LocalSchemaRegistry.diff_schemas(old, new)
    assert any("removed column 'removed_col'" in d for d in diffs)
    assert any("added column 'new_col'" in d for d in diffs)
    assert any("type changed" in d for d in diffs)


# ---------------------------------------------------------------------------
# LocalSchemaRegistry put_schema / get_schema round-trip
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_put_get_schema_roundtrip(tmp_path: Path):
    """Put a schema then get it back — should match exactly."""
    registry = LocalSchemaRegistry(tmp_path)
    schema = _make_schema()

    await registry.put_schema(schema)
    retrieved = await registry.get_schema("my_connector", "contacts")

    assert retrieved is not None
    assert retrieved.connector == schema.connector
    assert retrieved.datatype == schema.datatype
    assert retrieved.version == schema.version
    assert len(retrieved.columns) == len(schema.columns)
    for got, expected in zip(retrieved.columns, schema.columns):
        assert got.name == expected.name
        assert got.pg_type == expected.pg_type


@pytest.mark.anyio
async def test_get_schema_missing_returns_none(tmp_path: Path):
    """Getting a schema that doesn't exist returns None."""
    registry = LocalSchemaRegistry(tmp_path)
    result = await registry.get_schema("no_connector", "no_datatype")
    assert result is None


@pytest.mark.anyio
async def test_put_schema_overwrites_existing(tmp_path: Path):
    """Putting a schema twice overwrites the existing file."""
    registry = LocalSchemaRegistry(tmp_path)
    schema_v1 = _make_schema(version="1.0.0")
    schema_v2 = _make_schema(version="2.0.0")

    await registry.put_schema(schema_v1)
    await registry.put_schema(schema_v2)

    retrieved = await registry.get_schema("my_connector", "contacts")
    assert retrieved is not None
    assert retrieved.version == "2.0.0"


# ---------------------------------------------------------------------------
# infer_schema_from_record
# ---------------------------------------------------------------------------

def test_infer_schema_from_record_basic():
    """Infer schema from a sample record with mixed types."""
    record = {
        "id": "rec-1",
        "name": "Alice",
        "age": 30,
        "score": 9.5,
        "active": True,
        "metadata": {"key": "value"},
        "tags": ["a", "b"],
    }
    schema = infer_schema_from_record("my_connector", "users", "1.0.0", record)

    assert schema.connector == "my_connector"
    assert schema.datatype == "users"
    assert schema.version == "1.0.0"

    col_map = {c.name: c for c in schema.columns}
    assert col_map["id"].pg_type == "TEXT"
    assert col_map["name"].pg_type == "TEXT"
    assert col_map["age"].pg_type == "INTEGER"
    assert col_map["score"].pg_type == "FLOAT"
    assert col_map["active"].pg_type == "BOOLEAN"
    assert col_map["metadata"].pg_type == "JSONB"
    assert col_map["tags"].pg_type == "JSONB"


def test_infer_schema_with_field_mappings_cast():
    """Field mappings with casts override the inferred pg_type."""
    from unittest.mock import MagicMock

    record = {"id": "1", "created_at": "2026-01-01"}

    # Test without mappings first
    schema = infer_schema_from_record("conn", "items", "1.0.0", record)
    col_map = {c.name: c for c in schema.columns}
    assert col_map["created_at"].pg_type == "TEXT"  # Default: string → TEXT

    # With a mock field mapping that has a cast
    mock_fm = MagicMock()
    mock_fm.target_field = "created_at"
    mock_fm.source_field = "created_at"
    mock_fm.cast = "datetime"

    schema_with_cast = infer_schema_from_record("conn", "items", "1.0.0", record, [mock_fm])
    col_map2 = {c.name: c for c in schema_with_cast.columns}
    assert col_map2["created_at"].pg_type == "TIMESTAMPTZ"
