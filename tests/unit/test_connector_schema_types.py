"""Unit tests for ColumnSchema and ConnectorSchema Pydantic models."""
from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from inandout.schema_registry.types import ColumnSchema, ConnectorSchema


# --- ColumnSchema ---

def test_column_schema_minimal():
    col = ColumnSchema(name="id", pg_type="TEXT")
    assert col.name == "id"
    assert col.pg_type == "TEXT"


def test_column_schema_nullable_default_true():
    col = ColumnSchema(name="x", pg_type="INTEGER")
    assert col.nullable is True


def test_column_schema_nullable_false():
    col = ColumnSchema(name="x", pg_type="INTEGER", nullable=False)
    assert col.nullable is False


def test_column_schema_extra_field_forbidden():
    with pytest.raises(ValidationError):
        ColumnSchema(name="x", pg_type="TEXT", extra_field="bad")


def test_column_schema_missing_name_raises():
    with pytest.raises(ValidationError):
        ColumnSchema(pg_type="TEXT")


def test_column_schema_missing_pg_type_raises():
    with pytest.raises(ValidationError):
        ColumnSchema(name="x")


def test_column_schema_round_trip_json():
    col = ColumnSchema(name="email", pg_type="TEXT", nullable=True)
    dumped = col.model_dump_json()
    loaded = ColumnSchema.model_validate_json(dumped)
    assert loaded == col


# --- ConnectorSchema ---

def test_connector_schema_minimal():
    schema = ConnectorSchema(connector="crm", datatype="contacts", version="1", columns=[])
    assert schema.connector == "crm"
    assert schema.datatype == "contacts"
    assert schema.version == "1"
    assert schema.columns == []


def test_connector_schema_with_columns():
    col = ColumnSchema(name="id", pg_type="TEXT")
    schema = ConnectorSchema(connector="crm", datatype="contacts", version="1", columns=[col])
    assert len(schema.columns) == 1
    assert schema.columns[0].name == "id"


def test_connector_schema_extra_field_forbidden():
    with pytest.raises(ValidationError):
        ConnectorSchema(
            connector="crm",
            datatype="contacts",
            version="1",
            columns=[],
            extra_field="bad",
        )


def test_connector_schema_missing_connector_raises():
    with pytest.raises(ValidationError):
        ConnectorSchema(datatype="contacts", version="1", columns=[])


def test_connector_schema_round_trip_json():
    col = ColumnSchema(name="id", pg_type="TEXT", nullable=False)
    schema = ConnectorSchema(connector="crm", datatype="contacts", version="2", columns=[col])
    dumped = schema.model_dump_json()
    loaded = ConnectorSchema.model_validate_json(dumped)
    assert loaded.connector == "crm"
    assert loaded.version == "2"
    assert loaded.columns[0].nullable is False


def test_connector_schema_multiple_columns():
    cols = [ColumnSchema(name=n, pg_type="TEXT") for n in ("a", "b", "c")]
    schema = ConnectorSchema(connector="x", datatype="y", version="1", columns=cols)
    assert len(schema.columns) == 3
