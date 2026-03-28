"""Unit tests for LocalSchemaRegistry file I/O (get_schema, put_schema)."""
from __future__ import annotations

from pathlib import Path

import pytest

from inandout.schema_registry.local import LocalSchemaRegistry
from inandout.schema_registry.types import ColumnSchema, ConnectorSchema


def _schema(connector: str = "crm", datatype: str = "contacts") -> ConnectorSchema:
    return ConnectorSchema(
        connector=connector,
        datatype=datatype,
        version="1",
        columns=[
            ColumnSchema(name="id", pg_type="TEXT"),
            ColumnSchema(name="email", pg_type="TEXT"),
        ],
    )


async def test_get_schema_missing_returns_none(tmp_path: Path):
    registry = LocalSchemaRegistry(tmp_path)
    result = await registry.get_schema("crm", "contacts")
    assert result is None


async def test_put_then_get_roundtrip(tmp_path: Path):
    registry = LocalSchemaRegistry(tmp_path)
    schema = _schema()
    await registry.put_schema(schema)
    result = await registry.get_schema("crm", "contacts")
    assert result is not None
    assert result.connector == "crm"
    assert result.datatype == "contacts"
    assert result.version == "1"


async def test_roundtrip_preserves_columns(tmp_path: Path):
    registry = LocalSchemaRegistry(tmp_path)
    schema = _schema()
    await registry.put_schema(schema)
    result = await registry.get_schema("crm", "contacts")
    assert len(result.columns) == 2
    col_names = [c.name for c in result.columns]
    assert "id" in col_names
    assert "email" in col_names


async def test_put_creates_json_file(tmp_path: Path):
    registry = LocalSchemaRegistry(tmp_path)
    await registry.put_schema(_schema("sfdc", "leads"))
    files = list(tmp_path.glob("*.json"))
    assert len(files) == 1
    assert "sfdc" in files[0].name
    assert "leads" in files[0].name


async def test_put_overwrites_existing(tmp_path: Path):
    registry = LocalSchemaRegistry(tmp_path)
    v1 = ConnectorSchema(connector="crm", datatype="contacts", version="1", columns=[])
    v2 = ConnectorSchema(
        connector="crm",
        datatype="contacts",
        version="2",
        columns=[ColumnSchema(name="phone", pg_type="TEXT")],
    )
    await registry.put_schema(v1)
    await registry.put_schema(v2)
    result = await registry.get_schema("crm", "contacts")
    assert result.version == "2"
    assert len(result.columns) == 1


async def test_different_connectors_stored_separately(tmp_path: Path):
    registry = LocalSchemaRegistry(tmp_path)
    await registry.put_schema(_schema("crm", "contacts"))
    await registry.put_schema(_schema("erp", "orders"))
    crm = await registry.get_schema("crm", "contacts")
    erp = await registry.get_schema("erp", "orders")
    assert crm.connector == "crm"
    assert erp.connector == "erp"


async def test_directory_created_if_not_exists(tmp_path: Path):
    new_dir = tmp_path / "schemas" / "nested"
    registry = LocalSchemaRegistry(new_dir)
    assert new_dir.exists()


async def test_corrupted_file_returns_none(tmp_path: Path):
    registry = LocalSchemaRegistry(tmp_path)
    bad_file = tmp_path / "crm_contacts.json"
    bad_file.write_text("not valid json{{{{")
    result = await registry.get_schema("crm", "contacts")
    assert result is None
