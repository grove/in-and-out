"""Unit tests for source_table_ddl DDL generation.

Covers:
- CREATE TABLE IF NOT EXISTS with correct table name.
- All required columns: external_id, data, raw, _ingested_at, _sync_run_id,
  _raw_hash, _deleted, _deleted_at, _schema_version, _lineage.
- PRIMARY KEY (external_id).
- Non-public namespace prefixes the table name w/ CREATE SCHEMA.
- A _ingested_at index is included.
"""
from __future__ import annotations

import pytest

from inandout.postgres.schema import source_table_ddl


# ---------------------------------------------------------------------------
# Table naming
# ---------------------------------------------------------------------------

def test_source_table_ddl_correct_table_name():
    ddl = source_table_ddl("hubspot", "contacts")
    assert "inout_src_hubspot_contacts" in ddl


def test_source_table_ddl_uses_create_if_not_exists():
    ddl = source_table_ddl("hubspot", "contacts")
    assert "CREATE TABLE IF NOT EXISTS" in ddl


def test_source_table_ddl_namespace_prefixed():
    ddl = source_table_ddl("hubspot", "contacts", namespace="tenant_7")
    assert "tenant_7" in ddl
    assert "inout_src_hubspot_contacts" in ddl


def test_source_table_ddl_namespace_creates_schema():
    ddl = source_table_ddl("hubspot", "contacts", namespace="tenant_7")
    assert "CREATE SCHEMA IF NOT EXISTS tenant_7" in ddl


# ---------------------------------------------------------------------------
# Required columns
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("column", [
    "external_id",
    "data",
    "raw",
    "_ingested_at",
    "_sync_run_id",
    "_raw_hash",
    "_deleted",
    "_deleted_at",
    "_schema_version",
    "_lineage",
])
def test_source_table_ddl_contains_required_column(column: str):
    ddl = source_table_ddl("hubspot", "contacts")
    assert column in ddl, f"Expected column '{column}' in source table DDL"


# ---------------------------------------------------------------------------
# Primary key
# ---------------------------------------------------------------------------

def test_source_table_ddl_has_primary_key_on_external_id():
    ddl = source_table_ddl("hubspot", "contacts")
    assert "PRIMARY KEY" in ddl
    assert "external_id" in ddl


# ---------------------------------------------------------------------------
# Index
# ---------------------------------------------------------------------------

def test_source_table_ddl_creates_ingested_at_index():
    ddl = source_table_ddl("hubspot", "contacts")
    assert "CREATE INDEX" in ddl
    assert "_ingested_at" in ddl


def test_source_table_ddl_returns_string():
    assert isinstance(source_table_ddl("hubspot", "contacts"), str)
