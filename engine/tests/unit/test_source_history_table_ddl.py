"""Unit tests for source_history_table_ddl DDL generation.

Covers:
- Table name contains "inout_src_{connector}_{datatype}_history".
- CREATE TABLE IF NOT EXISTS semantics.
- Required columns present: _history_id, external_id, data, raw,
  _ingested_at, _sync_run_id, _raw_hash, _deleted, _deleted_at,
  _schema_version, _source_version.
- _history_id is BIGSERIAL PRIMARY KEY.
- Non-public namespace prefixes the table name.
- A CREATE INDEX on (external_id, _ingested_at DESC) is included.
"""
from __future__ import annotations

import pytest

from inandout.postgres.schema import source_history_table_ddl


# ---------------------------------------------------------------------------
# Table naming
# ---------------------------------------------------------------------------

def test_history_ddl_table_name_standard():
    ddl = source_history_table_ddl("hubspot", "contacts")
    assert "inout_src_hubspot_contacts_history" in ddl


def test_history_ddl_uses_create_if_not_exists():
    ddl = source_history_table_ddl("hubspot", "contacts")
    assert "CREATE TABLE IF NOT EXISTS" in ddl


def test_history_ddl_namespace_prefixed():
    ddl = source_history_table_ddl("hubspot", "contacts", namespace="tenant_42")
    assert "tenant_42" in ddl
    assert "inout_src_hubspot_contacts_history" in ddl


# ---------------------------------------------------------------------------
# Required columns
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("column", [
    "_history_id",
    "external_id",
    "data",
    "raw",
    "_ingested_at",
    "_sync_run_id",
    "_raw_hash",
    "_deleted",
    "_deleted_at",
    "_schema_version",
    "_source_version",
])
def test_history_ddl_contains_required_column(column: str):
    ddl = source_history_table_ddl("hubspot", "contacts")
    assert column in ddl, f"Expected column '{column}' in history DDL"


# ---------------------------------------------------------------------------
# _history_id is BIGSERIAL PRIMARY KEY
# ---------------------------------------------------------------------------

def test_history_ddl_history_id_is_bigserial_primary_key():
    ddl = source_history_table_ddl("hubspot", "contacts")
    assert "BIGSERIAL" in ddl
    assert "PRIMARY KEY" in ddl


# ---------------------------------------------------------------------------
# Index on external_id + _ingested_at
# ---------------------------------------------------------------------------

def test_history_ddl_creates_index_on_external_id():
    ddl = source_history_table_ddl("hubspot", "contacts")
    assert "CREATE INDEX" in ddl
    assert "external_id" in ddl


def test_history_ddl_returns_string():
    assert isinstance(source_history_table_ddl("hubspot", "contacts"), str)
