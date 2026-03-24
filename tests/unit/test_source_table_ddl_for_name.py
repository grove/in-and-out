"""Unit tests for source_table_ddl_for_name and its round-trip with source_table_name."""
from __future__ import annotations

import pytest

from inandout.postgres.schema import (
    source_table_ddl,
    source_table_ddl_for_name,
    source_table_name,
)


def test_ddl_contains_table_name():
    ddl = source_table_ddl_for_name("inout_src_crm_contacts")
    assert "inout_src_crm_contacts" in ddl


def test_ddl_has_create_table():
    ddl = source_table_ddl_for_name("inout_src_crm_contacts")
    assert "CREATE TABLE IF NOT EXISTS" in ddl


def test_ddl_has_required_columns():
    ddl = source_table_ddl_for_name("inout_src_any_tbl")
    for col in ("external_id", "data", "raw", "_ingested_at", "_raw_hash", "_deleted"):
        assert col in ddl, f"Expected column {col!r} in DDL"


def test_ddl_has_primary_key():
    ddl = source_table_ddl_for_name("inout_src_x_y")
    assert "PRIMARY KEY (external_id)" in ddl


def test_ddl_has_index():
    ddl = source_table_ddl_for_name("inout_src_x_y")
    assert "CREATE INDEX IF NOT EXISTS" in ddl
    assert "_ingested_at_idx" in ddl


def test_public_namespace_no_create_schema():
    ddl = source_table_ddl_for_name("inout_src_a_b", namespace="public")
    assert "CREATE SCHEMA" not in ddl


def test_custom_namespace_has_create_schema():
    ddl = source_table_ddl_for_name("tenant42.inout_src_a_b", namespace="tenant42")
    assert "CREATE SCHEMA IF NOT EXISTS tenant42" in ddl


def test_round_trip_public():
    """source_table_ddl_for_name(source_table_name(...)) == source_table_ddl(...)."""
    connector, datatype = "sfdc", "leads"
    table = source_table_name(connector, datatype)
    via_name = source_table_ddl_for_name(table)
    direct = source_table_ddl(connector, datatype)
    assert via_name == direct


def test_round_trip_custom_namespace():
    connector, datatype, ns = "sfdc", "leads", "tenant_x"
    table = source_table_name(connector, datatype, ns)
    via_name = source_table_ddl_for_name(table, namespace=ns)
    direct = source_table_ddl(connector, datatype, ns)
    assert via_name == direct


def test_table_name_in_index_name():
    ddl = source_table_ddl_for_name("inout_src_crm_contacts")
    assert "inout_src_crm_contacts_ingested_at_idx" in ddl


def test_schema_qualified_index_uses_underscore():
    """schema.table → schema_table in index name."""
    ddl = source_table_ddl_for_name("myschema.inout_src_crm_contacts", namespace="myschema")
    assert "myschema_inout_src_crm_contacts_ingested_at_idx" in ddl


def test_ddl_for_name_default_namespace_is_public():
    ddl_default = source_table_ddl_for_name("some_table")
    ddl_public = source_table_ddl_for_name("some_table", namespace="public")
    assert ddl_default == ddl_public
