"""Unit tests for dead_letter_table_name naming convention."""
from __future__ import annotations

import pytest

from inandout.postgres.schema import dead_letter_table_name


def test_basic_name():
    assert dead_letter_table_name("ingestion", "crm", "contacts") == "inout_dl_ingestion_crm_contacts"


def test_writeback_tool():
    assert dead_letter_table_name("writeback", "erp", "orders") == "inout_dl_writeback_erp_orders"


def test_public_namespace_is_bare():
    name = dead_letter_table_name("ingestion", "crm", "contacts", namespace="public")
    assert name == "inout_dl_ingestion_crm_contacts"
    assert "." not in name


def test_custom_namespace_prefixed():
    name = dead_letter_table_name("ingestion", "crm", "contacts", namespace="tenant_x")
    assert name == "tenant_x.inout_dl_ingestion_crm_contacts"


def test_starts_with_inout_dl():
    assert dead_letter_table_name("ingestion", "a", "b").startswith("inout_dl_")


def test_tool_in_name():
    name = dead_letter_table_name("mytool", "conn", "dtype")
    assert "mytool" in name


def test_connector_in_name():
    name = dead_letter_table_name("ingestion", "sfdc", "leads")
    assert "sfdc" in name


def test_datatype_in_name():
    name = dead_letter_table_name("ingestion", "sfdc", "leads")
    assert "leads" in name


def test_empty_namespace_is_bare():
    name = dead_letter_table_name("ingestion", "crm", "contacts", namespace="")
    assert "." not in name


def test_different_tools_produce_different_names():
    a = dead_letter_table_name("ingestion", "crm", "contacts")
    b = dead_letter_table_name("writeback", "crm", "contacts")
    assert a != b


def test_schema_qualified_dot_in_name():
    name = dead_letter_table_name("ingestion", "crm", "contacts", namespace="myschema")
    parts = name.split(".", 1)
    assert parts[0] == "myschema"
    assert parts[1].startswith("inout_dl_")
