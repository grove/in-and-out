"""Unit tests for source_table_name naming convention."""
from __future__ import annotations

import pytest

from inandout.postgres.schema import source_table_name


def test_public_namespace_bare():
    assert source_table_name("crm", "contacts") == "inout_src_crm_contacts"


def test_public_explicit_same_as_default():
    assert source_table_name("crm", "contacts", "public") == source_table_name("crm", "contacts")


def test_custom_namespace_prefixed():
    assert source_table_name("crm", "contacts", "tenant_x") == "tenant_x.inout_src_crm_contacts"


def test_empty_namespace_is_bare():
    assert source_table_name("crm", "contacts", "") == "inout_src_crm_contacts"


def test_starts_with_inout_src():
    assert source_table_name("a", "b").startswith("inout_src_")


def test_connector_in_name():
    assert "sfdc" in source_table_name("sfdc", "leads")


def test_datatype_in_name():
    assert "leads" in source_table_name("sfdc", "leads")


def test_shared_table_overrides_name():
    name = source_table_name("crm", "contacts", shared_table="shared_contacts")
    assert "shared_contacts" in name
    assert "crm" not in name


def test_shared_table_with_public_namespace_bare():
    name = source_table_name("crm", "contacts", "public", shared_table="shared_tbl")
    assert name == "inout_src_shared_tbl"
    assert "." not in name


def test_shared_table_with_custom_namespace():
    name = source_table_name("crm", "contacts", "ns", shared_table="shared_tbl")
    assert name == "ns.inout_src_shared_tbl"


def test_different_connectors_different_names():
    a = source_table_name("crm", "contacts")
    b = source_table_name("erp", "contacts")
    assert a != b


def test_different_datatypes_different_names():
    a = source_table_name("crm", "contacts")
    b = source_table_name("crm", "accounts")
    assert a != b


def test_schema_qualified_dot_notation():
    name = source_table_name("a", "b", "myschema")
    parts = name.split(".", 1)
    assert parts[0] == "myschema"
    assert parts[1] == "inout_src_a_b"
