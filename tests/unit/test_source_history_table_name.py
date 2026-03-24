"""Unit tests for source_history_table_name naming convention."""
from __future__ import annotations

import pytest

from inandout.postgres.schema import source_history_table_name


def test_public_namespace_bare():
    assert source_history_table_name("crm", "contacts") == "inout_src_crm_contacts_history"


def test_public_explicit_same_as_default():
    result_default = source_history_table_name("crm", "contacts")
    result_explicit = source_history_table_name("crm", "contacts", "public")
    assert result_default == result_explicit


def test_custom_namespace_prefixed():
    result = source_history_table_name("crm", "contacts", "tenant_x")
    assert result == "tenant_x.inout_src_crm_contacts_history"


def test_empty_namespace_is_bare():
    result = source_history_table_name("crm", "contacts", "")
    assert "." not in result


def test_contains_history_suffix():
    assert source_history_table_name("a", "b").endswith("_history")


def test_connector_in_name():
    assert "sfdc" in source_history_table_name("sfdc", "leads")


def test_datatype_in_name():
    assert "leads" in source_history_table_name("sfdc", "leads")


def test_starts_with_inout_src():
    assert source_history_table_name("a", "b").startswith("inout_src_")


def test_different_connectors_different_names():
    a = source_history_table_name("crm", "contacts")
    b = source_history_table_name("erp", "contacts")
    assert a != b


def test_different_datatypes_different_names():
    a = source_history_table_name("crm", "contacts")
    b = source_history_table_name("crm", "accounts")
    assert a != b


def test_schema_qualified_dot_notation():
    name = source_history_table_name("a", "b", "myschema")
    parts = name.split(".", 1)
    assert parts[0] == "myschema"
    assert parts[1] == "inout_src_a_b_history"


def test_different_from_source_table_name():
    from inandout.postgres.schema import source_table_name
    src = source_table_name("crm", "contacts")
    hist = source_history_table_name("crm", "contacts")
    assert src != hist
    assert "_history" in hist
    assert "_history" not in src
