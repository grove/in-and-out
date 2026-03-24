"""Unit tests for _safe_name in writeback/notify.py."""
from __future__ import annotations

import pytest

from inandout.writeback.notify import _safe_name


def test_plain_name_unchanged():
    assert _safe_name("mytable") == "mytable"


def test_dot_replaced_by_underscore():
    assert _safe_name("schema.table") == "schema_table"


def test_dash_replaced_by_underscore():
    assert _safe_name("my-table") == "my_table"


def test_multiple_dots():
    assert _safe_name("a.b.c") == "a_b_c"


def test_multiple_dashes():
    assert _safe_name("a-b-c") == "a_b_c"


def test_mixed_dot_and_dash():
    assert _safe_name("schema.my-table") == "schema_my_table"


def test_delta_table_name_format():
    """Typical delta table: _delta_<connector>_<datatype>."""
    assert _safe_name("_delta_sfdc_contacts") == "_delta_sfdc_contacts"


def test_schema_qualified_delta_table():
    assert _safe_name("tenant.delta_sfdc_contacts") == "tenant_delta_sfdc_contacts"


def test_empty_string():
    assert _safe_name("") == ""


def test_only_dots():
    assert _safe_name("...") == "___"


def test_only_dashes():
    assert _safe_name("---") == "___"


def test_underscores_preserved():
    assert _safe_name("already_safe") == "already_safe"


def test_idempotent_on_safe_names():
    name = "inout_delta_crm_leads"
    assert _safe_name(name) == name
