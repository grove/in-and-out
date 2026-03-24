"""Unit tests for _schema_prefix_ddl in postgres/schema.py."""
from __future__ import annotations

import pytest

from inandout.postgres.schema import _schema_prefix_ddl


def test_public_returns_empty_string():
    assert _schema_prefix_ddl("public") == ""


def test_empty_string_returns_empty_string():
    assert _schema_prefix_ddl("") == ""


def test_custom_namespace_creates_schema():
    result = _schema_prefix_ddl("tenant_1")
    assert result == "CREATE SCHEMA IF NOT EXISTS tenant_1;\n"


def test_custom_namespace_contains_name():
    ns = "my_custom_schema"
    result = _schema_prefix_ddl(ns)
    assert ns in result


def test_custom_namespace_ends_with_newline():
    result = _schema_prefix_ddl("some_ns")
    assert result.endswith("\n")


def test_custom_namespace_starts_with_create_schema():
    result = _schema_prefix_ddl("analytics")
    assert result.startswith("CREATE SCHEMA IF NOT EXISTS")


def test_public_is_not_create_schema():
    result = _schema_prefix_ddl("public")
    assert "CREATE SCHEMA" not in result


def test_various_custom_names():
    for ns in ("reporting", "integration", "tenant_xyz_123", "TEST_SCHEMA"):
        result = _schema_prefix_ddl(ns)
        assert f"CREATE SCHEMA IF NOT EXISTS {ns}" in result


def test_underscore_in_namespace():
    result = _schema_prefix_ddl("multi_word_ns")
    assert "multi_word_ns" in result


def test_numeric_suffix_namespace():
    result = _schema_prefix_ddl("tenant99")
    assert result == "CREATE SCHEMA IF NOT EXISTS tenant99;\n"


def test_public_variant_with_different_case_is_not_empty():
    # 'PUBLIC' != 'public', so it gets a CREATE SCHEMA line
    result = _schema_prefix_ddl("PUBLIC")
    assert "CREATE SCHEMA IF NOT EXISTS PUBLIC" in result
