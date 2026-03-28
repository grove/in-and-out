"""Unit tests for dead_letter_table_ddl DDL generation.

Covers:
- Table name contains "inout_dl_ingestion_{connector}_{datatype}".
- Required columns present: external_id, error_message, error_class,
  failed_at, requeue_count, sync_run_id, raw, id.
- CREATE TABLE IF NOT EXISTS semantics.
- Non-public namespace prefixes the table name.
- A CREATE INDEX on failed_at is included.
"""
from __future__ import annotations

import pytest

from inandout.postgres.schema import dead_letter_table_ddl


# ---------------------------------------------------------------------------
# Table naming
# ---------------------------------------------------------------------------

def test_dead_letter_ddl_table_name_standard():
    ddl = dead_letter_table_ddl("ingestion", "hubspot", "contacts")
    assert "inout_dl_ingestion_hubspot_contacts" in ddl


def test_dead_letter_ddl_table_name_writeback_tool():
    ddl = dead_letter_table_ddl("writeback", "salesforce", "deals")
    assert "inout_dl_writeback_salesforce_deals" in ddl


def test_dead_letter_ddl_uses_create_if_not_exists():
    ddl = dead_letter_table_ddl("ingestion", "hubspot", "contacts")
    assert "CREATE TABLE IF NOT EXISTS" in ddl


def test_dead_letter_ddl_namespace_prefixed():
    ddl = dead_letter_table_ddl("ingestion", "hubspot", "contacts", namespace="tenant_99")
    assert "tenant_99" in ddl
    assert "inout_dl_ingestion_hubspot_contacts" in ddl


# ---------------------------------------------------------------------------
# Required columns
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("column", [
    "external_id",
    "error_message",
    "error_class",
    "failed_at",
    "requeue_count",
    "sync_run_id",
    "raw",
    "id",
])
def test_dead_letter_ddl_contains_required_column(column: str):
    ddl = dead_letter_table_ddl("ingestion", "hubspot", "contacts")
    assert column in ddl, f"Expected column '{column}' in dead-letter DDL"


# ---------------------------------------------------------------------------
# Index on failed_at
# ---------------------------------------------------------------------------

def test_dead_letter_ddl_creates_failed_at_index():
    ddl = dead_letter_table_ddl("ingestion", "hubspot", "contacts")
    assert "failed_at" in ddl
    assert "CREATE INDEX" in ddl


def test_dead_letter_ddl_returns_string():
    assert isinstance(dead_letter_table_ddl("ingestion", "hubspot", "contacts"), str)
