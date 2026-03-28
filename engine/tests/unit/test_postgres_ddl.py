"""Unit tests for PostgreSQL DDL generation — no DB connection required."""
from __future__ import annotations

from inandout.postgres.schema import (
    source_table_name,
    source_history_table_name,
    dead_letter_table_name,
    source_table_ddl,
    source_history_table_ddl,
    OPERATIONAL_TABLES_DDL,
)


class TestTableNaming:
    def test_source_table_name(self):
        assert source_table_name("hubspot", "contacts") == "inout_src_hubspot_contacts"

    def test_source_history_table_name(self):
        assert source_history_table_name("hubspot", "contacts") == "inout_src_hubspot_contacts_history"

    def test_dead_letter_table_name(self):
        assert dead_letter_table_name("ingestion", "hubspot", "contacts") == "inout_dl_ingestion_hubspot_contacts"

    def test_source_table_name_different_connector(self):
        assert source_table_name("salesforce", "accounts") == "inout_src_salesforce_accounts"

    def test_dead_letter_writeback(self):
        assert dead_letter_table_name("writeback", "salesforce", "leads") == "inout_dl_writeback_salesforce_leads"


class TestSourceTableDDL:
    def test_create_table_present(self):
        ddl = source_table_ddl("hubspot", "contacts")
        assert "CREATE TABLE IF NOT EXISTS inout_src_hubspot_contacts" in ddl

    def test_primary_key_present(self):
        ddl = source_table_ddl("hubspot", "contacts")
        assert "PRIMARY KEY (external_id)" in ddl

    def test_required_columns_present(self):
        ddl = source_table_ddl("hubspot", "contacts")
        assert "external_id TEXT NOT NULL" in ddl
        assert "data        JSONB NOT NULL" in ddl
        assert "raw         JSONB NOT NULL" in ddl
        assert "_raw_hash       TEXT NOT NULL" in ddl
        assert "_deleted        BOOLEAN NOT NULL DEFAULT FALSE" in ddl

    def test_index_present(self):
        ddl = source_table_ddl("hubspot", "contacts")
        assert "CREATE INDEX IF NOT EXISTS inout_src_hubspot_contacts_ingested_at_idx" in ddl

    def test_different_connector(self):
        ddl = source_table_ddl("salesforce", "leads")
        assert "inout_src_salesforce_leads" in ddl


class TestSourceHistoryTableDDL:
    def test_create_table_present(self):
        ddl = source_history_table_ddl("hubspot", "contacts")
        assert "CREATE TABLE IF NOT EXISTS inout_src_hubspot_contacts_history" in ddl

    def test_bigserial_pk_present(self):
        ddl = source_history_table_ddl("hubspot", "contacts")
        assert "BIGSERIAL PRIMARY KEY" in ddl

    def test_index_present(self):
        ddl = source_history_table_ddl("hubspot", "contacts")
        assert "CREATE INDEX IF NOT EXISTS inout_src_hubspot_contacts_history_external_id_idx" in ddl


class TestOperationalTablesDDL:
    def test_sync_run_table(self):
        assert "CREATE TABLE IF NOT EXISTS inout_ops_sync_run" in OPERATIONAL_TABLES_DDL

    def test_watermark_table(self):
        assert "CREATE TABLE IF NOT EXISTS inout_ops_watermark" in OPERATIONAL_TABLES_DDL

    def test_control_table(self):
        assert "CREATE TABLE IF NOT EXISTS inout_ops_control" in OPERATIONAL_TABLES_DDL

    def test_sync_run_status_constraint(self):
        assert "CONSTRAINT valid_status CHECK" in OPERATIONAL_TABLES_DDL

    def test_watermark_references_sync_run(self):
        assert "REFERENCES inout_ops_sync_run(id)" in OPERATIONAL_TABLES_DDL
