"""Unit tests for multi-tenancy / namespace isolation in table naming."""
from __future__ import annotations

from inandout.postgres.schema import (
    dead_letter_table_name,
    source_history_table_name,
    source_table_name,
)


class TestSourceTableNameNamespace:
    def test_default_namespace_no_prefix(self):
        """Default namespace ('public') should NOT prefix the table name."""
        assert source_table_name("hub", "contacts") == "inout_src_hub_contacts"

    def test_explicit_public_no_prefix(self):
        assert source_table_name("hub", "contacts", namespace="public") == "inout_src_hub_contacts"

    def test_custom_namespace_prefixes_table(self):
        assert (
            source_table_name("hub", "contacts", namespace="tenant_a")
            == "tenant_a.inout_src_hub_contacts"
        )

    def test_different_connector_and_namespace(self):
        assert (
            source_table_name("salesforce", "leads", namespace="acme")
            == "acme.inout_src_salesforce_leads"
        )


class TestSourceHistoryTableNameNamespace:
    def test_default_namespace_no_prefix(self):
        assert (
            source_history_table_name("hub", "contacts")
            == "inout_src_hub_contacts_history"
        )

    def test_custom_namespace_prefixes_history_table(self):
        assert (
            source_history_table_name("hub", "contacts", namespace="tenant_b")
            == "tenant_b.inout_src_hub_contacts_history"
        )


class TestDeadLetterTableNameNamespace:
    def test_default_namespace_no_prefix(self):
        assert (
            dead_letter_table_name("ingestion", "hub", "contacts")
            == "inout_dl_ingestion_hub_contacts"
        )

    def test_custom_namespace_prefixes_dead_letter_table(self):
        assert (
            dead_letter_table_name("ingestion", "hub", "contacts", namespace="tenant_c")
            == "tenant_c.inout_dl_ingestion_hub_contacts"
        )
