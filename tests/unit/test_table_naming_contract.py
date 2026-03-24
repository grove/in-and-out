"""Unit tests for table naming functions (naming contract).

Covers source_table_name, dead_letter_table_name:
- public namespace produces bare table name (no schema prefix).
- non-public namespace produces "namespace.table_name".
- shared_table overrides the connector/datatype in source_table_name.
- dead_letter_table_name embeds the tool name (ingestion / writeback).
- All names follow the inout_ prefix convention.
"""
from __future__ import annotations

import pytest

from inandout.postgres.schema import (
    dead_letter_table_name,
    source_history_table_name,
    source_table_name,
)


# ---------------------------------------------------------------------------
# source_table_name
# ---------------------------------------------------------------------------

class TestSourceTableName:
    def test_public_namespace_no_prefix(self):
        assert source_table_name("hubspot", "contacts") == "inout_src_hubspot_contacts"

    def test_explicit_public_namespace_no_prefix(self):
        assert source_table_name("hubspot", "contacts", "public") == "inout_src_hubspot_contacts"

    def test_non_public_namespace_prefixes(self):
        name = source_table_name("hubspot", "contacts", "tenant_42")
        assert name == "tenant_42.inout_src_hubspot_contacts"

    def test_shared_table_overrides_connector_datatype(self):
        name = source_table_name("hubspot", "contacts", shared_table="crm_contacts")
        assert name == "inout_src_crm_contacts"

    def test_shared_table_in_non_public_namespace(self):
        name = source_table_name(
            "hubspot", "contacts",
            namespace="tenant_1",
            shared_table="crm_contacts",
        )
        assert name == "tenant_1.inout_src_crm_contacts"

    @pytest.mark.parametrize("connector,datatype", [
        ("salesforce", "deals"),
        ("stripe", "subscriptions"),
        ("intercom", "conversations"),
    ])
    def test_various_connectors_follow_convention(self, connector: str, datatype: str):
        name = source_table_name(connector, datatype)
        assert name == f"inout_src_{connector}_{datatype}"

    def test_starts_with_inout_prefix(self):
        assert source_table_name("hubspot", "contacts").startswith("inout_src_")


# ---------------------------------------------------------------------------
# dead_letter_table_name
# ---------------------------------------------------------------------------

class TestDeadLetterTableName:
    def test_ingestion_tool_public_namespace(self):
        name = dead_letter_table_name("ingestion", "hubspot", "contacts")
        assert name == "inout_dl_ingestion_hubspot_contacts"

    def test_writeback_tool_public_namespace(self):
        name = dead_letter_table_name("writeback", "salesforce", "deals")
        assert name == "inout_dl_writeback_salesforce_deals"

    def test_non_public_namespace_prefixes(self):
        name = dead_letter_table_name("ingestion", "hubspot", "contacts", "tenant_5")
        assert name == "tenant_5.inout_dl_ingestion_hubspot_contacts"

    def test_starts_with_inout_dl(self):
        name = dead_letter_table_name("ingestion", "hubspot", "contacts")
        assert name.startswith("inout_dl_")

    @pytest.mark.parametrize("tool,connector,datatype", [
        ("ingestion", "salesforce", "leads"),
        ("writeback", "stripe", "payments"),
    ])
    def test_various_combinations(self, tool: str, connector: str, datatype: str):
        name = dead_letter_table_name(tool, connector, datatype)
        assert name == f"inout_dl_{tool}_{connector}_{datatype}"


# ---------------------------------------------------------------------------
# source_history_table_name
# ---------------------------------------------------------------------------

class TestSourceHistoryTableName:
    def test_public_namespace(self):
        assert (
            source_history_table_name("hubspot", "contacts")
            == "inout_src_hubspot_contacts_history"
        )

    def test_non_public_namespace_prefixes(self):
        name = source_history_table_name("hubspot", "contacts", "tenant_3")
        assert name == "tenant_3.inout_src_hubspot_contacts_history"

    def test_ends_with_history_suffix(self):
        assert source_history_table_name("hubspot", "contacts").endswith("_history")
