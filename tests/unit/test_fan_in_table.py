"""Unit tests for multi-connector fan-in shared tables (T1 #46 A3)."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, call

import pytest


# ---------------------------------------------------------------------------
# source_table_name with shared_table
# ---------------------------------------------------------------------------

def test_source_table_name_without_shared_table():
    """Without shared_table, returns inout_src_{connector}_{datatype}."""
    from inandout.postgres.schema import source_table_name

    result = source_table_name("hubspot", "contacts")
    assert result == "inout_src_hubspot_contacts"


def test_source_table_name_with_shared_table():
    """With shared_table, returns inout_src_{shared_table}."""
    from inandout.postgres.schema import source_table_name

    result = source_table_name("hubspot", "contacts", shared_table="contacts_unified")
    assert result == "inout_src_contacts_unified"


def test_source_table_name_shared_table_with_namespace():
    """With shared_table + namespace, returns {namespace}.inout_src_{shared_table}."""
    from inandout.postgres.schema import source_table_name

    result = source_table_name("hubspot", "contacts", namespace="myschema", shared_table="contacts_unified")
    assert result == "myschema.inout_src_contacts_unified"


def test_source_table_name_without_shared_table_with_namespace():
    """Without shared_table + with namespace, returns {namespace}.inout_src_{connector}_{datatype}."""
    from inandout.postgres.schema import source_table_name

    result = source_table_name("hubspot", "contacts", namespace="myschema")
    assert result == "myschema.inout_src_hubspot_contacts"


# ---------------------------------------------------------------------------
# DatatypeConfig.shared_table field
# ---------------------------------------------------------------------------

def test_datatype_config_shared_table_default_none():
    """DatatypeConfig.shared_table defaults to None."""
    from inandout.config.connector import DatatypeConfig
    from inandout.config.ingestion import (
        HistoryMode,
        IngestionConfig,
        ListConfig,
        ScheduleConfig,
    )
    from inandout.config.pagination import CursorConfig, PaginationConfig, PaginationStrategy

    cfg = DatatypeConfig(
        ingestion=IngestionConfig(
            primary_key="id",
            history_mode=HistoryMode.overwrite,
            schedule=ScheduleConfig(interval="5m"),
            list=ListConfig(
                path="/contacts",
                pagination=PaginationConfig(
                    strategy=PaginationStrategy.cursor,
                    cursor=CursorConfig(response_path="next", request_param="after"),
                ),
            ),
        )
    )
    assert cfg.shared_table is None


def test_datatype_config_shared_table_set():
    """DatatypeConfig.shared_table can be set to a string."""
    from inandout.config.connector import DatatypeConfig
    from inandout.config.ingestion import (
        HistoryMode,
        IngestionConfig,
        ListConfig,
        ScheduleConfig,
    )
    from inandout.config.pagination import CursorConfig, PaginationConfig, PaginationStrategy

    cfg = DatatypeConfig(
        ingestion=IngestionConfig(
            primary_key="id",
            history_mode=HistoryMode.overwrite,
            schedule=ScheduleConfig(interval="5m"),
            list=ListConfig(
                path="/contacts",
                pagination=PaginationConfig(
                    strategy=PaginationStrategy.cursor,
                    cursor=CursorConfig(response_path="next", request_param="after"),
                ),
            ),
        ),
        shared_table="contacts_unified",
    )
    assert cfg.shared_table == "contacts_unified"


# ---------------------------------------------------------------------------
# _upsert_record with connector_col (shared table key)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upsert_record_shared_table_uses_connector_col():
    """When connector_col is set, upsert checks (external_id, _connector) conflict key."""
    from inandout.ingestion.engine import _upsert_record

    executed_sqls = []

    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor.fetchone = AsyncMock(return_value=None)  # new record
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    async def _capture_execute(sql, params=None):
        executed_sqls.append(sql)
        return mock_cursor

    mock_conn.execute = AsyncMock(side_effect=_capture_execute)

    run_id = uuid.uuid4()
    inserted, updated, _resurrected = await _upsert_record(
        mock_conn, "inout_src_contacts_unified",
        "ext_123", {"name": "Alice"}, "abc123", run_id,
        connector_col="hubspot",
    )

    # Should have checked using (external_id, _connector)
    first_sql = executed_sqls[0]
    assert "_connector" in first_sql
    assert "ext_123" in str(mock_conn.execute.call_args_list[0])


@pytest.mark.asyncio
async def test_upsert_record_no_shared_table_uses_external_id_only():
    """Without connector_col, upsert uses external_id only."""
    from inandout.ingestion.engine import _upsert_record

    executed_sqls = []
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor.fetchone = AsyncMock(return_value=None)  # new record

    async def _capture_execute(sql, params=None):
        executed_sqls.append(sql)
        return mock_cursor

    mock_conn.execute = AsyncMock(side_effect=_capture_execute)

    run_id = uuid.uuid4()
    await _upsert_record(
        mock_conn, "inout_src_hubspot_contacts",
        "ext_456", {"name": "Bob"}, "def456", run_id,
        connector_col=None,
    )

    # Standard path: only external_id in WHERE clause
    first_sql = executed_sqls[0]
    assert "external_id" in first_sql
    assert "_connector" not in first_sql


@pytest.mark.asyncio
async def test_upsert_record_shared_table_update_path():
    """When connector_col set and record exists with different hash, UPDATE is executed."""
    from inandout.ingestion.engine import _upsert_record

    executed_sqls = []
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    # First call (SELECT): returns existing row with different hash
    existing_row = ("old_hash_xyz", None)  # (hash, _deleted_at)
    mock_cursor.fetchone = AsyncMock(side_effect=[existing_row])

    call_count = 0

    async def _capture_execute(sql, params=None):
        nonlocal call_count
        call_count += 1
        executed_sqls.append(sql)
        if call_count == 1:
            # SELECT call returns old hash (not tombstoned)
            cur = AsyncMock()
            cur.fetchone = AsyncMock(return_value=existing_row)
            return cur
        return mock_cursor

    mock_conn.execute = AsyncMock(side_effect=_capture_execute)

    run_id = uuid.uuid4()
    inserted, updated, _resurrected = await _upsert_record(
        mock_conn, "inout_src_contacts_unified",
        "ext_789", {"name": "Charlie"}, "new_hash_abc", run_id,
        connector_col="salesforce",
    )

    assert updated == 1
    assert inserted == 0
    # UPDATE SQL should reference _connector
    update_sql = executed_sqls[1]
    assert "UPDATE" in update_sql
    assert "_connector" in update_sql
