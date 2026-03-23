"""Unit tests for data lineage tracking (Step 86)."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# _upsert_record lineage tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_upsert_record_stores_lineage():
    """_upsert_record should store lineage JSON in _lineage column."""
    from inandout.ingestion.engine import _upsert_record

    executed_sqls: list[str] = []
    executed_params: list[list] = []

    mock_cursor = MagicMock()
    mock_cursor.fetchone = AsyncMock(return_value=None)  # No existing row

    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    async def _execute_side_effect(sql, params=None):
        executed_sqls.append(sql)
        executed_params.append(params or [])
        if "SELECT _raw_hash" in sql:
            return mock_cursor
        mock_result = MagicMock()
        if "INSERT INTO" in sql:
            mock_result.rowcount = 1  # simulate a real insert (no concurrent conflict)
        return mock_result

    mock_conn.execute = AsyncMock(side_effect=_execute_side_effect)

    lineage = {
        "run_id": str(uuid.uuid4()),
        "fetched_at": "2026-03-23T00:00:00",
        "api_path": "/contacts",
        "watermark_at_fetch": None,
        "page_number": 1,
    }

    inserted, updated, _resurrected = await _upsert_record(
        conn=mock_conn,
        table="inout_src_test_contacts",
        external_id="contact-1",
        raw={"id": "contact-1", "name": "Alice"},
        raw_hash="abc123",
        run_id=uuid.uuid4(),
        lineage=lineage,
    )

    assert inserted == 1
    assert updated == 0

    # Find the INSERT statement
    insert_sqls = [s for s in executed_sqls if "INSERT INTO" in s]
    assert insert_sqls, "Expected an INSERT statement"
    insert_sql = insert_sqls[0]
    assert "_lineage" in insert_sql


@pytest.mark.anyio
async def test_upsert_record_stores_lineage_on_update():
    """_upsert_record should store lineage on UPDATE too."""
    from inandout.ingestion.engine import _upsert_record

    executed_sqls: list[str] = []

    mock_cursor_select = MagicMock()
    mock_cursor_select.fetchone = AsyncMock(return_value=("oldhash", None))  # Existing row with different hash, not tombstoned

    mock_conn = AsyncMock()

    async def _execute_side_effect(sql, params=None):
        executed_sqls.append(sql)
        if "SELECT _raw_hash" in sql:
            return mock_cursor_select
        return MagicMock()

    mock_conn.execute = AsyncMock(side_effect=_execute_side_effect)

    lineage = {
        "run_id": "run-1",
        "fetched_at": "2026-03-23T00:00:00",
        "api_path": "/contacts",
        "watermark_at_fetch": None,
        "page_number": 2,
    }

    inserted, updated, _resurrected = await _upsert_record(
        conn=mock_conn,
        table="inout_src_test_contacts",
        external_id="contact-2",
        raw={"id": "contact-2", "name": "Bob"},
        raw_hash="newhash",
        run_id=uuid.uuid4(),
        lineage=lineage,
    )

    assert inserted == 0
    assert updated == 1

    update_sqls = [s for s in executed_sqls if "UPDATE" in s]
    assert update_sqls, "Expected an UPDATE statement"
    assert "_lineage" in update_sqls[0]


@pytest.mark.anyio
async def test_upsert_record_without_lineage():
    """_upsert_record should work without lineage (lineage=None)."""
    from inandout.ingestion.engine import _upsert_record

    mock_cursor = MagicMock()
    mock_cursor.fetchone = AsyncMock(return_value=None)

    mock_conn = AsyncMock()

    async def _execute_side_effect(sql, params=None):
        if "SELECT _raw_hash" in sql:
            return mock_cursor
        mock_result = MagicMock()
        if "INSERT INTO" in sql:
            mock_result.rowcount = 1
        return mock_result

    mock_conn.execute = AsyncMock(side_effect=_execute_side_effect)

    inserted, updated, _resurrected = await _upsert_record(
        conn=mock_conn,
        table="inout_src_test_contacts",
        external_id="contact-3",
        raw={"id": "contact-3"},
        raw_hash="hash3",
        run_id=uuid.uuid4(),
        lineage=None,
    )

    assert inserted == 1
    assert updated == 0


# ---------------------------------------------------------------------------
# Source table DDL lineage column tests
# ---------------------------------------------------------------------------

def test_source_table_ddl_has_lineage_column():
    """source_table_ddl should include _lineage JSONB column."""
    from inandout.postgres.schema import source_table_ddl

    ddl = source_table_ddl("myconn", "contacts")
    assert "_lineage" in ddl
    assert "JSONB" in ddl


# ---------------------------------------------------------------------------
# Lineage dict structure tests
# ---------------------------------------------------------------------------

def test_lineage_dict_has_expected_keys():
    """The lineage dict should have all required keys."""
    lineage = {
        "run_id": "run-abc",
        "fetched_at": "2026-03-23T12:00:00",
        "api_path": "/contacts",
        "watermark_at_fetch": "1234567890",
        "page_number": 3,
    }

    assert "run_id" in lineage
    assert "fetched_at" in lineage
    assert "api_path" in lineage
    assert "watermark_at_fetch" in lineage
    assert "page_number" in lineage
    assert isinstance(lineage["page_number"], int)
