"""Unit tests for _write_history_record and history table support."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from inandout.ingestion.engine import _write_history_record


class TestWriteHistoryRecord:
    @pytest.mark.anyio
    async def test_write_history_record_calls_execute(self):
        """_write_history_record should INSERT into the history table with correct columns."""
        conn = MagicMock()
        conn.execute = AsyncMock()

        hist_table = "inout_src_test_contacts_history"
        external_id = "rec-123"
        raw = {"id": "rec-123", "name": "Alice"}
        raw_hash = "abc123"
        run_id = uuid.uuid4()

        await _write_history_record(conn, hist_table, external_id, raw, raw_hash, run_id)

        conn.execute.assert_called_once()
        call_args = conn.execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]

        # Check SQL is an INSERT (not upsert with ON CONFLICT)
        assert "INSERT INTO" in sql
        assert "ON CONFLICT" not in sql
        assert hist_table in sql

        # Check the columns
        assert "external_id" in sql
        assert "data" in sql
        assert "raw" in sql
        assert "_ingested_at" in sql
        assert "_sync_run_id" in sql
        assert "_raw_hash" in sql

        # _history_id is BIGSERIAL, should NOT be in the INSERT
        assert "_history_id" not in sql

        # Check params
        assert params[0] == external_id
        assert params[3] == run_id
        assert params[4] == raw_hash

    @pytest.mark.anyio
    async def test_write_history_record_serializes_json(self):
        """_write_history_record should serialize data as JSON string."""
        import orjson

        conn = MagicMock()
        conn.execute = AsyncMock()

        hist_table = "inout_src_test_contacts_history"
        external_id = "rec-456"
        raw = {"id": "rec-456", "value": 42, "nested": {"key": "val"}}
        raw_hash = "def456"
        run_id = uuid.uuid4()

        await _write_history_record(conn, hist_table, external_id, raw, raw_hash, run_id)

        call_args = conn.execute.call_args
        params = call_args[0][1]

        # data and raw should be JSON strings (index 1 and 2)
        data_str = params[1]
        raw_str = params[2]
        assert isinstance(data_str, str)
        parsed = orjson.loads(data_str)
        assert parsed["id"] == "rec-456"
        assert parsed["value"] == 42
        assert data_str == raw_str  # data and raw use the same serialized form

    @pytest.mark.anyio
    async def test_write_history_record_multiple_records(self):
        """Each call to _write_history_record creates a separate INSERT."""
        conn = MagicMock()
        conn.execute = AsyncMock()

        hist_table = "inout_src_test_items_history"
        run_id = uuid.uuid4()

        await _write_history_record(conn, hist_table, "id-1", {"id": "id-1"}, "hash1", run_id)
        await _write_history_record(conn, hist_table, "id-2", {"id": "id-2"}, "hash2", run_id)
        await _write_history_record(conn, hist_table, "id-1", {"id": "id-1", "updated": True}, "hash3", run_id)

        # Three separate inserts — no deduplication in history
        assert conn.execute.call_count == 3
