"""Tests: T2 #38 — effective protection_level stored in audit entries and persisted."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass


# ---------------------------------------------------------------------------
# WritebackResult._audit_entries is now a 5-tuple
# ---------------------------------------------------------------------------

def test_audit_entries_is_five_tuple():
    """WritebackResult._audit_entries stores 5-tuples with effective_protection_level."""
    from inandout.writeback.engine import WritebackResult

    r = WritebackResult(connector="crm", datatype="contacts", delta_table="delta")
    r._audit_entries.append(("ext-1", "update", {"name": "Alice"}, {"changed": {}}, "optimistic"))

    assert len(r._audit_entries) == 1
    ext_id, action, payload, diff, pl = r._audit_entries[0]
    assert ext_id == "ext-1"
    assert action == "update"
    assert pl == "optimistic"


def test_audit_entries_none_protection():
    """WritebackResult._audit_entries works with None protection_level (fallback string)."""
    from inandout.writeback.engine import WritebackResult

    r = WritebackResult(connector="crm", datatype="contacts", delta_table="delta")
    r._audit_entries.append(("ext-2", "delete", None, None, "none"))

    ext_id, action, payload, diff, pl = r._audit_entries[0]
    assert pl == "none"
    assert payload is None


# ---------------------------------------------------------------------------
# _write_feedback uses 5-tuple and includes protection_level in INSERT
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_write_feedback_includes_protection_level():
    """_write_feedback tries to insert protection_level in the DB row."""
    pool = MagicMock()
    conn_ctx = AsyncMock()
    conn = AsyncMock()
    conn_ctx.__aenter__ = AsyncMock(return_value=conn)
    conn_ctx.__aexit__ = AsyncMock(return_value=None)
    pool.connection = MagicMock(return_value=conn_ctx)

    execute_mock = AsyncMock()
    conn.execute = execute_mock
    conn.commit = AsyncMock()

    from inandout.writeback.engine import WritebackEngine, WritebackResult

    engine = WritebackEngine(pool=pool)
    result = WritebackResult(connector="test", datatype="contacts", delta_table="test_delta")
    result._audit_entries.append((
        "ext-1",
        "update",
        {"name": "Bob"},
        {"changed": {"name": {"from": "Alice", "to": "Bob"}}},
        "optimistic",
    ))

    rows = [{"external_id": "ext-1", "_action": "update"}]
    await engine._write_feedback(rows, result, MagicMock())

    assert execute_mock.called
    all_sqls = " ".join(str(c[0][0]) for c in execute_mock.call_args_list)
    # The first attempt should include protection_level
    assert "protection_level" in all_sqls


@pytest.mark.anyio
async def test_write_feedback_fallback_without_protection_level():
    """_write_feedback falls back gracefully when protection_level column is absent."""
    pool = MagicMock()
    conn_ctx = AsyncMock()
    conn = AsyncMock()
    conn_ctx.__aenter__ = AsyncMock(return_value=conn)
    conn_ctx.__aexit__ = AsyncMock(return_value=None)
    pool.connection = MagicMock(return_value=conn_ctx)

    call_count = 0

    async def execute_side_effect(sql, *args, **kwargs):
        nonlocal call_count
        # First call (with protection_level) fails; subsequent calls succeed
        if call_count == 0 and "protection_level" in str(sql):
            call_count += 1
            raise Exception("column protection_level does not exist")
        call_count += 1
        return AsyncMock()

    conn.execute = execute_side_effect
    conn.commit = AsyncMock()

    from inandout.writeback.engine import WritebackEngine, WritebackResult

    engine = WritebackEngine(pool=pool)
    result = WritebackResult(connector="test", datatype="contacts", delta_table="test_delta")
    result._audit_entries.append(("ext-1", "insert", {"name": "Eve"}, {}, "none"))

    rows = [{"external_id": "ext-1", "_action": "insert"}]
    # Should not raise; falls back to insert without protection_level column
    await engine._write_feedback(rows, result, MagicMock())
    assert call_count >= 2  # at least two attempts


# ---------------------------------------------------------------------------
# _dispatch_row populates effective_protection_level correctly
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_dispatch_row_audit_has_protection_level():
    """After a successful insert _dispatch_row records the protection_level in audit."""
    from inandout.writeback.engine import WritebackEngine, WritebackResult, ProtectionLevel

    pool = MagicMock()
    conn_ctx = AsyncMock()
    conn = AsyncMock()
    fake_cursor = AsyncMock()
    fake_cursor.fetchone = AsyncMock(return_value=None)
    conn.execute = AsyncMock(return_value=fake_cursor)
    conn.commit = AsyncMock()
    conn_ctx.__aenter__ = AsyncMock(return_value=conn)
    conn_ctx.__aexit__ = AsyncMock(return_value=None)
    pool.connection = MagicMock(return_value=conn_ctx)

    engine = WritebackEngine(pool=pool)
    result = WritebackResult(connector="crm", datatype="contacts", delta_table="delta")

    wb_cfg = MagicMock()
    wb_cfg.protection_level = ProtectionLevel.none
    wb_cfg.dry_run = False
    wb_cfg.idempotency_key_header = None
    wb_cfg.diff_fields = False
    wb_cfg.use_desired_state_table = False
    wb_cfg.crdt_type = None
    wb_cfg.enable_crash_recovery = False
    wb_cfg.operations = MagicMock()
    wb_cfg.operations.insert = MagicMock()
    wb_cfg.operations.insert.path = "/contacts"
    wb_cfg.operations.insert.method = "POST"

    import httpx
    mock_resp = MagicMock(spec=httpx.Response)
    mock_resp.is_success = True
    mock_resp.content = b'{"id": "new-123"}'
    mock_resp.status_code = 201
    mock_resp.raise_for_status = MagicMock()

    connector = MagicMock()
    connector.name = "crm"
    connector.connection = MagicMock()
    connector.connection.base_url = "https://api.example.com"
    connector.circuit_breaker = {}

    with patch("inandout.transport.http.HttpTransportAdapter") as MockAdapter:
        mock_transport = AsyncMock()
        mock_transport._raw_request = AsyncMock(return_value=mock_resp)
        MockAdapter.return_value.__aenter__ = AsyncMock(return_value=mock_transport)
        MockAdapter.return_value.__aexit__ = AsyncMock(return_value=None)

        async with MockAdapter(connector) as transport:
            await engine._dispatch_row(
                transport, connector, wb_cfg,
                action="insert",
                external_id="x-1",
                row={"name": "Alice"},
                log=MagicMock(),
                result=result,
            )

    assert len(result._audit_entries) == 1
    *_, effective_pl = result._audit_entries[0]
    assert effective_pl == "none"
