"""Unit tests for T2 #35: required_fields guard in WritebackEngine._dispatch_row."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock


# ---------------------------------------------------------------------------
# Config field
# ---------------------------------------------------------------------------

def _make_writeback_cfg(required_fields=None, **kwargs):
    from inandout.config.writeback import (
        ConflictResolution,
        OperationConfig,
        OperationsConfig,
        ProtectionLevel,
        UpdateOperationConfig,
        WritebackConfig,
    )
    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/contacts/${external_id}"),
        insert=OperationConfig(method="POST", path="/contacts"),
        update=UpdateOperationConfig(method="PATCH", path="/contacts/${external_id}"),
    )
    if required_fields is None:
        required_fields = []
    return WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert", "update"],
        operations=ops,
        required_fields=required_fields,
        **kwargs,
    )


def test_required_fields_default_empty():
    cfg = _make_writeback_cfg()
    assert cfg.required_fields == []


def test_required_fields_accepts_list():
    cfg = _make_writeback_cfg(required_fields=["email", "name"])
    assert cfg.required_fields == ["email", "name"]


# ---------------------------------------------------------------------------
# Engine _dispatch_row guard
# ---------------------------------------------------------------------------

def _make_engine():
    from inandout.writeback.engine import WritebackEngine
    engine = WritebackEngine.__new__(WritebackEngine)
    return engine


def _make_result(connector="c", datatype="d", delta_table="t"):
    from inandout.writeback.engine import WritebackResult
    return WritebackResult(connector=connector, datatype=datatype, delta_table=delta_table)


def _make_connector(name="test_connector"):
    connector = MagicMock()
    connector.name = name
    connector.circuit_breaker = None
    return connector


@pytest.mark.asyncio
async def test_required_fields_all_present_proceeds_to_dispatch():
    """When all required fields are present, dispatch proceeds normally."""
    engine = _make_engine()
    writeback_cfg = _make_writeback_cfg(required_fields=["email"])
    connector = _make_connector()
    result = _make_result()

    row = {"_ext_id": "001", "_action": "insert", "email": "test@example.com", "name": "Alice"}
    transport = AsyncMock()
    # configure transport so the dispatch completes without error
    transport.request = AsyncMock(return_value=MagicMock(status_code=201))
    log = MagicMock()

    # Patch circuit breaker to always allow
    with MagicMock() as _cb:
        pass

    from unittest.mock import patch
    with patch(
        "inandout.transport.circuit_breaker.get_circuit_breaker"
    ) as mock_get_cb:
        mock_cb = MagicMock()
        mock_cb.allow_request.return_value = True
        mock_cb.state = "closed"
        mock_get_cb.return_value = mock_cb

        initial_failed = result.failed
        # Should not increment failed for a present field
        try:
            await engine._dispatch_row(
                transport, connector, writeback_cfg,
                action="insert", external_id="001", row=row,
                log=log, result=result,
            )
        except Exception:
            pass  # HTTP call may fail, that's ok — we only care about the guard

    assert result.failed == initial_failed  # guard did not add to failed


@pytest.mark.asyncio
async def test_required_fields_missing_increments_failed():
    """When a required field is absent, failed is incremented and dispatch is skipped."""
    engine = _make_engine()
    writeback_cfg = _make_writeback_cfg(required_fields=["email"])
    connector = _make_connector()
    result = _make_result()

    row = {"_ext_id": "002", "_action": "insert", "name": "Bob"}  # missing 'email'
    transport = AsyncMock()
    log = MagicMock()

    from unittest.mock import patch
    with patch(
        "inandout.transport.circuit_breaker.get_circuit_breaker"
    ) as mock_get_cb:
        mock_cb = MagicMock()
        mock_cb.allow_request.return_value = True
        mock_get_cb.return_value = mock_cb

        await engine._dispatch_row(
            transport, connector, writeback_cfg,
            action="insert", external_id="002", row=row,
            log=log, result=result,
        )

    assert result.failed == 1
    assert "002" in result._failed_external_ids
    assert any("required_fields_missing" in entry[2] for entry in result._failed_entries)


@pytest.mark.asyncio
async def test_required_fields_missing_entry_includes_field_name():
    """The _failed_entries entry for a missing required field names the missing field."""
    engine = _make_engine()
    writeback_cfg = _make_writeback_cfg(required_fields=["email", "phone"])
    connector = _make_connector()
    result = _make_result()

    row = {"_ext_id": "003", "name": "Charlie"}  # missing 'email' and 'phone'
    transport = AsyncMock()
    log = MagicMock()

    from unittest.mock import patch
    with patch(
        "inandout.transport.circuit_breaker.get_circuit_breaker"
    ) as mock_get_cb:
        mock_cb = MagicMock()
        mock_cb.allow_request.return_value = True
        mock_get_cb.return_value = mock_cb

        await engine._dispatch_row(
            transport, connector, writeback_cfg,
            action="insert", external_id="003", row=row,
            log=log, result=result,
        )

    assert result.failed == 1
    reason = result._failed_entries[0][2]
    assert "email" in reason
    assert "phone" in reason


@pytest.mark.asyncio
async def test_required_fields_empty_list_no_guard():
    """With an empty required_fields list, dispatch performs no field check."""
    engine = _make_engine()
    writeback_cfg = _make_writeback_cfg(required_fields=[])
    connector = _make_connector()
    result = _make_result()

    # row with no business fields at all
    row = {"_ext_id": "004", "_action": "insert"}
    transport = AsyncMock()
    log = MagicMock()

    from unittest.mock import patch
    with patch(
        "inandout.transport.circuit_breaker.get_circuit_breaker"
    ) as mock_get_cb:
        mock_cb = MagicMock()
        mock_cb.allow_request.return_value = True
        mock_get_cb.return_value = mock_cb

        try:
            await engine._dispatch_row(
                transport, connector, writeback_cfg,
                action="insert", external_id="004", row=row,
                log=log, result=result,
            )
        except Exception:
            pass  # HTTP may fail — we only care that required_fields guard didn't trigger

    # Guard with empty list must not increment failed
    assert result.failed == 0
