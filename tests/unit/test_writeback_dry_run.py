"""Unit tests for writeback dry-run mode (T2 #27 B1)."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock


def _make_writeback_cfg(dry_run=False, **kwargs):
    """Build a minimal WritebackConfig."""
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
        delete=OperationConfig(method="DELETE", path="/contacts/${external_id}"),
    )
    return WritebackConfig(
        dry_run=dry_run,
        protection_level=ProtectionLevel.fire_and_forget,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert", "update", "delete"],
        operations=ops,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Config field
# ---------------------------------------------------------------------------

def test_writeback_config_dry_run_default_false():
    """WritebackConfig.dry_run defaults to False."""
    cfg = _make_writeback_cfg()
    assert cfg.dry_run is False


def test_writeback_config_dry_run_can_be_set():
    """WritebackConfig.dry_run can be set to True."""
    cfg = _make_writeback_cfg(dry_run=True)
    assert cfg.dry_run is True


# ---------------------------------------------------------------------------
# WritebackResult.dry_run_log
# ---------------------------------------------------------------------------

def test_writeback_result_has_dry_run_log():
    """WritebackResult should have a dry_run_log list."""
    from inandout.writeback.engine import WritebackResult

    result = WritebackResult(connector="c", datatype="d", delta_table="t")
    assert hasattr(result, "dry_run_log")
    assert isinstance(result.dry_run_log, list)
    assert len(result.dry_run_log) == 0


def test_writeback_result_dry_run_log_initially_empty():
    """dry_run_log is empty by default (not dry_run mode)."""
    from inandout.writeback.engine import WritebackResult

    result = WritebackResult(connector="c", datatype="d", delta_table="t")
    assert result.dry_run_log == []


# ---------------------------------------------------------------------------
# dry_run mode skips HTTP writes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dry_run_no_http_write_calls():
    """In dry_run=True, _dispatch_row must not make HTTP write calls."""
    from inandout.writeback.engine import WritebackEngine, WritebackResult

    pool = MagicMock()
    engine = WritebackEngine(pool)

    from inandout.config.connector import ConnectorConfig
    mock_connector = MagicMock()
    mock_connector.name = "hubspot"
    mock_connector.connection.base_url = "https://api.hubspot.com"

    writeback_cfg = _make_writeback_cfg(dry_run=True)
    result = WritebackResult(connector="hubspot", datatype="contacts", delta_table="t")

    mock_transport = AsyncMock()
    mock_transport._raw_request = AsyncMock()
    mock_transport._request = AsyncMock()

    row = {"id": "123", "name": "Alice"}

    await engine._dispatch_row(
        mock_transport, mock_connector, writeback_cfg,
        "insert", "123", row, MagicMock(), result,
    )

    # No HTTP write calls
    mock_transport._raw_request.assert_not_called()


@pytest.mark.asyncio
async def test_dry_run_populates_dry_run_log():
    """In dry_run=True, dry_run_log is populated with would-be write."""
    from inandout.writeback.engine import WritebackEngine, WritebackResult

    pool = MagicMock()
    engine = WritebackEngine(pool)

    mock_connector = MagicMock()
    mock_connector.name = "hubspot"
    mock_connector.connection.base_url = "https://api.hubspot.com"

    writeback_cfg = _make_writeback_cfg(dry_run=True)
    result = WritebackResult(connector="hubspot", datatype="contacts", delta_table="t")

    mock_transport = AsyncMock()
    mock_transport._raw_request = AsyncMock()
    mock_transport._request = AsyncMock()

    row = {"id": "123", "name": "Alice"}

    await engine._dispatch_row(
        mock_transport, mock_connector, writeback_cfg,
        "insert", "123", row, MagicMock(), result,
    )

    assert len(result.dry_run_log) == 1
    entry = result.dry_run_log[0]
    assert entry["action"] == "insert"
    assert entry["method"] == "POST"
    assert "/contacts" in entry["url"]


@pytest.mark.asyncio
async def test_dry_run_increments_skipped_not_processed():
    """In dry_run=True, skipped is incremented, processed is not."""
    from inandout.writeback.engine import WritebackEngine, WritebackResult

    pool = MagicMock()
    engine = WritebackEngine(pool)

    mock_connector = MagicMock()
    mock_connector.name = "hubspot"
    mock_connector.connection.base_url = "https://api.hubspot.com"

    writeback_cfg = _make_writeback_cfg(dry_run=True)
    result = WritebackResult(connector="hubspot", datatype="contacts", delta_table="t")

    mock_transport = AsyncMock()
    mock_transport._raw_request = AsyncMock()

    row = {"id": "456", "name": "Bob"}

    await engine._dispatch_row(
        mock_transport, mock_connector, writeback_cfg,
        "insert", "456", row, MagicMock(), result,
    )

    assert result.processed == 0
    assert result.skipped == 1


@pytest.mark.asyncio
async def test_dry_run_false_makes_http_calls():
    """In dry_run=False, _raw_request IS called normally."""
    from inandout.writeback.engine import WritebackEngine, WritebackResult

    pool = MagicMock()
    engine = WritebackEngine(pool)

    mock_connector = MagicMock()
    mock_connector.name = "hubspot"
    mock_connector.connection.base_url = "https://api.hubspot.com"

    writeback_cfg = _make_writeback_cfg(dry_run=False)
    result = WritebackResult(connector="hubspot", datatype="contacts", delta_table="t")

    mock_resp = MagicMock()
    mock_resp.content = b'{"id": "new_123"}'

    mock_transport = AsyncMock()
    mock_transport._raw_request = AsyncMock(return_value=mock_resp)
    # Pool connection for identity map
    pool.connection = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    row = {"id": "789", "name": "Charlie"}

    await engine._dispatch_row(
        mock_transport, mock_connector, writeback_cfg,
        "insert", "789", row, MagicMock(), result,
    )

    mock_transport._raw_request.assert_called_once()
    assert result.processed == 1
    assert result.dry_run_log == []


@pytest.mark.asyncio
async def test_dry_run_delete_logged_correctly():
    """In dry_run=True, delete action is logged with DELETE method."""
    from inandout.writeback.engine import WritebackEngine, WritebackResult

    pool = MagicMock()
    engine = WritebackEngine(pool)

    mock_connector = MagicMock()
    mock_connector.name = "hubspot"
    mock_connector.connection.base_url = "https://api.hubspot.com"

    writeback_cfg = _make_writeback_cfg(dry_run=True)
    result = WritebackResult(connector="hubspot", datatype="contacts", delta_table="t")

    mock_transport = AsyncMock()
    row = {"id": "999"}

    await engine._dispatch_row(
        mock_transport, mock_connector, writeback_cfg,
        "delete", "999", row, MagicMock(), result,
    )

    assert len(result.dry_run_log) == 1
    assert result.dry_run_log[0]["method"] == "DELETE"
    assert result.dry_run_log[0]["action"] == "delete"
