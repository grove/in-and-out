"""Unit tests for incremental writeback (diff_fields) — Step 42."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.config.writeback import (
    ConflictResolution,
    OperationConfig,
    OperationsConfig,
    ProtectionLevel,
    UpdateOperationConfig,
    WritebackConfig,
)
from inandout.writeback.engine import WritebackEngine, WritebackResult


def _make_writeback_cfg(diff_fields: bool = False) -> WritebackConfig:
    return WritebackConfig(
        protection_level=ProtectionLevel.fire_and_forget,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        diff_fields=diff_fields,
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/api/items/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/api/items/${external_id}"),
        ),
    )


def _make_connector(name: str = "test_connector") -> MagicMock:
    connector = MagicMock()
    connector.name = name
    return connector


@pytest.mark.anyio
async def test_diff_fields_no_changes_skipped():
    """When diff_fields=True and nothing changed, the row is skipped (no HTTP call)."""
    cfg = _make_writeback_cfg(diff_fields=True)
    connector = _make_connector()

    pool = AsyncMock()
    engine = WritebackEngine(pool)

    # Simulate the source table returning the same values as the row
    last_written = {"name": "Alice", "status": "active"}
    row = {"name": "Alice", "status": "active", "_action": "update"}
    result = WritebackResult(connector="test_connector", datatype="items", delta_table="_delta")

    # Mock pool.connection() to return the last_written dict
    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor.fetchone = AsyncMock(return_value=(last_written,))
    mock_conn.execute = AsyncMock(return_value=mock_cursor)
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    pool.connection = MagicMock(return_value=mock_conn)

    transport = AsyncMock()
    transport._request = AsyncMock()

    await engine._dispatch_row(
        transport, connector, cfg, "update", "item-1", row, MagicMock(), result
    )

    assert result.skipped == 1
    assert result.processed == 0
    transport._request.assert_not_called()


@pytest.mark.anyio
async def test_diff_fields_changed_fields_sends_diff():
    """When diff_fields=True and a field changed, only the diff is sent."""
    cfg = _make_writeback_cfg(diff_fields=True)
    connector = _make_connector()

    pool = AsyncMock()
    engine = WritebackEngine(pool)

    # status changed from "active" to "inactive"
    last_written = {"name": "Alice", "status": "active"}
    row = {"name": "Alice", "status": "inactive", "_action": "update"}
    result = WritebackResult(connector="test_connector", datatype="items", delta_table="_delta")

    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor.fetchone = AsyncMock(return_value=(last_written,))
    mock_conn.execute = AsyncMock(return_value=mock_cursor)
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.commit = AsyncMock()
    pool.connection = MagicMock(return_value=mock_conn)

    transport = AsyncMock()
    transport._request = AsyncMock()

    captured_payload = {}

    async def _capture_request(method, path, json=None, **kwargs):
        if json:
            captured_payload.update(json)

    transport._request = AsyncMock(side_effect=_capture_request)

    await engine._dispatch_row(
        transport, connector, cfg, "update", "item-1", row, MagicMock(), result
    )

    assert result.processed == 1
    # Only the changed field should be in the payload
    assert "status" in captured_payload
    assert captured_payload["status"] == "inactive"
    # Unchanged field should NOT be in the diff
    assert "name" not in captured_payload


@pytest.mark.anyio
async def test_diff_fields_false_sends_full_payload():
    """When diff_fields=False, the full payload is sent regardless of changes."""
    cfg = _make_writeback_cfg(diff_fields=False)
    connector = _make_connector()

    pool = AsyncMock()
    engine = WritebackEngine(pool)

    row = {"name": "Alice", "status": "active", "_action": "update"}
    result = WritebackResult(connector="test_connector", datatype="items", delta_table="_delta")

    transport = AsyncMock()
    captured_payload = {}

    async def _capture_request(method, path, json=None, **kwargs):
        if json:
            captured_payload.update(json)

    transport._request = AsyncMock(side_effect=_capture_request)

    await engine._dispatch_row(
        transport, connector, cfg, "update", "item-1", row, MagicMock(), result
    )

    assert result.processed == 1
    # Full payload: both fields sent
    assert "name" in captured_payload
    assert "status" in captured_payload
