"""Unit tests for T2 #20 — archive action safety guard for superseded identities."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


def _make_engine():
    """Build a WritebackEngine with a mocked pool."""
    from inandout.writeback.engine import WritebackEngine
    pool = MagicMock()
    engine = WritebackEngine.__new__(WritebackEngine)
    engine._pool = pool
    engine._reingest_counters = {}
    return engine


# ---------------------------------------------------------------------------
# _is_source_record_deleted
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_is_source_record_deleted_returns_true_when_deleted():
    """Returns True when _deleted = TRUE in the source table."""
    engine = _make_engine()

    cursor_mock = AsyncMock()
    cursor_mock.fetchone = AsyncMock(return_value=(True,))
    conn_mock = AsyncMock()
    conn_mock.execute = AsyncMock(return_value=cursor_mock)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn_mock)
    ctx.__aexit__ = AsyncMock(return_value=None)
    engine._pool.connection = MagicMock(return_value=ctx)

    connector = MagicMock()
    connector.name = "crm"
    result = await engine._is_source_record_deleted(connector, "contacts", "ext-123")
    assert result is True


@pytest.mark.asyncio
async def test_is_source_record_deleted_returns_false_when_not_deleted():
    """Returns False when _deleted = FALSE in the source table."""
    engine = _make_engine()

    cursor_mock = AsyncMock()
    cursor_mock.fetchone = AsyncMock(return_value=(False,))
    conn_mock = AsyncMock()
    conn_mock.execute = AsyncMock(return_value=cursor_mock)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn_mock)
    ctx.__aexit__ = AsyncMock(return_value=None)
    engine._pool.connection = MagicMock(return_value=ctx)

    connector = MagicMock()
    connector.name = "crm"
    result = await engine._is_source_record_deleted(connector, "contacts", "ext-456")
    assert result is False


@pytest.mark.asyncio
async def test_is_source_record_deleted_returns_false_when_no_row():
    """Returns False when no row exists in the source table."""
    engine = _make_engine()

    cursor_mock = AsyncMock()
    cursor_mock.fetchone = AsyncMock(return_value=None)
    conn_mock = AsyncMock()
    conn_mock.execute = AsyncMock(return_value=cursor_mock)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn_mock)
    ctx.__aexit__ = AsyncMock(return_value=None)
    engine._pool.connection = MagicMock(return_value=ctx)

    connector = MagicMock()
    connector.name = "crm"
    result = await engine._is_source_record_deleted(connector, "contacts", "ext-789")
    assert result is False


@pytest.mark.asyncio
async def test_is_source_record_deleted_returns_false_on_exception():
    """Returns False (allow operation) when DB query fails."""
    engine = _make_engine()
    engine._pool = MagicMock()
    engine._pool.connection = MagicMock(side_effect=Exception("DB down"))

    connector = MagicMock()
    connector.name = "crm"
    result = await engine._is_source_record_deleted(connector, "contacts", "ext-error")
    assert result is False


# ---------------------------------------------------------------------------
# Archive action skips when superseded
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_archive_action_skipped_when_source_deleted():
    """_dispatch_single_row skips archive and increments skipped when source is deleted."""
    from inandout.writeback.engine import WritebackEngine, WritebackResult
    from inandout.transport.circuit_breaker import reset_all
    reset_all()

    from inandout.config.writeback import (
        OperationConfig,
        OperationsConfig,
        ProtectionLevel,
        ConflictResolution,
        WritebackConfig,
    )
    from inandout.config.connector import ConnectorConfig

    # Connector config
    connector_cfg = MagicMock(spec=ConnectorConfig)
    connector_cfg.name = "crm"
    connector_cfg.connection = MagicMock()
    connector_cfg.connection.base_url = "https://api.example.com"
    connector_cfg.circuit_breaker = None

    # Writeback config with archive op
    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/contacts/${external_id}"),
        archive=OperationConfig(method="POST", path="/contacts/${external_id}/archive"),
    )
    writeback_cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["archive"],
        required_fields=[],
        operations=ops,
    )

    delta_row = {"external_id": "ext-deleted", "_action": "archive", "name": "Old Corp"}
    result = WritebackResult(connector="crm", datatype="contacts", delta_table="inout_wb_crm_contacts")

    pool = MagicMock()
    engine = WritebackEngine(pool)

    # Patch _is_source_record_deleted to return True (superseded)
    engine._is_source_record_deleted = AsyncMock(return_value=True)

    transport = AsyncMock()

    log = MagicMock()
    log.warning = MagicMock()
    log.info = MagicMock()

    await engine._dispatch_row(
        transport=transport,
        connector=connector_cfg,
        writeback_cfg=writeback_cfg,
        action="archive",
        external_id="ext-deleted",
        row=delta_row,
        log=log,
        result=result,
    )

    assert result.skipped == 1
    assert result.processed == 0
    # No HTTP call should have been made
    transport._request.assert_not_called()
    transport._raw_request.assert_not_called()
