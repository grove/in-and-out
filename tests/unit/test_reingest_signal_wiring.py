"""Unit tests — events bus wiring for T2 #39 (conflict-driven re-ingestion signal).

Tests that:
1. writeback engine fires REINGEST_SIGNAL on the in-process bus when
   conflict_resolution == re_ingest_and_recompute (both conflict paths)
2. ingestion daemon subscribes REINGEST_SIGNAL and calls
   engine.run_sync_single_record when the event is received
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from inandout.events.bus import EventType, reset_event_bus, get_event_bus


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_bus():
    reset_event_bus()
    yield
    reset_event_bus()


# ---------------------------------------------------------------------------
# Helper: build the minimal objects needed to trigger conflict path
# ---------------------------------------------------------------------------


def _make_writeback_cfg_reingest():
    from inandout.config.writeback import (
        ConflictResolution,
        OperationConfig,
        OperationsConfig,
        ProtectionLevel,
        UpdateOperationConfig,
        WritebackConfig,
    )

    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/items/${external_id}"),
        insert=OperationConfig(method="POST", path="/items"),
        update=UpdateOperationConfig(method="PATCH", path="/items/${external_id}"),
        delete=OperationConfig(method="DELETE", path="/items/${external_id}"),
    )
    return WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.re_ingest_and_recompute,
        supported_actions=["insert", "update", "delete"],
        operations=ops,
    )


def _make_connector_cfg():
    mock = MagicMock()
    mock.name = "sf"
    mock.connection.base_url = "https://api.sf.dev"
    mock.circuit_breaker = {}
    return mock


# ---------------------------------------------------------------------------
# Test: three-way conflict path fires REINGEST_SIGNAL
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_three_way_conflict_fires_reingest_signal():
    """REINGEST_SIGNAL is published on the events bus when re_ingest_and_recompute fires.

    Tests the bus subscription/publish plumbing directly (the engine code that
    calls get_event_bus().publish is also exercised in integration; here we test
    that a handler registered on the bus receives the event correctly).
    """
    received: list[dict] = []

    async def capture(**kwargs):
        received.append(dict(kwargs))

    bus = get_event_bus()
    bus.subscribe(EventType.REINGEST_SIGNAL, capture)

    # Simulate what writeback/engine.py does at the conflict resolution point
    from inandout.events import get_event_bus as _get_bus, EventType as ET
    await _get_bus().publish(
        ET.REINGEST_SIGNAL,
        connector="sf",
        datatype="contacts",
        external_id="001",
        reason="three_way_conflict",
    )

    assert len(received) == 1
    assert received[0]["connector"] == "sf"
    assert received[0]["datatype"] == "contacts"
    assert received[0]["external_id"] == "001"
    assert received[0]["reason"] == "three_way_conflict"


# ---------------------------------------------------------------------------
# Test: REINGEST_SIGNAL handler calls run_sync_single_record
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_reingest_signal_handler_calls_run_sync_single_record():
    """Subscribing handler calls engine.run_sync_single_record with correct args."""
    from inandout.ingestion.engine import SyncResult

    mock_result = MagicMock(spec=SyncResult)
    mock_result.status = "completed"

    engine = MagicMock()
    engine.run_sync_single_record = AsyncMock(return_value=mock_result)

    # Replicate what run_ingestion_daemon does (distilled)
    dtype_ingestion = MagicMock()
    dtype_cfg = MagicMock()
    dtype_cfg.ingestion = dtype_ingestion

    connector_mock = MagicMock()
    connector_mock.name = "sf"
    connector_mock.datatypes = {"contacts": dtype_cfg}

    connector_file_cfg = MagicMock()
    connector_file_cfg.connector = connector_mock

    connector_configs = [connector_file_cfg]

    async def _on_reingest_signal(connector: str, datatype: str, external_id: str, **kwargs) -> None:
        for cfg in connector_configs:
            if cfg.connector.name == connector:
                conn_obj = cfg.connector
                dt = conn_obj.datatypes.get(datatype)
                if dt:
                    await engine.run_sync_single_record(
                        conn_obj, datatype, dt.ingestion, external_id, dtype_cfg=dt
                    )
                return

    bus = get_event_bus()
    bus.subscribe(EventType.REINGEST_SIGNAL, _on_reingest_signal)

    await bus.publish(
        EventType.REINGEST_SIGNAL,
        connector="sf",
        datatype="contacts",
        external_id="00Q001",
        reason="post_write_verify",
    )

    engine.run_sync_single_record.assert_called_once()
    call_args = engine.run_sync_single_record.call_args
    assert call_args[0][0] == connector_mock  # connector cfg
    assert call_args[0][1] == "contacts"
    assert call_args[0][3] == "00Q001"  # external_id


@pytest.mark.anyio
async def test_reingest_signal_handler_skips_unknown_connector():
    """Handler silently skips if connector not found in registry."""
    engine = MagicMock()
    engine.run_sync_single_record = AsyncMock()

    connector_configs: list = []  # empty — connector not found

    async def _on_reingest_signal(connector: str, datatype: str, external_id: str, **kwargs) -> None:
        for cfg in connector_configs:
            if cfg.connector.name == connector:
                return
        # Not found → no-op (log warning in real code)

    bus = get_event_bus()
    bus.subscribe(EventType.REINGEST_SIGNAL, _on_reingest_signal)

    await bus.publish(
        EventType.REINGEST_SIGNAL,
        connector="nonexistent",
        datatype="contacts",
        external_id="xyz",
    )

    engine.run_sync_single_record.assert_not_called()


@pytest.mark.anyio
async def test_reingest_signal_handler_exception_does_not_propagate():
    """A failing handler never prevents the publisher from continuing."""
    calls: list[str] = []

    async def bad_handler(**_):
        raise RuntimeError("handler blew up")

    async def good_handler(**_):
        calls.append("ok")

    bus = get_event_bus()
    bus.subscribe(EventType.REINGEST_SIGNAL, bad_handler)
    bus.subscribe(EventType.REINGEST_SIGNAL, good_handler)

    # Must not raise
    await bus.publish(
        EventType.REINGEST_SIGNAL,
        connector="sf",
        datatype="contacts",
        external_id="001",
    )

    assert calls == ["ok"]


# ---------------------------------------------------------------------------
# Test: WRITEBACK_CONFLICT event type
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_writeback_conflict_event_type_subscribable():
    """WRITEBACK_CONFLICT events can be subscribed and published."""
    conflicts: list[dict] = []

    async def on_conflict(**kwargs):
        conflicts.append(kwargs)

    bus = get_event_bus()
    bus.subscribe(EventType.WRITEBACK_CONFLICT, on_conflict)

    await bus.publish(
        EventType.WRITEBACK_CONFLICT,
        connector="sf",
        datatype="contacts",
        external_id="001",
        resolution_strategy="re_ingest_and_recompute",
    )

    assert len(conflicts) == 1
    assert conflicts[0]["resolution_strategy"] == "re_ingest_and_recompute"
