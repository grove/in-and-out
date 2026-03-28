"""Tests for T2 #29 (batch response engine wiring) and T2 #38 (post_write_verify).

T2 #29: engine uses parse_batch_response to classify per-record outcomes after HTTP write.
T2 #38: _post_write_verify fires a conflict signal when GET response diverges from sent payload.
"""
from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.config.writeback import (
    BatchResponseConfig,
    ConflictResolution,
    OperationConfig,
    OperationsConfig,
    ProtectionLevel,
    UpdateOperationConfig,
    WritebackConfig,
)
from inandout.writeback.engine import WritebackEngine, WritebackResult, _check_batch_response


# ---------------------------------------------------------------------------
# T2 #29 — _check_batch_response helper unit tests
# ---------------------------------------------------------------------------

def test_check_batch_response_no_config_returns_true() -> None:
    cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
        ),
    )
    result = WritebackResult(connector="t", datatype="d", delta_table="_delta")
    assert _check_batch_response(b'{}', "id-1", cfg, result, "insert") is True
    assert result.failed == 0


def test_check_batch_response_success_record_returns_true() -> None:
    import json
    cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
        ),
        batch_response=BatchResponseConfig(
            success_path="results",
            record_id_path="id",
            status_path="status",
            success_statuses=["ok"],
        ),
    )
    body = json.dumps({"results": [{"id": "id-1", "status": "ok"}]}).encode()
    result = WritebackResult(connector="t", datatype="d", delta_table="_delta")
    result.processed = 1
    assert _check_batch_response(body, "id-1", cfg, result, "insert") is True
    assert result.failed == 0


def test_check_batch_response_failed_record_returns_false() -> None:
    import json
    cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
        ),
        batch_response=BatchResponseConfig(
            success_path="results",
            record_id_path="id",
            status_path="status",
            success_statuses=["ok"],
            error_path="error",
        ),
    )
    body = json.dumps({
        "results": [{"id": "id-1", "status": "error", "error": "duplicate_key"}]
    }).encode()
    result = WritebackResult(connector="t", datatype="d", delta_table="_delta")
    result.processed = 1
    outcome = _check_batch_response(body, "id-1", cfg, result, "insert")
    assert outcome is False
    assert result.failed == 1
    assert result.processed == 0  # decremented
    assert any("batch_response" in e[2] for e in result._failed_entries)


def test_check_batch_response_record_not_in_response_assumes_success() -> None:
    import json
    cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
        ),
        batch_response=BatchResponseConfig(
            success_path="results",
            record_id_path="id",
            status_path="status",
            success_statuses=["ok"],
        ),
    )
    body = json.dumps({"results": [{"id": "other-id", "status": "ok"}]}).encode()
    result = WritebackResult(connector="t", datatype="d", delta_table="_delta")
    result.processed = 1
    assert _check_batch_response(body, "id-1", cfg, result, "insert") is True
    assert result.failed == 0


def test_engine_insert_path_calls_check_batch_response() -> None:
    source = inspect.getsource(WritebackEngine._dispatch_row)
    assert "_check_batch_response" in source
    assert "insert_resp.content" in source


def test_engine_update_path_calls_check_batch_response() -> None:
    source = inspect.getsource(WritebackEngine._dispatch_row)
    # Should reference _upd_resp_content variable
    assert "_upd_resp_content" in source


# ---------------------------------------------------------------------------
# T2 #38 — _post_write_verify tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_post_write_verify_passes_when_remote_matches_sent() -> None:
    """No conflict increment when GET confirms what was written."""
    engine = WritebackEngine.__new__(WritebackEngine)
    engine._pool = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)
    mock_conn.execute = AsyncMock()
    mock_conn.commit = AsyncMock()
    engine._pool.connection = MagicMock(return_value=mock_conn)

    cfg = WritebackConfig(
        protection_level=ProtectionLevel.post_write_verify,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/c/${external_id}"),
        ),
    )
    connector = MagicMock()
    connector.name = "test"

    sent = {"name": "Alice", "status": "active"}
    remote = {"name": "Alice", "status": "active", "extra_field": "ignored"}

    import json as _json
    verify_resp = MagicMock()
    verify_resp.is_success = True
    verify_resp.content = _json.dumps(remote).encode()
    verify_resp.headers = {}

    transport = AsyncMock()
    transport._raw_request = AsyncMock(return_value=verify_resp)

    result = WritebackResult(connector="test", datatype="contacts", delta_table="_delta")
    result.processed = 1

    await engine._post_write_verify(
        transport, connector, cfg,
        cfg.operations, "update", "c-1", sent, result,
    )

    # No conflict: processed unchanged, failed still 0
    assert result.failed == 0
    assert result.processed == 1


@pytest.mark.anyio
async def test_post_write_verify_increments_failed_on_dead_letter_resolution() -> None:
    """When remote diverges and resolution=dead_letter, result.failed increments."""
    engine = WritebackEngine.__new__(WritebackEngine)
    engine._pool = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)
    mock_conn.execute = AsyncMock()
    mock_conn.commit = AsyncMock()
    engine._pool.connection = MagicMock(return_value=mock_conn)

    cfg = WritebackConfig(
        protection_level=ProtectionLevel.post_write_verify,
        conflict_resolution=ConflictResolution.dead_letter,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/c/${external_id}"),
        ),
    )
    connector = MagicMock()
    connector.name = "test"

    sent = {"name": "Alice"}
    remote = {"name": "Bob"}  # diverged!

    import json as _json
    verify_resp = MagicMock()
    verify_resp.is_success = True
    verify_resp.content = _json.dumps(remote).encode()
    verify_resp.headers = {}

    transport = AsyncMock()
    transport._raw_request = AsyncMock(return_value=verify_resp)

    result = WritebackResult(connector="test", datatype="contacts", delta_table="_delta")
    result.processed = 1

    await engine._post_write_verify(
        transport, connector, cfg,
        cfg.operations, "update", "c-1", sent, result,
    )

    assert result.failed == 1
    assert result.processed == 0  # decremented


@pytest.mark.anyio
async def test_post_write_verify_fires_reingest_signal_on_recompute() -> None:
    """When resolution=re_ingest_and_recompute and remote diverges, in-process signal fires."""
    engine = WritebackEngine.__new__(WritebackEngine)
    engine._pool = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)
    mock_conn.execute = AsyncMock()
    mock_conn.commit = AsyncMock()
    engine._pool.connection = MagicMock(return_value=mock_conn)

    cfg = WritebackConfig(
        protection_level=ProtectionLevel.post_write_verify,
        conflict_resolution=ConflictResolution.re_ingest_and_recompute,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/c/${external_id}"),
        ),
    )
    connector = MagicMock()
    connector.name = "test"

    sent = {"name": "Alice"}
    remote = {"name": "Bob"}  # diverged

    import json as _json
    verify_resp = MagicMock()
    verify_resp.is_success = True
    verify_resp.content = _json.dumps(remote).encode()
    verify_resp.headers = {}

    transport = AsyncMock()
    transport._raw_request = AsyncMock(return_value=verify_resp)

    result = WritebackResult(connector="test", datatype="contacts", delta_table="_delta")
    result.processed = 1

    publish_calls: list = []

    class FakeBus:
        async def publish(self, event_type, **kwargs):
            publish_calls.append({"event_type": event_type, **kwargs})

    with patch("inandout.events.get_event_bus", return_value=FakeBus()):
        await engine._post_write_verify(
            transport, connector, cfg,
            cfg.operations, "update", "c-1", sent, result,
        )

    assert any(c.get("external_id") == "c-1" for c in publish_calls), (
        f"Expected reingest signal published; calls: {publish_calls}"
    )


@pytest.mark.anyio
async def test_post_write_verify_with_response_field_map() -> None:
    """T2 #12: response_field_map is applied before mismatch detection in post_write_verify."""
    engine = WritebackEngine.__new__(WritebackEngine)
    engine._pool = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)
    mock_conn.execute = AsyncMock()
    mock_conn.commit = AsyncMock()
    engine._pool.connection = MagicMock(return_value=mock_conn)

    cfg = WritebackConfig(
        protection_level=ProtectionLevel.post_write_verify,
        conflict_resolution=ConflictResolution.dead_letter,
        supported_actions=["update"],
        response_field_map={"fullName": "name"},
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/c/${external_id}"),
        ),
    )
    connector = MagicMock()
    connector.name = "test"

    sent = {"name": "Alice"}
    # GET returns camelCase — after remapping "fullName" → "name", it matches sent
    remote = {"fullName": "Alice"}

    import json as _json
    verify_resp = MagicMock()
    verify_resp.is_success = True
    verify_resp.content = _json.dumps(remote).encode()
    verify_resp.headers = {}

    transport = AsyncMock()
    transport._raw_request = AsyncMock(return_value=verify_resp)

    result = WritebackResult(connector="test", datatype="contacts", delta_table="_delta")
    result.processed = 1

    await engine._post_write_verify(
        transport, connector, cfg,
        cfg.operations, "update", "c-1", sent, result,
    )

    # After remapping, name matches — no conflict
    assert result.failed == 0, (
        f"Expected no conflict after response_field_map normalization; failed={result.failed}"
    )
