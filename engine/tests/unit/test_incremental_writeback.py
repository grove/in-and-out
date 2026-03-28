"""Unit tests for incremental writeback (diff_fields) and T2 #5 dual-purpose GET."""
from __future__ import annotations

import inspect
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
        protection_level=ProtectionLevel.none,
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


# ---------------------------------------------------------------------------
# T2 #5: Dual-purpose GET — single preflight serves conflict detection + diff
# ---------------------------------------------------------------------------

def _make_desired_state_wb_cfg(diff_fields: bool = True) -> WritebackConfig:
    """WritebackConfig with use_desired_state_table=True so preflight GET occurs."""
    return WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        diff_fields=diff_fields,
        use_desired_state_table=True,
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/contacts/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/contacts/${external_id}"),
        ),
    )


def test_engine_dispatch_row_has_preflight_remote_data_sentinel() -> None:
    """_dispatch_row must declare _preflight_remote_data sentinel (T2 #5)."""
    source = inspect.getsource(WritebackEngine._dispatch_row)
    assert "_preflight_remote_data" in source


def test_engine_diff_via_preflight_get_logged() -> None:
    """_dispatch_row must log writeback_diff_via_preflight_get when using preflight data."""
    source = inspect.getsource(WritebackEngine._dispatch_row)
    assert "writeback_diff_via_preflight_get" in source


def test_engine_diff_uses_preflight_data_when_available() -> None:
    """When _preflight_remote_data is set, diff must use it instead of DB _last_written."""
    source = inspect.getsource(WritebackEngine._dispatch_row)
    # The preflight branch must check _preflight_remote_data is not None
    assert "_preflight_remote_data is not None" in source


def test_engine_diff_falls_back_to_db_when_no_preflight() -> None:
    """When _preflight_remote_data is None, diff falls back to DB _last_written query."""
    source = inspect.getsource(WritebackEngine._dispatch_row)
    # The fallback branch must query _last_written from the source table
    assert "_last_written" in source
    assert "SELECT _last_written" in source


@pytest.mark.anyio
async def test_diff_uses_preflight_remote_data_instead_of_db() -> None:
    """
    T2 #5: When use_desired_state_table=True (preflight GET runs) AND diff_fields=True,
    the PATCH must only contain the field that changed compared to the remote state.
    Only one GET should be issued (single GET serves both purposes).
    """
    cfg = _make_desired_state_wb_cfg(diff_fields=True)
    connector = _make_connector()

    pool = AsyncMock()
    engine = WritebackEngine(pool)

    # Preflight GET returns current remote state: name/status unchanged, score changed
    remote_state = {"name": "Alice", "status": "active", "score": 99}
    # Row we want to write: only score changed
    row = {
        "name": "Alice",
        "status": "active",
        "score": 150,
        "_action": "update",
    }
    result = WritebackResult(connector="test_connector", datatype="contacts", delta_table="_delta")

    mock_conn = AsyncMock()

    async def _execute(sql: str, params=None):
        cur = AsyncMock()
        cur.fetchone = AsyncMock(return_value=None)
        return cur

    mock_conn.execute = AsyncMock(side_effect=_execute)
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)
    mock_conn.commit = AsyncMock()
    pool.connection = MagicMock(return_value=mock_conn)

    import json as _json

    get_call_count = [0]
    patch_captured: list[dict] = []
    patch_response = MagicMock()
    patch_response.is_success = True
    patch_response.status_code = 200
    patch_response.content = b'{"id": "c1"}'
    patch_response.headers = {}

    async def _raw_request(method: str, path: str, **kwargs):
        if method == "GET":
            get_call_count[0] += 1
            resp = MagicMock()
            resp.is_success = True
            resp.content = _json.dumps(remote_state).encode()
            resp.headers = {}
            resp.status_code = 200
            return resp
        if method == "PATCH":
            patch_captured.append(kwargs.get("json", {}))
            return patch_response
        return MagicMock()

    async def _request(method: str, path: str, **kwargs):
        if method == "PATCH":
            patch_captured.append(kwargs.get("json", {}))
        return patch_response

    transport = AsyncMock()
    transport._raw_request = AsyncMock(side_effect=_raw_request)
    transport._request = AsyncMock(side_effect=_request)

    await engine._dispatch_row(
        transport, connector, cfg, "update", "contact-1", row, MagicMock(), result
    )

    # Verify only 1 GET was issued (single GET serves both conflict detection and diff)
    assert get_call_count[0] == 1, (
        f"T2 #5: expected exactly 1 GET (single preflight); got {get_call_count[0]}"
    )

    # PATCH must only contain the changed field (score)
    assert len(patch_captured) == 1, (
        f"Expected exactly 1 PATCH; got {len(patch_captured)}"
    )
    sent = patch_captured[0]
    assert "score" in sent, f"Expected 'score' in diff payload; got: {sent}"
    assert sent["score"] == 150
    # Unchanged fields must NOT be in the diff
    assert "name" not in sent, f"'name' (unchanged) must not be in diff; got: {sent}"
    assert "status" not in sent, f"'status' (unchanged) must not be in diff; got: {sent}"


@pytest.mark.anyio
async def test_diff_skips_row_when_preflight_shows_no_changes() -> None:
    """
    T2 #5: When preflight GET shows record is already up-to-date, row must be skipped.
    """
    cfg = _make_desired_state_wb_cfg(diff_fields=True)
    connector = _make_connector()

    pool = AsyncMock()
    engine = WritebackEngine(pool)

    # Remote state exactly matches what we want to write → no diff → skip
    current_remote = {"name": "Bob", "status": "inactive"}
    row = {"name": "Bob", "status": "inactive", "_action": "update"}
    result = WritebackResult(connector="test_connector", datatype="contacts", delta_table="_delta")

    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(
        return_value=AsyncMock(fetchone=AsyncMock(return_value=None))
    )
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)
    mock_conn.commit = AsyncMock()
    pool.connection = MagicMock(return_value=mock_conn)

    transport = AsyncMock()
    preflight_resp = MagicMock()
    import json
    preflight_resp.is_success = True
    preflight_resp.content = json.dumps(current_remote).encode()
    preflight_resp.headers = {}
    preflight_resp.status_code = 200
    transport._raw_request = AsyncMock(return_value=preflight_resp)

    await engine._dispatch_row(
        transport, connector, cfg, "update", "contact-2", row, MagicMock(), result
    )

    # Nothing changed → row skipped; no PATCH issued
    assert result.skipped == 1, f"Expected skipped=1; got {result.skipped}"
    assert result.processed == 0, f"Expected processed=0; got {result.processed}"
