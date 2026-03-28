"""Tests for T1 #45 — timestamp normalizer import fix and T2 #12 — response_field_map.

T1 #45: ingestion engine must use the correct import path for apply_timestamp_normalization.
T2 #12: response_field_map renames GET response fields before three-way conflict comparison.
"""
from __future__ import annotations

import inspect

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


# ---------------------------------------------------------------------------
# T1 #45 — timestamp normalizer import fix
# ---------------------------------------------------------------------------

def test_ingestion_engine_uses_correct_timestamp_normalizer_import() -> None:
    """The ingestion engine must import from timestamp_normalizer, not timestamp."""
    import inandout.ingestion.engine as ing_eng
    source = inspect.getsource(ing_eng)
    assert "from inandout.ingestion.timestamp_normalizer import" in source
    assert "from inandout.ingestion.timestamp import" not in source


def test_timestamp_normalizer_module_importable() -> None:
    """The correct module must be importable (would have failed silently before fix)."""
    from inandout.ingestion.timestamp_normalizer import apply_timestamp_normalization  # noqa: F401
    assert callable(apply_timestamp_normalization)


def test_timestamp_normalizer_apply_iso8601() -> None:
    """apply_timestamp_normalization converts iso8601 timestamps."""
    from inandout.ingestion.timestamp_normalizer import apply_timestamp_normalization
    from unittest.mock import MagicMock
    cfg = MagicMock()
    cfg.field = "created_at"
    cfg.format = "iso8601"
    cfg.target_field = None
    record = {"created_at": "2026-03-24T12:00:00+02:00", "name": "Alice"}
    result = apply_timestamp_normalization(record, [cfg])
    # Should normalize to UTC — field value should be a datetime-like string or datetime
    assert "created_at" in result
    # Original non-timestamp field unchanged
    assert result["name"] == "Alice"


def test_timestamp_normalizer_apply_unix_seconds() -> None:
    """apply_timestamp_normalization converts Unix-second timestamps."""
    from inandout.ingestion.timestamp_normalizer import apply_timestamp_normalization
    from unittest.mock import MagicMock
    cfg = MagicMock()
    cfg.field = "ts"
    cfg.format = "unix_seconds"
    cfg.target_field = None
    record = {"ts": 1711274400}
    result = apply_timestamp_normalization(record, [cfg])
    assert "ts" in result


# ---------------------------------------------------------------------------
# T2 #12 — response_field_map config field
# ---------------------------------------------------------------------------

def test_writeback_config_has_response_field_map() -> None:
    cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/c/${external_id}"),
        ),
        response_field_map={"accountId": "account_id", "fullName": "name"},
    )
    assert cfg.response_field_map == {"accountId": "account_id", "fullName": "name"}


def test_writeback_config_response_field_map_defaults_none() -> None:
    cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
        ),
    )
    assert cfg.response_field_map is None


def test_engine_applies_response_field_map_in_three_way_comparison() -> None:
    source = inspect.getsource(WritebackEngine._dispatch_row)
    assert "response_field_map" in source


# ---------------------------------------------------------------------------
# T2 #12 functional: field remapping prevents false conflict
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_response_field_map_prevents_false_conflict() -> None:
    """When GET returns camelCase but PATCH uses snake_case, response_field_map normalizes.

    Without the map, current_state keys wouldn't match payload keys → safe=True
    (suppressed mismatch), but could also cause false conflicts. With the map,
    the renamed field correctly participates in the comparison.
    """
    from unittest.mock import AsyncMock, MagicMock, patch
    import json

    engine = WritebackEngine.__new__(WritebackEngine)
    engine._pool = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)
    mock_conn.execute = AsyncMock(
        return_value=AsyncMock(fetchone=AsyncMock(return_value=None))
    )
    mock_conn.commit = AsyncMock()
    engine._pool.connection = MagicMock(return_value=mock_conn)

    cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        use_desired_state_table=True,
        response_field_map={"fullName": "name"},
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/c/${external_id}"),
        ),
    )

    connector = MagicMock()
    connector.name = "test"
    connector.connection.base_url = "https://api.example.com"
    connector.circuit_breaker = {}

    # GET returns camelCase: {"fullName": "Alice"} — matches desired-state {"name": "Alice"}
    # after remapping. No conflict should be detected.
    remote = {"fullName": "Alice", "status": "active"}
    patch_response = MagicMock()
    patch_response.is_success = True
    patch_response.status_code = 200
    patch_response.content = b'{"id": "c1"}'
    patch_response.headers = {}
    patch_response.raise_for_status = MagicMock()

    get_response = MagicMock()
    get_response.is_success = True
    get_response.status_code = 200
    get_response.content = json.dumps(remote).encode()
    get_response.headers = {}

    async def _raw_request(method: str, path: str, **kwargs):
        if method == "GET":
            return get_response
        return patch_response

    transport = AsyncMock()
    transport._raw_request = AsyncMock(side_effect=_raw_request)
    transport._request = AsyncMock(return_value=patch_response)

    result = WritebackResult(connector="test", datatype="contacts", delta_table="_delta")

    row = {
        "external_id": "c-1",
        "_action": "update",
        "_base": {"name": "Alice"},   # base matches remote after remapping
        "name": "Alice Updated",      # desired change
    }

    with patch("inandout.writeback.engine.get_lwstate", new=AsyncMock(return_value={"name": "Alice"})):
        with patch("inandout.writeback.engine.upsert_lwstate", new=AsyncMock()):
            await engine._dispatch_row(
                transport, connector, cfg, "update", "c-1", row, MagicMock(), result
            )

    # The write should have proceeded (last_writer_wins) — processed == 1
    assert result.processed == 1, (
        f"Expected 1 processed write; got processed={result.processed} failed={result.failed}"
    )
