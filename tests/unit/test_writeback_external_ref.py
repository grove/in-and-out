"""Unit tests for T2 #16 — external reference field writeback.

Source-inspection tests verify the engine injects cluster_id into the payload.
Functional tests verify the field appears in the dispatched HTTP payload.
"""
from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest

from inandout.config.writeback import (
    ConflictResolution,
    OperationConfig,
    OperationsConfig,
    ProtectionLevel,
    UpdateOperationConfig,
    WritebackConfig,
)
from inandout.writeback.engine import WritebackEngine, WritebackResult, _apply_writeback_transforms


# ---------------------------------------------------------------------------
# Source-inspection tests
# ---------------------------------------------------------------------------

def test_writeback_config_has_external_reference_field() -> None:
    cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
            insert=OperationConfig(method="POST", path="/contacts"),
        ),
        external_reference_field="mdm_id",
    )
    assert cfg.external_reference_field == "mdm_id"


def test_engine_applies_external_reference_via_transform_helper() -> None:
    source = inspect.getsource(WritebackEngine._dispatch_row)
    assert "_apply_writeback_transforms" in source


def test_apply_writeback_transforms_injects_cluster_id() -> None:
    cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
        ),
        external_reference_field="origin_id",
    )
    payload = {"name": "Alice"}
    row = {"name": "Alice", "_cluster_id": "cluster-abc-123"}
    result = _apply_writeback_transforms(payload, row, cfg)
    assert result["origin_id"] == "cluster-abc-123"


def test_apply_writeback_transforms_no_injection_when_field_not_configured() -> None:
    cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
        ),
    )
    payload = {"name": "Alice"}
    row = {"name": "Alice", "_cluster_id": "cluster-abc-123"}
    result = _apply_writeback_transforms(payload, row, cfg)
    # No extra field injected
    assert "origin_id" not in result
    assert result == {"name": "Alice"}


def test_apply_writeback_transforms_skips_injection_when_no_cluster_id() -> None:
    cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
        ),
        external_reference_field="origin_id",
    )
    payload = {"name": "Alice"}
    row = {"name": "Alice"}  # no cluster_id at all
    result = _apply_writeback_transforms(payload, row, cfg)
    # Field not injected when cluster_id is absent
    assert "origin_id" not in result


def test_apply_writeback_transforms_accepts_non_underscore_cluster_id() -> None:
    cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
        ),
        external_reference_field="mdm_ref",
    )
    payload = {"name": "Bob"}
    row = {"name": "Bob", "cluster_id": "cluster-xyz-789"}
    result = _apply_writeback_transforms(payload, row, cfg)
    assert result["mdm_ref"] == "cluster-xyz-789"


# ---------------------------------------------------------------------------
# Functional test: cluster_id appears in HTTP payload
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_external_reference_field_in_insert_payload() -> None:
    """When external_reference_field is set, cluster_id reaches the HTTP payload."""
    sent_payloads: list[dict] = []
    engine = WritebackEngine.__new__(WritebackEngine)
    engine._pool = MagicMock()
    # mock pool.connection() context manager
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
        supported_actions=["insert"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
            insert=OperationConfig(method="POST", path="/contacts"),
        ),
        external_reference_field="mdm_id",
    )

    connector = MagicMock()
    connector.name = "test"
    connector.connection.base_url = "https://api.example.com"

    insert_response = MagicMock()
    insert_response.status_code = 201
    insert_response.is_success = True
    insert_response.content = b'{"id": "new-123"}'
    insert_response.headers = {}
    insert_response.raise_for_status = MagicMock()

    async def _raw_request(method: str, path: str, **kwargs):
        if method == "POST":
            sent_payloads.append(kwargs.get("json", {}))
        return insert_response

    transport = AsyncMock()
    transport._raw_request = AsyncMock(side_effect=_raw_request)

    result = WritebackResult(connector="test", datatype="contacts", delta_table="_delta")

    row = {
        "external_id": None,
        "_action": "insert",
        "_cluster_id": "cluster-abc-999",
        "name": "Alice",
        "email": "alice@example.com",
    }

    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(engine, "_record_identity_map", AsyncMock())
        mp.setattr(engine, "_get_last_written", AsyncMock(return_value={}))

        await engine._dispatch_row(
            transport, connector, cfg, "insert", None, row, MagicMock(), result
        )

    assert len(sent_payloads) == 1, f"Expected 1 POST, got {len(sent_payloads)}"
    assert sent_payloads[0].get("mdm_id") == "cluster-abc-999", (
        f"Expected mdm_id=cluster-abc-999 in payload; got: {sent_payloads[0]}"
    )
