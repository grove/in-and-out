"""Unit tests for T2 #23 — pre-write payload validation.

Source-inspection tests verify the engine validates payloads before dispatch.
Unit tests cover the _validate_payload_schema helper.
Functional tests verify the engine dead-letters on validation failure.
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
from inandout.writeback.engine import (
    WritebackEngine,
    WritebackResult,
    _validate_payload_schema,
)


# ---------------------------------------------------------------------------
# Source-inspection tests
# ---------------------------------------------------------------------------

def test_writeback_config_has_payload_schema() -> None:
    cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
        ),
        payload_schema={"required": ["name"]},
    )
    assert cfg.payload_schema == {"required": ["name"]}


def test_engine_dispatch_row_calls_validate_payload_schema_warning() -> None:
    source = inspect.getsource(WritebackEngine._dispatch_row)
    assert "writeback_payload_validation_failed" in source


def test_engine_dispatch_row_checks_pw_schema() -> None:
    source = inspect.getsource(WritebackEngine._dispatch_row)
    assert "payload_schema" in source
    assert "_validate_payload_schema" in source


def test_validate_payload_schema_is_module_level() -> None:
    import inandout.writeback.engine as eng_mod
    assert callable(getattr(eng_mod, "_validate_payload_schema", None))


# ---------------------------------------------------------------------------
# _validate_payload_schema unit tests
# ---------------------------------------------------------------------------

def test_validate_passes_when_no_schema() -> None:
    errors = _validate_payload_schema({"name": "Alice"}, {})
    assert errors == []


def test_validate_required_field_present() -> None:
    errors = _validate_payload_schema(
        {"name": "Alice", "email": "a@b.com"},
        {"required": ["name", "email"]},
    )
    assert errors == []


def test_validate_required_field_missing() -> None:
    errors = _validate_payload_schema(
        {"name": "Alice"},
        {"required": ["name", "email"]},
    )
    assert any("email" in e for e in errors)


def test_validate_correct_type_passes() -> None:
    errors = _validate_payload_schema(
        {"score": 42},
        {"properties": {"score": {"type": "integer"}}},
    )
    assert errors == []


def test_validate_wrong_type_reported() -> None:
    errors = _validate_payload_schema(
        {"score": "forty-two"},
        {"properties": {"score": {"type": "integer"}}},
    )
    assert any("score" in e for e in errors)
    assert any("integer" in e for e in errors)


def test_validate_additional_properties_false_rejects_extra() -> None:
    errors = _validate_payload_schema(
        {"name": "Alice", "extra_field": "x"},
        {
            "properties": {"name": {"type": "string"}},
            "additionalProperties": False,
        },
    )
    assert any("extra_field" in e for e in errors)


def test_validate_additional_properties_true_allows_extra() -> None:
    errors = _validate_payload_schema(
        {"name": "Alice", "extra_field": "x"},
        {
            "properties": {"name": {"type": "string"}},
            "additionalProperties": True,
        },
    )
    assert errors == []


def test_validate_null_type() -> None:
    errors = _validate_payload_schema(
        {"deleted_at": None},
        {"properties": {"deleted_at": {"type": "null"}}},
    )
    assert errors == []


def test_validate_combined_required_and_type() -> None:
    errors = _validate_payload_schema(
        {"name": 123},
        {
            "required": ["name", "email"],
            "properties": {"name": {"type": "string"}},
        },
    )
    # Missing email AND wrong type for name
    assert any("email" in e for e in errors)
    assert any("name" in e for e in errors)


# ---------------------------------------------------------------------------
# Functional test: engine dead-letters on validation failure
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_engine_dead_letters_row_on_payload_validation_failure() -> None:
    """When payload_schema validation fails, row is counted as failed, not dispatched."""
    engine = WritebackEngine.__new__(WritebackEngine)
    engine._pool = AsyncMock()

    cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/c/${external_id}"),
        ),
        payload_schema={"required": ["name", "email"]},
    )

    connector = MagicMock()
    connector.name = "test"
    connector.connection.base_url = "https://api.example.com"

    transport = AsyncMock()
    transport._raw_request = AsyncMock()
    transport._request = AsyncMock()

    result = WritebackResult(connector="test", datatype="contacts", delta_table="_delta")

    row = {"external_id": "c-1", "_action": "update", "name": "Alice"}  # missing email

    await engine._dispatch_row(
        transport, connector, cfg, "update", "c-1", row, MagicMock(), result
    )

    assert result.failed == 1
    assert result.processed == 0
    assert any("payload_validation" in e[2] for e in result._failed_entries)
    # No HTTP call made
    transport._raw_request.assert_not_called()
    transport._request.assert_not_called()
