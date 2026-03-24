"""Unit tests for T2 #17 — pre-write data transformation.

Source-inspection tests verify the engine applies field mappings before dispatch.
Functional tests verify field rename / cast / strict-mode behaviour.
"""
from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest

from inandout.config.field_mapping import FieldMapping
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

def test_writeback_config_has_field_mappings() -> None:
    cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
            update=UpdateOperationConfig(method="PATCH", path="/c/${external_id}"),
        ),
    )
    assert hasattr(cfg, "field_mappings")
    assert isinstance(cfg.field_mappings, list)


def test_writeback_config_has_field_mappings_strict() -> None:
    cfg = WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/c/${external_id}"),
        ),
        field_mappings_strict=True,
    )
    assert cfg.field_mappings_strict is True


def test_engine_dispatch_row_calls_apply_writeback_transforms() -> None:
    source = inspect.getsource(WritebackEngine._dispatch_row)
    assert "_apply_writeback_transforms" in source


def test_apply_writeback_transforms_is_module_level() -> None:
    import inandout.writeback.engine as eng_mod
    assert callable(getattr(eng_mod, "_apply_writeback_transforms", None))


# ---------------------------------------------------------------------------
# Unit tests for _apply_writeback_transforms helper
# ---------------------------------------------------------------------------

def _minimal_cfg(**kwargs) -> WritebackConfig:
    return WritebackConfig(
        protection_level=ProtectionLevel.none,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/x"),
        ),
        **kwargs,
    )


def test_transform_no_mappings_returns_payload_unchanged() -> None:
    cfg = _minimal_cfg()
    payload = {"name": "Alice", "score": 10}
    row = {"name": "Alice", "score": 10}
    result = _apply_writeback_transforms(payload, row, cfg)
    assert result == {"name": "Alice", "score": 10}


def test_transform_renames_field() -> None:
    cfg = _minimal_cfg(
        field_mappings=[FieldMapping(source="name", target="full_name")],
    )
    payload = {"name": "Alice", "score": 10}
    row = {"name": "Alice", "score": 10}
    result = _apply_writeback_transforms(payload, row, cfg)
    assert "full_name" in result
    assert result["full_name"] == "Alice"
    # source field also present (non-strict)
    assert "score" in result


def test_transform_strict_drops_unmapped_fields() -> None:
    cfg = _minimal_cfg(
        field_mappings=[FieldMapping(source="name", target="full_name")],
        field_mappings_strict=True,
    )
    payload = {"name": "Alice", "score": 10}
    row = {"name": "Alice", "score": 10}
    result = _apply_writeback_transforms(payload, row, cfg)
    assert result == {"full_name": "Alice"}
    assert "score" not in result


def test_transform_casts_field() -> None:
    cfg = _minimal_cfg(
        field_mappings=[FieldMapping(source="score", target="score_str", cast="str")],
    )
    payload = {"score": 42}
    row = {"score": 42}
    result = _apply_writeback_transforms(payload, row, cfg)
    assert result["score_str"] == "42"
    assert isinstance(result["score_str"], str)


def test_transform_applies_default_when_source_missing() -> None:
    cfg = _minimal_cfg(
        field_mappings=[FieldMapping(source="missing_field", target="status", default="active")],
    )
    payload = {"name": "Bob"}
    row = {"name": "Bob"}
    result = _apply_writeback_transforms(payload, row, cfg)
    assert result["status"] == "active"
