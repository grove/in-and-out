"""Unit tests for WritebackConfig and related models."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.writeback import (
    ConflictResolution,
    OperationConfig,
    OperationsConfig,
    ProtectionLevel,
    WritebackConfig,
)


def _minimal_operations() -> OperationsConfig:
    return OperationsConfig(
        lookup=OperationConfig(method="GET", path="/contacts/${external_id}"),
    )


def _minimal_writeback(**overrides) -> dict:
    base = {
        "protection_level": 0,
        "conflict_resolution": "last_writer_wins",
        "supported_actions": ["insert", "update"],
        "operations": _minimal_operations(),
    }
    base.update(overrides)
    return base


# --- ProtectionLevel enum ---

def test_protection_level_none():
    assert ProtectionLevel.none == 0


def test_protection_level_conditional_write_required():
    assert ProtectionLevel.conditional_write_required == 1


def test_protection_level_optimistic():
    assert ProtectionLevel.optimistic == 2


def test_protection_level_post_write_verify():
    assert ProtectionLevel.post_write_verify == 3


# --- ConflictResolution enum ---

def test_conflict_resolution_valid_values():
    vals = {cr.value for cr in ConflictResolution}
    assert "dead_letter" in vals
    assert "last_writer_wins" in vals
    assert "skip_and_warn" in vals


# --- WritebackConfig ---

def test_minimal_valid():
    cfg = WritebackConfig(**_minimal_writeback())
    assert cfg.protection_level == ProtectionLevel.none


def test_dry_run_default_false():
    cfg = WritebackConfig(**_minimal_writeback())
    assert cfg.dry_run is False


def test_max_concurrent_writes_default_ten():
    cfg = WritebackConfig(**_minimal_writeback())
    assert cfg.max_concurrent_writes == 10


def test_batch_size_default_fifty():
    cfg = WritebackConfig(**_minimal_writeback())
    assert cfg.batch_size == 50


def test_dependencies_default_empty():
    cfg = WritebackConfig(**_minimal_writeback())
    assert cfg.dependencies == []


def test_write_dependencies_default_empty():
    cfg = WritebackConfig(**_minimal_writeback())
    assert cfg.write_dependencies == []


def test_supported_actions_stored():
    cfg = WritebackConfig(**_minimal_writeback(supported_actions=["insert"]))
    assert "insert" in cfg.supported_actions


def test_supported_actions_min_length_one():
    with pytest.raises(ValidationError):
        WritebackConfig(**_minimal_writeback(supported_actions=[]))


def test_missing_protected_level_raises():
    data = _minimal_writeback()
    del data["protection_level"]
    with pytest.raises(ValidationError):
        WritebackConfig(**data)


def test_missing_conflict_resolution_raises():
    data = _minimal_writeback()
    del data["conflict_resolution"]
    with pytest.raises(ValidationError):
        WritebackConfig(**data)


def test_operations_stored():
    cfg = WritebackConfig(**_minimal_writeback())
    assert cfg.operations.lookup.path == "/contacts/${external_id}"


def test_dry_run_true():
    cfg = WritebackConfig(**_minimal_writeback(dry_run=True))
    assert cfg.dry_run is True


def test_custom_max_concurrent_writes():
    cfg = WritebackConfig(**_minimal_writeback(max_concurrent_writes=3))
    assert cfg.max_concurrent_writes == 3


def test_max_concurrent_writes_minimum_one():
    with pytest.raises(ValidationError):
        WritebackConfig(**_minimal_writeback(max_concurrent_writes=0))
