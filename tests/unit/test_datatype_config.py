"""Unit tests for DatatypeConfig Pydantic model."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.connector import DatatypeConfig
from inandout.config.ingestion import HistoryMode, IngestionConfig, ScheduleConfig
from inandout.config.writeback import (
    ConflictResolution,
    OperationConfig,
    OperationsConfig,
    ProtectionLevel,
    WritebackConfig,
)


def _minimal_list() -> dict:
    return {"path": "/items", "pagination": {"strategy": "link_header"}}


def _minimal_ingestion() -> IngestionConfig:
    return IngestionConfig(
        primary_key="id",
        history_mode=HistoryMode.overwrite,
        schedule=ScheduleConfig(interval="30s"),
        **{"list": _minimal_list()},
    )


def _minimal_writeback() -> WritebackConfig:
    return WritebackConfig(
        protection_level=0,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["insert"],
        operations=OperationsConfig(
            lookup=OperationConfig(method="GET", path="/items/${external_id}"),
        ),
    )


# --- ingestion_or_writeback_required validator ---

def test_neither_ingestion_nor_writeback_raises():
    with pytest.raises(ValidationError, match="ingestion.*writeback|writeback.*ingestion"):
        DatatypeConfig()


def test_ingestion_only_valid():
    cfg = DatatypeConfig(ingestion=_minimal_ingestion())
    assert cfg.ingestion is not None
    assert cfg.writeback is None


def test_writeback_only_valid():
    cfg = DatatypeConfig(writeback=_minimal_writeback())
    assert cfg.writeback is not None
    assert cfg.ingestion is None


def test_both_valid():
    cfg = DatatypeConfig(
        ingestion=_minimal_ingestion(),
        writeback=_minimal_writeback(),
    )
    assert cfg.ingestion is not None
    assert cfg.writeback is not None


# --- Default field values ---

def test_field_mappings_default_empty():
    cfg = DatatypeConfig(ingestion=_minimal_ingestion())
    assert cfg.field_mappings == []


def test_strict_field_mapping_default_false():
    cfg = DatatypeConfig(ingestion=_minimal_ingestion())
    assert cfg.strict_field_mapping is False


def test_pii_fields_default_empty():
    cfg = DatatypeConfig(ingestion=_minimal_ingestion())
    assert cfg.pii_fields == []


def test_linked_objects_default_empty():
    cfg = DatatypeConfig(ingestion=_minimal_ingestion())
    assert cfg.linked_objects == []


def test_timestamp_fields_default_empty():
    cfg = DatatypeConfig(ingestion=_minimal_ingestion())
    assert cfg.timestamp_fields == []


def test_description_default_none():
    cfg = DatatypeConfig(ingestion=_minimal_ingestion())
    assert cfg.description is None


def test_quality_rules_default_none():
    cfg = DatatypeConfig(ingestion=_minimal_ingestion())
    assert cfg.quality_rules is None


def test_max_concurrent_writes_default_none():
    cfg = DatatypeConfig(ingestion=_minimal_ingestion())
    assert cfg.max_concurrent_writes is None


def test_shared_table_default_none():
    cfg = DatatypeConfig(ingestion=_minimal_ingestion())
    assert cfg.shared_table is None


def test_api_version_default_none():
    cfg = DatatypeConfig(ingestion=_minimal_ingestion())
    assert cfg.api_version is None


# --- Custom values ---

def test_pii_fields_set():
    cfg = DatatypeConfig(
        ingestion=_minimal_ingestion(),
        pii_fields=["email", "phone"],
    )
    assert "email" in cfg.pii_fields


def test_strict_field_mapping_true():
    cfg = DatatypeConfig(ingestion=_minimal_ingestion(), strict_field_mapping=True)
    assert cfg.strict_field_mapping is True


def test_extra_field_forbidden():
    with pytest.raises(ValidationError):
        DatatypeConfig(ingestion=_minimal_ingestion(), unknown_field="bad")
