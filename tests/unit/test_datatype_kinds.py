"""Unit tests for read-only datatype validation (T1 #23) and relationship datatypes (T1 #22)."""
from __future__ import annotations

import pytest

from inandout.config.connector import DatatypeConfig
from inandout.config.ingestion import IngestionConfig
from inandout.config.writeback import WritebackConfig, ProtectionLevel, ConflictResolution


def test_datatype_requires_ingestion_or_writeback():
    """Datatype must have at least one of ingestion or writeback."""
    with pytest.raises(ValueError, match="at least one of"):
        DatatypeConfig()


def test_datatype_can_be_ingestion_only():
    """Datatype can be ingestion-only (read-only)."""
    cfg = DatatypeConfig(
        kind="entity",
        ingestion=IngestionConfig(
            primary_key="id",
            history_mode="overwrite",
            schedule={"interval": "5m"},
            list={
                "method": "GET",
                "path": "/records",
                "record_selector": "items",
                "pagination": {"strategy": "offset", "offset": {"page_size": 100}},
            },
        ),
    )
    assert cfg.ingestion is not None
    assert cfg.writeback is None


def test_datatype_can_be_writeback_only():
    """Datatype can be writeback-only."""
    cfg = DatatypeConfig(
        writeback=WritebackConfig(
            protection_level=ProtectionLevel.none,
            conflict_resolution=ConflictResolution.last_writer_wins,
            supported_actions=["insert", "update"],
            operations={
                "lookup": {"method": "GET", "path": "/records/{id}"},
                "insert": {"method": "POST", "path": "/records"},
                "update": {"method": "PATCH", "path": "/records/{id}"},
            },
        ),
    )
    assert cfg.ingestion is None
    assert cfg.writeback is not None


def test_datatype_can_have_both_ingestion_and_writeback():
    """Datatype can have both ingestion and writeback (bidirectional)."""
    cfg = DatatypeConfig(
        kind="entity",
        ingestion=IngestionConfig(
            primary_key="id",
            history_mode="overwrite",
            schedule={"interval": "5m"},
            list={
                "method": "GET",
                "path": "/records",
                "record_selector": "items",
                "pagination": {"strategy": "offset", "offset": {"page_size": 100}},
            },
        ),
        writeback=WritebackConfig(
            protection_level=ProtectionLevel.none,
            conflict_resolution=ConflictResolution.last_writer_wins,
            supported_actions=["update"],
            operations={
                "lookup": {"method": "GET", "path": "/records/{id}"},
                "update": {"method": "PATCH", "path": "/records/{id}"},
            },
        ),
    )
    assert cfg.ingestion is not None
    assert cfg.writeback is not None


def test_relationship_datatype_can_be_configured():
    """Relationship datatypes should use kind='relationship'."""
    cfg = DatatypeConfig(
        kind="relationship",
        ingestion=IngestionConfig(
            primary_key="id",
            history_mode="append",  # Relationships often need append mode
            schedule={"interval": "10m"},
            list={
                "method": "GET",
                "path": "/memberships",
                "record_selector": "items",
                "pagination": {"strategy": "offset", "offset": {"page_size": 100}},
            },
        ),
        writeback=WritebackConfig(
            protection_level=ProtectionLevel.none,
            conflict_resolution=ConflictResolution.last_writer_wins,
            supported_actions=["insert", "delete"],  # Relationships typically need insert/delete
            operations={
                "lookup": {"method": "GET", "path": "/memberships/{id}"},
                "insert": {"method": "POST", "path": "/memberships"},
                "delete": {"method": "DELETE", "path": "/memberships/{id}"},
            },
        ),
    )
    assert cfg.kind == "relationship"
    assert cfg.ingestion is not None
    assert cfg.writeback is not None
