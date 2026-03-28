"""Unit tests for OutOfOrderConfig and OutOfOrderStrategy."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.ingestion import OutOfOrderConfig, OutOfOrderStrategy


# --- OutOfOrderStrategy enum ---

def test_strategy_accept_latest_timestamp():
    assert OutOfOrderStrategy.accept_latest_timestamp == "accept_latest_timestamp"


def test_strategy_accept_highest_sequence():
    assert OutOfOrderStrategy.accept_highest_sequence == "accept_highest_sequence"


def test_strategy_ignore():
    assert OutOfOrderStrategy.ignore == "ignore"


def test_invalid_strategy_raises():
    with pytest.raises(ValueError):
        OutOfOrderStrategy("unknown_strategy")


# --- OutOfOrderConfig model ---

def test_defaults():
    cfg = OutOfOrderConfig()
    assert cfg.strategy == OutOfOrderStrategy.accept_latest_timestamp
    assert cfg.timestamp_field == "updated_at"
    assert cfg.sequence_field is None


def test_custom_strategy():
    cfg = OutOfOrderConfig(strategy=OutOfOrderStrategy.accept_highest_sequence)
    assert cfg.strategy == OutOfOrderStrategy.accept_highest_sequence


def test_custom_timestamp_field():
    cfg = OutOfOrderConfig(timestamp_field="modified_at")
    assert cfg.timestamp_field == "modified_at"


def test_custom_sequence_field():
    cfg = OutOfOrderConfig(sequence_field="version")
    assert cfg.sequence_field == "version"


def test_ignore_strategy():
    cfg = OutOfOrderConfig(strategy=OutOfOrderStrategy.ignore)
    assert cfg.strategy == OutOfOrderStrategy.ignore


def test_extra_field_forbidden():
    with pytest.raises(ValidationError):
        OutOfOrderConfig(unknown_field="bad")


def test_round_trip_json():
    cfg = OutOfOrderConfig(
        strategy=OutOfOrderStrategy.accept_highest_sequence,
        timestamp_field="ts",
        sequence_field="seq",
    )
    loaded = OutOfOrderConfig.model_validate_json(cfg.model_dump_json())
    assert loaded.strategy == OutOfOrderStrategy.accept_highest_sequence
    assert loaded.sequence_field == "seq"


def test_from_string_strategy():
    cfg = OutOfOrderConfig(strategy="ignore")
    assert cfg.strategy == OutOfOrderStrategy.ignore
