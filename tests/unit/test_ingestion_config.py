"""Unit tests for IngestionConfig Pydantic model."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.ingestion import (
    HistoryMode,
    IngestionConfig,
    ScheduleConfig,
)


def _minimal_list_config() -> dict:
    return {"path": "/contacts", "pagination": {"strategy": "link_header"}}


def _minimal_ingestion(**overrides) -> dict:
    base = {
        "primary_key": "id",
        "history_mode": "overwrite",
        "schedule": {"interval": "30s"},
        "list": _minimal_list_config(),
    }
    base.update(overrides)
    return base


def test_minimal_ingestion_config():
    cfg = IngestionConfig(**_minimal_ingestion())
    assert cfg.primary_key == "id"
    assert cfg.history_mode == HistoryMode.overwrite


def test_history_mode_append():
    cfg = IngestionConfig(**_minimal_ingestion(history_mode="append"))
    assert cfg.history_mode == HistoryMode.append


def test_prune_orphan_columns_default_false():
    cfg = IngestionConfig(**_minimal_ingestion())
    assert cfg.prune_orphan_columns is False


def test_max_concurrent_fetches_default_one():
    cfg = IngestionConfig(**_minimal_ingestion())
    assert cfg.max_concurrent_fetches == 1


def test_bulk_upsert_batch_size_default_one():
    cfg = IngestionConfig(**_minimal_ingestion())
    assert cfg.bulk_upsert_batch_size == 1


def test_verify_deletion_default_true():
    cfg = IngestionConfig(**_minimal_ingestion())
    assert cfg.verify_deletion is True


def test_checkpoint_every_n_pages_default_zero():
    cfg = IngestionConfig(**_minimal_ingestion())
    assert cfg.checkpoint_every_n_pages == 0


def test_webhook_events_default_none():
    cfg = IngestionConfig(**_minimal_ingestion())
    assert cfg.webhook_events is None


def test_primary_key_list():
    cfg = IngestionConfig(**_minimal_ingestion(primary_key=["account_id", "contact_id"]))
    assert cfg.primary_key == ["account_id", "contact_id"]


def test_missing_primary_key_raises():
    data = _minimal_ingestion()
    del data["primary_key"]
    with pytest.raises(ValidationError):
        IngestionConfig(**data)


def test_missing_history_mode_raises():
    data = _minimal_ingestion()
    del data["history_mode"]
    with pytest.raises(ValidationError):
        IngestionConfig(**data)


def test_schedule_interval():
    cfg = IngestionConfig(**_minimal_ingestion(schedule={"interval": "5m"}))
    assert cfg.schedule.interval == "5m"


def test_schedule_cron():
    cfg = IngestionConfig(**_minimal_ingestion(schedule={"cron": "0 * * * *"}))
    assert cfg.schedule.cron == "0 * * * *"


def test_schedule_requires_interval_or_cron():
    with pytest.raises(ValidationError):
        IngestionConfig(**_minimal_ingestion(schedule={}))
