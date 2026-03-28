"""Unit tests for ScheduleConfig Pydantic model."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.ingestion import ScheduleConfig


def test_interval_only_valid():
    cfg = ScheduleConfig(interval="30s")
    assert cfg.interval == "30s"
    assert cfg.cron is None


def test_cron_only_valid():
    cfg = ScheduleConfig(cron="0 * * * *")
    assert cfg.cron == "0 * * * *"
    assert cfg.interval is None


def test_both_interval_and_cron_allowed():
    # The validator only checks that at least one is set; both is valid
    cfg = ScheduleConfig(interval="1h", cron="0 * * * *")
    assert cfg.interval == "1h"
    assert cfg.cron == "0 * * * *"


def test_neither_interval_nor_cron_raises():
    with pytest.raises(ValidationError, match="interval.*cron|cron.*interval"):
        ScheduleConfig()


def test_extra_field_forbidden():
    with pytest.raises(ValidationError):
        ScheduleConfig(interval="30s", unknown="bad")


def test_max_lag_seconds_default_none():
    cfg = ScheduleConfig(interval="30s")
    assert cfg.max_lag_seconds is None


def test_max_lag_seconds_set():
    cfg = ScheduleConfig(interval="30s", max_lag_seconds=120)
    assert cfg.max_lag_seconds == 120


def test_interval_various_units():
    for val in ["10s", "5m", "2h", "1d"]:
        cfg = ScheduleConfig(interval=val)
        assert cfg.interval == val


def test_cron_hourly():
    cfg = ScheduleConfig(cron="0 * * * *")
    assert cfg.cron == "0 * * * *"


def test_round_trip_json():
    cfg = ScheduleConfig(interval="30s", max_lag_seconds=60)
    loaded = ScheduleConfig.model_validate_json(cfg.model_dump_json())
    assert loaded.interval == "30s"
    assert loaded.max_lag_seconds == 60
