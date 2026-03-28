"""Unit tests for RetryBudgetConfig."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.connector import RetryBudgetConfig


def test_defaults():
    cfg = RetryBudgetConfig()
    assert cfg.max_attempts == 1000
    assert cfg.window_secs == 3600.0


def test_custom_max_attempts():
    cfg = RetryBudgetConfig(max_attempts=50)
    assert cfg.max_attempts == 50


def test_custom_window_secs():
    cfg = RetryBudgetConfig(window_secs=7200.0)
    assert cfg.window_secs == 7200.0


def test_both_custom():
    cfg = RetryBudgetConfig(max_attempts=100, window_secs=1800.0)
    assert cfg.max_attempts == 100
    assert cfg.window_secs == 1800.0


def test_extra_field_forbidden():
    with pytest.raises(ValidationError):
        RetryBudgetConfig(unknown="bad")


def test_max_attempts_is_int():
    cfg = RetryBudgetConfig()
    assert isinstance(cfg.max_attempts, int)


def test_window_secs_is_float():
    cfg = RetryBudgetConfig()
    assert isinstance(cfg.window_secs, float)


def test_round_trip_json():
    cfg = RetryBudgetConfig(max_attempts=200, window_secs=900.0)
    loaded = RetryBudgetConfig.model_validate_json(cfg.model_dump_json())
    assert loaded.max_attempts == 200
    assert loaded.window_secs == 900.0


def test_max_attempts_one():
    cfg = RetryBudgetConfig(max_attempts=1)
    assert cfg.max_attempts == 1


def test_window_secs_small():
    cfg = RetryBudgetConfig(window_secs=60.0)
    assert cfg.window_secs == 60.0
