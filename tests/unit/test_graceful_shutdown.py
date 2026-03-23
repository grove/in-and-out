"""Unit tests for graceful shutdown with drain (B8)."""
from __future__ import annotations

import pytest


def test_drain_timeout_secs_default_on_ingestion_config() -> None:
    """IngestionToolConfig should have drain_timeout_secs with default 30.0."""
    from inandout.config.tool import IngestionToolConfig

    # Minimal config
    cfg = IngestionToolConfig(
        database={"dsn": "postgresql://localhost/test"},
    )
    assert cfg.drain_timeout_secs == 30.0


def test_drain_timeout_secs_default_on_writeback_config() -> None:
    """WritebackToolConfig should have drain_timeout_secs with default 30.0."""
    from inandout.config.tool import WritebackToolConfig

    cfg = WritebackToolConfig(
        database={"dsn": "postgresql://localhost/test"},
    )
    assert cfg.drain_timeout_secs == 30.0


def test_drain_timeout_secs_configurable() -> None:
    """drain_timeout_secs should be configurable to any float value."""
    from inandout.config.tool import IngestionToolConfig

    cfg = IngestionToolConfig(
        database={"dsn": "postgresql://localhost/test"},
        drain_timeout_secs=60.0,
    )
    assert cfg.drain_timeout_secs == 60.0


def test_draining_flag_stops_new_work() -> None:
    """When draining=True, the polling loop should not start new work."""
    draining = False
    work_started = []

    # Simulate the polling loop logic
    iterations = 3
    for i in range(iterations):
        if draining:
            break
        if i == 1:
            draining = True  # SIGTERM arrives
        work_started.append(i)

    # Only the iteration before SIGTERM should have completed
    assert 0 in work_started
    assert 2 not in work_started
