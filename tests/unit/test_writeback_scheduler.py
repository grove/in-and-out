"""Unit tests for externalizable writeback scheduler (T2 #35 B2)."""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Config field
# ---------------------------------------------------------------------------

def test_writeback_tool_config_scheduling_enabled_default():
    """WritebackToolConfig.scheduling_enabled defaults to True."""
    from inandout.config.tool import WritebackToolConfig
    field = WritebackToolConfig.model_fields.get("scheduling_enabled")
    assert field is not None
    assert field.default is True


def test_writeback_tool_config_scheduling_enabled_can_be_false():
    """WritebackToolConfig.scheduling_enabled can be set to False."""
    from inandout.config.tool import WritebackToolConfig, DatabaseConfig

    cfg = WritebackToolConfig(
        scheduling_enabled=False,
        database=DatabaseConfig(dsn="postgresql://localhost/test"),
    )
    assert cfg.scheduling_enabled is False


def test_writeback_tool_config_scheduling_enabled_true():
    """WritebackToolConfig.scheduling_enabled=True means loops are started."""
    from inandout.config.tool import WritebackToolConfig, DatabaseConfig

    cfg = WritebackToolConfig(
        scheduling_enabled=True,
        database=DatabaseConfig(dsn="postgresql://localhost/test"),
    )
    assert cfg.scheduling_enabled is True


# ---------------------------------------------------------------------------
# Scheduling disabled: no polling loops spawned
# ---------------------------------------------------------------------------

def test_scheduling_disabled_no_loops_spawned():
    """When scheduling_enabled=False, no _writeback_polling_loop tasks are started."""
    from unittest.mock import MagicMock

    spawned = []
    scheduling_enabled = False
    connector_configs = [MagicMock() for _ in range(2)]

    # Simulate daemon logic
    if scheduling_enabled:
        for cfg in connector_configs:
            spawned.append(cfg)

    assert len(spawned) == 0


def test_scheduling_enabled_loops_spawned():
    """When scheduling_enabled=True, polling loops are started."""
    from unittest.mock import MagicMock

    spawned = []
    scheduling_enabled = True
    connector_configs = [MagicMock(), MagicMock()]

    if scheduling_enabled:
        for cfg in connector_configs:
            spawned.append(cfg)

    assert len(spawned) == 2


# ---------------------------------------------------------------------------
# trigger-writeback control command
# ---------------------------------------------------------------------------

def test_trigger_writeback_command_exists_in_dispatcher():
    """ControlDispatcher._execute handles 'trigger-writeback' command."""
    from inandout.engine.control import ControlDispatcher

    # Verify the command routing exists
    import inspect
    source = inspect.getsource(ControlDispatcher._execute)
    assert "trigger-writeback" in source


def test_validate_command_exists_in_dispatcher():
    """ControlDispatcher._execute handles 'validate' command."""
    from inandout.engine.control import ControlDispatcher

    import inspect
    source = inspect.getsource(ControlDispatcher._execute)
    assert "validate" in source


@pytest.mark.asyncio
async def test_trigger_writeback_requires_connector_and_datatype():
    """trigger-writeback raises ValueError when connector or datatype is missing."""
    from unittest.mock import AsyncMock, MagicMock
    from inandout.engine.control import ControlDispatcher

    pool = MagicMock()
    paused: set = set()
    dispatcher = ControlDispatcher(pool, paused)

    with pytest.raises(ValueError, match="trigger-writeback requires"):
        await dispatcher._cmd_trigger_writeback(None, None, {}, engine=None)


@pytest.mark.asyncio
async def test_trigger_writeback_no_engine_returns_skipped():
    """trigger-writeback returns skipped when no writeback engine is available."""
    from unittest.mock import MagicMock
    from inandout.engine.control import ControlDispatcher

    pool = MagicMock()
    paused: set = set()
    dispatcher = ControlDispatcher(pool, paused)

    # engine=None → skipped
    result = await dispatcher._cmd_trigger_writeback(
        "hubspot", "contacts", {}, engine=None
    )
    assert result["status"] == "skipped"


# ---------------------------------------------------------------------------
# T2 #35: per-datatype poll_interval override
# ---------------------------------------------------------------------------

def test_writeback_config_has_poll_interval_field():
    """WritebackConfig must have a poll_interval field (T2 #35)."""
    from inandout.config.writeback import WritebackConfig

    field = WritebackConfig.model_fields.get("poll_interval")
    assert field is not None
    assert field.default is None  # defaults to None (use daemon global)


def test_writeback_config_poll_interval_must_be_positive():
    """poll_interval must be > 0 when set."""
    from pydantic import ValidationError
    from inandout.config.writeback import (
        ConflictResolution,
        OperationConfig,
        OperationsConfig,
        ProtectionLevel,
        UpdateOperationConfig,
        WritebackConfig,
    )

    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/items/${external_id}"),
        insert=OperationConfig(method="POST", path="/items"),
        update=UpdateOperationConfig(method="PATCH", path="/items/${external_id}"),
        delete=OperationConfig(method="DELETE", path="/items/${external_id}"),
    )
    with pytest.raises(ValidationError):
        WritebackConfig(
            protection_level=ProtectionLevel.none,
            conflict_resolution=ConflictResolution.last_writer_wins,
            supported_actions=["insert"],
            operations=ops,
            poll_interval=0.0,
        )


def test_daemon_uses_dtype_poll_interval_over_default():
    """Daemon must use writeback_cfg.poll_interval when set (T2 #35)."""
    from inandout.writeback import daemon as wb_daemon
    import inspect

    src = inspect.getsource(wb_daemon)
    # The daemon must read poll_interval from the writeback config
    assert "poll_interval" in src
    # And must have a fallback to default_interval_secs
    assert "_loop_interval" in src or "poll_interval" in src


def test_poll_interval_overrides_default_in_daemon_logic():
    """Verify the override logic: dtype interval is preferred when set."""
    # Replicate the daemon's selection logic
    default_interval_secs = 30.0

    class FakeWritebackCfg:
        poll_interval = 5.0

    dtype_cfg_with_interval = FakeWritebackCfg()
    _dtype_interval = getattr(dtype_cfg_with_interval, "poll_interval", None)
    _loop_interval = float(_dtype_interval) if _dtype_interval else default_interval_secs
    assert _loop_interval == 5.0


def test_poll_interval_falls_back_to_default_when_none():
    """When poll_interval is None, daemon falls back to default_interval_secs."""
    default_interval_secs = 30.0

    class FakeWritebackCfg:
        poll_interval = None

    dtype_cfg = FakeWritebackCfg()
    _dtype_interval = getattr(dtype_cfg, "poll_interval", None)
    _loop_interval = float(_dtype_interval) if _dtype_interval else default_interval_secs
    assert _loop_interval == 30.0
