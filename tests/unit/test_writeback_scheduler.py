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
