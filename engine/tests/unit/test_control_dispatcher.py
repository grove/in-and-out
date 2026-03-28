"""Unit tests for the control command dispatcher."""
from __future__ import annotations

import pytest

from inandout.engine.control import ControlDispatcher, is_paused


# ---------------------------------------------------------------------------
# is_paused helper
# ---------------------------------------------------------------------------

def test_is_paused_exact_match():
    paused = {("hubspot", "contacts")}
    assert is_paused(paused, "hubspot", "contacts") is True


def test_is_paused_wildcard():
    paused = {("hubspot", "*")}
    assert is_paused(paused, "hubspot", "contacts") is True
    assert is_paused(paused, "hubspot", "deals") is True


def test_is_not_paused():
    paused = {("other", "contacts")}
    assert is_paused(paused, "hubspot", "contacts") is False


def test_empty_paused_set():
    assert is_paused(set(), "hubspot", "contacts") is False


# ---------------------------------------------------------------------------
# pause / resume commands
# ---------------------------------------------------------------------------

def _make_dispatcher(paused: set | None = None) -> ControlDispatcher:
    return ControlDispatcher(pool=None, paused_connectors=paused if paused is not None else set())  # type: ignore[arg-type]


def test_pause_adds_to_set():
    paused: set = set()
    d = _make_dispatcher(paused)
    result = d._cmd_pause_connector("hubspot", "contacts")
    assert ("hubspot", "contacts") in paused
    assert "paused" in result


def test_pause_without_connector_raises():
    d = _make_dispatcher()
    with pytest.raises(ValueError, match="connector"):
        d._cmd_pause_connector(None, "contacts")


def test_resume_removes_from_set():
    paused = {("hubspot", "contacts")}
    d = _make_dispatcher(paused)
    d._cmd_resume_connector("hubspot", "contacts")
    assert ("hubspot", "contacts") not in paused


def test_resume_nonexistent_is_noop():
    paused: set = set()
    d = _make_dispatcher(paused)
    d._cmd_resume_connector("hubspot", "contacts")  # should not raise
    assert len(paused) == 0


def test_resume_without_connector_raises():
    d = _make_dispatcher()
    with pytest.raises(ValueError, match="connector"):
        d._cmd_resume_connector(None, "contacts")


def test_pause_wildcard():
    paused: set = set()
    d = _make_dispatcher(paused)
    d._cmd_pause_connector("hubspot", None)
    assert ("hubspot", "*") in paused
    assert is_paused(paused, "hubspot", "contacts") is True


# ---------------------------------------------------------------------------
# Unknown command
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_unknown_command_raises():
    d = _make_dispatcher()
    with pytest.raises(ValueError, match="Unknown command"):
        await d._execute("no_such_cmd", "hubspot", "contacts", {}, None)


# ---------------------------------------------------------------------------
# requeue_dead_letter without engine raises
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_requeue_dead_letter_without_engine_raises():
    d = _make_dispatcher()
    with pytest.raises(RuntimeError, match="IngestionEngine"):
        await d._cmd_requeue_dead_letter("hubspot", "contacts", {}, engine=None)


@pytest.mark.anyio
async def test_requeue_dead_letter_missing_connector_raises():
    d = _make_dispatcher()
    with pytest.raises(ValueError, match="connector"):
        await d._cmd_requeue_dead_letter(None, None, {}, engine=object())


# ---------------------------------------------------------------------------
# Cron scheduling helper
# ---------------------------------------------------------------------------

def test_next_interval_secs_uses_interval():
    from inandout.ingestion.daemon import _next_interval_secs
    from inandout.config.ingestion import ScheduleConfig
    schedule = ScheduleConfig(interval="5m")
    result = _next_interval_secs(schedule, 300.0)
    assert result == 300.0


def test_next_interval_secs_uses_cron():
    from inandout.ingestion.daemon import _next_interval_secs
    from inandout.config.ingestion import ScheduleConfig
    schedule = ScheduleConfig(cron="*/5 * * * *")  # every 5 minutes
    result = _next_interval_secs(schedule, 300.0)
    # Should be between 0 and 300 seconds
    assert 0.0 <= result <= 300.0


def test_next_interval_secs_bad_cron_falls_back():
    from inandout.ingestion.daemon import _next_interval_secs
    from inandout.config.ingestion import ScheduleConfig
    schedule = ScheduleConfig(cron="not_a_valid_cron")
    result = _next_interval_secs(schedule, 42.0)
    assert result == 42.0
