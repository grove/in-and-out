"""Unit tests for event sourcing / CDC fan-out (Step 69)."""
from __future__ import annotations

import json
import sys
from io import StringIO
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# EventOutputConfig
# ---------------------------------------------------------------------------

def test_event_output_config_defaults():
    from inandout.events.config import EventOutputConfig

    cfg = EventOutputConfig()
    assert cfg.enabled is False
    assert cfg.backend == "pg_notify"
    assert cfg.channel == "inandout_events"
    assert cfg.topic == "inandout-events"
    assert cfg.include_raw is False


# ---------------------------------------------------------------------------
# StdoutPublisher
# ---------------------------------------------------------------------------

async def test_stdout_publisher_emits_correct_json(capsys):
    from inandout.events.publisher import StdoutPublisher

    pub = StdoutPublisher()
    event = {
        "connector": "hubspot",
        "datatype": "contacts",
        "external_id": "123",
        "action": "upsert",
        "run_id": "run-uuid",
        "timestamp": "2026-01-01T00:00:00+00:00",
    }
    await pub.publish(event)

    captured = capsys.readouterr()
    parsed = json.loads(captured.out.strip())
    assert parsed["connector"] == "hubspot"
    assert parsed["external_id"] == "123"
    assert parsed["action"] == "upsert"


async def test_stdout_publisher_include_raw(capsys):
    from inandout.events.publisher import StdoutPublisher, build_event

    pub = StdoutPublisher()
    event = build_event(
        connector="sfdc",
        datatype="accounts",
        external_id="a1",
        action="upsert",
        run_id="r1",
        raw={"field": "value"},
        include_raw=True,
    )
    await pub.publish(event)

    captured = capsys.readouterr()
    parsed = json.loads(captured.out.strip())
    assert "raw" in parsed
    assert parsed["raw"] == {"field": "value"}


async def test_stdout_publisher_no_raw_by_default(capsys):
    from inandout.events.publisher import StdoutPublisher, build_event

    pub = StdoutPublisher()
    event = build_event(
        connector="sfdc",
        datatype="accounts",
        external_id="a1",
        action="upsert",
        run_id="r1",
        raw={"field": "value"},
        include_raw=False,
    )
    await pub.publish(event)

    captured = capsys.readouterr()
    parsed = json.loads(captured.out.strip())
    assert "raw" not in parsed


# ---------------------------------------------------------------------------
# PgNotifyPublisher
# ---------------------------------------------------------------------------

async def test_pg_notify_publisher_executes_notify():
    from inandout.events.publisher import PgNotifyPublisher

    mock_conn = AsyncMock()
    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=mock_conn),
        __aexit__=AsyncMock(return_value=None),
    ))

    pub = PgNotifyPublisher(pool=mock_pool, channel="test_channel")
    event = {"connector": "hub", "action": "upsert", "external_id": "1"}
    await pub.publish(event)

    mock_conn.execute.assert_called_once()
    call_args = mock_conn.execute.call_args[0][0]
    assert "NOTIFY test_channel" in call_args
    assert "connector" in call_args


async def test_pg_notify_publisher_uses_configured_channel():
    from inandout.events.publisher import PgNotifyPublisher

    mock_conn = AsyncMock()
    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=AsyncMock(
        __aenter__=AsyncMock(return_value=mock_conn),
        __aexit__=AsyncMock(return_value=None),
    ))

    pub = PgNotifyPublisher(pool=mock_pool, channel="my_custom_channel")
    await pub.publish({"x": 1})

    call_args = mock_conn.execute.call_args[0][0]
    assert "my_custom_channel" in call_args


# ---------------------------------------------------------------------------
# get_publisher factory
# ---------------------------------------------------------------------------

def test_get_publisher_stdout():
    from inandout.events.publisher import get_publisher, StdoutPublisher
    from inandout.events.config import EventOutputConfig

    cfg = EventOutputConfig(backend="stdout")
    pub = get_publisher(cfg)
    assert isinstance(pub, StdoutPublisher)


def test_get_publisher_pg_notify():
    from inandout.events.publisher import get_publisher, PgNotifyPublisher
    from inandout.events.config import EventOutputConfig

    cfg = EventOutputConfig(backend="pg_notify", channel="ch1")
    mock_pool = MagicMock()
    pub = get_publisher(cfg, pool=mock_pool)
    assert isinstance(pub, PgNotifyPublisher)


def test_get_publisher_kafka_raises_without_package():
    from inandout.events.publisher import get_publisher
    from inandout.events.config import EventOutputConfig

    cfg = EventOutputConfig(backend="kafka")
    with pytest.raises((NotImplementedError, ImportError)):
        get_publisher(cfg)


def test_get_publisher_kinesis_raises_without_package():
    from inandout.events.publisher import get_publisher
    from inandout.events.config import EventOutputConfig

    cfg = EventOutputConfig(backend="kinesis")
    with pytest.raises((NotImplementedError, ImportError)):
        get_publisher(cfg)


def test_get_publisher_unknown_backend_raises():
    from inandout.events.publisher import get_publisher
    from inandout.events.config import EventOutputConfig

    # Use model_construct to bypass validation
    cfg = EventOutputConfig.model_construct(backend="unknown")
    with pytest.raises(ValueError):
        get_publisher(cfg)


# ---------------------------------------------------------------------------
# build_event
# ---------------------------------------------------------------------------

def test_build_event_structure():
    from inandout.events.publisher import build_event

    event = build_event(
        connector="hub",
        datatype="contacts",
        external_id="42",
        action="upsert",
        run_id="run-1",
    )
    assert event["connector"] == "hub"
    assert event["datatype"] == "contacts"
    assert event["external_id"] == "42"
    assert event["action"] == "upsert"
    assert event["run_id"] == "run-1"
    assert "timestamp" in event
    assert "raw" not in event


def test_build_event_include_raw():
    from inandout.events.publisher import build_event

    raw = {"name": "Alice"}
    event = build_event(
        connector="h", datatype="c", external_id="1",
        action="upsert", run_id="r", raw=raw, include_raw=True,
    )
    assert event["raw"] == raw
