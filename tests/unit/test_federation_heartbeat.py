"""Unit tests for the federation heartbeat module."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.federation.heartbeat import (
    FederationHeartbeat,
    ConnectorDataypeHealth,
    get_instance_id,
    report_heartbeat,
    heartbeat_loop,
)


# ---------------------------------------------------------------------------
# Instance ID
# ---------------------------------------------------------------------------


def test_get_instance_id_is_stable():
    id1 = get_instance_id()
    id2 = get_instance_id()
    assert id1 == id2
    assert isinstance(id1, str)
    assert len(id1) > 0


def test_instance_id_contains_hostname():
    import socket
    iid = get_instance_id()
    assert socket.gethostname() in iid


# ---------------------------------------------------------------------------
# FederationHeartbeat
# ---------------------------------------------------------------------------


def test_heartbeat_starts_empty():
    hb = FederationHeartbeat()
    assert hb.snapshots() == []


def test_heartbeat_update_creates_entry():
    hb = FederationHeartbeat()
    hb.update("salesforce", "contacts", health_score=0.9)
    snaps = hb.snapshots()
    assert len(snaps) == 1
    assert snaps[0].connector == "salesforce"
    assert snaps[0].datatype == "contacts"
    assert snaps[0].health_score == 0.9
    assert snaps[0].circuit_breaker_state == "closed"
    assert snaps[0].dead_letter_depth == 0


def test_heartbeat_update_overwrites_existing():
    hb = FederationHeartbeat()
    hb.update("salesforce", "contacts", health_score=0.9)
    hb.update("salesforce", "contacts", health_score=0.1, circuit_breaker_state="open")
    snaps = hb.snapshots()
    assert len(snaps) == 1  # still one entry for the same (connector, datatype)
    assert snaps[0].health_score == 0.1
    assert snaps[0].circuit_breaker_state == "open"


def test_heartbeat_multiple_datatypes():
    hb = FederationHeartbeat()
    hb.update("salesforce", "contacts")
    hb.update("salesforce", "accounts")
    hb.update("hubspot", "deals")
    assert len(hb.snapshots()) == 3


def test_heartbeat_default_namespace():
    hb = FederationHeartbeat()
    assert hb.namespace == "public"


def test_heartbeat_custom_namespace():
    hb = FederationHeartbeat(namespace="tenant_acme")
    assert hb.namespace == "tenant_acme"


# ---------------------------------------------------------------------------
# report_heartbeat
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_report_heartbeat_empty_returns_zero():
    pool = MagicMock()
    hb = FederationHeartbeat()
    result = await report_heartbeat(pool, hb)
    assert result == 0


@pytest.mark.anyio
async def test_report_heartbeat_upserts_rows():
    hb = FederationHeartbeat()
    hb.update("salesforce", "contacts", health_score=1.0)
    hb.update("salesforce", "accounts", health_score=0.8)

    conn = AsyncMock()
    conn.execute = AsyncMock()
    conn.commit = AsyncMock()
    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    result = await report_heartbeat(pool, hb, instance_id="test-instance-01")
    assert result == 2
    assert conn.execute.call_count == 2


@pytest.mark.anyio
async def test_report_heartbeat_passes_instance_id():
    hb = FederationHeartbeat()
    hb.update("sf", "leads")

    conn = AsyncMock()
    conn.execute = AsyncMock()
    conn.commit = AsyncMock()
    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    await report_heartbeat(pool, hb, instance_id="my-explicit-id")
    call_args = conn.execute.call_args
    params = call_args[0][1]  # second positional arg is the params list
    assert params[0] == "my-explicit-id"


@pytest.mark.anyio
async def test_report_heartbeat_exception_does_not_raise():
    """Heartbeat failure must never crash the daemon."""
    pool = MagicMock()
    pool.connection.side_effect = RuntimeError("db gone")

    hb = FederationHeartbeat()
    hb.update("sf", "leads")

    # should not raise
    result = await report_heartbeat(pool, hb)
    assert result == 0


# ---------------------------------------------------------------------------
# heartbeat_loop
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_heartbeat_loop_stops_when_flag_set():
    calls: list[str] = []

    async def fake_sleep(_: float) -> None:
        # On the second sleep, signal stop
        calls.append("sleep")

    stop_after = [False]

    async def patched_report(pool, hb, **kwargs):
        calls.append("report")

    stop_counter = [0]

    def should_stop() -> bool:
        stop_counter[0] += 1
        return stop_counter[0] >= 3  # stop on 3rd check

    pool = MagicMock()
    hb = FederationHeartbeat()

    with (
        patch("inandout.federation.heartbeat.asyncio.sleep", new=fake_sleep),
        patch("inandout.federation.heartbeat.report_heartbeat", new=patched_report),
    ):
        await heartbeat_loop(pool, hb, interval_secs=0.01, should_stop=should_stop)

    assert "report" in calls
