"""Integration tests for T1 #44: source-unavailability handling.

When a connector is marked unhealthy in ``inout_ops_connector_health``, syncs
for that connector/datatype must be skipped while the cooldown window is
active.  Once the window expires (or after a successful sync), the connector
must be marked healthy again and normal syncs resume.

GOAL.md T1 #44: on all-retries-exhausted, mark connector unhealthy, back off
using an exponential cooldown, and auto-recover when the source is reachable.
"""
from __future__ import annotations

import datetime
import os

import pytest
import respx
import httpx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import (
    ConnectorConfig,
    ConnectionConfig,
    DatatypeConfig,
    GenerationProfile,
)
from inandout.config.ingestion import (
    IngestionConfig,
    HistoryMode,
    ListConfig,
    ScheduleConfig,
)
from inandout.config.pagination import (
    PaginationConfig,
    PaginationStrategy,
    CursorConfig,
)
from inandout.ingestion.engine import IngestionEngine
from inandout.transport.circuit_breaker import get_circuit_breaker, reset_all


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_available(), reason="Docker not available"
)

_CONNECTOR = "unavail_test"
_DATATYPE = "accounts"
_BASE_URL = "https://api.unavail-test.example.com"
os.environ["INOUT_CREDENTIAL_UNAVAIL_KEY"] = "dummy"


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="UnavailTestSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="unavail_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        # Low failure_threshold so breaker opens quickly in tests
        circuit_breaker={"failure_threshold": 1, "recovery_timeout": 0.05},
        datatypes={
            _DATATYPE: DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/accounts",
                            record_selector="results",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.cursor,
                                cursor=CursorConfig(
                                    request_param="cursor",
                                    response_path="next_cursor",
                                ),
                            ),
                        )
                    },
                    unavailability_cooldown_secs=300,
                    unavailability_backoff_multiplier=2.0,
                    unavailability_backoff_ceiling_secs=3600.0,
                )
            )
        },
    )


async def _mark_unhealthy(pool, *, minutes_ago: int = 1) -> None:
    """Directly insert an unhealthy row with a configurable age."""
    ts = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=minutes_ago)
    async with pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO inout_ops_connector_health
                (connector, datatype, status, marked_unhealthy_at, reason, updated_at)
            VALUES (%s, %s, 'unhealthy', %s, 'test injection', NOW())
            ON CONFLICT (connector, datatype)
            DO UPDATE SET
                status             = 'unhealthy',
                marked_unhealthy_at = EXCLUDED.marked_unhealthy_at,
                reason             = EXCLUDED.reason,
                updated_at         = NOW()
            """,
            [_CONNECTOR, _DATATYPE, ts],
        )
        await conn.commit()


async def _get_health_row(pool) -> dict | None:
    async with pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT status, marked_unhealthy_at FROM inout_ops_connector_health "
            "WHERE connector = %s AND datatype = %s",
            [_CONNECTOR, _DATATYPE],
        )).fetchone()
    if row is None:
        return None
    return {"status": row[0], "marked_unhealthy_at": row[1]}


@pytest.mark.anyio
async def test_sync_skipped_when_connector_unhealthy(pool):
    """T1 #44: sync returns status='skipped' when connector is within cooldown window."""
    # Ensure circuit breaker is clean
    reset_all()

    connector = _make_connector()

    # Mark connector unhealthy — 1 minute ago is well within the 300-second cooldown
    await _mark_unhealthy(pool, minutes_ago=1)

    engine = IngestionEngine(pool=pool, namespace="test")
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "skipped", (
        f"Expected 'skipped' but got '{result.status}' — "
        "engine should honour the cooldown window"
    )


@pytest.mark.anyio
async def test_sync_resumes_after_cooldown_expires(pool):
    """T1 #44: sync proceeds normally once the unhealthy cooldown window has expired."""
    reset_all()

    connector = _make_connector()

    # Mark unhealthy 10 hours ago — far beyond the 300-second cooldown
    await _mark_unhealthy(pool, minutes_ago=600)

    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/accounts").mock(
            return_value=httpx.Response(
                200,
                json={"results": [{"id": "a1", "name": "Acme"}], "next_cursor": None},
            )
        )
        engine = IngestionEngine(pool=pool, namespace="test")
        ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    # After a successful sync the engine clears the unhealthy mark
    assert result.status == "completed", (
        f"Expected 'completed' after cooldown expiry, got '{result.status}'"
    )
    health = await _get_health_row(pool)
    assert health is not None
    assert health["status"] == "healthy", (
        f"Expected health status='healthy' after successful sync, got {health['status']!r}"
    )


@pytest.mark.anyio
async def test_connector_marked_unhealthy_on_failure(pool):
    """T1 #44: A failing sync opens the circuit breaker and writes unhealthy to health table."""
    reset_all()

    # Pre-register circuit breaker with threshold=1 so the FIRST failure opens it
    # (the ingestion engine uses get_circuit_breaker() which returns the cached CB)
    get_circuit_breaker(_CONNECTOR + "_fail", _DATATYPE, failure_threshold=1, recovery_timeout=3600)

    # Clear any existing health row for a fresh connector name
    async with pool.connection() as conn:
        await conn.execute(
            "DELETE FROM inout_ops_connector_health WHERE connector = %s",
            [_CONNECTOR + "_fail"],
        )
        await conn.commit()

    connector = ConnectorConfig(
        name=_CONNECTOR + "_fail",
        system="UnavailTestSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="unavail_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        # failure_threshold=1 → opens immediately on first failure
        circuit_breaker={"failure_threshold": 1, "recovery_timeout": 3600},
        datatypes={
            _DATATYPE: DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/accounts",
                            record_selector="results",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.cursor,
                                cursor=CursorConfig(
                                    request_param="cursor",
                                    response_path="next_cursor",
                                ),
                            ),
                        )
                    },
                )
            )
        },
    )

    # Simulate a 503 Service Unavailable — engine should fail and mark unhealthy
    with respx.mock(base_url=_BASE_URL) as mock:
        mock.get("/v1/accounts").mock(
            return_value=httpx.Response(503, json={"error": "service unavailable"})
        )
        engine = IngestionEngine(pool=pool, namespace="test")
        ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "failed", (
        f"Expected sync to fail on 503, got '{result.status}'"
    )

    # The health table should now carry an 'unhealthy' row
    async with pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT status FROM inout_ops_connector_health "
            "WHERE connector = %s AND datatype = %s",
            [_CONNECTOR + "_fail", _DATATYPE],
        )).fetchone()

    assert row is not None, "Expected a row in inout_ops_connector_health after failure"
    assert row[0] == "unhealthy", (
        f"Expected status='unhealthy' after circuit opened, got {row[0]!r}"
    )
