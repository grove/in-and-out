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


@pytest.mark.anyio
async def test_unhealthy_connector_does_not_block_other_connectors(pool):
    """T1 #44: one unhealthy connector must not block other connectors' syncs.

    Running two connectors where one is marked unhealthy; the second connector
    must still complete its sync successfully.
    """
    reset_all()

    DATATYPE = "items"

    # Build a helper to create a lightweight connector
    def _make_connector_for(name: str, base_url: str) -> ConnectorConfig:
        cred = f"{name}_key".lower().replace("-", "_")
        os.environ[f"INOUT_CREDENTIAL_{cred.upper()}"] = "dummy"
        return ConnectorConfig(
            name=name,
            system="MultiConnectorTest",
            generation_profile=GenerationProfile.ingestion_polling_readonly,
            api_version="v1",
            connection=ConnectionConfig(base_url=base_url),
            auth=ApiKeyAuth(
                type="api_key",
                credential_ref=cred,
                api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
            ),
            datatypes={
                DATATYPE: DatatypeConfig(
                    ingestion=IngestionConfig(
                        primary_key="id",
                        history_mode=HistoryMode.overwrite,
                        schedule=ScheduleConfig(interval="5m"),
                        **{
                            "list": ListConfig(
                                method="GET",
                                path="/v1/items",
                                record_selector="items",
                                pagination=PaginationConfig(
                                    strategy=PaginationStrategy.cursor,
                                    cursor=CursorConfig(request_param="cursor", response_path="next_cursor"),
                                ),
                            )
                        },
                        unavailability_cooldown_secs=300,
                    )
                )
            },
        )

    SICK_CONNECTOR = "unavail_sick"
    HEALTHY_CONNECTOR = "unavail_healthy"
    SICK_URL = "https://api.sick.example.com"
    HEALTHY_URL = "https://api.healthy.example.com"

    sick_conn = _make_connector_for(SICK_CONNECTOR, SICK_URL)
    healthy_conn = _make_connector_for(HEALTHY_CONNECTOR, HEALTHY_URL)

    # Mark sick connector as unhealthy (within cooldown)
    ts = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=30)
    async with pool.connection() as conn:
        await conn.execute(
            """
            INSERT INTO inout_ops_connector_health
                (connector, datatype, status, marked_unhealthy_at, reason, updated_at)
            VALUES (%s, %s, 'unhealthy', %s, 'test injection', NOW())
            ON CONFLICT (connector, datatype) DO UPDATE SET
                status = 'unhealthy', marked_unhealthy_at = EXCLUDED.marked_unhealthy_at,
                reason = EXCLUDED.reason, updated_at = NOW()
            """,
            [SICK_CONNECTOR, DATATYPE, ts],
        )
        await conn.commit()

    # Clear any health row for healthy connector
    async with pool.connection() as conn:
        await conn.execute(
            "DELETE FROM inout_ops_connector_health WHERE connector = %s",
            [HEALTHY_CONNECTOR],
        )
        await conn.commit()

    engine = IngestionEngine(pool=pool, namespace="test")

    # Sick connector sync (should skip)
    sick_result = await engine.run_sync(sick_conn, DATATYPE, sick_conn.datatypes[DATATYPE].ingestion)
    assert sick_result.status == "skipped", (
        f"Sick connector should be skipped; got '{sick_result.status}'"
    )

    # Healthy connector sync (should complete despite sick neighbor)
    with respx.mock(base_url=HEALTHY_URL, assert_all_called=False) as mock:
        mock.get("/v1/items").mock(
            return_value=httpx.Response(200, json={"items": [{"id": "h1"}], "next_cursor": None})
        )
        healthy_result = await engine.run_sync(
            healthy_conn, DATATYPE, healthy_conn.datatypes[DATATYPE].ingestion
        )

    assert healthy_result.status == "completed", (
        f"Healthy connector must not be blocked by sick neighbor; got '{healthy_result.status}'"
    )
    assert healthy_result.records_inserted >= 1


@pytest.mark.anyio
async def test_exponential_backoff_increases_cooldown(pool):
    """T1 #44: successive skips on the same connector increase the effective cooldown.

    The engine doubles _base by 2^skip_n, so the second skip should have a longer
    cooldown. We verify the in-memory skip counter increments.
    """
    reset_all()

    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    # Mark unhealthy 1 minute ago (within 5-minute base cooldown)
    await _mark_unhealthy(pool, minutes_ago=1)

    engine = IngestionEngine(pool=pool, namespace="test")

    # First skip
    result1 = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)
    assert result1.status == "skipped"

    skip_count_after_first = engine._unavailability_skip_counts.get(
        (connector.name, _DATATYPE), 0
    )
    assert skip_count_after_first >= 1, (
        f"Expected skip counter >= 1 after first skip, got {skip_count_after_first}"
    )

    # Second skip — skip counter increments again
    result2 = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)
    assert result2.status == "skipped"

    skip_count_after_second = engine._unavailability_skip_counts.get(
        (connector.name, _DATATYPE), 0
    )
    assert skip_count_after_second > skip_count_after_first, (
        f"Expected skip counter to increase: {skip_count_after_first} → {skip_count_after_second}"
    )


@pytest.mark.anyio
async def test_health_cleared_on_successful_sync_after_cooldown(pool):
    """T1 #44: after the cooldown expires and sync succeeds, health row transitions to healthy."""
    reset_all()

    connector = _make_connector()
    ingestion_cfg = connector.datatypes[_DATATYPE].ingestion

    # Mark unhealthy 10 hours ago so cooldown has long expired
    await _mark_unhealthy(pool, minutes_ago=600)

    # Verify it was unhealthy before
    health_before = await _get_health_row(pool)
    assert health_before is not None
    assert health_before["status"] == "unhealthy"

    # Run a successful sync
    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get("/v1/accounts").mock(
            return_value=httpx.Response(
                200, json={"results": [{"id": "h1"}], "next_cursor": None}
            )
        )
        engine = IngestionEngine(pool=pool, namespace="test")
        result = await engine.run_sync(connector, _DATATYPE, ingestion_cfg)

    assert result.status == "completed", f"Expected completed, got {result.status}"

    health_after = await _get_health_row(pool)
    assert health_after is not None, "Health row should still exist after sync"
    assert health_after["status"] == "healthy", (
        f"Expected health='healthy' after successful sync; got {health_after['status']!r}"
    )
