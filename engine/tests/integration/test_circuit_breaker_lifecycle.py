"""Integration tests for circuit breaker state machine lifecycle (T1 #13, T2 #25).

Verifies the full CLOSED → OPEN → HALF_OPEN → CLOSED transition sequence against a
real PostgreSQL-backed WritebackEngine with a respx-mocked HTTP endpoint.
"""
from __future__ import annotations

import asyncio
import os
import re

import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.writeback import (
    WritebackConfig,
    ProtectionLevel,
    ConflictResolution,
    OperationsConfig,
    OperationConfig,
    UpdateOperationConfig,
)
from inandout.transport.circuit_breaker import get_circuit_breaker, CircuitState
from inandout.writeback.engine import WritebackEngine


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_available(),
    reason="Docker not available",
)

_CONNECTOR = "cb_lifecycle"
_DATATYPE = "widgets"
_BASE_URL = "https://api.cb-test.example.com"


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="CBTestSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="cb_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        # failure_threshold=3: circuit opens after 3 consecutive failures
        # recovery_timeout=0.05: transitions to HALF_OPEN after 50 ms
        circuit_breaker={"failure_threshold": 3, "recovery_timeout": 0.05},
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["insert", "update", "delete"],
                    enable_crash_recovery=False,
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/{_DATATYPE}/${{external_id}}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/{_DATATYPE}/${{external_id}}"),
                        insert=OperationConfig(method="POST", path=f"/v1/{_DATATYPE}"),
                        delete=OperationConfig(method="DELETE", path=f"/v1/{_DATATYPE}/${{external_id}}"),
                    ),
                )
            )
        },
    )


def _make_wb_cfg() -> WritebackConfig:
    connector = _make_connector()
    return connector.datatypes[_DATATYPE].writeback  # type: ignore[return-value]


async def _create_delta_table(pool, table_name: str) -> None:
    async with pool.connection() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'update',
                _cluster_id TEXT
            )
        """)
        await conn.commit()


async def _insert_delta_row(pool, table_name: str, external_id: str, action: str = "update") -> None:
    async with pool.connection() as conn:
        await conn.execute(
            f"INSERT INTO {table_name} (external_id, name, _action) VALUES (%s, %s, %s)",
            [external_id, f"Widget {external_id}", action],
        )
        await conn.commit()


@pytest.mark.anyio
async def test_circuit_opens_after_consecutive_failures(pool):
    """Circuit trips CLOSED → OPEN after failure_threshold consecutive HTTP failures.

    T1 #13 / T2 #25: after N consecutive failures, the circuit opens and all
    subsequent requests for that connector/datatype are rejected fast (skipped)
    without making any HTTP calls.
    """
    os.environ["INOUT_CREDENTIAL_CB_TEST_KEY"] = "dummy"

    delta_table = "_delta_cb_lifecycle_open"
    await _create_delta_table(pool, delta_table)
    await _insert_delta_row(pool, delta_table, "w1")

    connector = _make_connector()
    wb_cfg = _make_wb_cfg()
    engine = WritebackEngine(pool)

    # Confirm circuit starts CLOSED
    cb = get_circuit_breaker(_CONNECTOR, _DATATYPE, failure_threshold=3, recovery_timeout=0.05)
    assert cb.state == CircuitState.closed

    # Run 3 cycles each returning 422 (data_error: non-retryable, instant failure)
    for _ in range(3):
        with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
            mock.patch(re.compile(rf"/v1/{_DATATYPE}/\w+")).mock(
                return_value=httpx.Response(422, json={"error": "unprocessable"})
            )
            result = await engine.run_writeback_cycle(
                connector, _DATATYPE, wb_cfg, delta_table,
                max_concurrent_writes_override=1,
            )
        assert result.failed == 1, "Each cycle should fail once"

    # Circuit must now be OPEN
    assert cb.state == CircuitState.open

    # Next cycle: row is skipped (circuit open — no HTTP call made)
    with respx.mock(base_url=_BASE_URL, assert_all_called=True) as mock:
        # No routes registered → any real HTTP call would fail the test
        result_open = await engine.run_writeback_cycle(
            connector, _DATATYPE, wb_cfg, delta_table,
            max_concurrent_writes_override=1,
        )

    assert result_open.skipped >= 1, "Rows must be skipped when circuit is open"
    assert result_open.failed == 0, "No HTTP failures expected when circuit is open"


@pytest.mark.anyio
async def test_circuit_recovers_after_timeout(pool):
    """Circuit transitions OPEN → HALF_OPEN → CLOSED when the probe succeeds.

    T1 #13 / T2 #25: after recovery_timeout elapses the breaker self-transitions
    to HALF_OPEN and the next successful request resets it to CLOSED.
    """
    os.environ["INOUT_CREDENTIAL_CB_TEST_KEY"] = "dummy"

    delta_table = "_delta_cb_lifecycle_recover"
    await _create_delta_table(pool, delta_table)
    await _insert_delta_row(pool, delta_table, "w2")

    connector = _make_connector()
    wb_cfg = _make_wb_cfg()
    engine = WritebackEngine(pool)

    # Pre-trip the circuit breaker directly (simulates prior failures)
    cb = get_circuit_breaker(_CONNECTOR, _DATATYPE, failure_threshold=3, recovery_timeout=0.05)
    for _ in range(3):
        cb.record_failure()
    assert cb.state == CircuitState.open

    # Wait for recovery_timeout to expire → HALF_OPEN
    await asyncio.sleep(0.12)
    assert cb.state == CircuitState.half_open

    # Run cycle with a successful response → probe passes → CLOSED
    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch(re.compile(rf"/v1/{_DATATYPE}/\w+")).mock(
            return_value=httpx.Response(200, json={"id": "w2", "name": "Widget w2"})
        )
        result = await engine.run_writeback_cycle(
            connector, _DATATYPE, wb_cfg, delta_table,
            max_concurrent_writes_override=1,
        )

    assert cb.state == CircuitState.closed, "Probe success must close the circuit"
    assert result.processed >= 1, "At least the probe row should be processed"
    assert result.failed == 0


@pytest.mark.anyio
async def test_half_open_probe_failure_reopens_circuit(pool):
    """HALF_OPEN probe failure re-opens the circuit (HALF_OPEN → OPEN).

    T1 #13 / T2 #25: if the probe request in HALF_OPEN fails, the circuit must
    re-open immediately rather than transitioning to CLOSED.
    """
    os.environ["INOUT_CREDENTIAL_CB_TEST_KEY"] = "dummy"

    delta_table = "_delta_cb_lifecycle_halfopen_fail"
    await _create_delta_table(pool, delta_table)
    await _insert_delta_row(pool, delta_table, "w3")

    connector = _make_connector()
    wb_cfg = _make_wb_cfg()
    engine = WritebackEngine(pool)

    # Pre-trip the circuit breaker and wait for HALF_OPEN
    cb = get_circuit_breaker(_CONNECTOR, _DATATYPE, failure_threshold=3, recovery_timeout=0.05)
    for _ in range(3):
        cb.record_failure()
    await asyncio.sleep(0.12)
    assert cb.state == CircuitState.half_open

    # Run cycle with a failing response → probe fails → circuit re-opens
    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch(re.compile(rf"/v1/{_DATATYPE}/\w+")).mock(
            return_value=httpx.Response(503, json={"error": "unavailable"})
        )
        # 503 is transient — would normally retry. Use side_effect to return 422 instead
        # so we get a non-retryable failure and avoid long sleep delays.
        mock.patch(re.compile(rf"/v1/{_DATATYPE}/\w+")).mock(
            return_value=httpx.Response(422, json={"error": "probe failed"})
        )
        await engine.run_writeback_cycle(
            connector, _DATATYPE, wb_cfg, delta_table,
            max_concurrent_writes_override=1,
        )

    assert cb.state == CircuitState.open, "Failed probe must re-open the circuit"
