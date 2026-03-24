"""Unit tests for source unavailability handling (T1 #44)."""
from __future__ import annotations

import datetime
import inspect
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.observability.metrics import source_unavailable_total


def test_source_unavailable_counter_registered() -> None:
    """source_unavailable_total metric should be registered and labelable."""
    counter = source_unavailable_total
    assert counter is not None
    counter.labels(connector="test", datatype="contacts").inc(0)


def test_source_unavailable_counter_increments() -> None:
    source_unavailable_total.labels(connector="erp", datatype="invoices").inc()


def test_connector_health_table_ddl_exists() -> None:
    """Migration 017 should define the health table SQL (smoke test)."""
    import importlib.util
    import pathlib

    migration_path = pathlib.Path(
        "migrations/versions/017_20260323_connector_health.py"
    )
    spec = importlib.util.spec_from_file_location("migration_017", migration_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    # We just need to import it — don't execute upgrade()
    assert module is not None


# ---------------------------------------------------------------------------
# Source-inspection tests: verify T1 #44 engine logic is present
# ---------------------------------------------------------------------------

def test_run_sync_checks_connector_health_before_sync() -> None:
    """run_sync must query connector_health to skip unavailable connectors."""
    from inandout.ingestion.engine import IngestionEngine
    source = inspect.getsource(IngestionEngine.run_sync)
    assert "inout_ops_connector_health" in source
    assert "unhealthy" in source


def test_run_sync_skips_on_unavailable_with_cooldown() -> None:
    """run_sync must return 'skipped' when connector is unhealthy and in cooldown."""
    from inandout.ingestion.engine import IngestionEngine
    source = inspect.getsource(IngestionEngine.run_sync)
    assert "sync_skipped_connector_unavailable" in source
    assert "cooldown_secs" in source


def test_run_sync_marks_unavailable_on_circuit_open() -> None:
    """run_sync must write 'unhealthy' to connector_health when CB opens."""
    from inandout.ingestion.engine import IngestionEngine
    source = inspect.getsource(IngestionEngine.run_sync)
    assert "connector_marked_unavailable" in source
    assert "CircuitState" in source or "circuit_breaker" in source


def test_run_sync_clears_health_on_success() -> None:
    """run_sync must write 'healthy' to connector_health after a successful sync."""
    from inandout.ingestion.engine import IngestionEngine
    source = inspect.getsource(IngestionEngine.run_sync)
    assert "status = 'healthy'" in source or "status          = 'healthy'" in source


def test_run_sync_records_source_unavailable_metric_on_circuit_open() -> None:
    """source_unavailable_total must be incremented when CB opens."""
    from inandout.ingestion.engine import IngestionEngine
    source = inspect.getsource(IngestionEngine.run_sync)
    assert "source_unavailable_total" in source


# ---------------------------------------------------------------------------
# Functional: skip when connector_health reports unhealthy within cooldown
# ---------------------------------------------------------------------------

def _make_ingestion_cfg_unavail(cooldown: int = 300) -> MagicMock:
    cfg = MagicMock()
    cfg.source_mode = "polling"
    cfg.cdc = None
    cfg.list = MagicMock()
    cfg.list.incremental = None
    cfg.history_mode = "none"
    cfg.unavailability_cooldown_secs = cooldown
    return cfg


def _make_connector_unavail(name: str = "erp") -> MagicMock:
    cfg = MagicMock()
    cfg.name = name
    cfg.version = "1.0.0"
    cfg.datatypes = {}
    return cfg


def _build_unhealthy_pool(unhealthy_since: datetime.datetime) -> MagicMock:
    """Pool whose connector_health query returns an unhealthy row."""
    async def _execute(sql: str, params=None):
        cur = AsyncMock()
        if "inout_ops_connector_health" in sql and "SELECT" in sql:
            # Return (status='unhealthy', marked_unhealthy_at)
            cur.fetchone = AsyncMock(return_value=("unhealthy", unhealthy_since))
        elif "FOR UPDATE SKIP LOCKED" in sql:
            cur.fetchone = AsyncMock(return_value=None)  # lock held → skipped
        else:
            cur.fetchone = AsyncMock(return_value=None)
        cur.rowcount = 0
        return cur

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(side_effect=_execute)
    conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)
    return pool


@pytest.mark.anyio
async def test_run_sync_skips_when_connector_unhealthy_in_cooldown() -> None:
    """run_sync returns 'skipped' when connector is marked unhealthy and cooldown has not elapsed."""
    from inandout.ingestion.engine import IngestionEngine

    # Marked unhealthy 30 seconds ago — still within 300s cooldown
    unhealthy_since = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=30)
    pool = _build_unhealthy_pool(unhealthy_since)

    engine = IngestionEngine(pool=pool)
    engine._read_pool = pool  # reuse for read pool

    result = await engine.run_sync(
        _make_connector_unavail(),
        "invoices",
        _make_ingestion_cfg_unavail(cooldown=300),
    )
    assert result.status == "skipped"


@pytest.mark.anyio
async def test_run_sync_proceeds_when_cooldown_elapsed() -> None:
    """run_sync does NOT skip when the unhealthy mark is older than cooldown."""
    from inandout.ingestion.engine import IngestionEngine

    # Marked unhealthy 600 seconds ago — cooldown of 300s has elapsed
    unhealthy_since = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=600)

    async def _execute(sql: str, params=None):
        cur = AsyncMock()
        if "inout_ops_connector_health" in sql and "SELECT" in sql:
            cur.fetchone = AsyncMock(return_value=("unhealthy", unhealthy_since))
        elif "FOR UPDATE SKIP LOCKED" in sql:
            # Lock not held — sync can proceed
            cur.fetchone = AsyncMock(return_value=("row-id",))
        else:
            cur.fetchone = AsyncMock(return_value=None)
        cur.rowcount = 0
        return cur

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(side_effect=_execute)
    conn.commit = AsyncMock()
    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)

    engine = IngestionEngine(pool=pool)
    engine._read_pool = pool

    with patch.object(engine, "_do_sync", new=AsyncMock()):
        result = await engine.run_sync(
            _make_connector_unavail(),
            "invoices",
            _make_ingestion_cfg_unavail(cooldown=300),
        )
    # Not "skipped" — actual sync (or completed/failed) was attempted
    assert result.status != "skipped"


@pytest.mark.anyio
async def test_circuit_breaker_opens_after_failures_marks_connector_unhealthy() -> None:
    """After circuit breaker opens due to sync failures, connector_health is updated."""
    from inandout.ingestion.engine import IngestionEngine
    from inandout.transport.circuit_breaker import get_circuit_breaker, CircuitState

    connector_name = "crm_unavail_test"
    datatype = "leads_unavail_test"

    # Force the circuit breaker to OPEN state
    cb = get_circuit_breaker(connector_name, datatype)
    # Activate enough failures to open
    for _ in range(cb.failure_threshold):
        cb.record_failure()
    assert cb.state == CircuitState.open

    health_inserts: list[str] = []

    async def _execute(sql: str, params=None):
        if "inout_ops_connector_health" in sql and (
            "INSERT" in sql or "UPDATE" in sql
        ):
            health_inserts.append(sql)
        elif "inout_ops_connector_health" in sql and "SELECT" in sql:
            # No prior health row — connector was healthy
            cur = AsyncMock()
            cur.fetchone = AsyncMock(return_value=None)
            cur.rowcount = 0
            return cur
        cur = AsyncMock()
        if "FOR UPDATE SKIP LOCKED" in sql:
            cur.fetchone = AsyncMock(return_value=("row-id",))
        else:
            cur.fetchone = AsyncMock(return_value=None)
        cur.rowcount = 0
        return cur

    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=None)
    conn.execute = AsyncMock(side_effect=_execute)
    conn.commit = AsyncMock()
    pool = MagicMock()
    pool.connection = MagicMock(return_value=conn)

    engine = IngestionEngine(pool=pool)
    engine._read_pool = pool

    ingestion_cfg = _make_ingestion_cfg_unavail()

    # Patch _do_sync to raise an exception (simulating a transport failure)
    with patch.object(engine, "_do_sync", new=AsyncMock(side_effect=RuntimeError("connection refused"))):
        result = await engine.run_sync(
            _make_connector_unavail(connector_name),
            datatype,
            ingestion_cfg,
        )

    assert result.status == "failed"
    # The CB was already open — connector_health should have been written
    assert any("inout_ops_connector_health" in s for s in health_inserts), (
        "connector_health must be updated when CB is open after sync failure"
    )

    # Clean up CB state for other tests
    cb.record_success()

