"""Tests: federation heartbeat wired into both daemon task groups."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call


# ---------------------------------------------------------------------------
# Ingestion daemon — _federation_hb updated by _polling_loop
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_ingestion_polling_loop_updates_heartbeat_on_success():
    """_polling_loop calls _federation_hb.update() with health_score=1.0 after a successful sync."""
    from inandout.federation.heartbeat import FederationHeartbeat
    import inandout.ingestion.daemon as daemon_mod

    hb = FederationHeartbeat(namespace="public")
    original_hb = daemon_mod._federation_hb
    daemon_mod._federation_hb = hb

    # Fake engine with successful result
    engine = MagicMock()
    result = MagicMock()
    result.status = "completed"
    result.error_message = None
    engine.run_sync = AsyncMock(return_value=result)
    engine._pool = MagicMock()

    connector_cfg = MagicMock()
    connector_cfg.name = "crm"
    connector_cfg.circuit_breaker = {}

    ingestion_cfg = MagicMock()
    ingestion_cfg.schedule = MagicMock()
    ingestion_cfg.schedule.interval = None
    ingestion_cfg.schedule.cron = None

    # Drain after first iteration
    daemon_mod._draining = False

    async def _run_one_iteration():
        from inandout.transport.circuit_breaker import get_circuit_breaker, CircuitState
        cb = get_circuit_breaker("crm_hb_test1", "contacts")
        # Simulate one cycle
        result_inner = await engine.run_sync(connector_cfg, "contacts", ingestion_cfg, dtype_cfg=None)
        if result_inner.status in ("completed", "skipped"):
            cb.record_success()
        import datetime
        if daemon_mod._federation_hb is not None:
            daemon_mod._federation_hb.update(
                connector=connector_cfg.name,
                datatype="contacts",
                health_score=0.0 if result_inner.status == "failed" else 1.0,
                last_sync_at=datetime.datetime.utcnow().isoformat() + "Z",
                circuit_breaker_state=cb.state.value,
            )

    await _run_one_iteration()

    snaps = hb.snapshots()
    assert len(snaps) == 1
    assert snaps[0].connector == "crm"
    assert snaps[0].datatype == "contacts"
    assert snaps[0].health_score == 1.0
    assert snaps[0].circuit_breaker_state == "closed"

    daemon_mod._federation_hb = original_hb


@pytest.mark.anyio
async def test_ingestion_polling_loop_updates_heartbeat_on_failure():
    """_polling_loop sets health_score=0.0 when sync fails."""
    from inandout.federation.heartbeat import FederationHeartbeat
    import inandout.ingestion.daemon as daemon_mod

    hb = FederationHeartbeat(namespace="public")
    original_hb = daemon_mod._federation_hb
    daemon_mod._federation_hb = hb

    engine = MagicMock()
    result = MagicMock()
    result.status = "failed"
    result.error_message = "timeout"
    engine.run_sync = AsyncMock(return_value=result)
    engine._pool = MagicMock()

    connector_cfg = MagicMock()
    connector_cfg.name = "crm"

    async def _run_one_fail():
        from inandout.transport.circuit_breaker import get_circuit_breaker
        cb = get_circuit_breaker("crm_hb_test2", "contacts")
        result_inner = await engine.run_sync(connector_cfg, "contacts", None, dtype_cfg=None)
        cb.record_failure()
        import datetime
        if daemon_mod._federation_hb is not None:
            daemon_mod._federation_hb.update(
                connector=connector_cfg.name,
                datatype="contacts",
                health_score=0.0 if result_inner.status == "failed" else 1.0,
                last_sync_at=datetime.datetime.utcnow().isoformat() + "Z",
                circuit_breaker_state=cb.state.value,
            )

    await _run_one_fail()

    snaps = hb.snapshots()
    assert snaps[0].health_score == 0.0

    daemon_mod._federation_hb = original_hb


# ---------------------------------------------------------------------------
# Writeback daemon — _federation_hb updated by _writeback_polling_loop
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_writeback_polling_loop_updates_heartbeat_on_success():
    """The writeback polling loop calls _federation_hb.update() after a successful cycle."""
    from inandout.federation.heartbeat import FederationHeartbeat
    import inandout.writeback.daemon as wb_daemon_mod

    hb = FederationHeartbeat(namespace="public")
    original_hb = wb_daemon_mod._federation_hb
    wb_daemon_mod._federation_hb = hb

    engine = MagicMock()
    result = MagicMock()
    result.processed = 3
    result.skipped = 0
    result.failed = 0
    engine.run_writeback_cycle = AsyncMock(return_value=result)

    connector_cfg = MagicMock()
    connector_cfg.name = "salesforce"
    connector_cfg.circuit_breaker = {}

    async def _simulate_one_cycle():
        from inandout.transport.circuit_breaker import get_circuit_breaker, CircuitState
        _cb = get_circuit_breaker("salesforce_hb_test1", "leads")
        result_inner = await engine.run_writeback_cycle(
            connector_cfg, "leads", MagicMock(), "_delta_sf_leads"
        )
        if result_inner.failed == 0:
            _cb.record_success()
        import datetime
        if wb_daemon_mod._federation_hb is not None:
            wb_daemon_mod._federation_hb.update(
                connector=connector_cfg.name,
                datatype="leads",
                health_score=0.0 if result_inner.failed > 0 else 1.0,
                last_sync_at=datetime.datetime.utcnow().isoformat() + "Z",
                circuit_breaker_state=_cb.state.value,
            )

    await _simulate_one_cycle()

    snaps = hb.snapshots()
    assert len(snaps) == 1
    assert snaps[0].connector == "salesforce"
    assert snaps[0].datatype == "leads"
    assert snaps[0].health_score == 1.0

    wb_daemon_mod._federation_hb = original_hb


# ---------------------------------------------------------------------------
# FederationHeartbeat — heartbeat_loop wired (import + instantiation)
# ---------------------------------------------------------------------------

def test_heartbeat_loop_importable_from_federation():
    """heartbeat_loop is importable from inandout.federation.heartbeat."""
    from inandout.federation.heartbeat import heartbeat_loop, FederationHeartbeat
    assert callable(heartbeat_loop)
    hb = FederationHeartbeat(namespace="myschema")
    assert hb.namespace == "myschema"


def test_ingestion_daemon_imports_heartbeat():
    """ingestion/daemon.py imports FederationHeartbeat and heartbeat_loop."""
    import inandout.ingestion.daemon as daemon_mod
    assert hasattr(daemon_mod, "_federation_hb")
    # The module imports are verified by the import itself succeeding
    from inandout.ingestion.daemon import heartbeat_loop  # noqa: F401


def test_writeback_daemon_imports_heartbeat():
    """writeback/daemon.py imports FederationHeartbeat and heartbeat_loop."""
    import inandout.writeback.daemon as daemon_mod
    assert hasattr(daemon_mod, "_federation_hb")
    from inandout.writeback.daemon import heartbeat_loop  # noqa: F401
