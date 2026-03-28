"""Unit tests for writeback daemon circuit-breaker alerting wiring."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from inandout.alerting.config import AlertingConfig, WebhookAlertingConfig
from inandout.alerting.dispatcher import AlertDispatcher, AlertEventType
from inandout.transport.circuit_breaker import CircuitState, reset_all


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clear_cb_registry():
    reset_all()
    yield
    reset_all()


def _make_connector(failure_threshold: int = 2, recovery_timeout: float = 60.0) -> MagicMock:
    mock = MagicMock()
    mock.name = "sf"
    mock.circuit_breaker = {
        "failure_threshold": failure_threshold,
        "recovery_timeout": recovery_timeout,
    }
    return mock


def _make_writeback_cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.protection_level = "optimistic"
    return cfg


def _make_engine(
    *,
    processed: int = 1,
    failed: int = 0,
    raise_exc: Exception | None = None,
) -> MagicMock:
    result = MagicMock()
    result.processed = processed
    result.failed = failed
    result.skipped = 0

    engine = MagicMock()
    if raise_exc:
        engine.run_writeback_cycle = AsyncMock(side_effect=raise_exc)
    else:
        engine.run_writeback_cycle = AsyncMock(return_value=result)
    return engine


def _make_dispatcher() -> tuple[AlertDispatcher, list[tuple]]:
    """Return an AlertDispatcher and a list that records dispatched events."""
    events: list[tuple] = []

    cfg = AlertingConfig(
        enabled=True,
        webhook=WebhookAlertingConfig(url="https://example.com/alert"),
    )
    dispatcher = AlertDispatcher(cfg)
    # Patch internal _dispatch_webhook to capture calls without HTTP
    original_dispatch = dispatcher.dispatch

    async def spy_dispatch(event_type, *, connector, datatype, message="", **kwargs):
        events.append((event_type, connector, datatype, message))

    dispatcher.dispatch = spy_dispatch  # type: ignore[method-assign]
    return dispatcher, events


# ---------------------------------------------------------------------------
# _writeback_polling_loop circuit-breaker alerting
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_no_alert_when_writes_succeed():
    """Successful writes should not trigger any alert."""
    import inandout.writeback.daemon as daemon_mod

    _, events = _make_dispatcher()

    connector = _make_connector(failure_threshold=2)
    engine = _make_engine(processed=3, failed=0)

    stop_after = [0]

    async def fake_sleep(_: float) -> None:
        stop_after[0] += 1
        if stop_after[0] >= 1:
            import inandout.writeback.daemon as d
            d._draining = True

    with patch("inandout.writeback.daemon.anyio.sleep", new=fake_sleep):
        daemon_mod._draining = False
        await daemon_mod._writeback_polling_loop(
            engine,
            connector,
            "contacts",
            _make_writeback_cfg(),
            "inout_delta_sf_contacts",
            interval_secs=0.0,
        )
        daemon_mod._draining = False  # reset

    assert events == []


@pytest.mark.anyio
async def test_circuit_breaker_open_fires_alert():
    """When failure_threshold is reached, circuit_breaker_open alert fires."""
    import inandout.writeback.daemon as daemon_mod

    dispatcher, events = _make_dispatcher()

    # Patch the module-level _alert_dispatcher
    original = daemon_mod._alert_dispatcher
    daemon_mod._alert_dispatcher = dispatcher

    connector = _make_connector(failure_threshold=1)  # opens on first failure
    engine = _make_engine(processed=0, failed=1)

    stop_after = [0]

    async def fake_sleep(_: float) -> None:
        stop_after[0] += 1
        if stop_after[0] >= 1:
            daemon_mod._draining = True

    try:
        with patch("inandout.writeback.daemon.anyio.sleep", new=fake_sleep):
            daemon_mod._draining = False
            await daemon_mod._writeback_polling_loop(
                engine,
                connector,
                "contacts",
                _make_writeback_cfg(),
                "inout_delta_sf_contacts",
                interval_secs=0.0,
            )
            daemon_mod._draining = False
    finally:
        daemon_mod._alert_dispatcher = original

    assert any(e[0] == AlertEventType.circuit_breaker_open for e in events)


@pytest.mark.anyio
async def test_exception_with_open_cb_fires_connector_unavailable():
    """An exception on a cycle that trips the CB fires connector_unavailable."""
    import inandout.writeback.daemon as daemon_mod

    dispatcher, events = _make_dispatcher()

    original = daemon_mod._alert_dispatcher
    daemon_mod._alert_dispatcher = dispatcher

    connector = _make_connector(failure_threshold=1)
    engine = _make_engine(raise_exc=RuntimeError("Connection refused"))

    stop_after = [0]

    async def fake_sleep(_: float) -> None:
        stop_after[0] += 1
        if stop_after[0] >= 1:
            daemon_mod._draining = True

    try:
        with patch("inandout.writeback.daemon.anyio.sleep", new=fake_sleep):
            daemon_mod._draining = False
            await daemon_mod._writeback_polling_loop(
                engine,
                connector,
                "contacts",
                _make_writeback_cfg(),
                "inout_delta_sf_contacts",
                interval_secs=0.0,
            )
            daemon_mod._draining = False
    finally:
        daemon_mod._alert_dispatcher = original

    assert any(e[0] == AlertEventType.connector_unavailable for e in events)


@pytest.mark.anyio
async def test_circuit_breaker_closed_alert_fires_on_recovery():
    """After CB opens then recovers, circuit_breaker_closed is dispatched."""
    import inandout.writeback.daemon as daemon_mod
    from inandout.transport.circuit_breaker import get_circuit_breaker

    dispatcher, events = _make_dispatcher()

    original = daemon_mod._alert_dispatcher
    daemon_mod._alert_dispatcher = dispatcher

    # Use a long recovery_timeout so the CB stays in 'open' state (not half_open)
    connector = _make_connector(failure_threshold=1, recovery_timeout=3600.0)

    # Open the circuit manually — stays open because recovery_timeout is large
    cb = get_circuit_breaker("sf", "contacts", failure_threshold=1, recovery_timeout=3600.0)
    cb.record_failure()  # opens it (threshold=1)
    assert cb.state == CircuitState.open

    # The engine succeeds — this call should trigger the closed alert
    engine = _make_engine(processed=1, failed=0)

    stop_after = [0]

    async def fake_sleep(_: float) -> None:
        stop_after[0] += 1
        if stop_after[0] >= 1:
            daemon_mod._draining = True

    try:
        with patch("inandout.writeback.daemon.anyio.sleep", new=fake_sleep):
            daemon_mod._draining = False
            await daemon_mod._writeback_polling_loop(
                engine,
                connector,
                "contacts",
                _make_writeback_cfg(),
                "inout_delta_sf_contacts",
                interval_secs=0.0,
            )
            daemon_mod._draining = False
    finally:
        daemon_mod._alert_dispatcher = original

    assert any(e[0] == AlertEventType.circuit_breaker_closed for e in events)
