"""Writeback daemon — long-lived process polling delta tables and dispatching HTTP writes."""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import anyio
import structlog
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from inandout.alerting.dispatcher import AlertDispatcher, AlertEventType
from inandout.config._duration import parse_duration
from inandout.config.loader import load_connector, load_writeback_tool_config
from inandout.config.tool import WritebackToolConfig
from inandout.engine.control import ControlDispatcher
from inandout.federation.heartbeat import FederationHeartbeat, heartbeat_loop
from inandout.postgres.housekeeping import run_housekeeping
from inandout.postgres.pool import create_pool
from inandout.postgres.version_check import SchemaVersionMismatch, check_schema_version
from inandout.writeback.engine import WritebackEngine

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Health / readiness endpoints
# ---------------------------------------------------------------------------

async def _health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


# Module-level draining flag — set to True when SIGTERM/SIGINT received.
_draining: bool = False
_drain_event: asyncio.Event | None = None  # wakes sleeping loops immediately on drain
_uvicorn_servers: list = []  # registered by startup; signalled on drain
_alert_dispatcher: AlertDispatcher | None = None
_federation_hb: FederationHeartbeat | None = None


class _NoSignalServer(uvicorn.Server):
    """uvicorn.Server subclass that skips signal handler installation.

    uvicorn.Server.capture_signals() replaces SIGTERM/SIGINT with its own
    handler, overwriting the drain handler we install earlier.  By overriding
    capture_signals() as a no-op we keep our handler, and _trigger_drain()
    sets srv.should_exit = True directly to trigger a graceful uvicorn stop.
    """

    import contextlib as _contextlib

    @_contextlib.contextmanager  # type: ignore[misc]
    def capture_signals(self):
        yield


def _trigger_drain() -> None:
    """Called by ControlDispatcher when a 'drain' control command is received."""
    global _draining
    _draining = True
    if _drain_event is not None:
        try:
            asyncio.get_event_loop().call_soon_threadsafe(_drain_event.set)
        except RuntimeError:
            pass
    for srv in _uvicorn_servers:
        srv.should_exit = True
    logger.info("writeback_drain_control_command_received")


async def _sleep_or_drain(secs: float) -> None:
    """Sleep for *secs* but return immediately when the drain event fires."""
    if _draining or _drain_event is None:
        return
    try:
        await asyncio.wait_for(_drain_event.wait(), timeout=secs)
    except asyncio.TimeoutError:
        pass


async def _ready(request: Request) -> JSONResponse:
    if _draining:
        return JSONResponse({"status": "draining"}, status_code=503)
    # Schema-manager component gate check
    from inandout.postgres.component_gate import get_desired_state
    pool = request.app.state.pool if hasattr(request.app.state, "pool") else None
    if pool:
        desired = await get_desired_state(pool, "writeback")
        if desired == "stopped":
            return JSONResponse({"status": "stopped"}, status_code=503)
    return JSONResponse({"status": "ready", "connectors": []})


async def _mode(request: Request) -> JSONResponse:
    """Report the current schema-manager desired state for observability."""
    from inandout.postgres.component_gate import get_desired_state
    pool = request.app.state.pool if hasattr(request.app.state, "pool") else None
    state = "unknown"
    if _draining:
        state = "draining"
    elif pool:
        desired = await get_desired_state(pool, "writeback")
        state = desired or "waiting"  # waiting = schema-manager hasn't started yet
    return JSONResponse({"mode": state})


def _build_health_app(pool: Any = None) -> Starlette:
    app = Starlette(routes=[
        Route("/health", _health),
        Route("/ready", _ready),
        Route("/mode", _mode),
    ])
    app.state.pool = pool
    return app


# ---------------------------------------------------------------------------
# Writeback polling loop for one connector/datatype
# ---------------------------------------------------------------------------

async def _writeback_polling_loop(
    engine: WritebackEngine,
    connector_cfg: Any,
    datatype: str,
    writeback_cfg: Any,
    delta_table: str,
    interval_secs: float,
    max_concurrent_writes_override: int | None = None,
) -> None:
    global _draining
    log = logger.bind(connector=connector_cfg.name, datatype=datatype)
    log.info("writeback_polling_loop_started", interval_secs=interval_secs, delta_table=delta_table)
    from inandout.transport.circuit_breaker import CircuitState, get_circuit_breaker
    _cb_cfg = getattr(connector_cfg, "circuit_breaker", None) or {}
    _cb = get_circuit_breaker(
        connector_cfg.name,
        datatype,
        failure_threshold=int(_cb_cfg.get("failure_threshold", 5) if isinstance(_cb_cfg, dict) else 5),
        recovery_timeout=float(_cb_cfg.get("recovery_timeout", 60.0) if isinstance(_cb_cfg, dict) else 60.0),
    )

    # Schema-manager component gate — wraps each work cycle with an advisory
    # lock so the schema-manager can safely apply DDL between cycles.
    from inandout.postgres.component_gate import ComponentGate
    gate = ComponentGate(engine._pool, "writeback")

    while True:
        # Check draining flag at the TOP of each iteration (after completing current cycle)
        if _draining:
            log.info("writeback_draining_exiting_loop", connector=connector_cfg.name, datatype=datatype)
            break

        # Schema-manager component gate check — pause when desired='stopped',
        # and acquire shared advisory lock for the duration of the work cycle.
        async with gate.work_cycle() as cycle:
            if not cycle.allowed:
                log.debug("writeback_schema_manager_paused", state=cycle.state)
                await _sleep_or_drain(5)
                continue

            # If schema-manager set desired='shadow', skip actual API writes.
            # The engine itself handles this when cycle.state == 'shadow'
            # (writeback writes to shadow_log instead of calling APIs).
            _shadow_mode = cycle.state == "shadow"

            try:
                result = await engine.run_writeback_cycle(
                    connector_cfg, datatype, writeback_cfg, delta_table,
                    max_concurrent_writes_override=max_concurrent_writes_override,
                    shadow_mode=_shadow_mode,
                )
                log.info(
                    "writeback_poll_complete",
                    processed=result.processed,
                    skipped=result.skipped,
                    failed=result.failed,
                    shadow=_shadow_mode,
                )
                # Track circuit breaker: failures increment; success resets it
                if result.failed > 0:
                    _cb.record_failure()
                    if _cb.state == CircuitState.open and _alert_dispatcher:
                        await _alert_dispatcher.dispatch(
                            AlertEventType.circuit_breaker_open,
                            connector=connector_cfg.name,
                            datatype=datatype,
                            message=f"{result.failed} write(s) failed — circuit breaker opened",
                            detail={"failed": result.failed},
                        )
                else:
                    prev_state = _cb.state
                    _cb.record_success()
                    if prev_state == CircuitState.open and _cb.state == CircuitState.closed and _alert_dispatcher:
                        await _alert_dispatcher.dispatch(
                            AlertEventType.circuit_breaker_closed,
                            connector=connector_cfg.name,
                            datatype=datatype,
                            message="circuit breaker closed — writes recovering",
                        )
                # Update federation heartbeat after each writeback cycle
                if _federation_hb is not None:
                    import datetime
                    from inandout.observability.health_score import compute_health_score
                    _hs = await compute_health_score(engine._pool, connector_cfg.name, datatype)
                    _federation_hb.update(
                        connector=connector_cfg.name,
                        datatype=datatype,
                        health_score=_hs,
                        last_sync_at=datetime.datetime.now(datetime.UTC).isoformat(),
                        circuit_breaker_state=_cb.state.value,
                    )
            except Exception as exc:
                log.error("writeback_poll_error", error=str(exc))
                _cb.record_failure()
                if _cb.state == CircuitState.open and _alert_dispatcher:
                    await _alert_dispatcher.dispatch(
                        AlertEventType.connector_unavailable,
                        connector=connector_cfg.name,
                        datatype=datatype,
                        message=str(exc),
                    )
                if _federation_hb is not None:
                    from inandout.observability.health_score import compute_health_score
                    _hs_exc = await compute_health_score(engine._pool, connector_cfg.name, datatype)
                    _federation_hb.update(
                        connector=connector_cfg.name,
                        datatype=datatype,
                        health_score=_hs_exc,
                        circuit_breaker_state=_cb.state.value,
                    )

        # T2 #33: clamp sleep to batch_max_age_secs when set (close batch if oldest row exceeds age limit)
        _bma = getattr(writeback_cfg, "batch_max_age_secs", None)
        _effective_sleep = min(interval_secs, float(_bma)) if _bma is not None else interval_secs
        await _sleep_or_drain(_effective_sleep)
        if _draining:
            log.info("writeback_draining_exiting_loop", connector=connector_cfg.name, datatype=datatype)
            break


async def _writeback_loop_streaming(
    engine: WritebackEngine,
    pool: Any,
    connector_cfg: Any,
    datatype: str,
    writeback_cfg: Any,
    delta_table: str,
) -> None:
    """Streaming writeback mode: run a cycle on each LISTEN/NOTIFY notification."""
    from inandout.writeback.notify import listen_for_deltas

    log = logger.bind(connector=connector_cfg.name, datatype=datatype)
    log.info("writeback_streaming_loop_started", delta_table=delta_table)
    try:
        async for payload in listen_for_deltas(pool):
            # Drain check: stop processing new notifications once draining
            if _draining:
                log.info("writeback_streaming_loop_draining")
                break
            # payload format: "connector:datatype"
            if payload and ":" in payload:
                notified_connector, notified_datatype = payload.split(":", 1)
                if notified_connector != connector_cfg.name or notified_datatype != datatype:
                    continue
            try:
                result = await engine.run_writeback_cycle(
                    connector_cfg, datatype, writeback_cfg, delta_table
                )
                log.info(
                    "writeback_notify_cycle_complete",
                    processed=result.processed,
                    skipped=result.skipped,
                    failed=result.failed,
                )
            except Exception as exc:
                log.error("writeback_notify_cycle_error", error=str(exc))
    except Exception as exc:
        log.error("writeback_streaming_loop_error", error=str(exc))


# ---------------------------------------------------------------------------
# Housekeeping loop
# ---------------------------------------------------------------------------

async def _housekeeping_loop(
    pool: Any,
    config: WritebackToolConfig,
    connector_datatypes: list[tuple[str, str]],
    interval_secs: float,
) -> None:
    log = logger.bind(component="writeback_housekeeping_loop")
    log.info("writeback_housekeeping_loop_started", interval_secs=interval_secs)
    while True:
        if _draining:
            log.info("writeback_housekeeping_loop_draining")
            break
        await _sleep_or_drain(interval_secs)
        if _draining:  # re-check after sleep
            break
        try:
            await run_housekeeping(pool, config.housekeeping, connector_datatypes)
        except Exception as exc:
            log.error("writeback_housekeeping_failed", error=str(exc))


# ---------------------------------------------------------------------------
# Control table poller
# ---------------------------------------------------------------------------

async def _control_table_poller(
    dispatcher: ControlDispatcher,
    poll_secs: float,
) -> None:
    log = logger.bind(component="writeback_control_table_poller")
    log.info("control_table_poller_started")
    while True:
        if _draining:
            log.info("writeback_control_table_poller_draining")
            break
        try:
            count = await dispatcher.dispatch_pending(engine=None)
            if count:
                log.info("control_commands_dispatched", count=count)
        except Exception as exc:
            log.error("control_table_poll_error", error=str(exc))
        await _sleep_or_drain(poll_secs)
        if _draining:
            break


# ---------------------------------------------------------------------------
# Main daemon entrypoint
# ---------------------------------------------------------------------------

async def run_writeback_daemon(config_path: str | Path) -> None:
    from inandout.observability import configure_logging, configure_metrics, configure_tracing

    config: WritebackToolConfig = load_writeback_tool_config(config_path)

    # Initialise alert dispatcher if configured
    global _alert_dispatcher
    if config.alerting and config.alerting.enabled:
        _alert_dispatcher = AlertDispatcher(config.alerting)
        logger.info("writeback_alert_dispatcher_initialised")

    configure_logging(format=config.observability.logging.format, level=config.observability.logging.level)
    configure_metrics()
    configure_tracing(
        enabled=config.observability.tracing.enabled,
        otlp_endpoint=config.observability.tracing.otlp_endpoint,
        sample_rate=config.observability.tracing.sample_rate,
    )

    log = logger.bind(component="writeback_daemon")
    log.info("daemon_starting", connectors_dir=config.connectors_dir)

    connectors_dir = Path(config.connectors_dir)
    connector_configs = []
    if connectors_dir.exists():
        for yaml_path in sorted(connectors_dir.glob("*.yaml")):
            try:
                cfg = load_connector(yaml_path)
                connector_configs.append(cfg)
                log.info("connector_loaded", connector=cfg.connector.name)
            except Exception as exc:
                log.error("connector_load_failed", path=str(yaml_path), error=str(exc))

    pool = await create_pool(config.database)

    # Refuse to start if schema version doesn't match (B7)
    try:
        await check_schema_version(pool)
        log.info("schema_version_ok")
    except (SchemaVersionMismatch, RuntimeError) as exc:
        log.error("schema_version_mismatch", error=str(exc))
        await pool.close()
        raise SystemExit(1) from exc

    # Schema contract: freeze mode — tables must pre-exist (created by schema-manager).
    from inandout.postgres.schema import set_schema_contract
    try:
        async with pool.connection() as _gate_conn:
            _has_gate = await (await _gate_conn.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_name = 'component_state'"
            )).fetchone()
        if _has_gate:
            set_schema_contract("freeze")
            log.info("schema_contract_freeze", reason="schema-manager detected")
    except Exception:
        pass  # component_state table doesn't exist — keep evolve mode

    engine = WritebackEngine(pool)

    paused_connectors: set[tuple[str, str]] = set()
    dispatcher = ControlDispatcher(pool, paused_connectors, target_tool="writeback", drain_callback=_trigger_drain)

    control_poll_secs = parse_duration(config.control_table.poll_interval)
    batch_wait = config.defaults.batch.max_wait if config.defaults.batch else "5s"
    default_interval_secs = parse_duration(batch_wait)
    housekeeping_interval_secs = parse_duration(config.housekeeping.interval)

    # Collect (connector, datatype) pairs for housekeeping
    connector_datatypes: list[tuple[str, str]] = [
        (cfg.connector.name, dtype_name)
        for cfg in connector_configs
        for dtype_name in cfg.connector.datatypes
    ]

    host, port_str = config.health_server.listen.rsplit(":", 1)
    health_server_config = uvicorn.Config(
        _build_health_app(pool=pool), host=host, port=int(port_str), log_level="warning",
    )
    health_server = _NoSignalServer(health_server_config)
    _uvicorn_servers.append(health_server)

    async def _run_health_server() -> None:
        await health_server.serve()

    # Slot monitor fallback state
    _use_polling_fallback: dict[str, bool] = {"enabled": False}

    def _on_slot_fallback() -> None:
        _use_polling_fallback["enabled"] = True
        logger.warning("writeback_fallback_to_polling_due_to_slot_lag")

    async def _slot_monitor_loop() -> None:
        from inandout.writeback.slot_monitor import monitor_replication_slot
        if not config.replication_slot.slot_name:
            return
        await monitor_replication_slot(
            pool, config.replication_slot, _on_slot_fallback,
            should_stop=lambda: _draining,
        )

    # B2: check if scheduling is enabled
    scheduling_enabled = getattr(config, "scheduling_enabled", True)
    if not scheduling_enabled:
        log.warning("writeback_scheduler_disabled", reason="scheduling_enabled=False")

    # Initialise federation heartbeat
    global _federation_hb
    _federation_hb = FederationHeartbeat(namespace=getattr(config.database, "schema", "public"))

    log.info("daemon_started")

    import signal as _signal

    global _drain_event
    _drain_event = asyncio.Event()

    def _set_draining(sig: int, frame: Any) -> None:
        global _draining
        _draining = True
        if _drain_event is not None:
            try:
                asyncio.get_event_loop().call_soon_threadsafe(_drain_event.set)
            except RuntimeError:
                pass
        for srv in _uvicorn_servers:
            srv.should_exit = True
        log.info("writeback_drain_signal_received", signal=sig)

    try:
        _signal.signal(_signal.SIGTERM, _set_draining)
        _signal.signal(_signal.SIGINT, _set_draining)
    except (OSError, AttributeError):
        pass  # Windows or restricted environment

    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(_run_health_server)
            tg.start_soon(_control_table_poller, dispatcher, control_poll_secs)
            tg.start_soon(
                _housekeeping_loop,
                pool,
                config,
                connector_datatypes,
                housekeeping_interval_secs,
            )
            if config.replication_slot.slot_name:
                tg.start_soon(_slot_monitor_loop)
            tg.start_soon(
                heartbeat_loop, pool, _federation_hb, 30.0, lambda: _draining,
                None, _drain_event,
            )

            # B2: only start polling loops when scheduling is enabled
            if scheduling_enabled:
                for connector_file_cfg in connector_configs:
                    connector_cfg = connector_file_cfg.connector
                    for dtype_name, dtype_cfg in connector_cfg.datatypes.items():
                        if dtype_cfg.writeback is None:
                            continue
                        if dtype_cfg.writeback.use_desired_state_table:
                            delta_table = f"inout_dst_{connector_cfg.name}_{dtype_name}"
                        else:
                            delta_table = f"_delta_{connector_cfg.name}_{dtype_name}"
                        if dtype_cfg.writeback.streaming:
                            tg.start_soon(
                                _writeback_loop_streaming,
                                engine,
                                pool,
                                connector_cfg,
                                dtype_name,
                                dtype_cfg.writeback,
                                delta_table,
                            )
                        else:
                            dtype_max_writes = getattr(dtype_cfg, "max_concurrent_writes", None)
                            # T2 #35: use per-datatype poll_interval when configured
                            _dtype_interval = getattr(dtype_cfg.writeback, "poll_interval", None)
                            _loop_interval = float(_dtype_interval) if _dtype_interval else default_interval_secs
                            tg.start_soon(
                                _writeback_polling_loop,
                                engine,
                                connector_cfg,
                                dtype_name,
                                dtype_cfg.writeback,
                                delta_table,
                                _loop_interval,
                                dtype_max_writes,
                            )
    finally:
        log.info("writeback_drained")
        log.info("daemon_stopping")
        await pool.close()
        log.info("daemon_stopped")
