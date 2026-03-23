"""Writeback daemon — long-lived process polling delta tables and dispatching HTTP writes."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import anyio
import structlog
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from inandout.config._duration import parse_duration
from inandout.config.loader import load_connector, load_writeback_tool_config
from inandout.config.tool import WritebackToolConfig
from inandout.engine.control import ControlDispatcher
from inandout.postgres.pool import create_pool
from inandout.writeback.engine import WritebackEngine

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Health / readiness endpoints
# ---------------------------------------------------------------------------

async def _health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


async def _ready(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ready", "connectors": []})


def _build_health_app() -> Starlette:
    return Starlette(routes=[
        Route("/health", _health),
        Route("/ready", _ready),
    ])


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
    log = logger.bind(connector=connector_cfg.name, datatype=datatype)
    log.info("writeback_polling_loop_started", interval_secs=interval_secs, delta_table=delta_table)
    while True:
        try:
            result = await engine.run_writeback_cycle(
                connector_cfg, datatype, writeback_cfg, delta_table,
                max_concurrent_writes_override=max_concurrent_writes_override,
            )
            log.info(
                "writeback_poll_complete",
                processed=result.processed,
                skipped=result.skipped,
                failed=result.failed,
            )
        except Exception as exc:
            log.error("writeback_poll_error", error=str(exc))
        await anyio.sleep(interval_secs)


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
# Control table poller
# ---------------------------------------------------------------------------

async def _control_table_poller(
    dispatcher: ControlDispatcher,
    poll_secs: float,
) -> None:
    log = logger.bind(component="writeback_control_table_poller")
    log.info("control_table_poller_started")
    while True:
        try:
            count = await dispatcher.dispatch_pending(engine=None)
            if count:
                log.info("control_commands_dispatched", count=count)
        except Exception as exc:
            log.error("control_table_poll_error", error=str(exc))
        await anyio.sleep(poll_secs)


# ---------------------------------------------------------------------------
# Main daemon entrypoint
# ---------------------------------------------------------------------------

async def run_writeback_daemon(config_path: str | Path) -> None:
    from inandout.observability import configure_logging, configure_metrics, configure_tracing

    config: WritebackToolConfig = load_writeback_tool_config(config_path)

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
    engine = WritebackEngine(pool)

    paused_connectors: set[tuple[str, str]] = set()
    dispatcher = ControlDispatcher(pool, paused_connectors)

    control_poll_secs = parse_duration(config.control_table.poll_interval)
    batch_wait = config.defaults.batch.max_wait if config.defaults.batch else "5s"
    default_interval_secs = parse_duration(batch_wait)

    host, port_str = config.health_server.listen.rsplit(":", 1)
    health_server_config = uvicorn.Config(
        _build_health_app(), host=host, port=int(port_str), log_level="warning"
    )
    health_server = uvicorn.Server(health_server_config)

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
        await monitor_replication_slot(pool, config.replication_slot, _on_slot_fallback)

    log.info("daemon_started")

    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(_run_health_server)
            tg.start_soon(_control_table_poller, dispatcher, control_poll_secs)
            if config.replication_slot.slot_name:
                tg.start_soon(_slot_monitor_loop)

            for connector_file_cfg in connector_configs:
                connector_cfg = connector_file_cfg.connector
                for dtype_name, dtype_cfg in connector_cfg.datatypes.items():
                    if dtype_cfg.writeback is None:
                        continue
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
                        tg.start_soon(
                            _writeback_polling_loop,
                            engine,
                            connector_cfg,
                            dtype_name,
                            dtype_cfg.writeback,
                            delta_table,
                            default_interval_secs,
                            dtype_max_writes,
                        )
    finally:
        log.info("daemon_stopping")
        await pool.close()
        log.info("daemon_stopped")
