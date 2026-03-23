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
) -> None:
    log = logger.bind(connector=connector_cfg.name, datatype=datatype)
    log.info("writeback_polling_loop_started", interval_secs=interval_secs, delta_table=delta_table)
    while True:
        try:
            result = await engine.run_writeback_cycle(
                connector_cfg, datatype, writeback_cfg, delta_table
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

    log.info("daemon_started")

    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(_run_health_server)
            tg.start_soon(_control_table_poller, dispatcher, control_poll_secs)

            for connector_file_cfg in connector_configs:
                connector_cfg = connector_file_cfg.connector
                for dtype_name, dtype_cfg in connector_cfg.datatypes.items():
                    if dtype_cfg.writeback is None:
                        continue
                    delta_table = f"_delta_{connector_cfg.name}_{dtype_name}"
                    tg.start_soon(
                        _writeback_polling_loop,
                        engine,
                        connector_cfg,
                        dtype_name,
                        dtype_cfg.writeback,
                        delta_table,
                        default_interval_secs,
                    )
    finally:
        log.info("daemon_stopping")
        await pool.close()
        log.info("daemon_stopped")
