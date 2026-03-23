"""Ingestion daemon — long-lived process managing polling loops and webhook receiver."""
from __future__ import annotations

import signal
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
from inandout.config.loader import load_connector, load_ingestion_tool_config
from inandout.config.tool import IngestionToolConfig
from inandout.ingestion.engine import IngestionEngine
from inandout.postgres.pool import create_pool

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
# Polling loop for one connector/datatype
# ---------------------------------------------------------------------------

async def _polling_loop(
    engine: IngestionEngine,
    connector_cfg: Any,
    datatype: str,
    ingestion_cfg: Any,
    interval_secs: float,
) -> None:
    log = logger.bind(connector=connector_cfg.name, datatype=datatype)
    log.info("polling_loop_started", interval_secs=interval_secs)
    while True:
        try:
            result = await engine.run_sync(connector_cfg, datatype, ingestion_cfg)
            log.info("poll_complete", status=result.status)
        except Exception as exc:
            log.error("poll_error", error=str(exc))
        await anyio.sleep(interval_secs)


# ---------------------------------------------------------------------------
# Control table poller
# ---------------------------------------------------------------------------

async def _control_table_poller(pool: Any, poll_secs: float) -> None:
    log = logger.bind(component="control_table_poller")
    log.info("control_table_poller_started")
    while True:
        # TODO: poll inout_ops_control for pending commands and execute them
        await anyio.sleep(poll_secs)


# ---------------------------------------------------------------------------
# Main daemon entrypoint
# ---------------------------------------------------------------------------

async def run_ingestion_daemon(config_path: str | Path) -> None:
    config: IngestionToolConfig = load_ingestion_tool_config(config_path)
    log = logger.bind(component="ingestion_daemon")
    log.info("daemon_starting", connectors_dir=config.connectors_dir)

    # Load all connector configs
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
    engine = IngestionEngine(pool)

    # Parse shutdown drain timeout
    drain_secs = parse_duration(config.shutdown.drain_timeout)
    control_poll_secs = parse_duration(config.control_table.poll_interval)

    # Build health server
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
            # Health endpoints
            tg.start_soon(_run_health_server)

            # Control table poller
            tg.start_soon(_control_table_poller, pool, control_poll_secs)

            # One polling loop per ingestion datatype
            for connector_file_cfg in connector_configs:
                connector_cfg = connector_file_cfg.connector
                for dtype_name, dtype_cfg in connector_cfg.datatypes.items():
                    if dtype_cfg.ingestion is None:
                        continue
                    schedule = dtype_cfg.ingestion.schedule
                    interval_secs = parse_duration(
                        schedule.interval or schedule.cron or "5m"
                    ) if schedule.interval else parse_duration(
                        config.defaults.scheduling.default_interval
                        if config.defaults.scheduling else "5m"
                    )
                    tg.start_soon(
                        _polling_loop,
                        engine,
                        connector_cfg,
                        dtype_name,
                        dtype_cfg.ingestion,
                        interval_secs,
                    )

    finally:
        log.info("daemon_stopping")
        await pool.close()
        log.info("daemon_stopped")
