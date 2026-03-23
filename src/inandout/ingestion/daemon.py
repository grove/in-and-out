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
from inandout.ingestion.webhooks import handle_webhook
from inandout.postgres.pool import create_pool

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Health / readiness endpoints
# ---------------------------------------------------------------------------

async def _health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


async def _ready(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ready", "connectors": []})


def _build_app(
    engine: IngestionEngine,
    connector_configs: list,
) -> Starlette:
    """Build the Starlette app with health endpoints and per-connector webhook routes."""
    from starlette.routing import Route

    routes = [
        Route("/health", _health),
        Route("/ready", _ready),
    ]

    # Register one POST route per connector that has a webhook config
    for connector_file_cfg in connector_configs:
        connector_cfg = connector_file_cfg.connector
        webhook_cfg = getattr(connector_cfg, "webhook", None)
        if webhook_cfg is None:
            continue

        # Capture loop variables in closure
        def _make_handler(c_cfg: Any, w_cfg: Any) -> Any:
            async def _webhook_handler(request: Request) -> Any:
                return await handle_webhook(request, c_cfg, w_cfg, engine)
            return _webhook_handler

        routes.append(Route(webhook_cfg.path, _make_handler(connector_cfg, webhook_cfg), methods=["POST"]))

    return Starlette(routes=routes)


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
    from inandout.transport.circuit_breaker import get_circuit_breaker

    log = logger.bind(connector=connector_cfg.name, datatype=datatype)
    log.info("polling_loop_started", interval_secs=interval_secs)
    cb = get_circuit_breaker(connector_cfg.name, datatype)

    while True:
        if not cb.allow_request():
            log.warning("poll_skipped_circuit_open", state=cb.state)
            await anyio.sleep(interval_secs)
            continue
        try:
            result = await engine.run_sync(connector_cfg, datatype, ingestion_cfg)
            if result.status in ("completed", "skipped"):
                cb.record_success()
            elif result.status == "failed":
                cb.record_failure()
            log.info("poll_complete", status=result.status)
        except Exception as exc:
            cb.record_failure()
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
    from inandout.observability import configure_logging, configure_metrics, configure_tracing

    config: IngestionToolConfig = load_ingestion_tool_config(config_path)

    configure_logging(
        format=config.observability.logging.format,
        level=config.observability.logging.level,
    )
    configure_metrics()
    configure_tracing(
        enabled=config.observability.tracing.enabled,
        otlp_endpoint=config.observability.tracing.otlp_endpoint,
        sample_rate=config.observability.tracing.sample_rate,
    )

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

    # Build app (health + webhook routes)
    host, port_str = config.health_server.listen.rsplit(":", 1)
    health_server_config = uvicorn.Config(
        _build_app(engine, connector_configs), host=host, port=int(port_str), log_level="warning"
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
