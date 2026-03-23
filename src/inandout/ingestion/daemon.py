"""Ingestion daemon — long-lived process managing polling loops and webhook receiver."""
from __future__ import annotations

import signal
import threading
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
from inandout.engine.control import ControlDispatcher, is_paused
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


def _build_app(engine: IngestionEngine, connector_configs: list) -> Starlette:
    routes = [
        Route("/health", _health),
        Route("/ready", _ready),
    ]
    for connector_file_cfg in connector_configs:
        connector_cfg = connector_file_cfg.connector
        webhook_cfg = getattr(connector_cfg, "webhook", None)
        if webhook_cfg is None:
            continue

        def _make_handler(c_cfg: Any, w_cfg: Any) -> Any:
            async def _webhook_handler(request: Request) -> Any:
                return await handle_webhook(request, c_cfg, w_cfg, engine)
            return _webhook_handler

        routes.append(Route(webhook_cfg.path, _make_handler(connector_cfg, webhook_cfg), methods=["POST"]))

    return Starlette(routes=routes)


# ---------------------------------------------------------------------------
# Cron scheduling helper
# ---------------------------------------------------------------------------

def _next_interval_secs(schedule: Any, default_interval_secs: float) -> float:
    """Return seconds to sleep before the next poll tick."""
    if schedule.cron:
        try:
            import datetime
            from croniter import croniter
            now = datetime.datetime.now(datetime.timezone.utc)
            cron = croniter(schedule.cron, now)
            next_run = cron.get_next(datetime.datetime)
            return max(0.0, (next_run - now).total_seconds())
        except Exception as exc:
            logger.warning("cron_parse_failed", cron=schedule.cron, error=str(exc))
            return default_interval_secs
    return default_interval_secs


# ---------------------------------------------------------------------------
# Polling loop for one connector/datatype
# ---------------------------------------------------------------------------

async def _polling_loop(
    engine: IngestionEngine,
    connector_cfg: Any,
    datatype: str,
    ingestion_cfg: Any,
    default_interval_secs: float,
    paused_connectors: set[tuple[str, str]],
) -> None:
    from inandout.transport.circuit_breaker import get_circuit_breaker

    log = logger.bind(connector=connector_cfg.name, datatype=datatype)
    schedule = ingestion_cfg.schedule
    interval_secs = default_interval_secs if not schedule.interval else parse_duration(schedule.interval)
    log.info("polling_loop_started", interval_secs=interval_secs, cron=schedule.cron)
    cb = get_circuit_breaker(connector_cfg.name, datatype)

    while True:
        # Pause check
        if is_paused(paused_connectors, connector_cfg.name, datatype):
            log.info("polling_loop_paused")
            await anyio.sleep(interval_secs)
            continue

        # Circuit breaker check
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

        sleep_secs = _next_interval_secs(schedule, interval_secs)
        await anyio.sleep(sleep_secs)


# ---------------------------------------------------------------------------
# Control table poller
# ---------------------------------------------------------------------------

async def _control_table_poller(
    dispatcher: ControlDispatcher,
    engine: IngestionEngine,
    poll_secs: float,
) -> None:
    log = logger.bind(component="control_table_poller")
    log.info("control_table_poller_started")
    while True:
        try:
            count = await dispatcher.dispatch_pending(engine=engine)
            if count:
                log.info("control_commands_dispatched", count=count)
        except Exception as exc:
            log.error("control_table_poll_error", error=str(exc))
        await anyio.sleep(poll_secs)


# ---------------------------------------------------------------------------
# SIGHUP hot-reload support
# ---------------------------------------------------------------------------

def _make_reload_watcher() -> tuple[threading.Event, Any]:
    """Return (flag, signal_handler) for SIGHUP hot-reload."""
    flag = threading.Event()

    def _handler(sig: int, frame: Any) -> None:
        flag.set()

    return flag, _handler


async def _run_connector_tasks(
    tg: Any,
    engine: IngestionEngine,
    connector_configs: list,
    default_interval_secs: float,
    paused_connectors: set[tuple[str, str]],
) -> None:
    """Start one polling task per ingestion-capable (connector, datatype)."""
    for connector_file_cfg in connector_configs:
        connector_cfg = connector_file_cfg.connector
        for dtype_name, dtype_cfg in connector_cfg.datatypes.items():
            if dtype_cfg.ingestion is None:
                continue
            tg.start_soon(
                _polling_loop,
                engine,
                connector_cfg,
                dtype_name,
                dtype_cfg.ingestion,
                default_interval_secs,
                paused_connectors,
            )


# ---------------------------------------------------------------------------
# Main daemon entrypoint
# ---------------------------------------------------------------------------

async def run_ingestion_daemon(config_path: str | Path) -> None:
    from inandout.observability import configure_logging, configure_metrics, configure_tracing

    config: IngestionToolConfig = load_ingestion_tool_config(config_path)

    configure_logging(format=config.observability.logging.format, level=config.observability.logging.level)
    configure_metrics()
    configure_tracing(
        enabled=config.observability.tracing.enabled,
        otlp_endpoint=config.observability.tracing.otlp_endpoint,
        sample_rate=config.observability.tracing.sample_rate,
    )

    log = logger.bind(component="ingestion_daemon")
    log.info("daemon_starting", connectors_dir=config.connectors_dir)

    pool = await create_pool(config.database)
    engine = IngestionEngine(pool)

    paused_connectors: set[tuple[str, str]] = set()
    dispatcher = ControlDispatcher(pool, paused_connectors)

    control_poll_secs = parse_duration(config.control_table.poll_interval)
    default_interval_secs = parse_duration(
        config.defaults.scheduling.default_interval if config.defaults.scheduling else "5m"
    )

    # Install SIGHUP handler for hot-reload
    reload_flag, sighup_handler = _make_reload_watcher()
    try:
        signal.signal(signal.SIGHUP, sighup_handler)
    except (OSError, AttributeError):
        pass  # Windows or restricted environment — skip SIGHUP

    def _load_connectors() -> list:
        connectors_dir = Path(config.connectors_dir)
        loaded = []
        if not connectors_dir.exists():
            return loaded
        for yaml_path in sorted(connectors_dir.glob("*.yaml")):
            try:
                cfg = load_connector(yaml_path)
                loaded.append(cfg)
                log.info("connector_loaded", connector=cfg.connector.name)
            except Exception as exc:
                log.error("connector_load_failed", path=str(yaml_path), error=str(exc))
        return loaded

    connector_configs = _load_connectors()

    # Build HTTP app (health + webhook routes)
    host, port_str = config.health_server.listen.rsplit(":", 1)
    health_server_config = uvicorn.Config(
        _build_app(engine, connector_configs), host=host, port=int(port_str), log_level="warning"
    )
    health_server = uvicorn.Server(health_server_config)

    async def _run_health_server() -> None:
        await health_server.serve()

    async def _hot_reload_watcher(outer_tg: Any) -> None:
        """Watch the reload flag; on SIGHUP reload connectors and restart polling tasks."""
        while True:
            await anyio.sleep(1.0)
            if not reload_flag.is_set():
                continue
            reload_flag.clear()
            log.info("sighup_received_reloading_connectors")
            new_configs = _load_connectors()
            old_names = {c.connector.name for c in connector_configs}
            new_names = {c.connector.name for c in new_configs}
            added = new_names - old_names
            removed = old_names - new_names
            if added:
                log.info("connectors_added", names=sorted(added))
            if removed:
                log.info("connectors_removed_requires_restart", names=sorted(removed))
            # Start polling loops for newly added connectors
            for cfg in new_configs:
                if cfg.connector.name in added:
                    connector_configs.append(cfg)
                    for dtype_name, dtype_cfg in cfg.connector.datatypes.items():
                        if dtype_cfg.ingestion is None:
                            continue
                        outer_tg.start_soon(
                            _polling_loop,
                            engine,
                            cfg.connector,
                            dtype_name,
                            dtype_cfg.ingestion,
                            default_interval_secs,
                            paused_connectors,
                        )

    log.info("daemon_started")

    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(_run_health_server)
            tg.start_soon(_control_table_poller, dispatcher, engine, control_poll_secs)
            tg.start_soon(_hot_reload_watcher, tg)
            await _run_connector_tasks(tg, engine, connector_configs, default_interval_secs, paused_connectors)
    finally:
        log.info("daemon_stopping")
        await pool.close()
        log.info("daemon_stopped")
