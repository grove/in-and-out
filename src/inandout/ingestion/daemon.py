"""Ingestion daemon — long-lived process managing polling loops and webhook receiver."""
from __future__ import annotations

import signal
import threading
from pathlib import Path
from typing import Any

import anyio
import structlog
import uvicorn
from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware
from prometheus_client import make_asgi_app as prometheus_make_asgi_app
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from inandout.config._duration import parse_duration
from inandout.config.loader import load_connector, load_ingestion_tool_config
from inandout.config.tool import IngestionToolConfig
from inandout.engine.control import ControlDispatcher, is_paused
from inandout.ingestion.engine import IngestionEngine
from inandout.ingestion.webhooks import handle_webhook
from inandout.observability.metrics import REGISTRY
from inandout.postgres.housekeeping import run_housekeeping
from inandout.postgres.pool import create_pool
from inandout.postgres.version_check import SchemaVersionMismatch, check_schema_version
from inandout.secrets import configure_backend


def _setup_credential_backend(config: IngestionToolConfig) -> None:
    """Configure the module-level secret backend from tool config."""
    from inandout.secrets.backend import (
        AwsSecretsManagerBackend,
        EnvSecretBackend,
        GcpSecretManagerBackend,
        VaultSecretBackend,
    )

    backend_type = config.credential_backend
    cfg = config.credential_backend_config

    if backend_type == "env":
        configure_backend(EnvSecretBackend(prefix=cfg.get("prefix", "INOUT_CREDENTIAL_")))
    elif backend_type == "vault":
        configure_backend(
            VaultSecretBackend(
                addr=cfg["addr"],
                token=cfg["token"],
                mount=cfg.get("mount", "secret"),
            )
        )
    elif backend_type == "aws_sm":
        configure_backend(AwsSecretsManagerBackend(region=cfg["region"]))
    elif backend_type == "gcp_sm":
        configure_backend(GcpSecretManagerBackend(project=cfg["project"]))
    else:
        logger.warning("unknown_credential_backend", backend=backend_type)

logger = structlog.get_logger(__name__)

# Drain flag — set by SIGTERM/SIGINT or a 'drain' control command.
# All polling loops check this at the top of each iteration and exit cleanly.
_draining: bool = False


def _trigger_drain(sig: int = 0, frame: object = None) -> None:  # noqa: ARG001
    """Set the drain flag; polling loops will exit after their current iteration."""
    global _draining
    _draining = True
    logger.info("ingestion_drain_signal_received", signal=sig)


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
    pool: Any = None,
    api_auth: Any = None,
) -> Any:
    from fastapi import FastAPI
    from inandout.api import build_api_router
    from inandout.ui import build_ui_router

    routes = [
        Route("/health", _health),
        Route("/ready", _ready),
        Mount("/metrics", prometheus_make_asgi_app(registry=REGISTRY)),
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

    # Mount FastAPI management API
    api_router = build_api_router(pool=pool)
    api_app = FastAPI(title="in-and-out management API", docs_url="/api/docs")
    api_app.include_router(api_router, prefix="/api")
    routes.append(Mount("/api", app=api_app))

    # Mount Web UI
    try:
        routes.append(build_ui_router())
    except Exception:
        pass  # UI mount is best-effort

    app = Starlette(routes=routes)
    otel_app = OpenTelemetryMiddleware(app)
    if api_auth is not None:
        from inandout.api.auth import BearerTokenMiddleware
        return BearerTokenMiddleware(otel_app, api_auth=api_auth)
    return otel_app


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
# Connector health persistence helpers
# ---------------------------------------------------------------------------

async def _mark_connector_unavailable(pool: Any, connector: str, datatype: str, reason: str) -> None:
    """Persist connector-unavailable status to inout_ops_connector_health (T1 #44)."""
    try:
        async with pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO inout_ops_connector_health
                    (connector, datatype, status, marked_unhealthy_at, reason, updated_at)
                VALUES (%s, %s, 'unavailable', NOW(), %s, NOW())
                ON CONFLICT (connector, datatype) DO UPDATE
                SET status = 'unavailable',
                    marked_unhealthy_at = COALESCE(
                        inout_ops_connector_health.marked_unhealthy_at, NOW()
                    ),
                    reason = EXCLUDED.reason,
                    updated_at = NOW()
                """,
                [connector, datatype, reason],
            )
            await conn.commit()
    except Exception:
        pass  # Health table may not exist yet — never mask the polling error


async def _mark_connector_healthy(pool: Any, connector: str, datatype: str) -> None:
    """Clear unavailable status in inout_ops_connector_health (T1 #44)."""
    try:
        async with pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO inout_ops_connector_health
                    (connector, datatype, status, last_healthy_at, marked_unhealthy_at, reason, updated_at)
                VALUES (%s, %s, 'healthy', NOW(), NULL, NULL, NOW())
                ON CONFLICT (connector, datatype) DO UPDATE
                SET status = 'healthy',
                    last_healthy_at = NOW(),
                    marked_unhealthy_at = NULL,
                    reason = NULL,
                    updated_at = NOW()
                """,
                [connector, datatype],
            )
            await conn.commit()
    except Exception:
        pass


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
    dtype_cfg: Any = None,
) -> None:
    from inandout.transport.circuit_breaker import CircuitState, get_circuit_breaker

    log = logger.bind(connector=connector_cfg.name, datatype=datatype)
    schedule = ingestion_cfg.schedule
    interval_secs = default_interval_secs if not schedule.interval else parse_duration(schedule.interval)
    log.info("polling_loop_started", interval_secs=interval_secs, cron=schedule.cron)
    cb = get_circuit_breaker(connector_cfg.name, datatype)

    while True:
        # Drain check — exit cleanly when SIGTERM received or 'drain' command issued
        if _draining:
            log.info("polling_loop_draining")
            break

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
            result = await engine.run_sync(connector_cfg, datatype, ingestion_cfg, dtype_cfg=dtype_cfg)
            if result.status in ("completed", "skipped"):
                cb.record_success()
                if cb.state == CircuitState.closed:
                    await _mark_connector_healthy(engine._pool, connector_cfg.name, datatype)
            elif result.status == "failed":
                cb.record_failure()
                if cb.state == CircuitState.open:
                    await _mark_connector_unavailable(
                        engine._pool, connector_cfg.name, datatype,
                        reason=result.error_message or "consecutive failures exceeded threshold",
                    )
            log.info("poll_complete", status=result.status)
        except Exception as exc:
            cb.record_failure()
            log.error("poll_error", error=str(exc))
            if cb.state == CircuitState.open:
                await _mark_connector_unavailable(
                    engine._pool, connector_cfg.name, datatype,
                    reason=str(exc),
                )

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
# Housekeeping loop
# ---------------------------------------------------------------------------

async def _housekeeping_loop(
    pool: Any,
    config: IngestionToolConfig,
    connector_datatypes: list[tuple[str, str]],
    interval_secs: float,
) -> None:
    log = logger.bind(component="housekeeping_loop")
    while True:
        if _draining:
            log.info("housekeeping_loop_draining")
            break
        await anyio.sleep(interval_secs)
        if _draining:  # re-check after sleep so we don't run after drain
            break
        try:
            await run_housekeeping(pool, config.housekeeping, connector_datatypes)
        except Exception as exc:
            log.error("housekeeping_failed", error=str(exc))


async def _sla_check_loop(
    pool: Any,
    connector_configs: list,
    interval_secs: float = 60.0,
) -> None:
    """Dedicated SLA check loop that runs every 60s independent of polling loops."""
    from inandout.observability.sla import check_all_slas

    log = logger.bind(component="sla_check_loop")
    log.info("sla_check_loop_started")
    while True:
        if _draining:
            log.info("sla_check_loop_draining")
            break
        await anyio.sleep(interval_secs)
        if _draining:
            break
        try:
            results = await check_all_slas(pool, connector_configs)
            if results:
                violated_count = sum(1 for v in results.values() if v)
                log.info("sla_check_complete", checked=len(results), violated=violated_count)
        except Exception as exc:
            log.error("sla_check_failed", error=str(exc))


# ---------------------------------------------------------------------------
# SIGHUP hot-reload support
# ---------------------------------------------------------------------------

def _check_api_version_deprecations(connector_configs: list) -> None:
    """A6: Check api_version_deprecation_date for all connectors and log warnings/errors."""
    import datetime

    _log = logger.bind(component="api_version_check")
    today = datetime.date.today()
    for cfg in connector_configs:
        connector_cfg = cfg.connector
        dep_date_str = getattr(connector_cfg, "api_version_deprecation_date", None)
        if not dep_date_str:
            continue
        warning_days = getattr(connector_cfg, "api_version_warning_days", 60)
        try:
            dep_date = datetime.date.fromisoformat(dep_date_str)
        except ValueError:
            continue
        days_remaining = (dep_date - today).days
        if days_remaining < 0:
            _log.error(
                "api_version_past_deprecation_date",
                connector=connector_cfg.name,
                api_version=connector_cfg.api_version,
                deprecation_date=dep_date_str,
                days_past=abs(days_remaining),
            )
        elif days_remaining <= warning_days:
            _log.warning(
                "api_version_approaching_deprecation",
                connector=connector_cfg.name,
                api_version=connector_cfg.api_version,
                deprecation_date=dep_date_str,
                days_remaining=days_remaining,
            )


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
    """Start one polling task per ingestion-capable (connector, datatype).

    A4: If connector_cfg.accounts is non-empty, spawn one loop per account
    with account-scoped config overrides.
    """
    from inandout.config.connector import ConnectorConfig

    for connector_file_cfg in connector_configs:
        connector_cfg = connector_file_cfg.connector
        accounts = getattr(connector_cfg, "accounts", [])

        if accounts:
            # A4: multi-account — spawn one loop per account per datatype
            for account in accounts:
                # Build account-scoped connector config
                import copy
                account_connector_cfg = copy.deepcopy(connector_cfg)
                # Override credential_ref and base_url if provided
                if account.base_url is not None:
                    # Replace connection.base_url
                    object.__setattr__(
                        account_connector_cfg.connection, "base_url", account.base_url
                    )
                # Embed account_id in connector name for scoping
                object.__setattr__(
                    account_connector_cfg,
                    "name",
                    f"{connector_cfg.name}",  # keep original name; account_id tracked via log
                )

                log_base = logger.bind(
                    connector=connector_cfg.name,
                    account_id=account.account_id,
                )
                log_base.info("polling_loop_started", account_id=account.account_id)

                for dtype_name, dtype_cfg in connector_cfg.datatypes.items():
                    if dtype_cfg.ingestion is None:
                        continue
                    tg.start_soon(
                        _polling_loop,
                        engine,
                        account_connector_cfg,
                        dtype_name,
                        dtype_cfg.ingestion,
                        default_interval_secs,
                        paused_connectors,
                        dtype_cfg,
                    )
        else:
            # Standard single-account path
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
                    dtype_cfg,
                )


# ---------------------------------------------------------------------------
# Main daemon entrypoint
# ---------------------------------------------------------------------------

async def run_ingestion_daemon(config_path: str | Path) -> None:
    from inandout.observability import configure_logging, configure_metrics, configure_tracing

    config: IngestionToolConfig = load_ingestion_tool_config(config_path)

    _setup_credential_backend(config)

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

    # Refuse to start if schema version doesn't match (B7)
    try:
        await check_schema_version(pool)
        log.info("schema_version_ok")
    except (SchemaVersionMismatch, RuntimeError) as exc:
        log.error("schema_version_mismatch", error=str(exc))
        await pool.close()
        raise SystemExit(1) from exc

    # Create read pool if configured
    from inandout.postgres.pool import create_read_pool
    read_pool = await create_read_pool(config.database)

    # Create event publisher if configured
    publisher = None
    event_cfg = getattr(config, "event_output", None)
    if event_cfg is not None and getattr(event_cfg, "enabled", False):
        try:
            from inandout.events.publisher import get_publisher
            publisher = get_publisher(event_cfg, pool)
            log.info("event_publisher_created", backend=event_cfg.backend)
        except Exception as exc:
            log.warning("event_publisher_init_failed", error=str(exc))

    engine = IngestionEngine(pool, read_pool=read_pool, publisher=publisher)

    paused_connectors: set[tuple[str, str]] = set()
    dispatcher = ControlDispatcher(pool, paused_connectors, target_tool="ingestion", drain_callback=_trigger_drain)

    control_poll_secs = parse_duration(config.control_table.poll_interval)
    default_interval_secs = parse_duration(
        config.defaults.scheduling.default_interval if config.defaults.scheduling else "5m"
    )
    housekeeping_interval_secs = parse_duration(config.housekeeping.interval)

    # Install SIGTERM/SIGINT handler for graceful drain (polls loops exit after current iteration)
    try:
        signal.signal(signal.SIGTERM, _trigger_drain)
        signal.signal(signal.SIGINT, _trigger_drain)
    except (OSError, AttributeError):
        pass  # Windows or restricted environment

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

    # Apply topological sort so connectors with dependencies run after their dependencies
    try:
        from inandout.config.dependency_graph import topological_sort
        connector_configs = topological_sort(connector_configs)
        log.info(
            "connector_start_order",
            order=[c.connector.name for c in connector_configs],
        )
    except Exception as exc:
        log.warning("connector_sort_failed", error=str(exc))

    # Collect (connector, datatype) pairs for housekeeping
    connector_datatypes: list[tuple[str, str]] = [
        (cfg.connector.name, dtype_name)
        for cfg in connector_configs
        for dtype_name in cfg.connector.datatypes
    ]

    # Build HTTP app (health + webhook routes + metrics)
    host, port_str = config.health_server.listen.rsplit(":", 1)
    health_server_config = uvicorn.Config(
        _build_app(engine, connector_configs, pool=pool, api_auth=config.api_auth),
        host=host,
        port=int(port_str),
        log_level="warning",
    )
    health_server = uvicorn.Server(health_server_config)

    # Build webhook-only server if configured
    webhook_server_cfg = getattr(config, "webhook_server", None)
    webhook_server: uvicorn.Server | None = None
    if webhook_server_cfg is not None and getattr(webhook_server_cfg, "listen", None):
        from inandout.ingestion.webhook_server import build_webhook_app

        webhook_app = build_webhook_app(engine, connector_configs, webhook_server_cfg)
        webhook_host, webhook_port_str = webhook_server_cfg.listen.rsplit(":", 1)

        webhook_uvicorn_kwargs: dict = {}
        tls_cert = getattr(webhook_server_cfg, "tls_cert_file", None)
        tls_key = getattr(webhook_server_cfg, "tls_key_file", None)
        if tls_cert and tls_key:
            webhook_uvicorn_kwargs["ssl_certfile"] = tls_cert
            webhook_uvicorn_kwargs["ssl_keyfile"] = tls_key
        elif tls_cert or tls_key:
            log.warning(
                "webhook_tls_incomplete",
                reason="Both tls_cert_file and tls_key_file required for TLS; proceeding without TLS",
            )

        webhook_server_config = uvicorn.Config(
            webhook_app,
            host=webhook_host,
            port=int(webhook_port_str),
            log_level="warning",
            **webhook_uvicorn_kwargs,
        )
        webhook_server = uvicorn.Server(webhook_server_config)

    async def _run_health_server() -> None:
        await health_server.serve()

    async def _apply_connector_changes(
        outer_tg: Any,
        changed_paths: set[Any] | None = None,
        new_configs: list | None = None,
    ) -> None:
        """Apply connector config changes — handles adds, updates, and removals."""
        if new_configs is None:
            new_configs = _load_connectors()
        old_names = {c.connector.name for c in connector_configs}
        new_names = {c.connector.name for c in new_configs}
        added = new_names - old_names
        removed = old_names - new_names
        updated = new_names & old_names

        if added:
            log.info("connectors_added", names=sorted(added))
        if removed:
            for name in removed:
                log.info("connector_removed_requires_restart", connector=name)

        # Handle updated connectors (config changed, same name)
        old_map = {c.connector.name: c for c in connector_configs}
        for cfg in new_configs:
            name = cfg.connector.name
            if name in updated and name in old_map:
                old_cfg = old_map[name]
                if cfg.connector != old_cfg.connector:
                    log.info("connector_config_reloaded", connector=name)
                    # Update in-memory config
                    for i, c in enumerate(connector_configs):
                        if c.connector.name == name:
                            connector_configs[i] = cfg
                            break

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
                        dtype_cfg,
                    )

    async def _hot_reload_watcher(outer_tg: Any) -> None:
        """Watch the reload flag; on SIGHUP reload connectors and restart polling tasks."""
        # Try file watcher first (watchfiles), fall back to SIGHUP
        try:
            from inandout.ingestion.watcher import hot_reload_loop

            async def _on_file_change(changed_paths: set[Any]) -> None:
                log.info("connector_files_changed", count=len(changed_paths))
                # Re-parse only the changed files
                new_configs = []
                for p in changed_paths:
                    try:
                        cfg = load_connector(p)
                        new_configs.append(cfg)
                        log.info("connector_reloaded_from_file", path=str(p), connector=cfg.connector.name)
                    except Exception as exc:
                        log.error("connector_reload_failed", path=str(p), error=str(exc))
                # For files that weren't changed, keep existing configs
                changed_connector_names = {c.connector.name for c in new_configs}
                unchanged = [c for c in connector_configs if c.connector.name not in changed_connector_names]
                merged = unchanged + new_configs
                await _apply_connector_changes(outer_tg, changed_paths=changed_paths, new_configs=merged)

            connectors_path = Path(config.connectors_dir)
            if connectors_path.exists():
                await hot_reload_loop(connectors_path, _on_file_change)
                return
        except ImportError:
            pass

        # Fallback: SIGHUP-based reload
        while True:
            await anyio.sleep(1.0)
            if not reload_flag.is_set():
                continue
            reload_flag.clear()
            log.info("sighup_received_reloading_connectors")
            new_configs = _load_connectors()
            await _apply_connector_changes(outer_tg, new_configs=new_configs)

    # Discover and register plugin hooks from installed packages
    try:
        from inandout.plugins.discovery import discover_and_register_hooks
        n_hooks = discover_and_register_hooks()
        log.info("plugin_hooks_registered", count=n_hooks)
    except Exception as exc:
        log.warning("plugin_hook_discovery_failed", error=str(exc))

    # Prepare federation reporter if enabled
    federation_reporter = None
    if config.federation.enabled:
        from inandout.federation.reporter import FederationReporter, _default_instance_id
        instance_id = _default_instance_id()
        federation_reporter = FederationReporter(pool, instance_id, config.namespace)
        log.info("federation_enabled", instance_id=instance_id)

    # A6: Check API version deprecation dates for all loaded connectors
    _check_api_version_deprecations(connector_configs)

    log.info("daemon_started")

    async def _plugin_reload_loop() -> None:
        """Poll plugin versions and re-register on changes."""
        from inandout.plugins.version_watcher import watch_plugin_versions
        from inandout.plugins.discovery import discover_and_register_hooks

        async def _on_plugin_change(pkg_name: str, old_version: str, new_version: str) -> None:
            log.info(
                "plugin_package_updated",
                package=pkg_name,
                old_version=old_version,
                new_version=new_version,
            )
            try:
                n = discover_and_register_hooks()
                log.info("plugin_hooks_reregistered", count=n, trigger=pkg_name)
            except Exception as exc:
                log.warning("plugin_hooks_reregister_failed", error=str(exc))

        try:
            await watch_plugin_versions(_on_plugin_change, should_stop=lambda: _draining)
        except Exception as exc:
            log.warning("plugin_reload_loop_exited", error=str(exc))

    async def _federation_loop() -> None:
        """Periodically report health data to the federation table."""
        if federation_reporter is None:
            return

        from inandout.transport.circuit_breaker import get_circuit_breaker, CircuitState

        while True:
            # Exit cleanly on drain/SIGTERM
            if _draining:
                break
            await anyio.sleep(config.federation.report_interval_secs)
            for connector_file_cfg in connector_configs:
                connector_cfg = connector_file_cfg.connector
                for dtype_name in connector_cfg.datatypes:
                    try:
                        cb = get_circuit_breaker(connector_cfg.name, dtype_name)
                        if cb.state == CircuitState.open:
                            cb_score = 0.0
                        elif cb.state == CircuitState.half_open:
                            cb_score = 0.5
                        else:
                            cb_score = 1.0

                        # Get last sync info
                        last_sync_at = None
                        try:
                            async with pool.connection() as conn:
                                row = await (await conn.execute(
                                    """
                                    SELECT finished_at FROM inout_ops_sync_run
                                    WHERE connector = %s AND datatype = %s
                                    ORDER BY started_at DESC LIMIT 1
                                    """,
                                    [connector_cfg.name, dtype_name],
                                )).fetchone()
                                if row:
                                    last_sync_at = row[0]
                        except Exception:
                            pass

                        await federation_reporter.report(
                            connector=connector_cfg.name,
                            datatype=dtype_name,
                            health_score=cb_score,
                            last_sync_at=last_sync_at,
                            circuit_state=str(cb.state),
                            dl_depth=0,
                        )
                    except Exception as exc:
                        log.warning(
                            "federation_report_error",
                            connector=connector_cfg.name,
                            datatype=dtype_name,
                            error=str(exc),
                        )

    async def _run_webhook_server() -> None:
        if webhook_server is not None:
            await webhook_server.serve()

    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(_run_health_server)
            if webhook_server is not None:
                tg.start_soon(_run_webhook_server)
            tg.start_soon(_control_table_poller, dispatcher, engine, control_poll_secs)
            tg.start_soon(_hot_reload_watcher, tg)
            tg.start_soon(
                _housekeeping_loop,
                pool,
                config,
                connector_datatypes,
                housekeeping_interval_secs,
            )
            tg.start_soon(_sla_check_loop, pool, connector_configs)
            tg.start_soon(_plugin_reload_loop)
            if config.federation.enabled:
                tg.start_soon(_federation_loop)
            await _run_connector_tasks(tg, engine, connector_configs, default_interval_secs, paused_connectors)
    finally:
        log.info("daemon_stopping")
        await pool.close()
        log.info("daemon_stopped")
