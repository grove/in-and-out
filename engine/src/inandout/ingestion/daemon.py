"""Ingestion daemon — long-lived process managing polling loops and webhook receiver."""
from __future__ import annotations

import signal
import threading
from pathlib import Path
from typing import Any

import anyio
import structlog
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware
from prometheus_client import make_asgi_app as prometheus_make_asgi_app

from inandout.alerting.dispatcher import AlertDispatcher, AlertEventType
from inandout.config._duration import parse_duration
from inandout.config.loader import load_connector, load_ingestion_tool_config
from inandout.config.tool import IngestionToolConfig
from inandout.engine.control import ControlDispatcher, is_paused
from inandout.federation.heartbeat import FederationHeartbeat, heartbeat_loop
from inandout.ingestion.engine import IngestionEngine
from inandout.ingestion.webhooks import handle_webhook
from inandout.observability.metrics import REGISTRY
from inandout.postgres.housekeeping import run_housekeeping
from inandout.postgres.pool import create_pool
from inandout.postgres.version_check import SchemaVersionMismatch, check_schema_version
from inandout.secrets import configure_backend

logger = structlog.get_logger(__name__)


def _check_api_deprecations(connector_configs: list[Any], log: Any) -> None:
    """Check for API version deprecations and emit warnings (T1 #39).
    
    Warns when api_version_deprecation_date is within api_version_warning_days days,
    or when api_deprecation_deadline has passed.
    """
    import datetime as _dt
    
    now = _dt.datetime.now(_dt.timezone.utc).date()
    
    for cfg in connector_configs:
        connector = cfg.connector
        
        # Check api_version_deprecation_date
        if connector.api_version_deprecation_date:
            try:
                deadline = _dt.date.fromisoformat(connector.api_version_deprecation_date)
                days_until = (deadline - now).days
                warning_days = connector.api_version_warning_days
                
                if days_until <= 0:
                    log.error(
                        "api_version_deprecated",
                        connector=connector.name,
                        api_version=connector.api_version,
                        deprecation_date=connector.api_version_deprecation_date,
                        message=f"API version {connector.api_version} is DEPRECATED (deadline passed)",
                    )
                elif days_until <= warning_days:
                    log.warning(
                        "api_version_deprecation_approaching",
                        connector=connector.name,
                        api_version=connector.api_version,
                        deprecation_date=connector.api_version_deprecation_date,
                        days_remaining=days_until,
                        message=f"API version {connector.api_version} will be deprecated in {days_until} days",
                    )
            except (ValueError, TypeError):
                log.warning(
                    "api_deprecation_date_invalid",
                    connector=connector.name,
                    date=connector.api_version_deprecation_date,
                )
        
        # Check legacy api_deprecation_deadline field
        if connector.api_deprecation_deadline:
            try:
                deadline = _dt.date.fromisoformat(connector.api_deprecation_deadline)
                days_until = (deadline - now).days
                
                if days_until <= 0:
                    log.error(
                        "api_deprecated",
                        connector=connector.name,
                        deprecation_deadline=connector.api_deprecation_deadline,
                        message="API is DEPRECATED (deadline passed)",
                    )
                elif days_until <= 60:
                    log.warning(
                        "api_deprecation_approaching",
                        connector=connector.name,
                        deprecation_deadline=connector.api_deprecation_deadline,
                        days_remaining=days_until,
                        message=f"API will be deprecated in {days_until} days",
                    )
            except (ValueError, TypeError):
                log.warning(
                    "api_deprecation_deadline_invalid",
                    connector=connector.name,
                    deadline=connector.api_deprecation_deadline,
                )


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


# Drain flag — set by SIGTERM/SIGINT or a 'drain' control command.
# All polling loops check this at the top of each iteration and exit cleanly.
_draining: bool = False
_alert_dispatcher: AlertDispatcher | None = None
_federation_hb: FederationHeartbeat | None = None


def _trigger_drain(sig: int = 0, frame: object = None) -> None:  # noqa: ARG001
    """Set the drain flag; polling loops will exit after their current iteration."""
    global _draining
    _draining = True
    logger.info("ingestion_drain_signal_received", signal=sig)


# ---------------------------------------------------------------------------
# Health / readiness endpoints
# ---------------------------------------------------------------------------

async def _health(request: Request) -> JSONResponse:
    """Liveness probe - returns 200 if process is alive."""
    return JSONResponse({"status": "ok"})


def _make_ready_handler(pool: Any, connector_configs: list) -> Any:
    """Build /ready handler with access to pool and connector configs."""
    async def _ready(request: Request) -> JSONResponse:
        """
        Readiness probe - returns connector status details.
        Per GOAL.md: includes which connectors are active, paused, or circuit-broken.
        """
        connectors: dict[str, dict] = {}
        
        if pool:
            try:
                async with pool.connection() as conn:
                    # Check connector health status
                    rows = await conn.execute(
                        "SELECT connector, datatype, status, reason FROM inout_ops_connector_health"
                    )
                    health_rows = await rows.fetchall()
                    
                    # Check paused connectors
                    pause_rows_cursor = await conn.execute(
                        "SELECT connector, datatype FROM inout_ops_control WHERE command = 'pause'"
                    )
                    pause_rows = await pause_rows_cursor.fetchall()
                    
                    # Build status map
                    for row in health_rows:
                        conn_name = row[0]
                        dt = row[1]
                        status = row[2]
                        reason = row[3]
                        
                        if conn_name not in connectors:
                            connectors[conn_name] = {"status": "active", "datatypes": {}}
                        
                        connectors[conn_name]["datatypes"][dt] = {
                            "status": status,
                            "reason": reason,
                        }
                        
                        if status == "unavailable":
                            connectors[conn_name]["status"] = "circuit_broken"
                    
                    # Mark paused
                    for row in pause_rows:
                        conn_name = row[0]
                        dt = row[1]
                        
                        if conn_name not in connectors:
                            connectors[conn_name] = {"status": "paused", "datatypes": {}}
                        else:
                            connectors[conn_name]["status"] = "paused"
                        
                        if dt:
                            connectors[conn_name]["datatypes"][dt] = {"status": "paused"}
                    
            except Exception as exc:
                logger.warning("ready_check_failed", error=str(exc))
        
        # Add configured connectors not in status map (active by default)
        for cfg in connector_configs:
            conn_name = cfg.connector.name
            if conn_name not in connectors:
                connectors[conn_name] = {"status": "active", "datatypes": {}}
        
        return JSONResponse({
            "status": "ready",
            "connectors": connectors,
        })
    
    return _ready


def _build_app(
    engine: IngestionEngine,
    connector_configs: list,
    pool: Any = None,
) -> Any:
    from inandout.api import build_api_router

    app = FastAPI(title="in-and-out", docs_url="/api/docs")
    app.add_api_route("/health", _health)
    app.add_api_route("/ready", _make_ready_handler(pool, connector_configs))
    app.mount("/metrics", prometheus_make_asgi_app(registry=REGISTRY))

    for connector_file_cfg in connector_configs:
        connector_cfg = connector_file_cfg.connector
        webhook_cfg = getattr(connector_cfg, "webhook", None)
        if webhook_cfg is None:
            continue

        def _make_handler(c_cfg: Any, w_cfg: Any) -> Any:
            async def _webhook_handler(request: Request) -> Any:
                return await handle_webhook(request, c_cfg, w_cfg, engine)
            return _webhook_handler

        app.add_api_route(webhook_cfg.path, _make_handler(connector_cfg, webhook_cfg), methods=["POST"])

    # Include management API router directly on the root app
    api_router = build_api_router(pool=pool)
    app.include_router(api_router, prefix="/api")

    otel_app = OpenTelemetryMiddleware(app)
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

async def _mark_connector_unavailable(
    pool: Any,
    connector: str,
    datatype: str,
    reason: str,
    dispatcher: AlertDispatcher | None = None,
) -> None:
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

    if dispatcher:
        await dispatcher.dispatch(
            AlertEventType.connector_unavailable,
            connector=connector,
            datatype=datatype,
            message=reason,
        )
    elif _alert_dispatcher:
        await _alert_dispatcher.dispatch(
            AlertEventType.connector_unavailable,
            connector=connector,
            datatype=datatype,
            message=reason,
        )


async def _mark_connector_healthy(
    pool: Any,
    connector: str,
    datatype: str,
    dispatcher: AlertDispatcher | None = None,
) -> None:
    """Clear unavailable status in inout_ops_connector_health (T1 #44)."""
    was_unhealthy = False
    try:
        async with pool.connection() as conn:
            row = await conn.fetchrow(
                "SELECT status FROM inout_ops_connector_health WHERE connector = %s AND datatype = %s",
                [connector, datatype],
            )
            was_unhealthy = row is not None and row["status"] == "unavailable"
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

    if dispatcher and was_unhealthy:
        await dispatcher.dispatch(
            AlertEventType.connector_recovered,
            connector=connector,
            datatype=datatype,
            message="connector is healthy again",
        )
    elif _alert_dispatcher and was_unhealthy:
        await _alert_dispatcher.dispatch(
            AlertEventType.connector_recovered,
            connector=connector,
            datatype=datatype,
            message="connector is healthy again",
        )


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
            # Update federation heartbeat after each sync
            if _federation_hb is not None:
                import datetime
                from inandout.observability.health_score import compute_health_score
                _hs = await compute_health_score(engine._pool, connector_cfg.name, datatype)
                _federation_hb.update(
                    connector=connector_cfg.name,
                    datatype=datatype,
                    health_score=_hs,
                    last_sync_at=datetime.datetime.now(datetime.UTC).isoformat(),
                    circuit_breaker_state=cb.state.value,
                )
        except Exception as exc:
            cb.record_failure()
            log.error("poll_error", error=str(exc))
            if cb.state == CircuitState.open:
                await _mark_connector_unavailable(
                    engine._pool, connector_cfg.name, datatype,
                    reason=str(exc),
                )
            if _federation_hb is not None:
                from inandout.observability.health_score import compute_health_score
                _hs_exc = await compute_health_score(engine._pool, connector_cfg.name, datatype)
                _federation_hb.update(
                    connector=connector_cfg.name,
                    datatype=datatype,
                    health_score=_hs_exc,
                    circuit_breaker_state=cb.state.value,
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
        if _draining:
            log.info("control_table_poller_draining")
            break
        try:
            count = await dispatcher.dispatch_pending(engine=engine)
            if count:
                log.info("control_commands_dispatched", count=count)
        except Exception as exc:
            log.error("control_table_poll_error", error=str(exc))
        await anyio.sleep(poll_secs)
        if _draining:
            break


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
# Webhook dedup TTL cleanup
# ---------------------------------------------------------------------------

async def _webhook_dedup_cleanup_loop(
    pool: Any,
    connector_configs: list,
    interval_secs: float = 3600.0,
) -> None:
    """Periodically delete expired rows from inout_ops_webhook_seen.

    Runs every *interval_secs* (default 1 h).  For each connector that has
    ``event_id_field`` configured, rows older than its ``dedup_ttl`` are
    removed.  Errors are swallowed so a missing table never crashes the daemon.
    """
    from inandout.config._duration import parse_duration

    log = logger.bind(component="webhook_dedup_cleanup_loop")
    log.info("webhook_dedup_cleanup_loop_started")
    while True:
        if _draining:
            log.info("webhook_dedup_cleanup_loop_draining")
            break
        await anyio.sleep(interval_secs)
        if _draining:
            break
        for connector_file_cfg in connector_configs:
            connector_cfg = connector_file_cfg.connector
            wh_cfg = connector_cfg.webhooks
            if wh_cfg is None or wh_cfg.event_id_field is None:
                continue
            try:
                ttl_secs = parse_duration(wh_cfg.dedup_ttl)
            except Exception:
                ttl_secs = 86400.0  # 24 h fallback
            try:
                async with pool.connection() as conn:
                    cur = await conn.execute(
                        """
                        DELETE FROM inout_ops_webhook_seen
                        WHERE connector = %s
                          AND received_at < NOW() - INTERVAL '1 second' * %s
                        """,
                        [connector_cfg.name, ttl_secs],
                    )
                    await conn.commit()
                    deleted = cur.rowcount if cur.rowcount is not None else 0
                    if deleted:
                        log.info(
                            "webhook_dedup_cleanup",
                            connector=connector_cfg.name,
                            deleted=deleted,
                            ttl_secs=ttl_secs,
                        )
            except Exception as exc:
                log.warning(
                    "webhook_dedup_cleanup_failed",
                    connector=connector_cfg.name,
                    error=str(exc),
                )


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

    # Initialise alert dispatcher if configured
    global _alert_dispatcher
    if config.alerting and config.alerting.enabled:
        from inandout.alerting.dispatcher import AlertDispatcher as _AlertDispatcher
        _alert_dispatcher = _AlertDispatcher(config.alerting)
        logger.info("alert_dispatcher_initialised")

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

    engine = IngestionEngine(pool, read_pool=read_pool)

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

    # T1 #39: Warn about API version deprecations
    _check_api_deprecations(connector_configs, log)

    # Build connector config map for delta-only protection
    connector_map = {cfg.connector.name: cfg.connector for cfg in connector_configs}

    # Create control dispatcher with connector map
    paused_connectors: set[tuple[str, str]] = set()
    dispatcher = ControlDispatcher(
        pool, paused_connectors,
        target_tool="ingestion",
        drain_callback=_trigger_drain,
        reload_callback=reload_flag.set,
        connectors=connector_map,
    )

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
        _build_app(engine, connector_configs, pool=pool),
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
        if _draining:
            return

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
                await hot_reload_loop(connectors_path, _on_file_change, should_stop=lambda: _draining)
                return
        except ImportError:
            pass

        # Fallback: SIGHUP-based reload
        while True:
            if _draining:
                break
            await anyio.sleep(1.0)
            if _draining:
                break
            if not reload_flag.is_set():
                continue
            reload_flag.clear()
            log.info("sighup_received_reloading_connectors")
            new_configs = _load_connectors()
            await _apply_connector_changes(outer_tg, new_configs=new_configs)

    # A6: Check API version deprecation dates for all loaded connectors
    _check_api_version_deprecations(connector_configs)

    # Subscribe to in-process REINGEST_SIGNAL events from the writeback engine (T2 #39).
    # When writeback detects a conflict with resolution=re_ingest_and_recompute,
    # it publishes this event so that a co-located ingestion daemon can react
    # immediately without waiting for the next DB control-table poll.
    async def _on_reingest_signal(connector: str, datatype: str, external_id: str, **kwargs: Any) -> None:
        _log = logger.bind(connector=connector, datatype=datatype, external_id=external_id)
        _log.info("reingest_signal_received", reason=kwargs.get("reason", "unknown"))
        connector_cfg_obj = None
        ingestion_cfg_obj = None
        dtype_cfg_obj = None
        for cfg in connector_configs:
            if cfg.connector.name == connector:
                connector_cfg_obj = cfg.connector
                dt = cfg.connector.datatypes.get(datatype)
                if dt:
                    dtype_cfg_obj = dt
                    ingestion_cfg_obj = dt.ingestion
                break
        if connector_cfg_obj is None or ingestion_cfg_obj is None:
            _log.warning("reingest_signal_connector_not_found")
            return
        try:
            await engine.run_sync_single_record(
                connector_cfg_obj, datatype, ingestion_cfg_obj, external_id, dtype_cfg=dtype_cfg_obj
            )
            _log.info("reingest_signal_completed")
        except Exception as exc:
            _log.error("reingest_signal_failed", error=str(exc))

    from inandout.events import EventType, get_event_bus
    get_event_bus().subscribe(EventType.REINGEST_SIGNAL, _on_reingest_signal)

    # Initialise federation heartbeat
    global _federation_hb
    _federation_hb = FederationHeartbeat(namespace=getattr(config.database, "schema", "public"))

    log.info("daemon_started")

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
            if any(
                cfg.connector.webhooks is not None
                and cfg.connector.webhooks.event_id_field is not None
                for cfg in connector_configs
            ):
                tg.start_soon(_webhook_dedup_cleanup_loop, pool, connector_configs)
            tg.start_soon(heartbeat_loop, pool, _federation_hb, 30.0, lambda: _draining)
            await _run_connector_tasks(tg, engine, connector_configs, default_interval_secs, paused_connectors)
    finally:
        log.info("daemon_stopping")
        await pool.close()
        log.info("daemon_stopped")
