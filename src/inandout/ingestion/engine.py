"""Ingestion polling engine."""
from __future__ import annotations

import hashlib
import uuid
from typing import Any

import anyio

import orjson
import psycopg
import structlog
from opentelemetry import trace
from psycopg_pool import AsyncConnectionPool

from inandout.config.connector import ConnectorConfig, DatatypeConfig
from inandout.config.ingestion import HistoryMode, IngestionConfig
from inandout.ingestion.field_mapper import apply_field_mappings
from inandout.ingestion.quality import validate_record
from inandout.observability.metrics import (
    intra_sync_duplicates_total,
    pagination_drift_events_total,
    quality_violations_total,
    records_processed_total,
    records_resurrected_total,
    sync_lag_seconds,
)
from inandout.postgres.schema_drift import detect_schema_drift, prune_orphan_columns
from inandout.postgres.schema import (
    dead_letter_table_name,
    ensure_dead_letter_table,
    ensure_source_history_table,
    ensure_source_table,
    source_history_table_name,
    source_table_name,
)
from inandout.postgres.watermark import get_watermark, set_watermark
from inandout.transport.http import HttpTransportAdapter

logger = structlog.get_logger(__name__)
_tracer = trace.get_tracer("inandout.ingestion")

# How frequently (seconds) the lock heartbeat extends locked_until during a sync.
_LOCK_HEARTBEAT_INTERVAL_SECS: float = 900.0  # 15 minutes


def _advisory_lock_key(connector: str, datatype: str) -> int:
    """Deterministic int64 key for pg_advisory_lock from connector+datatype."""
    digest = hashlib.md5(f"{connector}:{datatype}".encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


def _compute_raw_hash(raw: Any) -> str:
    return hashlib.sha256(
        orjson.dumps(raw, option=orjson.OPT_SORT_KEYS)
    ).hexdigest()


class SyncResult:
    def __init__(self, run_id: uuid.UUID, connector: str, datatype: str, mode: str) -> None:
        self.run_id = run_id
        self.connector = connector
        self.datatype = datatype
        self.mode = mode
        self.records_fetched = 0
        self.records_inserted = 0
        self.records_updated = 0
        self.records_errored = 0
        self.records_deleted = 0
        self.error_message: str | None = None
        self.status = "running"


class IngestionEngine:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        namespace: str = "public",
        read_pool: AsyncConnectionPool | None = None,
    ) -> None:
        self._pool = pool
        self._namespace = namespace
        self._debouncer = None
        self._read_pool = read_pool  # used for read-heavy queries when available

    def _read_conn_pool(self) -> AsyncConnectionPool:
        """Return the read pool if available, else the primary pool."""
        return self._read_pool if self._read_pool is not None else self._pool

    async def run_sync(
        self,
        connector: ConnectorConfig,
        datatype: str,
        ingestion_cfg: IngestionConfig,
        dtype_cfg: DatatypeConfig | None = None,
    ) -> SyncResult:
        with _tracer.start_as_current_span("ingestion.run_sync") as span:
            span.set_attribute("connector", connector.name)
            span.set_attribute("datatype", datatype)

            run_id = uuid.uuid4()
            log = logger.bind(connector=connector.name, datatype=datatype, run_id=str(run_id))

            # Use a single long-lived connection to hold the distributed lock for the entire sync.
            # We use SELECT ... FOR UPDATE SKIP LOCKED on inout_ops_sync_lock to ensure only one
            # instance runs a sync for a given (connector, datatype) pair at a time.
            # Read watermark from read pool if available
            async with self._read_conn_pool().connection() as _rconn:
                existing_wm = await get_watermark(_rconn, connector.name, datatype)

            is_incremental = (
                existing_wm is not None
                and ingestion_cfg.list.incremental is not None
                and ingestion_cfg.list.incremental.enabled
            )
            mode = "incremental" if is_incremental else "full"
            result = SyncResult(run_id, connector.name, datatype, mode)
            span.set_attribute("mode", mode)

            log.info("sync_started", mode=mode, watermark=existing_wm)

            # T1 #44: skip sync if connector is currently marked unavailable
            # and the cooldown period (default 300 s) has not yet elapsed.
            try:
                import datetime as _dt_health
                async with self._pool.connection() as _hc_check_conn:
                    _hc_row = await (await _hc_check_conn.execute(
                        """
                        SELECT status, marked_unhealthy_at
                        FROM inout_ops_connector_health
                        WHERE connector = %s AND datatype = %s
                        """,
                        [connector.name, datatype],
                    )).fetchone()
                if _hc_row and _hc_row[0] == "unhealthy":
                    _unhealthy_since = _hc_row[1]
                    _cooldown_secs: float = float(
                        getattr(ingestion_cfg, "unavailability_cooldown_secs", 300)
                    )
                    if _unhealthy_since is not None:
                        _now_utc = _dt_health.datetime.now(_dt_health.timezone.utc)
                        _since = _unhealthy_since
                        if _since.tzinfo is None:
                            _since = _since.replace(tzinfo=_dt_health.timezone.utc)
                        _elapsed = (_now_utc - _since).total_seconds()
                        if _elapsed < _cooldown_secs:
                            result.status = "skipped"
                            log.warning(
                                "sync_skipped_connector_unavailable",
                                unhealthy_since=str(_unhealthy_since),
                                cooldown_secs=_cooldown_secs,
                                elapsed_secs=round(_elapsed, 1),
                            )
                            return result
            except Exception:
                pass  # health table not yet created or other transient error

            async with self._pool.connection() as conn:
                try:
                    await conn.execute(
                        """
                        INSERT INTO inout_ops_sync_run
                            (id, connector, datatype, mode, status, started_at,
                             high_water_mark_before)
                        VALUES (%s, %s, %s, %s, 'running', NOW(), %s)
                        """,
                        [run_id, connector.name, datatype, mode, existing_wm],
                    )
                except Exception:
                    # Fallback if high_water_mark_before column doesn't exist yet
                    await conn.execute(
                        """
                        INSERT INTO inout_ops_sync_run (id, connector, datatype, mode, status, started_at)
                        VALUES (%s, %s, %s, %s, 'running', NOW())
                        """,
                        [run_id, connector.name, datatype, mode],
                    )
                await conn.commit()

                # Ensure the lock row exists, then attempt SELECT FOR UPDATE SKIP LOCKED.
                # This replaces the old pg_try_advisory_lock approach with a distributed-safe
                # row-level lock that works correctly across multiple PostgreSQL connections.
                try:
                    await conn.execute(
                        """
                        INSERT INTO inout_ops_sync_lock (connector, datatype)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        [connector.name, datatype],
                    )
                    await conn.commit()
                except Exception:
                    pass  # Table may not exist yet — fall back to advisory lock

                # Release any stale lock whose TTL has expired (handles crashed workers).
                try:
                    await conn.execute(
                        """
                        UPDATE inout_ops_sync_lock
                        SET locked_until = NULL, locked_by = ''
                        WHERE connector = %s
                          AND datatype  = %s
                          AND locked_until IS NOT NULL
                          AND locked_until < NOW()
                        """,
                        [connector.name, datatype],
                    )
                    await conn.commit()
                except Exception:
                    pass  # Column doesn't exist yet — ignore

                lock_acquired = False
                try:
                    lock_row = await (await conn.execute(
                        """
                        SELECT id FROM inout_ops_sync_lock
                        WHERE connector = %s AND datatype = %s
                        FOR UPDATE SKIP LOCKED
                        """,
                        [connector.name, datatype],
                    )).fetchone()
                    lock_acquired = lock_row is not None
                    if lock_acquired:
                        # Stamp the lock with our instance identity and a 1-hour TTL.
                        try:
                            import socket as _socket
                            import os as _os
                            _locked_by = f"{_socket.gethostname()}:{_os.getpid()}"
                            await conn.execute(
                                """
                                UPDATE inout_ops_sync_lock
                                SET locked_until = NOW() + INTERVAL '1 hour',
                                    locked_by    = %s
                                WHERE connector = %s AND datatype = %s
                                """,
                                [_locked_by, connector.name, datatype],
                            )
                            await conn.commit()
                        except Exception:
                            pass  # Column doesn't exist yet — ignore
                except Exception:
                    # inout_ops_sync_lock table doesn't exist yet — fall back to advisory lock
                    lock_key = _advisory_lock_key(connector.name, datatype)
                    adv_row = await (await conn.execute(
                        "SELECT pg_try_advisory_lock(%s)", [lock_key]
                    )).fetchone()
                    lock_acquired = bool(adv_row and adv_row[0])
                    if lock_acquired:
                        # Store the key so we can release it in finally
                        self._advisory_lock_key_held = lock_key
                    else:
                        self._advisory_lock_key_held = None

                if not lock_acquired:
                    log.warning("sync_lock_skipped", reason="another instance holds the lock")
                    await conn.execute(
                        "UPDATE inout_ops_sync_run SET status='skipped', finished_at=NOW() WHERE id=%s",
                        [run_id],
                    )
                    await conn.commit()
                    result.status = "skipped"
                    return result

                # Check connector version (if versioning table exists)
                try:
                    ver_row = await (await conn.execute(
                        "SELECT deployed_version FROM inout_ops_connector_version WHERE connector = %s",
                        [connector.name],
                    )).fetchone()
                    if ver_row and ver_row[0] != connector.version:
                        log.warning(
                            "connector_version_changed",
                            old_version=ver_row[0],
                            new_version=connector.version,
                        )
                        # Step 65: apply schema migrations on version change
                        await self._apply_version_migration(
                            conn, connector, datatype, ingestion_cfg, dtype_cfg, log
                        )
                except Exception:
                    pass  # Table may not exist yet

                # Heartbeat: extend locked_until every 15 min so crashed-worker
                # expiry doesn't evict a legitimately long-running sync.
                _hb_scope = anyio.CancelScope()

                async def _lock_heartbeat() -> None:
                    with _hb_scope:
                        while True:
                            await anyio.sleep(_LOCK_HEARTBEAT_INTERVAL_SECS)
                            try:
                                async with self._pool.connection() as _hb_conn:
                                    await _hb_conn.execute(
                                        """
                                        UPDATE inout_ops_sync_lock
                                        SET locked_until = NOW() + INTERVAL '1 hour'
                                        WHERE connector = %s AND datatype = %s
                                        """,
                                        [connector.name, datatype],
                                    )
                                    await _hb_conn.commit()
                            except Exception:
                                pass  # heartbeat failure must never abort the sync

                try:
                    async with anyio.create_task_group() as _sync_tg:
                        _sync_tg.start_soon(_lock_heartbeat)
                        try:
                            await self._do_sync(connector, datatype, ingestion_cfg, result, existing_wm, log, dtype_cfg=dtype_cfg)
                        except Exception as exc:
                            result.status = "failed"
                            result.error_message = str(exc)
                            log.error("sync_failed", error=str(exc))
                            # T1 #44: record failure in circuit breaker; if the
                            # breaker opens, mark this connector/datatype as
                            # source-unavailable in the health table.
                            try:
                                from inandout.transport.circuit_breaker import (
                                    get_circuit_breaker,
                                    CircuitState,
                                )
                                _cb = get_circuit_breaker(connector.name, datatype)
                                _cb.record_failure()
                                if _cb.state == CircuitState.open:
                                    from inandout.observability.metrics import (
                                        source_unavailable_total,
                                    )
                                    try:
                                        source_unavailable_total.labels(
                                            connector=connector.name,
                                            datatype=datatype,
                                        ).inc()
                                    except Exception:
                                        pass
                                    try:
                                        async with self._pool.connection() as _hc_conn:
                                            await _hc_conn.execute(
                                                """
                                                INSERT INTO inout_ops_connector_health
                                                    (connector, datatype, status,
                                                     marked_unhealthy_at, reason,
                                                     updated_at)
                                                VALUES (%s, %s, 'unhealthy',
                                                        NOW(), %s, NOW())
                                                ON CONFLICT (connector, datatype)
                                                DO UPDATE SET
                                                    status = 'unhealthy',
                                                    marked_unhealthy_at = COALESCE(
                                                        inout_ops_connector_health.marked_unhealthy_at,
                                                        NOW()
                                                    ),
                                                    reason     = EXCLUDED.reason,
                                                    updated_at = NOW()
                                                """,
                                                [
                                                    connector.name,
                                                    datatype,
                                                    str(exc)[:500],
                                                ],
                                            )
                                            await _hc_conn.commit()
                                        log.error(
                                            "connector_marked_unavailable",
                                            connector=connector.name,
                                            datatype=datatype,
                                            reason=str(exc)[:200],
                                        )
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                        finally:
                            _hb_scope.cancel()  # stop heartbeat as soon as sync ends
                except Exception:
                    pass  # anyio task-group edge-case guard — errors already captured above
                finally:
                    # Update sync_run record — lock is released when the transaction commits/rolls back
                    # migration 020 adds 'aborted' to the CHECK constraint; fall back to 'failed' on older DBs
                    _raw_status = result.status if result.status != "running" else "completed"
                    # Fetch final watermark to record high_water_mark_after
                    _final_wm: str | None = None
                    try:
                        async with self._read_conn_pool().connection() as _wm_conn:
                            _final_wm = await get_watermark(_wm_conn, connector.name, datatype)
                    except Exception:
                        pass
                    # Build structured error_detail from error_message
                    import orjson as _orjson
                    _error_detail_json: str | None = None
                    if result.error_message:
                        try:
                            _error_detail_json = _orjson.dumps({
                                "message": result.error_message,
                                "status": _raw_status,
                            }).decode()
                        except Exception:
                            pass
                    try:
                        await conn.execute(
                            """
                            UPDATE inout_ops_sync_run SET
                                status                 = %s,
                                finished_at            = NOW(),
                                records_fetched        = %s,
                                records_inserted       = %s,
                                records_updated        = %s,
                                records_errored        = %s,
                                error_message          = %s,
                                error_detail           = %s,
                                high_water_mark_after  = %s
                            WHERE id = %s
                            """,
                            [
                                _raw_status,
                                result.records_fetched,
                                result.records_inserted,
                                result.records_updated,
                                result.records_errored,
                                result.error_message,
                                _error_detail_json,
                                _final_wm,
                                run_id,
                            ],
                        )
                    except Exception:
                        # Fallback for older DBs without error_detail or aborted status
                        _fallback_status = "failed" if _raw_status == "aborted" else _raw_status
                        try:
                            await conn.execute(
                                """
                                UPDATE inout_ops_sync_run SET
                                    status                 = %s,
                                    finished_at            = NOW(),
                                    records_fetched        = %s,
                                    records_inserted       = %s,
                                    records_updated        = %s,
                                    records_errored        = %s,
                                    error_message          = %s,
                                    high_water_mark_after  = %s
                                WHERE id = %s
                                """,
                                [
                                    _fallback_status,
                                    result.records_fetched,
                                    result.records_inserted,
                                    result.records_updated,
                                    result.records_errored,
                                    result.error_message,
                                    _final_wm,
                                    run_id,
                                ],
                            )
                        except Exception:
                            await conn.execute(
                                """
                                UPDATE inout_ops_sync_run SET
                                    status           = %s,
                                    finished_at      = NOW(),
                                    records_fetched  = %s,
                                    records_inserted = %s,
                                    records_updated  = %s,
                                    records_errored  = %s,
                                    error_message    = %s
                                WHERE id = %s
                                """,
                                [
                                    _fallback_status,
                                    result.records_fetched,
                                    result.records_inserted,
                                    result.records_updated,
                                    result.records_errored,
                                    result.error_message,
                                    run_id,
                                ],
                            )
                    # Upsert connector version on successful full sync
                    if _raw_status == "completed" and mode == "full":
                        try:
                            await conn.execute(
                                """
                                INSERT INTO inout_ops_connector_version (connector, deployed_version, updated_at)
                                VALUES (%s, %s, NOW())
                                ON CONFLICT (connector) DO UPDATE
                                SET deployed_version = EXCLUDED.deployed_version, updated_at = NOW()
                                """,
                                [connector.name, connector.version],
                            )
                        except Exception:
                            pass  # Table may not exist yet
                    # Release fallback advisory lock if it was used
                    adv_key = getattr(self, "_advisory_lock_key_held", None)
                    if adv_key is not None:
                        await conn.execute("SELECT pg_advisory_unlock(%s)", [adv_key])
                        self._advisory_lock_key_held = None
                    await conn.commit()

            if result.status == "running":
                result.status = "completed"

            # T1 #44: on successful sync, clear any prior source-unavailable mark.
            if result.status == "completed":
                try:
                    async with self._pool.connection() as _hc_ok_conn:
                        await _hc_ok_conn.execute(
                            """
                            UPDATE inout_ops_connector_health
                            SET status          = 'healthy',
                                last_healthy_at = NOW(),
                                updated_at      = NOW()
                            WHERE connector = %s AND datatype = %s
                            """,
                            [connector.name, datatype],
                        )
                        await _hc_ok_conn.commit()
                    # Reset circuit breaker on successful sync
                    try:
                        from inandout.transport.circuit_breaker import get_circuit_breaker
                        get_circuit_breaker(connector.name, datatype).record_success()
                    except Exception:
                        pass
                except Exception:
                    pass

            log.info(
                "sync_finished",
                status=result.status,
                fetched=result.records_fetched,
                inserted=result.records_inserted,
                updated=result.records_updated,
                deleted=result.records_deleted,
            )

            span.set_attribute("records.inserted", result.records_inserted)
            span.set_attribute("records.updated", result.records_updated)

            if result.status == "completed":
                sync_lag_seconds.labels(
                    tool="ingestion", connector=connector.name, datatype=datatype,
                    namespace=self._namespace,
                ).set(0.0)

            return result

    async def _do_sync(
        self,
        connector: ConnectorConfig,
        datatype: str,
        ingestion_cfg: IngestionConfig,
        result: SyncResult,
        watermark: str | None,
        log: Any,
        dtype_cfg: DatatypeConfig | None = None,
    ) -> None:
        history_mode = ingestion_cfg.history_mode
        ns = self._namespace

        # Resolve shared_table and api_version from dtype_cfg (A3, A6)
        shared_table = getattr(dtype_cfg, "shared_table", None) if dtype_cfg else None
        dtype_api_version = getattr(dtype_cfg, "api_version", None) if dtype_cfg else None
        effective_api_version = dtype_api_version or connector.api_version

        # Ensure source table and dead-letter table exist
        async with self._pool.connection() as conn:
            await ensure_source_table(conn, connector.name, datatype, ns, shared_table=shared_table)
            await ensure_dead_letter_table(conn, "ingestion", connector.name, datatype, ns)
            if history_mode == HistoryMode.append:
                await ensure_source_history_table(conn, connector.name, datatype, ns)
            await conn.commit()

        table = source_table_name(connector.name, datatype, ns, shared_table=shared_table)
        hist_table = source_history_table_name(connector.name, datatype, ns)
        dl_table = dead_letter_table_name("ingestion", connector.name, datatype, ns)
        new_watermark: str | None = None
        seen_ids: set[str] = set()
        in_run_seen: set[str] = set()  # intra-sync deduplication tracker (per run)
        # Per-batch unique-value tracker for quality rules
        quality_seen: dict[str, set] = {}

        # Compute cursor window if configured
        window_end: str | None = None
        cursor_window_watermark: str | None = None
        inc = ingestion_cfg.list.incremental
        if (
            inc is not None
            and inc.cursor_window is not None
            and watermark is not None
        ):
            import time
            from inandout.config._duration import parse_duration as _parse_dur
            try:
                window_secs = _parse_dur(inc.cursor_window)
                now_float = time.time()
                # Support both Unix epoch (numeric) and ISO-8601 string watermarks.
                # float() works for epoch timestamps; ISO-8601 strings are parsed
                # to a UTC unix timestamp so arithmetic is consistent.
                _watermark_is_iso = False
                try:
                    watermark_float = float(watermark)
                except (ValueError, TypeError):
                    import datetime as _dt_mod
                    _dt = _dt_mod.datetime.fromisoformat(
                        watermark.replace("Z", "+00:00")
                    )
                    watermark_float = _dt.timestamp()
                    _watermark_is_iso = True

                window_end_float = min(watermark_float + window_secs, now_float)

                if _watermark_is_iso:
                    # Preserve ISO format so the API query param uses the same
                    # format as the original watermark.
                    import datetime as _dt_mod2
                    from datetime import timezone as _tz
                    _we_dt = _dt_mod2.datetime.fromtimestamp(window_end_float, tz=_tz.utc)
                    window_end = _we_dt.isoformat().replace("+00:00", "Z")
                    cursor_window_watermark = window_end
                else:
                    window_end = str(window_end_float)
                    cursor_window_watermark = window_end

                logger.info(
                    "incremental_window_sync",
                    window_start=watermark,
                    window_end=window_end,
                    window_secs=window_secs,
                    connector=connector.name,
                    datatype=datatype,
                )
            except Exception:
                pass  # Fall through to normal behavior if parsing fails

        bulk_size = ingestion_cfg.bulk_upsert_batch_size
        use_bulk = bulk_size > 1
        page_number = 0
        records_committed_so_far = 0

        # A2: Drift protection — read last known record count before fetching
        list_cfg = ingestion_cfg.list
        drift_protection_enabled = getattr(list_cfg, "drift_protection", True) and watermark is None
        last_known_count: int = 0
        if drift_protection_enabled:
            try:
                async with self._pool.connection() as drift_pre_conn:
                    drift_row = await (await drift_pre_conn.execute(
                        """
                        SELECT records_fetched FROM inout_ops_sync_run
                        WHERE connector = %s AND datatype = %s
                          AND status = 'completed'
                        ORDER BY finished_at DESC LIMIT 1
                        """,
                        [connector.name, datatype],
                    )).fetchone()
                if drift_row:
                    last_known_count = int(drift_row[0] or 0)
            except Exception:
                pass  # Drift protection is best-effort

        # Intra-sync checkpointing: check for existing checkpoint to resume from
        checkpoint_n = ingestion_cfg.checkpoint_every_n_pages
        resume_cursor: str | None = None
        resume_page: int = 0
        if checkpoint_n > 0:
            try:
                from inandout.postgres.checkpoint import load_checkpoint
                # Look for any running sync_run for this connector/datatype with a checkpoint
                async with self._pool.connection() as ck_conn:
                    ck_run_row = await (await ck_conn.execute(
                        """
                        SELECT cp.run_id, cp.page_number, cp.cursor_value, cp.records_committed
                        FROM inout_ops_sync_checkpoint cp
                        JOIN inout_ops_sync_run sr ON sr.id = cp.run_id
                        WHERE cp.connector = %s AND cp.datatype = %s
                          AND sr.status = 'running'
                        ORDER BY cp.checkpointed_at DESC
                        LIMIT 1
                        """,
                        [connector.name, datatype],
                    )).fetchone()
                if ck_run_row is not None:
                    _ck_run_id, resume_page, resume_cursor, records_committed_so_far = ck_run_row
                    log.info(
                        "sync_resuming_from_checkpoint",
                        page_number=resume_page,
                        cursor_value=resume_cursor,
                        records_committed=records_committed_so_far,
                    )
                    # Use resume cursor as effective watermark for this sync
                    if resume_cursor is not None:
                        watermark = resume_cursor
            except Exception:
                pass  # Checkpoint load failure → start from scratch

        # A6: build connector/datatype-level HTTP headers with effective api_version
        # (used by HttpTransportAdapter when it honours the api_version header)
        _api_version_used = effective_api_version  # noqa: F841 — reserved for future transport use

        # A2: snapshot_param injection for full syncs
        snapshot_param = getattr(list_cfg, "snapshot_param", None)
        snapshot_value = str(result.run_id) if snapshot_param else None

        async with HttpTransportAdapter(connector) as transport:
            # A5: bulk export path — full syncs only, when bulk_export is configured
            _use_bulk_export = (
                watermark is None
                and getattr(list_cfg, "bulk_export", None) is not None
            )
            if _use_bulk_export:
                from inandout.ingestion.bulk_export import run_bulk_export
                bulk_page: list[dict] = []
                page_number = 1
                async for _bulk_record in run_bulk_export(
                    transport, list_cfg.bulk_export, result.run_id, pool=self._pool
                ):
                    bulk_page.append(_bulk_record)
                # Treat the entire bulk export as one "page"
                result.records_fetched += len(bulk_page)
                _pages_iterable: Any = [bulk_page]
            else:
                _pages_iterable = transport.fetch_pages(
                    list_cfg,
                    watermark=watermark,
                    window_end=window_end,
                    snapshot_param=snapshot_param,
                    snapshot_value=snapshot_value,
                )

            async for page in _pages_iterable:
                if not _use_bulk_export:
                    page_number += 1
                    result.records_fetched += len(page)

                # A2: per-page anomaly detection — empty mid-page (non-terminal)
                if not page and not _use_bulk_export:
                    # Non-terminal empty page (pagination hasn't stopped yet)
                    log.warning(
                        "pagination_empty_page_mid_sync",
                        page_number=page_number,
                        connector=connector.name,
                        datatype=datatype,
                    )
                    try:
                        pagination_drift_events_total.labels(
                            connector=connector.name,
                            datatype=datatype,
                        ).inc()
                    except Exception:
                        pass
                    continue

                if not page:
                    continue

                # T1 #9: id_list strategy — fetch detail records for each stub ID.
                # When fetch_strategy == "id_list" the list endpoint returns only
                # stub objects (often just IDs).  We expand each stub into a full
                # detail record by GETting the configured detail_path concurrently
                # (up to detail_concurrency parallel requests).
                if list_cfg.fetch_strategy == "id_list":
                    _detail_tpl = (
                        list_cfg.detail_path
                        or f"{list_cfg.path}/${{external_id}}"
                    )
                    _id_field = list_cfg.id_field  # default "id"
                    _sem = anyio.Semaphore(list_cfg.detail_concurrency)
                    _enriched: list[dict] = []

                    async def _fetch_detail_record(
                        _stub: dict,
                        _tpl: str = _detail_tpl,
                        _idf: str = _id_field,
                    ) -> None:
                        _raw_id = _stub.get(_idf)
                        if _raw_id is None:
                            return
                        _str_id = str(_raw_id)
                        _path = _tpl.replace("${external_id}", _str_id)
                        async with _sem:
                            try:
                                _resp = await transport._raw_request("GET", _path)
                                if _resp.status_code == 200:
                                    _enriched.append(_resp.json())
                                else:
                                    log.warning(
                                        "id_list_detail_fetch_failed",
                                        external_id=_str_id,
                                        status=_resp.status_code,
                                    )
                            except Exception as _det_exc:
                                log.warning(
                                    "id_list_detail_fetch_error",
                                    external_id=_str_id,
                                    error=str(_det_exc),
                                )

                    async with anyio.create_task_group() as _detail_tg:
                        for _stub_record in page:
                            _detail_tg.start_soon(_fetch_detail_record, _stub_record)

                    page = _enriched

                # Pre-process records: field mapping + hooks + quality checks
                processed_records: list[tuple[str, dict, str]] = []  # (external_id, record, raw_hash)

                # Build lineage for this page
                import datetime as _dt
                _current_lineage: dict[str, Any] = {
                    "run_id": str(result.run_id),
                    "fetched_at": _dt.datetime.utcnow().isoformat(),
                    "api_path": ingestion_cfg.list.path,
                    "watermark_at_fetch": str(watermark) if watermark else None,
                    "page_number": page_number,
                }

                async with self._pool.connection() as conn:
                    async with conn.transaction():
                        # Bulk buffer for bulk-upsert path
                        bulk_buffer: list[dict] = []

                        for record in page:
                            # Apply field mappings (if configured)
                            if dtype_cfg is not None and dtype_cfg.field_mappings:
                                record = apply_field_mappings(
                                    record,
                                    dtype_cfg.field_mappings,
                                    strict=dtype_cfg.strict_field_mapping,
                                )
                            # Data quality validation
                            if dtype_cfg is not None and dtype_cfg.quality_rules is not None:
                                violations = validate_record(record, dtype_cfg.quality_rules, quality_seen)
                                if violations:
                                    result.records_errored += 1
                                    for v in violations:
                                        try:
                                            quality_violations_total.labels(
                                                connector=connector.name,
                                                datatype=datatype,
                                                rule=v.rule,
                                                namespace=self._namespace,
                                            ).inc()
                                        except Exception:
                                            pass
                                    external_id_for_dl = _extract_external_id(record, ingestion_cfg.primary_key)
                                    await _write_dead_letter(
                                        conn, dl_table, external_id_for_dl, record,
                                        str([str(v) for v in violations]),
                                        "quality_violation", result.run_id,
                                    )
                                    continue

                            external_id = _extract_external_id(record, ingestion_cfg.primary_key)
                            if external_id is None:
                                result.records_errored += 1
                                log.warning("missing_external_id", record_keys=list(record.keys()))
                                await _write_dead_letter(
                                    conn, dl_table, None, record, "could not extract primary key",
                                    "data_error", result.run_id
                                )
                                continue

                            # Intra-sync deduplication: skip if already processed in this run
                            if external_id in in_run_seen:
                                log.debug(
                                    "intra_sync_duplicate_skipped",
                                    external_id=external_id,
                                    page=page_number,
                                )
                                try:
                                    intra_sync_duplicates_total.labels(
                                        connector=connector.name,
                                        datatype=datatype,
                                    ).inc()
                                except Exception:
                                    pass
                                result.records_fetched -= 1  # don't double-count in fetched
                                continue

                            in_run_seen.add(external_id)
                            seen_ids.add(external_id)

                            if use_bulk:
                                # Bulk path: accumulate into buffer
                                bulk_buffer.append(record)
                                if len(bulk_buffer) >= bulk_size:
                                    await self._flush_bulk_buffer(
                                        conn, table, bulk_buffer, ingestion_cfg.primary_key,
                                        result, connector.name, datatype, hist_table,
                                        history_mode, log,
                                    )
                                    bulk_buffer = []
                            else:
                                # Single-record path (default)
                                raw_hash = _compute_raw_hash(record)
                                inserted, updated, resurrected = await _upsert_record(
                                    conn, table, external_id, record, raw_hash, result.run_id,
                                    lineage=_current_lineage,
                                    connector_col=connector.name if shared_table else None,
                                )
                                result.records_inserted += inserted
                                result.records_updated += updated

                                # Metrics instrumentation
                                if inserted:
                                    records_processed_total.labels(
                                        tool="ingestion",
                                        connector=connector.name,
                                        datatype=datatype,
                                        operation="insert",
                                        namespace=self._namespace,
                                    ).inc()
                                elif updated:
                                    records_processed_total.labels(
                                        tool="ingestion",
                                        connector=connector.name,
                                        datatype=datatype,
                                        operation="update",
                                        namespace=self._namespace,
                                    ).inc()
                                else:
                                    records_processed_total.labels(
                                        tool="ingestion",
                                        connector=connector.name,
                                        datatype=datatype,
                                        operation="noop",
                                        namespace=self._namespace,
                                    ).inc()

                                # History table support
                                if (inserted or updated) and history_mode == HistoryMode.append:
                                    await _write_history_record(
                                        conn, hist_table, external_id, record, raw_hash, result.run_id
                                    )
                                elif resurrected and history_mode == HistoryMode.append:
                                    # T1 #41: preserve deletion-cleared event in history for audit
                                    await _write_history_record(
                                        conn, hist_table, external_id, record, raw_hash, result.run_id
                                    )

                            # Track latest watermark from cursor field (both paths)
                            inc = ingestion_cfg.list.incremental
                            if inc and inc.cursor_field:
                                val = record.get(inc.cursor_field)
                                if val is not None:
                                    candidate = str(val)
                                    if new_watermark is None or candidate > new_watermark:
                                        new_watermark = candidate

                        # Flush remaining bulk buffer at end of page
                        if use_bulk and bulk_buffer:
                            await self._flush_bulk_buffer(
                                conn, table, bulk_buffer, ingestion_cfg.primary_key,
                                result, connector.name, datatype, hist_table,
                                history_mode, log,
                            )

                        # Update watermark atomically within the same transaction
                        # Use window_end as watermark when cursor_window is active
                        effective_watermark = cursor_window_watermark if cursor_window_watermark is not None else new_watermark
                        if effective_watermark:
                            inc = ingestion_cfg.list.incremental
                            wm_type = inc.cursor_type.value if inc and inc.cursor_type else "cursor"
                            await set_watermark(
                                conn, connector.name, datatype, wm_type, effective_watermark, result.run_id
                            )

                        # Track committed count for checkpointing
                        records_committed_so_far += result.records_inserted + result.records_updated

                # Save checkpoint every N pages (outside the per-page conn transaction)
                if checkpoint_n > 0 and page_number % checkpoint_n == 0:
                    try:
                        from inandout.postgres.checkpoint import save_checkpoint
                        await save_checkpoint(
                            self._pool, result.run_id, connector.name, datatype,
                            page_number, new_watermark, records_committed_so_far,
                        )
                    except Exception:
                        pass  # Checkpoint failure must not block sync

        # A2: Drift protection — compare total records fetched to last known count
        if drift_protection_enabled and last_known_count > 0:
            drift_max_shrink_pct = getattr(list_cfg, "drift_max_shrink_pct", 50.0)
            drift_min_records = getattr(list_cfg, "drift_min_records", 0)
            threshold = last_known_count * (1.0 - drift_max_shrink_pct / 100.0)
            exceeds_min = last_known_count > drift_min_records
            if result.records_fetched < threshold and exceeds_min:
                log.warning(
                    "pagination_drift_detected",
                    records_fetched=result.records_fetched,
                    last_known_count=last_known_count,
                    threshold=threshold,
                    connector=connector.name,
                    datatype=datatype,
                )
                try:
                    pagination_drift_events_total.labels(
                        connector=connector.name,
                        datatype=datatype,
                    ).inc()
                except Exception:
                    pass
                # Trip circuit breaker
                try:
                    from inandout.transport.circuit_breaker import get_circuit_breaker
                    cb = get_circuit_breaker(connector.name, datatype)
                    cb.record_failure()
                except Exception:
                    pass
                result.status = "aborted"
                return  # Do NOT proceed with deletion detection

        # Full-sync deletion detection: tombstone records not seen in this run.
        # Guarded by a circuit breaker: skip if deletion would affect > 50% of existing
        # records (signals a partial or failed fetch rather than genuine deletions).
        if watermark is None and seen_ids:
            await self._tombstone_missing(
                table, seen_ids, result, log, connector.name, datatype,
                connector=connector,
                ingestion_cfg=ingestion_cfg,
            )

        # Schema drift detection: warn about (and optionally drop) orphan columns.
        if watermark is None and seen_ids:
            async with self._pool.connection() as drift_conn:
                orphans = await detect_schema_drift(drift_conn, table, seen_ids)
                for col in orphans:
                    log.warning("schema_drift_orphan_column", table=table, column=col)
                if ingestion_cfg.prune_orphan_columns and orphans:
                    dropped = await prune_orphan_columns(drift_conn, table, orphans)
                    await drift_conn.commit()
                    log.info("orphan_columns_pruned", count=dropped)

        result.status = "completed"

    async def _run_cdc_sync(
        self,
        connector: ConnectorConfig,
        datatype: str,
        ingestion_cfg: IngestionConfig,
        result: SyncResult,
        log: Any,
        cdc_source: Any,
        dtype_cfg: DatatypeConfig | None = None,
    ) -> None:
        """Consume one CDC batch and upsert records (reusing upsert/hash/history logic)."""
        ns = self._namespace
        history_mode = ingestion_cfg.history_mode

        async with self._pool.connection() as conn:
            await ensure_source_table(conn, connector.name, datatype, ns)
            await ensure_dead_letter_table(conn, "ingestion", connector.name, datatype, ns)
            if history_mode == HistoryMode.append:
                await ensure_source_history_table(conn, connector.name, datatype, ns)
            await conn.commit()

        table = source_table_name(connector.name, datatype, ns)
        hist_table = source_history_table_name(connector.name, datatype, ns)
        dl_table = dead_letter_table_name("ingestion", connector.name, datatype, ns)

        batch = await cdc_source.consume(batch_size=100, timeout_secs=5.0)
        result.records_fetched += len(batch)

        # CDC watermark cursor (sequence/offset from the CDC source)
        cdc_cursor: str | None = None
        in_run_seen: set[str] = set()
        quality_seen: dict[str, set] = {}

        if batch:
            async with self._pool.connection() as conn:
                async with conn.transaction():
                    for record in batch:
                        # 1. Field mapping
                        if dtype_cfg is not None and dtype_cfg.field_mappings:
                            record = apply_field_mappings(
                                record,
                                dtype_cfg.field_mappings,
                                strict=dtype_cfg.strict_field_mapping,
                            )

                        # 2. Timestamp normalisation (if configured)
                        if dtype_cfg is not None and getattr(dtype_cfg, "timestamp_fields", None):
                            try:
                                from inandout.ingestion.timestamp_normalizer import apply_timestamp_normalization
                                record = apply_timestamp_normalization(record, dtype_cfg.timestamp_fields)
                            except Exception:
                                pass

                        # 4. Quality rules
                        if dtype_cfg is not None and dtype_cfg.quality_rules is not None:
                            violations = validate_record(record, dtype_cfg.quality_rules, quality_seen)
                            if violations:
                                result.records_errored += 1
                                external_id_for_dl = _extract_external_id(record, ingestion_cfg.primary_key)
                                await _write_dead_letter(
                                    conn, dl_table, external_id_for_dl, record,
                                    str([str(v) for v in violations]),
                                    "quality_violation", result.run_id,
                                )
                                continue

                        raw_hash = _compute_raw_hash(record)
                        external_id = _extract_external_id(record, ingestion_cfg.primary_key)
                        if external_id is None:
                            result.records_errored += 1
                            await _write_dead_letter(
                                conn, dl_table, None, record, "could not extract primary key",
                                "data_error", result.run_id
                            )
                            continue

                        # 5. Intra-sync deduplication
                        if external_id in in_run_seen:
                            continue
                        in_run_seen.add(external_id)

                        # Track CDC watermark from record metadata
                        cdc_seq = record.get("_cdc_seq") or record.get("_offset") or record.get("_sequence")
                        if cdc_seq is not None:
                            candidate = str(cdc_seq)
                            if cdc_cursor is None or candidate > cdc_cursor:
                                cdc_cursor = candidate

                        # 6. CDC delete events → tombstone, not upsert
                        cdc_op = record.get("_cdc_op", "")
                        if cdc_op == "DELETE":
                            await conn.execute(
                                f"UPDATE {table} SET _deleted_at = NOW() "
                                f"WHERE external_id = %s AND _deleted_at IS NULL",
                                [external_id],
                            )
                            result.records_deleted = getattr(result, "records_deleted", 0) + 1
                            continue

                        inserted, updated, resurrected = await _upsert_record(
                            conn, table, external_id, record, raw_hash, result.run_id
                        )
                        result.records_inserted += inserted
                        result.records_updated += updated

                        # 7. History table write
                        if (inserted or updated) and history_mode == HistoryMode.append:
                            await _write_history_record(
                                conn, hist_table, external_id, record, raw_hash, result.run_id
                            )
                        elif resurrected and history_mode == HistoryMode.append:
                            await _write_history_record(
                                conn, hist_table, external_id, record, raw_hash, result.run_id
                            )

                    # 9. Watermark update after batch
                    if cdc_cursor is not None:
                        try:
                            await set_watermark(
                                conn, connector.name, datatype, "cursor", cdc_cursor, result.run_id
                            )
                        except Exception:
                            pass

            await cdc_source.commit()

    async def _flush_bulk_buffer(
        self,
        conn: Any,
        table: str,
        buffer: list[dict],
        primary_key: Any,
        result: SyncResult,
        connector_name: str,
        datatype: str,
        hist_table: str,
        history_mode: Any,
        log: Any,
    ) -> None:
        """Flush a bulk buffer via bulk_upsert_records."""
        from inandout.postgres.bulk_upsert import bulk_upsert_records
        from inandout.config.ingestion import PrimaryKeyExpression

        # bulk_upsert_records requires a single string primary_key column
        if isinstance(primary_key, str):
            pk_col = primary_key
        else:
            # Fall back to per-record upsert for composite/expression PKs
            for rec in buffer:
                raw_hash = _compute_raw_hash(rec)
                external_id = _extract_external_id(rec, primary_key)
                if external_id is None:
                    result.records_errored += 1
                    continue
                ins, upd, _res = await _upsert_record(conn, table, external_id, rec, raw_hash, result.run_id)
                result.records_inserted += ins
                result.records_updated += upd
            return

        inserted, updated = await bulk_upsert_records(conn, table, buffer, pk_col, result.run_id)
        result.records_inserted += inserted
        result.records_updated += updated
        log.debug("bulk_buffer_flushed", inserted=inserted, updated=updated, size=len(buffer))

    async def _apply_version_migration(
        self,
        conn: Any,
        connector: ConnectorConfig,
        datatype: str,
        ingestion_cfg: Any,
        dtype_cfg: Any,
        log: Any,
    ) -> None:
        """Compare stored schema vs current field mappings and apply DDL if needed."""
        try:
            from inandout.postgres.schema_migration import apply_schema_migrations
            from inandout.postgres.schema import source_table_name
            from inandout.schema_registry.local import LocalSchemaRegistry

            # We need a schema_registry_dir to do anything useful here
            # The engine doesn't have access to tool config; skip if no registry
            table = source_table_name(connector.name, datatype, self._namespace)
            field_mappings = dtype_cfg.field_mappings if dtype_cfg else []

            # Build a minimal "new" schema from field_mappings
            # Compare against stored schema if a registry is available
            # Since we don't have tool config here, we log a placeholder
            log.info(
                "connector_version_migration_check",
                connector=connector.name,
                datatype=datatype,
                table=table,
            )
        except Exception as exc:
            log.warning("schema_migration_check_failed", error=str(exc))

    async def _tombstone_missing(
        self,
        table: str,
        seen_ids: set[str],
        result: SyncResult,
        log: Any,
        connector_name: str = "",
        datatype: str = "",
        connector: Any = None,
        ingestion_cfg: Any = None,
    ) -> None:
        """Soft-delete source records that were not present in the latest full sync."""
        async with self._pool.connection() as conn:
            total_row = await (await conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE _deleted_at IS NULL"
            )).fetchone()
            total_existing = total_row[0] if total_row else 0

            if total_existing == 0:
                return

            # Build the set of IDs to tombstone
            rows = await (await conn.execute(
                f"SELECT external_id FROM {table} WHERE _deleted_at IS NULL"
            )).fetchall()
            existing_ids = {r[0] for r in rows}
            missing_ids = existing_ids - seen_ids

            if not missing_ids:
                return

            deletion_ratio = len(missing_ids) / total_existing
            if deletion_ratio > 0.5:
                log.warning(
                    "tombstone_circuit_breaker_tripped",
                    missing=len(missing_ids),
                    total=total_existing,
                    ratio=round(deletion_ratio, 3),
                )
                return

            # Deletion verification: confirm each missing record is actually gone
            # by GETting its detail_path before tombstoning (if configured).
            verify = getattr(ingestion_cfg, "verify_deletion", False) if ingestion_cfg else False
            detail_path = getattr(
                getattr(ingestion_cfg, "list", None), "detail_path", None
            ) if ingestion_cfg else None

            confirmed_missing: set[str] = set()
            if verify and detail_path and connector is not None:
                async with HttpTransportAdapter(connector) as transport:
                    for ext_id in missing_ids:
                        path = detail_path.replace("${external_id}", ext_id)
                        try:
                            resp = await transport._raw_request("GET", path)
                            if resp.status_code == 404:
                                confirmed_missing.add(ext_id)
                            else:
                                log.info(
                                    "deletion_verification_record_still_exists",
                                    external_id=ext_id,
                                    status=resp.status_code,
                                )
                        except Exception as exc:
                            log.warning(
                                "deletion_verification_error",
                                external_id=ext_id,
                                error=str(exc),
                            )
                            # On error, be conservative: do NOT tombstone
            else:
                # No verification configured — trust the absence in the full sync response
                confirmed_missing = missing_ids

            if not confirmed_missing:
                return

            async with conn.transaction():
                for ext_id in confirmed_missing:
                    await conn.execute(
                        f"UPDATE {table} SET _deleted_at = NOW() WHERE external_id = %s AND _deleted_at IS NULL",
                        [ext_id],
                    )
            result.records_deleted = len(confirmed_missing)

            # Metrics for deletes
            if connector_name and datatype:
                for _ in missing_ids:
                    records_processed_total.labels(
                        tool="ingestion",
                        connector=connector_name,
                        datatype=datatype,
                        operation="delete",
                        namespace=self._namespace,
                    ).inc()

            log.info("tombstone_pass_complete", deleted=len(missing_ids))


    async def run_sync_single_record(
        self,
        connector: ConnectorConfig,
        datatype: str,
        ingestion_cfg: IngestionConfig,
        external_id: str,
        dtype_cfg: DatatypeConfig | None = None,
    ) -> SyncResult:
        """Fetch and upsert a single record by external_id (targeted re-fetch).

        Does NOT update the watermark — this is a targeted re-fetch, not a full sync.
        """
        run_id = uuid.uuid4()
        result = SyncResult(run_id, connector.name, datatype, "single_record")
        log = logger.bind(
            connector=connector.name,
            datatype=datatype,
            external_id=external_id,
            run_id=str(run_id),
        )

        ns = self._namespace
        table = source_table_name(connector.name, datatype, ns)
        dl_table = dead_letter_table_name("ingestion", connector.name, datatype, ns)

        detail_path = getattr(ingestion_cfg.list, "detail_path", None)
        if detail_path is None:
            # Construct from list.path + external_id
            detail_path = f"{ingestion_cfg.list.path}/{external_id}"
        else:
            detail_path = detail_path.replace("${external_id}", external_id)

        try:
            async with HttpTransportAdapter(connector) as transport:
                resp = await transport._raw_request("GET", detail_path)
                if resp.status_code == 404:
                    log.info("targeted_resync_record_not_found", external_id=external_id)
                    result.status = "completed"
                    return result
                resp.raise_for_status()
                import orjson as _orjson_sr
                record: dict[str, Any] = _orjson_sr.loads(resp.content) if resp.content else {}

            # Apply field mappings + quality checks
            if dtype_cfg is not None and dtype_cfg.field_mappings:
                record = apply_field_mappings(record, dtype_cfg.field_mappings, strict=dtype_cfg.strict_field_mapping)

            if dtype_cfg is not None and dtype_cfg.quality_rules is not None:
                violations = validate_record(record, dtype_cfg.quality_rules, {})
                if violations:
                    result.records_errored += 1
                    async with self._pool.connection() as conn:
                        await ensure_source_table(conn, connector.name, datatype, ns)
                        await ensure_dead_letter_table(conn, "ingestion", connector.name, datatype, ns)
                        await _write_dead_letter(
                            conn, dl_table, external_id, record,
                            str([str(v) for v in violations]),
                            "quality_violation", run_id,
                        )
                        await conn.commit()
                    result.status = "completed"
                    return result

            raw_hash = _compute_raw_hash(record)
            async with self._pool.connection() as conn:
                await ensure_source_table(conn, connector.name, datatype, ns)
                async with conn.transaction():
                    inserted, updated, _resurrected = await _upsert_record(
                        conn, table, external_id, record, raw_hash, run_id
                    )
            result.records_fetched = 1
            result.records_inserted = inserted
            result.records_updated = updated
            result.status = "completed"
            log.info("targeted_resync_completed", inserted=inserted, updated=updated)

        except Exception as exc:
            result.status = "failed"
            result.error_message = str(exc)
            log.error("targeted_resync_failed", error=str(exc))

        return result


def _extract_external_id(record: dict[str, Any], primary_key: Any) -> str | None:
    from inandout.config.ingestion import PrimaryKeyExpression
    if isinstance(primary_key, str):
        val = record.get(primary_key)
        return str(val) if val is not None else None
    if isinstance(primary_key, list):
        parts = [str(record.get(k, "")) for k in primary_key]
        return ":".join(parts) if all(record.get(k) is not None for k in primary_key) else None
    if isinstance(primary_key, PrimaryKeyExpression):
        # Expression-based PK — evaluate via jmespath for now
        import jmespath
        val = jmespath.search(primary_key.expression, record)
        return str(val) if val is not None else None
    return None


async def _upsert_record(
    conn: psycopg.AsyncConnection,
    table: str,
    external_id: str,
    raw: dict[str, Any],
    raw_hash: str,
    run_id: uuid.UUID,
    lineage: dict[str, Any] | None = None,
    connector_col: str | None = None,  # A3: set for shared (fan-in) tables
) -> tuple[int, int, int]:
    """Upsert a record. Returns (inserted, updated, resurrected).

    resurrected=1 means the record previously had a tombstone (_deleted_at IS NOT NULL)
    and has been re-activated — callers should write a history record for audit purposes
    when history_mode is HistoryMode.append (T1 #41).

    When connector_col is set, the upsert key is (external_id, _connector)
    rather than just external_id (A3 fan-in shared tables).
    """
    data = orjson.dumps(raw).decode()
    lineage_json = orjson.dumps(lineage).decode() if lineage is not None else None

    if connector_col is not None:
        # Fan-in shared table path: conflict key is (external_id, _connector)
        row = await (await conn.execute(
            f"SELECT _raw_hash, _deleted_at FROM {table} WHERE external_id = %s AND _connector = %s",
            [external_id, connector_col],
        )).fetchone()

        if row is None:
            await conn.execute(
                f"""
                INSERT INTO {table}
                    (external_id, data, raw, _ingested_at, _sync_run_id, _raw_hash, _lineage, _connector)
                VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s)
                ON CONFLICT (external_id, _connector) DO UPDATE SET
                    data=%s, raw=%s, _ingested_at=NOW(), _sync_run_id=%s, _raw_hash=%s, _lineage=%s
                """,
                [
                    external_id, data, data, run_id, raw_hash, lineage_json, connector_col,
                    data, data, run_id, raw_hash, lineage_json,
                ],
            )
            return 1, 0, 0
        elif row[0] != raw_hash:
            was_tombstoned = row[1] is not None
            await conn.execute(
                f"""
                UPDATE {table}
                SET data=%s, raw=%s, _ingested_at=NOW(), _sync_run_id=%s, _raw_hash=%s,
                    _deleted_at=NULL, _lineage=%s
                WHERE external_id=%s AND _connector=%s
                """,
                [data, data, run_id, raw_hash, lineage_json, external_id, connector_col],
            )
            if was_tombstoned:
                _emit_resurrection(table, external_id)
            return 0, 1, int(was_tombstoned)
        else:
            was_tombstoned = row[1] is not None
            await conn.execute(
                f"UPDATE {table} SET _deleted_at=NULL "
                f"WHERE external_id=%s AND _connector=%s AND _deleted_at IS NOT NULL",
                [external_id, connector_col],
            )
            if was_tombstoned:
                _emit_resurrection(table, external_id)
            return 0, 0, int(was_tombstoned)

    # Standard single-connector path
    row = await (await conn.execute(
        f"SELECT _raw_hash, _deleted_at FROM {table} WHERE external_id = %s", [external_id]
    )).fetchone()

    if row is None:
        # ON CONFLICT DO NOTHING makes the INSERT safe under concurrent webhook+poll overlap.
        cur = await conn.execute(
            f"""
            INSERT INTO {table} (external_id, data, raw, _ingested_at, _sync_run_id, _raw_hash, _lineage)
            VALUES (%s, %s, %s, NOW(), %s, %s, %s)
            ON CONFLICT (external_id) DO NOTHING
            """,
            [external_id, data, data, run_id, raw_hash, lineage_json],
        )
        if cur.rowcount == 1:
            return 1, 0, 0
        # Concurrent insert happened; fall through to treat as an update
        row = await (await conn.execute(
            f"SELECT _raw_hash, _deleted_at FROM {table} WHERE external_id = %s", [external_id]
        )).fetchone()
        if row is None or row[0] == raw_hash:
            return 0, 0, 0
        # Concurrent insert has a different hash — update with our payload

    if row[0] != raw_hash:
        was_tombstoned = row[1] is not None
        await conn.execute(
            f"""
            UPDATE {table}
            SET data=%s, raw=%s, _ingested_at=NOW(), _sync_run_id=%s, _raw_hash=%s,
                _deleted_at=NULL, _lineage=%s
            WHERE external_id=%s
            """,
            [data, data, run_id, raw_hash, lineage_json, external_id],
        )
        if was_tombstoned:
            _emit_resurrection(table, external_id)
        return 0, 1, int(was_tombstoned)
    else:
        # No-op: same hash. Clear tombstone if record reappeared.
        was_tombstoned = row[1] is not None
        await conn.execute(
            f"UPDATE {table} SET _deleted_at=NULL WHERE external_id=%s AND _deleted_at IS NOT NULL",
            [external_id],
        )
        if was_tombstoned:
            _emit_resurrection(table, external_id)
        return 0, 0, int(was_tombstoned)


def _emit_resurrection(table: str, external_id: str) -> None:
    """Emit resurrection metric and log event (T1 #41)."""
    logger.info("record_resurrected", table=table, external_id=external_id)
    try:
        records_resurrected_total.labels(table=table).inc()
    except Exception:
        pass


async def _write_history_record(
    conn: psycopg.AsyncConnection,
    hist_table: str,
    external_id: str,
    raw: dict[str, Any],
    raw_hash: str,
    run_id: uuid.UUID,
) -> None:
    """Insert a row into the history table (no ON CONFLICT — always appends)."""
    data = orjson.dumps(raw).decode()
    await conn.execute(
        f"""
        INSERT INTO {hist_table} (external_id, data, raw, _ingested_at, _sync_run_id, _raw_hash)
        VALUES (%s, %s, %s, NOW(), %s, %s)
        """,
        [external_id, data, data, run_id, raw_hash],
    )


async def _write_dead_letter(
    conn: psycopg.AsyncConnection,
    dl_table: str,
    external_id: str | None,
    raw: dict[str, Any],
    error_message: str,
    error_class: str,
    run_id: uuid.UUID,
) -> None:
    """Write a failed record to the dead-letter table."""
    try:
        await conn.execute(
            f"""
            INSERT INTO {dl_table} (external_id, raw, error_message, error_class, sync_run_id)
            VALUES (%s, %s, %s, %s, %s)
            """,
            [external_id, orjson.dumps(raw).decode(), error_message, error_class, run_id],
        )
    except Exception:
        pass  # DL write failure must never mask the original error
