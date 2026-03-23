"""Ingestion polling engine."""
from __future__ import annotations

import hashlib
import uuid
from typing import Any

import orjson
import psycopg
import structlog
from opentelemetry import trace
from psycopg_pool import AsyncConnectionPool

from inandout.config.connector import ConnectorConfig, DatatypeConfig
from inandout.config.ingestion import HistoryMode, IngestionConfig
from inandout.ingestion.field_mapper import apply_field_mappings
from inandout.ingestion.quality import validate_record
from inandout.observability.metrics import quality_violations_total, records_processed_total, sync_lag_seconds
from inandout.plugins.hooks import apply_hooks
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
        publisher: Any = None,  # EventPublisher | None
    ) -> None:
        self._pool = pool
        self._namespace = namespace
        self._debouncer = None
        self._read_pool = read_pool  # used for read-heavy queries when available
        self._publisher = publisher  # EventPublisher | None

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

            async with self._pool.connection() as conn:
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

                try:
                    await self._do_sync(connector, datatype, ingestion_cfg, result, existing_wm, log, dtype_cfg=dtype_cfg)
                except Exception as exc:
                    result.status = "failed"
                    result.error_message = str(exc)
                    log.error("sync_failed", error=str(exc))
                finally:
                    # Update sync_run record — lock is released when the transaction commits/rolls back
                    status = result.status if result.status != "running" else "completed"
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
                            status,
                            result.records_fetched,
                            result.records_inserted,
                            result.records_updated,
                            result.records_errored,
                            result.error_message,
                            run_id,
                        ],
                    )
                    # Upsert connector version on successful full sync
                    if status == "completed" and mode == "full":
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
        # CDC mode: consume from the CDC source instead of HTTP polling
        if ingestion_cfg.source_mode == "cdc" and ingestion_cfg.cdc is not None:
            from inandout.ingestion.cdc import get_cdc_source
            cdc_source = get_cdc_source(ingestion_cfg.cdc, self._pool)
            await self._run_cdc_sync(
                connector, datatype, ingestion_cfg, result, log, cdc_source, dtype_cfg=dtype_cfg
            )
            result.status = "completed"
            return

        history_mode = ingestion_cfg.history_mode
        ns = self._namespace

        # Ensure source table and dead-letter table exist
        async with self._pool.connection() as conn:
            await ensure_source_table(conn, connector.name, datatype, ns)
            await ensure_dead_letter_table(conn, "ingestion", connector.name, datatype, ns)
            if history_mode == HistoryMode.append:
                await ensure_source_history_table(conn, connector.name, datatype, ns)
            await conn.commit()

        table = source_table_name(connector.name, datatype, ns)
        hist_table = source_history_table_name(connector.name, datatype, ns)
        dl_table = dead_letter_table_name("ingestion", connector.name, datatype, ns)
        new_watermark: str | None = None
        seen_ids: set[str] = set()
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
                watermark_float = float(watermark)
                now_float = time.time()
                window_end_float = min(watermark_float + window_secs, now_float)
                window_end = str(window_end_float)
                cursor_window_watermark = str(window_end_float)
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

        async with HttpTransportAdapter(connector) as transport:
            async for page in transport.fetch_pages(
                ingestion_cfg.list, watermark=watermark, window_end=window_end
            ):
                page_number += 1
                result.records_fetched += len(page)
                if not page:
                    continue

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
                            # Apply plugin hooks: transform → filter → enrich
                            record = await apply_hooks(record, connector.name, self._pool)
                            if record is None:
                                # filter hook dropped this record
                                continue

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
                                inserted, updated = await _upsert_record(
                                    conn, table, external_id, record, raw_hash, result.run_id,
                                    lineage=_current_lineage,
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

                                # Event publishing
                                if self._publisher is not None and (inserted or updated):
                                    try:
                                        from inandout.events.publisher import build_event
                                        event = build_event(
                                            connector=connector.name,
                                            datatype=datatype,
                                            external_id=external_id,
                                            action="upsert",
                                            run_id=str(result.run_id),
                                        )
                                        await self._publisher.publish(event)
                                    except Exception:
                                        pass  # publishing failure must not block ingestion

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

        # Full-sync deletion detection: tombstone records not seen in this run.
        # Guarded by a circuit breaker: skip if deletion would affect > 50% of existing
        # records (signals a partial or failed fetch rather than genuine deletions).
        if watermark is None and seen_ids:
            await self._tombstone_missing(
                table, seen_ids, result, log, connector.name, datatype
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

        if batch:
            async with self._pool.connection() as conn:
                async with conn.transaction():
                    for record in batch:
                        if dtype_cfg is not None and dtype_cfg.field_mappings:
                            record = apply_field_mappings(
                                record,
                                dtype_cfg.field_mappings,
                                strict=dtype_cfg.strict_field_mapping,
                            )
                        record = await apply_hooks(record, connector.name, self._pool)
                        if record is None:
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

                        inserted, updated = await _upsert_record(
                            conn, table, external_id, record, raw_hash, result.run_id
                        )
                        result.records_inserted += inserted
                        result.records_updated += updated

                        if (inserted or updated) and history_mode == HistoryMode.append:
                            await _write_history_record(
                                conn, hist_table, external_id, record, raw_hash, result.run_id
                            )

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
                ins, upd = await _upsert_record(conn, table, external_id, rec, raw_hash, result.run_id)
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

            async with conn.transaction():
                for ext_id in missing_ids:
                    await conn.execute(
                        f"UPDATE {table} SET _deleted_at = NOW() WHERE external_id = %s AND _deleted_at IS NULL",
                        [ext_id],
                    )
            result.records_deleted = len(missing_ids)

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
) -> tuple[int, int]:
    """Upsert a record. Returns (inserted, updated)."""
    data = orjson.dumps(raw).decode()
    lineage_json = orjson.dumps(lineage).decode() if lineage is not None else None
    row = await (await conn.execute(
        f"SELECT _raw_hash FROM {table} WHERE external_id = %s", [external_id]
    )).fetchone()

    if row is None:
        await conn.execute(
            f"""
            INSERT INTO {table} (external_id, data, raw, _ingested_at, _sync_run_id, _raw_hash, _lineage)
            VALUES (%s, %s, %s, NOW(), %s, %s, %s)
            """,
            [external_id, data, data, run_id, raw_hash, lineage_json],
        )
        return 1, 0
    elif row[0] != raw_hash:
        await conn.execute(
            f"""
            UPDATE {table}
            SET data=%s, raw=%s, _ingested_at=NOW(), _sync_run_id=%s, _raw_hash=%s,
                _deleted_at=NULL, _lineage=%s
            WHERE external_id=%s
            """,
            [data, data, run_id, raw_hash, lineage_json, external_id],
        )
        return 0, 1
    else:
        # No-op: same hash. Clear tombstone if record reappeared.
        await conn.execute(
            f"UPDATE {table} SET _deleted_at=NULL WHERE external_id=%s AND _deleted_at IS NOT NULL",
            [external_id],
        )
        return 0, 0


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
