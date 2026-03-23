"""Ingestion polling engine."""
from __future__ import annotations

import hashlib
import uuid
from typing import Any

import orjson
import psycopg
import structlog
from psycopg_pool import AsyncConnectionPool

from inandout.config.connector import ConnectorConfig
from inandout.config.ingestion import IngestionConfig
from inandout.postgres.schema import (
    source_table_name,
    ensure_source_table,
    dead_letter_table_name,
    ensure_dead_letter_table,
)
from inandout.postgres.watermark import get_watermark, set_watermark
from inandout.transport.http import HttpTransportAdapter

logger = structlog.get_logger(__name__)


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
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    async def run_sync(
        self,
        connector: ConnectorConfig,
        datatype: str,
        ingestion_cfg: IngestionConfig,
    ) -> SyncResult:
        run_id = uuid.uuid4()
        log = logger.bind(connector=connector.name, datatype=datatype, run_id=str(run_id))

        # Use a single long-lived connection to hold the advisory lock for the entire sync.
        # PostgreSQL advisory locks are session-scoped (connection-scoped): acquiring and
        # releasing on different connections would leave the lock permanently held or make
        # the unlock a silent no-op. Keeping one connection open guarantees correctness.
        async with self._pool.connection() as conn:
            existing_wm = await get_watermark(conn, connector.name, datatype)
            is_incremental = (
                existing_wm is not None
                and ingestion_cfg.list.incremental is not None
                and ingestion_cfg.list.incremental.enabled
            )
            mode = "incremental" if is_incremental else "full"
            result = SyncResult(run_id, connector.name, datatype, mode)

            log.info("sync_started", mode=mode, watermark=existing_wm)

            await conn.execute(
                """
                INSERT INTO inout_ops_sync_run (id, connector, datatype, mode, status, started_at)
                VALUES (%s, %s, %s, %s, 'running', NOW())
                """,
                [run_id, connector.name, datatype, mode],
            )
            await conn.commit()

            # Acquire advisory lock (non-blocking) on this connection.
            lock_key = _advisory_lock_key(connector.name, datatype)
            row = await (await conn.execute(
                "SELECT pg_try_advisory_lock(%s)", [lock_key]
            )).fetchone()
            if not row or not row[0]:
                log.warning("advisory_lock_skipped", reason="another instance holds the lock")
                await conn.execute(
                    "UPDATE inout_ops_sync_run SET status='skipped', finished_at=NOW() WHERE id=%s",
                    [run_id],
                )
                await conn.commit()
                result.status = "skipped"
                return result

            try:
                await self._do_sync(connector, datatype, ingestion_cfg, result, existing_wm, log)
            except Exception as exc:
                result.status = "failed"
                result.error_message = str(exc)
                log.error("sync_failed", error=str(exc))
            finally:
                # Update sync_run record and release the lock on the SAME connection
                # that acquired it, so the unlock is not a no-op.
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
                await conn.execute("SELECT pg_advisory_unlock(%s)", [lock_key])
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
        return result

    async def _do_sync(
        self,
        connector: ConnectorConfig,
        datatype: str,
        ingestion_cfg: IngestionConfig,
        result: SyncResult,
        watermark: str | None,
        log: Any,
    ) -> None:
        # Ensure source table and dead-letter table exist
        async with self._pool.connection() as conn:
            await ensure_source_table(conn, connector.name, datatype)
            await ensure_dead_letter_table(conn, "ingestion", connector.name, datatype)
            await conn.commit()

        table = source_table_name(connector.name, datatype)
        dl_table = dead_letter_table_name("ingestion", connector.name, datatype)
        new_watermark: str | None = None
        seen_ids: set[str] = set()

        async with HttpTransportAdapter(connector) as transport:
            async for page in transport.fetch_pages(ingestion_cfg.list, watermark=watermark):
                result.records_fetched += len(page)
                if not page:
                    continue

                async with self._pool.connection() as conn:
                    async with conn.transaction():
                        for record in page:
                            raw_hash = _compute_raw_hash(record)
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
                            inserted, updated = await _upsert_record(
                                conn, table, external_id, record, raw_hash, result.run_id
                            )
                            result.records_inserted += inserted
                            result.records_updated += updated

                            # Track latest watermark from cursor field
                            inc = ingestion_cfg.list.incremental
                            if inc and inc.cursor_field:
                                val = record.get(inc.cursor_field)
                                if val is not None:
                                    candidate = str(val)
                                    if new_watermark is None or candidate > new_watermark:
                                        new_watermark = candidate

                        # Update watermark atomically within the same transaction
                        if new_watermark:
                            inc = ingestion_cfg.list.incremental
                            wm_type = inc.cursor_type.value if inc and inc.cursor_type else "cursor"
                            await set_watermark(
                                conn, connector.name, datatype, wm_type, new_watermark, result.run_id
                            )

        # Full-sync deletion detection: tombstone records not seen in this run.
        # Guarded by a circuit breaker: skip if deletion would affect > 50% of existing
        # records (signals a partial or failed fetch rather than genuine deletions).
        if watermark is None and seen_ids:
            await self._tombstone_missing(table, seen_ids, result, log)

        result.status = "completed"

    async def _tombstone_missing(
        self,
        table: str,
        seen_ids: set[str],
        result: SyncResult,
        log: Any,
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
) -> tuple[int, int]:
    """Upsert a record. Returns (inserted, updated)."""
    data = orjson.dumps(raw).decode()
    row = await (await conn.execute(
        f"SELECT _raw_hash FROM {table} WHERE external_id = %s", [external_id]
    )).fetchone()

    if row is None:
        await conn.execute(
            f"""
            INSERT INTO {table} (external_id, data, raw, _ingested_at, _sync_run_id, _raw_hash)
            VALUES (%s, %s, %s, NOW(), %s, %s)
            """,
            [external_id, data, data, run_id, raw_hash],
        )
        return 1, 0
    elif row[0] != raw_hash:
        await conn.execute(
            f"""
            UPDATE {table}
            SET data=%s, raw=%s, _ingested_at=NOW(), _sync_run_id=%s, _raw_hash=%s,
                _deleted_at=NULL
            WHERE external_id=%s
            """,
            [data, data, run_id, raw_hash, external_id],
        )
        return 0, 1
    else:
        # No-op: same hash. Clear tombstone if record reappeared.
        await conn.execute(
            f"UPDATE {table} SET _deleted_at=NULL WHERE external_id=%s AND _deleted_at IS NOT NULL",
            [external_id],
        )
        return 0, 0


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
