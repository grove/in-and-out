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
from inandout.postgres.schema import source_table_name, ensure_source_table
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

        # Determine mode
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

        async with self._pool.connection() as conn:
            # Create sync run record
            await conn.execute(
                """
                INSERT INTO inout_ops_sync_run (id, connector, datatype, mode, status, started_at)
                VALUES (%s, %s, %s, %s, 'running', NOW())
                """,
                [run_id, connector.name, datatype, mode],
            )
            await conn.commit()

            # Acquire advisory lock (non-blocking)
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
            async with self._pool.connection() as conn:
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
                        result.status if result.status != "running" else "completed",
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
        # Ensure source table exists
        async with self._pool.connection() as conn:
            await ensure_source_table(conn, connector.name, datatype)
            await conn.commit()

        table = source_table_name(connector.name, datatype)
        new_watermark: str | None = None

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
                                continue

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

        result.status = "completed"


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
            SET data=%s, raw=%s, _ingested_at=NOW(), _sync_run_id=%s, _raw_hash=%s
            WHERE external_id=%s
            """,
            [data, data, run_id, raw_hash, external_id],
        )
        return 0, 1
    else:
        return 0, 0  # no-op: same hash
