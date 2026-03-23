"""Writeback engine: polls delta tables and dispatches HTTP operations."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

import httpx
import orjson
import psycopg
import structlog
from psycopg_pool import AsyncConnectionPool

from inandout.config.connector import ConnectorConfig
from inandout.config.writeback import WritebackConfig
from inandout.transport.http import HttpTransportAdapter

logger = structlog.get_logger(__name__)


def _advisory_lock_key(connector: str, datatype: str) -> int:
    """Deterministic int64 key for pg_advisory_lock from connector+datatype."""
    digest = hashlib.md5(f"{connector}:{datatype}".encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


@dataclass
class WritebackResult:
    connector: str
    datatype: str
    delta_table: str
    processed: int = 0
    skipped: int = 0
    failed: int = 0
    error_message: str | None = None


class WritebackEngine:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    async def run_writeback_cycle(
        self,
        connector: ConnectorConfig,
        datatype: str,
        writeback_cfg: WritebackConfig,
        delta_table: str,
    ) -> WritebackResult:
        log = logger.bind(connector=connector.name, datatype=datatype, delta_table=delta_table)
        result = WritebackResult(
            connector=connector.name,
            datatype=datatype,
            delta_table=delta_table,
        )

        async with self._pool.connection() as conn:
            lock_key = _advisory_lock_key(connector.name, datatype)
            row = await (await conn.execute(
                "SELECT pg_try_advisory_lock(%s)", [lock_key]
            )).fetchone()
            if not row or not row[0]:
                log.warning("writeback_advisory_lock_skipped", reason="another instance holds the lock")
                result.skipped = 1
                return result

        try:
            rows = await self._fetch_delta_rows(conn, delta_table, log, result)
            if rows is None:
                return result

            async with HttpTransportAdapter(connector) as transport:
                for row in rows:
                    action = row.get("_action", "")
                    external_id = row.get("external_id") or row.get("_cluster_id", "")
                    await self._dispatch_row(
                        transport, connector, writeback_cfg,
                        action, external_id, row, log, result
                    )

            await self._write_feedback(rows, result, log)

        except Exception as exc:
            result.error_message = str(exc)
            log.error("writeback_cycle_failed", error=str(exc))
        finally:
            async with self._pool.connection() as conn:
                await conn.execute("SELECT pg_advisory_unlock(%s)", [lock_key])
                await conn.commit()

        return result

    async def _fetch_delta_rows(
        self,
        conn: psycopg.AsyncConnection,
        delta_table: str,
        log: object,
        result: WritebackResult,
    ) -> list[dict] | None:
        """Fetch up to 50 non-noop rows from the delta table. Returns None if table doesn't exist."""
        try:
            async with self._pool.connection() as fetch_conn:
                rows_raw = await (await fetch_conn.execute(
                    f"SELECT * FROM {delta_table} WHERE _action != 'noop' LIMIT 50"
                )).fetchall()
                if not rows_raw:
                    return []
                # Get column names
                cur = await fetch_conn.execute(
                    f"SELECT * FROM {delta_table} WHERE _action != 'noop' LIMIT 50"
                )
                col_names = [desc[0] for desc in cur.description or []]
                rows = [dict(zip(col_names, row)) for row in rows_raw]
                return rows
        except psycopg.errors.UndefinedTable:
            logger.warning("delta_table_not_found", delta_table=delta_table)
            result.skipped = 1
            return None

    async def _dispatch_row(
        self,
        transport: HttpTransportAdapter,
        connector: ConnectorConfig,
        writeback_cfg: WritebackConfig,
        action: str,
        external_id: str,
        row: dict,
        log: object,
        result: WritebackResult,
    ) -> None:
        """Dispatch one delta row via HTTP."""
        ops = writeback_cfg.operations

        def interpolate_path(path: str) -> str:
            return path.replace("${external_id}", external_id or "")

        try:
            if action == "insert":
                if ops.insert is None:
                    result.skipped += 1
                    return
                payload = {k: v for k, v in row.items() if not k.startswith("_")}
                path = interpolate_path(ops.insert.path)
                await transport._request(ops.insert.method.upper(), path, json=payload)
                result.processed += 1

            elif action == "update":
                if ops.update is None:
                    result.skipped += 1
                    return
                payload = {k: v for k, v in row.items() if not k.startswith("_")}
                path = interpolate_path(ops.update.path)
                await transport._request(ops.update.method.upper(), path, json=payload)
                result.processed += 1

            elif action == "delete":
                if ops.delete is None:
                    result.skipped += 1
                    return
                path = interpolate_path(ops.delete.path)
                await transport._request(ops.delete.method.upper(), path)
                result.processed += 1

            elif action == "archive":
                if ops.archive is None:
                    result.skipped += 1
                    return
                payload = {k: v for k, v in row.items() if not k.startswith("_")}
                path = interpolate_path(ops.archive.path)
                await transport._request(ops.archive.method.upper(), path, json=payload)
                result.processed += 1

            else:
                logger.warning("unsupported_writeback_action", action=action)
                result.skipped += 1

        except httpx.HTTPError as exc:
            logger.error("writeback_http_error", action=action, external_id=external_id, error=str(exc))
            result.failed += 1

    async def _write_feedback(
        self,
        rows: list[dict],
        result: WritebackResult,
        log: object,
    ) -> None:
        """Write feedback to inout_ops_writeback_result log table if it exists."""
        if not rows:
            return
        try:
            async with self._pool.connection() as conn:
                for row in rows:
                    action = row.get("_action", "")
                    external_id = row.get("external_id") or row.get("_cluster_id", "")
                    await conn.execute(
                        """
                        INSERT INTO inout_ops_writeback_result
                            (connector, datatype, delta_table, action, external_id, status, processed_at)
                        VALUES (%s, %s, %s, %s, %s, 'ok', NOW())
                        """,
                        [
                            result.connector,
                            result.datatype,
                            result.delta_table,
                            action,
                            external_id,
                        ],
                    )
                await conn.commit()
        except psycopg.errors.UndefinedTable:
            logger.debug("writeback_result_table_not_found_skipping_feedback")
        except Exception as exc:
            logger.warning("writeback_feedback_write_failed", error=str(exc))
