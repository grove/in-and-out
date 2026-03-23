"""Writeback engine: polls delta tables and dispatches HTTP operations."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

import anyio
import httpx
import orjson
import psycopg
import structlog
from opentelemetry import trace
from psycopg_pool import AsyncConnectionPool

from inandout.config.connector import ConnectorConfig
from inandout.config.writeback import ProtectionLevel, WritebackConfig
from inandout.transport.http import HttpTransportAdapter

logger = structlog.get_logger(__name__)
_tracer = trace.get_tracer("inandout.writeback")


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
    conflicts: int = 0
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
        with _tracer.start_as_current_span("writeback.run_cycle") as span:
            span.set_attribute("connector", connector.name)
            span.set_attribute("datatype", datatype)
            span.set_attribute("delta_table", delta_table)
            return await self._run_writeback_cycle_inner(
                connector, datatype, writeback_cfg, delta_table, span
            )

    async def _run_writeback_cycle_inner(
        self,
        connector: ConnectorConfig,
        datatype: str,
        writeback_cfg: WritebackConfig,
        delta_table: str,
        span: Any,
    ) -> WritebackResult:
        log = logger.bind(connector=connector.name, datatype=datatype, delta_table=delta_table)
        result = WritebackResult(
            connector=connector.name,
            datatype=datatype,
            delta_table=delta_table,
        )

        # Single long-lived connection holds the advisory lock for the entire cycle.
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
                rows = await self._fetch_delta_rows(
                    delta_table, log, result, batch_size=writeback_cfg.batch_size
                )
                if rows is None:
                    return result

                semaphore = anyio.Semaphore(writeback_cfg.max_concurrent_writes)

                async with HttpTransportAdapter(connector) as transport:
                    async with anyio.create_task_group() as tg:
                        for row_data in rows:
                            action = row_data.get("_action", "")
                            external_id = row_data.get("external_id") or row_data.get("_cluster_id", "")

                            async def _dispatch_with_semaphore(
                                _action: str = action,
                                _external_id: str = external_id,
                                _row: dict = row_data,
                            ) -> None:
                                async with semaphore:
                                    await self._dispatch_row(
                                        transport, connector, writeback_cfg,
                                        _action, _external_id, _row, log, result
                                    )

                            tg.start_soon(_dispatch_with_semaphore)

                await self._write_feedback(rows, result, log)

            except Exception as exc:
                result.error_message = str(exc)
                log.error("writeback_cycle_failed", error=str(exc))
            finally:
                # Release the lock on the same connection that acquired it.
                await conn.execute("SELECT pg_advisory_unlock(%s)", [lock_key])
                await conn.commit()

        return result

    async def _fetch_delta_rows(
        self,
        delta_table: str,
        log: object,
        result: WritebackResult,
        batch_size: int = 50,
    ) -> list[dict] | None:
        """Fetch up to *batch_size* non-noop rows from the delta table. Returns None if table doesn't exist."""
        try:
            async with self._pool.connection() as fetch_conn:
                cur = await fetch_conn.execute(
                    f"SELECT * FROM {delta_table} WHERE _action != 'noop' LIMIT {batch_size}"
                )
                col_names = [desc[0] for desc in cur.description or []]
                rows_raw = await cur.fetchall()
                if not rows_raw:
                    return []
                return [dict(zip(col_names, row)) for row in rows_raw]
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

                if writeback_cfg.protection_level == ProtectionLevel.optimistic:
                    # Fetch ETag via lookup GET
                    lookup_path = interpolate_path(ops.lookup.path)
                    try:
                        lookup_resp = await transport._raw_request(
                            ops.lookup.method.upper(), lookup_path
                        )
                        etag = lookup_resp.headers.get(writeback_cfg.etag_header, "")
                    except httpx.HTTPError:
                        etag = ""

                    extra_headers: dict[str, str] = {}
                    if etag:
                        extra_headers[writeback_cfg.if_match_header] = etag

                    try:
                        resp = await transport._raw_request(
                            ops.update.method.upper(),
                            path,
                            json=payload,
                            headers=extra_headers,
                        )
                        if resp.status_code == 412:
                            logger.warning(
                                "writeback_conflict_412",
                                action=action,
                                external_id=external_id,
                            )
                            result.conflicts += 1
                            result.skipped += 1
                            return
                        resp.raise_for_status()
                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code == 412:
                            logger.warning(
                                "writeback_conflict_412",
                                action=action,
                                external_id=external_id,
                            )
                            result.conflicts += 1
                            result.skipped += 1
                            return
                        raise
                else:
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
        """Write feedback to inout_ops_writeback_result log table."""
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
