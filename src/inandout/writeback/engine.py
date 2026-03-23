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
from inandout.config.writeback import ConflictResolution, ProtectionLevel, WritebackConfig
from inandout.observability.metrics import conflicts_detected_total
from inandout.transport.http import HttpTransportAdapter
from inandout.writeback.merge_hooks import merge_hook_registry

logger = structlog.get_logger(__name__)
_tracer = trace.get_tracer("inandout.writeback")


def _advisory_lock_key(connector: str, datatype: str) -> int:
    """Deterministic int64 key for pg_advisory_lock from connector+datatype."""
    digest = hashlib.md5(f"{connector}:{datatype}".encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


def _compute_row_hash(row: dict[str, Any]) -> str:
    """Stable SHA-256 hash of non-_ fields of a row (sorted keys, orjson)."""
    payload = {k: v for k, v in row.items() if not k.startswith("_")}
    return hashlib.sha256(
        orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)
    ).hexdigest()


def _compute_field_diff(
    last_written: dict[str, Any],
    sent_payload: dict[str, Any],
) -> dict[str, Any]:
    """Compute field diff between last_written and sent_payload."""
    added: list[str] = []
    removed: list[str] = []
    changed: dict[str, dict[str, Any]] = {}

    for k, v in sent_payload.items():
        if k not in last_written:
            added.append(k)
        elif last_written[k] != v:
            changed[k] = {"from": last_written[k], "to": v}

    for k in last_written:
        if k not in sent_payload:
            removed.append(k)

    return {"added": added, "removed": removed, "changed": changed}


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
    # Accumulates (external_id, action, payload, diff) for audit trail
    _audit_entries: list[tuple[str, str, dict[str, Any] | None, dict[str, Any] | None]] = field(
        default_factory=list
    )


class WritebackEngine:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self._pool = pool

    async def run_writeback_cycle(
        self,
        connector: ConnectorConfig,
        datatype: str,
        writeback_cfg: WritebackConfig,
        delta_table: str,
        max_concurrent_writes_override: int | None = None,
    ) -> WritebackResult:
        with _tracer.start_as_current_span("writeback.run_cycle") as span:
            span.set_attribute("connector", connector.name)
            span.set_attribute("datatype", datatype)
            span.set_attribute("delta_table", delta_table)
            return await self._run_writeback_cycle_inner(
                connector, datatype, writeback_cfg, delta_table, span,
                max_concurrent_writes_override=max_concurrent_writes_override,
            )

    async def _run_writeback_cycle_inner(
        self,
        connector: ConnectorConfig,
        datatype: str,
        writeback_cfg: WritebackConfig,
        delta_table: str,
        span: Any,
        max_concurrent_writes_override: int | None = None,
    ) -> WritebackResult:
        log = logger.bind(connector=connector.name, datatype=datatype, delta_table=delta_table)
        result = WritebackResult(
            connector=connector.name,
            datatype=datatype,
            delta_table=delta_table,
        )

        # Determine effective max_concurrent_writes
        effective_max_writes = (
            max_concurrent_writes_override
            if max_concurrent_writes_override is not None
            else writeback_cfg.max_concurrent_writes
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

                # Step 67: crash recovery — skip already-sent rows from audit log
                if writeback_cfg.enable_crash_recovery and rows:
                    rows = await self._deduplicate_with_audit(
                        rows, connector.name, datatype, delta_table, log, result
                    )

                semaphore = anyio.Semaphore(effective_max_writes)

                async with HttpTransportAdapter(connector) as transport:
                    # Group rows by external_id to preserve per-id ordering
                    grouped: dict[str, list[dict[str, Any]]] = {}
                    for row_data in rows:
                        ext_id = row_data.get("external_id") or row_data.get("_cluster_id", "")
                        if ext_id not in grouped:
                            grouped[ext_id] = []
                        grouped[ext_id].append(row_data)

                    async def _dispatch_group(
                        group_rows: list[dict[str, Any]],
                    ) -> None:
                        async with semaphore:
                            for row_data in group_rows:
                                action = row_data.get("_action", "")
                                external_id = row_data.get("external_id") or row_data.get("_cluster_id", "")
                                await self._dispatch_row(
                                    transport, connector, writeback_cfg,
                                    action, external_id, row_data, log, result
                                )

                    async with anyio.create_task_group() as tg:
                        for group_rows in grouped.values():
                            tg.start_soon(_dispatch_group, group_rows)

                await self._write_feedback(rows, result, log)

            except Exception as exc:
                result.error_message = str(exc)
                log.error("writeback_cycle_failed", error=str(exc))
            finally:
                # Release the lock on the same connection that acquired it.
                await conn.execute("SELECT pg_advisory_unlock(%s)", [lock_key])
                await conn.commit()

        return result

    async def _deduplicate_with_audit(
        self,
        rows: list[dict],
        connector: str,
        datatype: str,
        delta_table: str,
        log: object,
        result: WritebackResult,
    ) -> list[dict]:
        """Filter out rows that were already successfully sent (crash recovery)."""
        try:
            async with self._pool.connection() as conn:
                audit_rows = await (await conn.execute(
                    """
                    SELECT external_id, action
                    FROM inout_ops_writeback_result
                    WHERE connector = %s AND datatype = %s AND delta_table = %s
                      AND processed_at > NOW() - INTERVAL '1 hour'
                      AND status = 'ok'
                    """,
                    [connector, datatype, delta_table],
                )).fetchall()
        except Exception:
            # If the audit table doesn't exist yet, skip deduplication
            return rows

        already_sent: set[tuple[str, str]] = {
            (str(r[0]), str(r[1])) for r in audit_rows
        }
        if not already_sent:
            return rows

        filtered = []
        skipped = 0
        for row in rows:
            external_id = str(row.get("external_id") or row.get("_cluster_id", ""))
            action = str(row.get("_action", ""))
            if (external_id, action) in already_sent:
                skipped += 1
            else:
                filtered.append(row)

        if skipped:
            logger.info(
                "writeback_resume_skipped_rows",
                connector=connector,
                datatype=datatype,
                skipped=skipped,
            )
        return filtered

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

    async def _get_last_written(
        self,
        connector: ConnectorConfig,
        datatype: str,
        external_id: str,
    ) -> dict[str, Any]:
        """Fetch _last_written dict for the given external_id from the source table."""
        try:
            from inandout.postgres.schema import source_table_name
            src_table = source_table_name(connector.name, datatype)
            async with self._pool.connection() as conn:
                lw_row = await (await conn.execute(
                    f"SELECT _last_written FROM {src_table} WHERE external_id = %s",
                    [external_id],
                )).fetchone()
            if lw_row and lw_row[0]:
                return lw_row[0] if isinstance(lw_row[0], dict) else {}
        except Exception:
            pass
        return {}

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
        # Fan-in join enrichment before dispatching
        if writeback_cfg.join_sources:
            from inandout.writeback.fan_in import enrich_with_join_sources
            row = await enrich_with_join_sources(self._pool, row, writeback_cfg.join_sources)

        ops = writeback_cfg.operations

        def interpolate_path(path: str) -> str:
            return path.replace("${external_id}", external_id or "")

        def _make_extra_headers(payload: dict[str, Any]) -> dict[str, str]:
            """Build extra headers including idempotency key if configured."""
            headers: dict[str, str] = {}
            if writeback_cfg.idempotency_key_header:
                raw_hash = _compute_row_hash(row)
                key_material = f"{connector.name}:{result.datatype}:{external_id}:{raw_hash}"
                idempotency_key = hashlib.sha256(key_material.encode()).hexdigest()
                headers[writeback_cfg.idempotency_key_header] = idempotency_key
            return headers

        try:
            if action == "insert":
                if ops.insert is None:
                    result.skipped += 1
                    return
                payload = {k: v for k, v in row.items() if not k.startswith("_")}
                path = interpolate_path(ops.insert.path)
                extra_headers = _make_extra_headers(payload)
                if extra_headers:
                    insert_resp = await transport._raw_request(ops.insert.method.upper(), path, json=payload, headers=extra_headers)
                else:
                    insert_resp = await transport._raw_request(ops.insert.method.upper(), path, json=payload)
                # Capture internal_id from response body (id or connector.name + "_id" field)
                try:
                    resp_body: dict[str, Any] = {}
                    try:
                        resp_body = orjson.loads(insert_resp.content) if insert_resp.content else {}
                    except Exception:
                        pass
                    internal_id: str | None = None
                    for id_field in ("id", f"{result.connector}_id", f"{result.datatype}_id"):
                        if id_field in resp_body:
                            internal_id = str(resp_body[id_field])
                            break
                    if internal_id and external_id:
                        await self._record_identity_map(
                            connector=result.connector,
                            datatype=result.datatype,
                            external_id=external_id,
                            internal_id=internal_id,
                        )
                except Exception:
                    pass  # Identity map failure must not block writeback
                # Record audit
                last_written: dict[str, Any] = {}
                diff = _compute_field_diff(last_written, payload)
                result._audit_entries.append((external_id, action, payload, diff))
                result.processed += 1

            elif action == "update":
                if ops.update is None:
                    result.skipped += 1
                    return
                payload = {k: v for k, v in row.items() if not k.startswith("_")}
                path = interpolate_path(ops.update.path)

                # Incremental writeback: only send changed fields
                if writeback_cfg.diff_fields and external_id:
                    try:
                        from inandout.postgres.schema import source_table_name
                        src_table = source_table_name(connector.name, result.datatype)
                        async with self._pool.connection() as diff_conn:
                            lw_row = await (await diff_conn.execute(
                                f"SELECT _last_written FROM {src_table} WHERE external_id = %s",
                                [external_id],
                            )).fetchone()
                        if lw_row and lw_row[0]:
                            last_written = lw_row[0] if isinstance(lw_row[0], dict) else {}
                            payload = {k: v for k, v in payload.items() if last_written.get(k) != v}
                        if not payload:
                            result.skipped += 1
                            return
                    except Exception:
                        pass  # Fall through to full payload if diff fails

                sent_payload: dict[str, Any] | None = None
                sent_diff: dict[str, Any] | None = None

                if writeback_cfg.protection_level == ProtectionLevel.optimistic:
                    # Fetch ETag via lookup GET
                    lookup_path = interpolate_path(ops.lookup.path)
                    try:
                        lookup_resp = await transport._raw_request(
                            ops.lookup.method.upper(), lookup_path
                        )
                        etag = lookup_resp.headers.get(writeback_cfg.etag_header, "")
                        remote_data: dict[str, Any] = {}
                        try:
                            remote_data = orjson.loads(lookup_resp.content)
                        except Exception:
                            remote_data = {}
                    except httpx.HTTPError:
                        etag = ""
                        remote_data = {}

                    # Apply conflict resolution strategy
                    conflict_resolution = writeback_cfg.conflict_resolution

                    if conflict_resolution == ConflictResolution.server_wins:
                        # Compare remote vs _last_written for any server-changed fields
                        last_written = await self._get_last_written(connector, result.datatype, external_id)
                        non_under_fields = {k: v for k, v in row.items() if not k.startswith("_")}
                        server_changed = False
                        for field_name, local_val in non_under_fields.items():
                            last_val = last_written.get(field_name)
                            remote_val = remote_data.get(field_name)
                            if remote_val is not None and remote_val != last_val:
                                server_changed = True
                                break
                        if server_changed:
                            logger.warning(
                                "writeback_conflict_server_wins",
                                action=action,
                                external_id=external_id,
                            )
                            try:
                                conflicts_detected_total.labels(
                                    connector=connector.name,
                                    datatype=result.datatype,
                                    resolution="server_wins",
                                    namespace="public",
                                ).inc()
                            except Exception:
                                pass
                            result.skipped += 1
                            result.conflicts += 1
                            return
                        # No conflict — proceed normally with the original payload
                        final_payload = payload

                    elif conflict_resolution == ConflictResolution.merge_fields:
                        last_written = await self._get_last_written(connector, result.datatype, external_id)
                        merged: dict[str, Any] = {}
                        conflicted_fields: list[str] = []
                        for field_name, local_val in payload.items():
                            last_val = last_written.get(field_name)
                            remote_val = remote_data.get(field_name)
                            if remote_val is not None and remote_val != last_val:
                                # Server changed this field — keep server value
                                merged[field_name] = remote_val
                                conflicted_fields.append(field_name)
                            else:
                                # Server unchanged or field not in remote — use local value
                                merged[field_name] = local_val
                        if conflicted_fields:
                            logger.info(
                                "writeback_conflict_merged",
                                action=action,
                                external_id=external_id,
                                conflicted_fields=conflicted_fields,
                            )
                            try:
                                conflicts_detected_total.labels(
                                    connector=connector.name,
                                    datatype=result.datatype,
                                    resolution="merge_fields",
                                    namespace="public",
                                ).inc()
                            except Exception:
                                pass
                        final_payload = merged

                    elif conflict_resolution == ConflictResolution.custom_merge:
                        hook = merge_hook_registry.get(connector.name, result.datatype)
                        last_written = await self._get_last_written(connector, result.datatype, external_id)
                        if hook is not None:
                            logger.info(
                                "writeback_custom_merge",
                                action=action,
                                external_id=external_id,
                            )
                            final_payload = await hook(payload, remote_data, last_written)
                        else:
                            # Fall back to merge_fields
                            logger.warning(
                                "writeback_custom_merge_no_hook",
                                connector=connector.name,
                                datatype=result.datatype,
                            )
                            merged = {}
                            for field_name, local_val in payload.items():
                                last_val = last_written.get(field_name)
                                remote_val = remote_data.get(field_name)
                                if remote_val is not None and remote_val != last_val:
                                    merged[field_name] = remote_val
                                else:
                                    merged[field_name] = local_val
                            final_payload = merged

                    else:
                        # last_writer_wins (default) — use local payload as-is
                        final_payload = payload

                    extra_headers: dict[str, str] = {}
                    if etag:
                        extra_headers[writeback_cfg.if_match_header] = etag
                    # Add idempotency key if configured
                    extra_headers.update(_make_extra_headers(final_payload))

                    try:
                        resp = await transport._raw_request(
                            ops.update.method.upper(),
                            path,
                            json=final_payload,
                            headers=extra_headers,
                        )
                        if resp.status_code == 412:
                            logger.warning(
                                "writeback_conflict_412",
                                action=action,
                                external_id=external_id,
                            )
                            try:
                                conflicts_detected_total.labels(
                                    connector=connector.name,
                                    datatype=result.datatype,
                                    resolution="412_precondition_failed",
                                    namespace="public",
                                ).inc()
                            except Exception:
                                pass
                            result.conflicts += 1
                            result.skipped += 1
                            return
                        resp.raise_for_status()
                        sent_payload = final_payload
                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code == 412:
                            logger.warning(
                                "writeback_conflict_412",
                                action=action,
                                external_id=external_id,
                            )
                            try:
                                conflicts_detected_total.labels(
                                    connector=connector.name,
                                    datatype=result.datatype,
                                    resolution="412_precondition_failed",
                                    namespace="public",
                                ).inc()
                            except Exception:
                                pass
                            result.conflicts += 1
                            result.skipped += 1
                            return
                        raise
                else:
                    extra_headers = _make_extra_headers(payload)
                    if extra_headers:
                        await transport._raw_request(ops.update.method.upper(), path, json=payload, headers=extra_headers)
                    else:
                        await transport._request(ops.update.method.upper(), path, json=payload)
                    sent_payload = payload

                # Compute audit diff
                if sent_payload is not None:
                    lw_for_diff = await self._get_last_written(connector, result.datatype, external_id)
                    sent_diff = _compute_field_diff(lw_for_diff, sent_payload)

                # Update _last_written on successful PATCH when diff_fields is enabled
                if writeback_cfg.diff_fields and external_id:
                    try:
                        import orjson as _orjson
                        from inandout.postgres.schema import source_table_name as _src_table_name
                        src_table = _src_table_name(connector.name, result.datatype)
                        full_payload = {k: v for k, v in row.items() if not k.startswith("_")}
                        async with self._pool.connection() as lw_conn:
                            await lw_conn.execute(
                                f"UPDATE {src_table} SET _last_written = %s WHERE external_id = %s",
                                [_orjson.dumps(full_payload).decode(), external_id],
                            )
                            await lw_conn.commit()
                    except Exception:
                        pass  # Non-critical: don't fail writeback over _last_written update

                result._audit_entries.append((external_id, action, sent_payload, sent_diff))
                result.processed += 1

            elif action == "delete":
                if ops.delete is None:
                    result.skipped += 1
                    return
                path = interpolate_path(ops.delete.path)
                extra_headers = _make_extra_headers({})
                if extra_headers:
                    await transport._raw_request(ops.delete.method.upper(), path, headers=extra_headers)
                else:
                    await transport._request(ops.delete.method.upper(), path)
                result._audit_entries.append((external_id, action, None, None))
                result.processed += 1

            elif action == "archive":
                if ops.archive is None:
                    result.skipped += 1
                    return
                payload = {k: v for k, v in row.items() if not k.startswith("_")}
                path = interpolate_path(ops.archive.path)
                extra_headers = _make_extra_headers(payload)
                if extra_headers:
                    await transport._raw_request(ops.archive.method.upper(), path, json=payload, headers=extra_headers)
                else:
                    await transport._request(ops.archive.method.upper(), path, json=payload)
                result._audit_entries.append((external_id, action, payload, None))
                result.processed += 1

            else:
                logger.warning("unsupported_writeback_action", action=action)
                result.skipped += 1

        except httpx.HTTPError as exc:
            logger.error("writeback_http_error", action=action, external_id=external_id, error=str(exc))
            result.failed += 1

    async def _record_identity_map(
        self,
        connector: str,
        datatype: str,
        external_id: str,
        internal_id: str,
    ) -> None:
        """Upsert a (connector, datatype, external_id) → internal_id mapping."""
        try:
            async with self._pool.connection() as conn:
                await conn.execute(
                    """
                    INSERT INTO inout_ops_identity_map
                        (connector, datatype, external_id, internal_id, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (connector, datatype, external_id) DO UPDATE
                    SET internal_id = EXCLUDED.internal_id, updated_at = NOW()
                    """,
                    [connector, datatype, external_id, internal_id],
                )
                await conn.commit()
        except psycopg.errors.UndefinedTable:
            pass  # Migration not yet run — silently skip
        except Exception as exc:
            logger.warning("identity_map_write_failed", error=str(exc))

    async def _write_feedback(
        self,
        rows: list[dict],
        result: WritebackResult,
        log: object,
    ) -> None:
        """Write feedback to inout_ops_writeback_result log table."""
        if not rows:
            return
        # Build audit map from accumulated entries
        audit_map: dict[tuple[str, str], tuple[dict[str, Any] | None, dict[str, Any] | None]] = {}
        for ext_id, act, payload, diff in result._audit_entries:
            audit_map[(ext_id, act)] = (payload, diff)

        try:
            async with self._pool.connection() as conn:
                for row in rows:
                    action = row.get("_action", "")
                    external_id = row.get("external_id") or row.get("_cluster_id", "")
                    payload_snap, field_diff = audit_map.get((external_id, action), (None, None))

                    # Try inserting with audit columns (migration 006 may not exist yet)
                    try:
                        await conn.execute(
                            """
                            INSERT INTO inout_ops_writeback_result
                                (connector, datatype, delta_table, action, external_id, status,
                                 payload_snapshot, field_diff, processed_at)
                            VALUES (%s, %s, %s, %s, %s, 'ok', %s, %s, NOW())
                            """,
                            [
                                result.connector,
                                result.datatype,
                                result.delta_table,
                                action,
                                external_id,
                                orjson.dumps(payload_snap).decode() if payload_snap is not None else None,
                                orjson.dumps(field_diff).decode() if field_diff is not None else None,
                            ],
                        )
                    except Exception:
                        # Fall back to insert without audit columns
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
