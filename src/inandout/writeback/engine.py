"""Writeback engine: polls delta tables and dispatches HTTP operations."""
from __future__ import annotations

import hashlib
import uuid
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
from inandout.postgres.desired_state import get_lwstate, update_desired_state_status, upsert_lwstate
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
    # B1: populated when dry_run=True
    dry_run_log: list[dict[str, Any]] = field(default_factory=list)
    # Accumulates (external_id, action, payload, diff) for audit trail
    _audit_entries: list[tuple[str, str, dict[str, Any] | None, dict[str, Any] | None]] = field(
        default_factory=list
    )
    # Tracks external IDs that resulted in a write failure (HTTP error or dead-letter)
    _failed_external_ids: set[str] = field(default_factory=set)
    # Accumulates (external_id, action, error_message) for failed-row audit trail
    _failed_entries: list[tuple[str, str, str]] = field(default_factory=list)
    # Stable UUID generated once per run_writeback_cycle() call; used as the per-cycle
    # deduplication key in inout_ops_writeback_result (migration 022).
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))


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
                        rows, connector.name, datatype, delta_table, log, result,
                        run_id=result.run_id,
                    )

                # Dependency ordering within write batch
                write_deps = getattr(writeback_cfg, "write_dependencies", [])
                if write_deps and rows:
                    from inandout.writeback.ordering import topological_sort_rows
                    rows = topological_sort_rows(rows, write_deps)
                    # Separate cycle-errored rows and send to dead-letter
                    cycle_rows = [r for r in rows if r.get("_cycle_error")]
                    rows = [r for r in rows if not r.get("_cycle_error")]
                    for cycle_row in cycle_rows:
                        ext_id = cycle_row.get("external_id") or cycle_row.get("_cluster_id", "")
                        log.warning(
                            "writeback_dependency_cycle_row",
                            external_id=ext_id,
                            group_id=cycle_row.get("_group_id"),
                        )
                        result.failed += 1

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
                if writeback_cfg.use_desired_state_table:
                    await self._update_desired_state_statuses(
                        rows, result, connector.name, datatype
                    )

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
        run_id: str | None = None,
    ) -> list[dict]:
        """Filter out rows that were already successfully sent (crash recovery).

        When *run_id* is provided (migration 022+), queries by the stable per-cycle
        UUID so deduplication is exact even for long-running batches.  Falls back
        to a 1-hour processed_at window for older rows without a run_id.
        """
        try:
            async with self._pool.connection() as conn:
                if run_id:
                    import uuid as _uuid
                    try:
                        run_uuid = _uuid.UUID(run_id)
                        audit_rows = await (await conn.execute(
                            """
                            SELECT external_id, action
                            FROM inout_ops_writeback_result
                            WHERE connector = %s AND datatype = %s AND delta_table = %s
                              AND run_id = %s
                              AND status = 'ok'
                            """,
                            [connector, datatype, delta_table, run_uuid],
                        )).fetchall()
                    except Exception:
                        # run_id column may not exist on pre-022 DBs — fall back
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
                else:
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
        dry_run = getattr(writeback_cfg, "dry_run", False)

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

        def _log_dry_run(
            op_action: str,
            method: str,
            url: str,
            headers: dict,
            body: dict,
            conflict_detected: bool = False,
        ) -> None:
            """Append to dry_run_log and increment skipped counter."""
            result.dry_run_log.append({
                "action": op_action,
                "method": method,
                "url": url,
                "headers": headers,
                "body": body,
                "conflict_detected": conflict_detected,
            })
            result.skipped += 1

        try:
            if action == "insert":
                if ops.insert is None:
                    result.skipped += 1
                    return
                payload = {k: v for k, v in row.items() if not k.startswith("_")}
                path = interpolate_path(ops.insert.path)
                extra_headers = _make_extra_headers(payload)

                # B1: dry_run — log the would-be write, skip actual HTTP call
                if dry_run:
                    base_url = connector.connection.base_url.rstrip("/")
                    _log_dry_run(action, ops.insert.method.upper(), f"{base_url}{path}", extra_headers, payload)
                    return

                if extra_headers:
                    insert_resp = await transport._raw_request(ops.insert.method.upper(), path, json=payload, headers=extra_headers)
                else:
                    insert_resp = await transport._raw_request(ops.insert.method.upper(), path, json=payload)
                insert_resp.raise_for_status()
                # Extract the target-system's assigned ID from a successful response body
                # and upsert inout_ops_identity_map: cluster_id → target external_id
                try:
                    resp_body: dict[str, Any] = {}
                    try:
                        resp_body = orjson.loads(insert_resp.content) if insert_resp.content else {}
                    except Exception:
                        pass
                    # Candidate field names for the target-system-assigned ID
                    id_candidates = (
                        "id",
                        f"{result.datatype}_id",
                        f"{result.connector}_id",
                        "externalId",
                        "external_id",
                    )
                    returned_id: str | None = None
                    for id_field in id_candidates:
                        if id_field in resp_body:
                            returned_id = str(resp_body[id_field])
                            break
                    # Use the MDM cluster_id as the canonical key when available
                    cluster_id = str(
                        row.get("cluster_id") or row.get("_cluster_id") or external_id
                    )
                    if returned_id and cluster_id:
                        await self._record_identity_map(
                            connector=result.connector,
                            datatype=result.datatype,
                            external_id=cluster_id,
                            internal_id=returned_id,
                        )
                except Exception:
                    pass  # Identity map failure must not block writeback
                # Record audit
                last_written: dict[str, Any] = {}
                diff = _compute_field_diff(last_written, payload)
                result._audit_entries.append((external_id, action, payload, diff))

                # Level 3: post-write verification — GET and compare against what was sent
                if writeback_cfg.protection_level == ProtectionLevel.post_write_verify:
                    await self._post_write_verify(
                        transport, connector, writeback_cfg, ops,
                        action, external_id, payload, result,
                    )

                result.processed += 1

            elif action == "update":
                if ops.update is None:
                    result.skipped += 1
                    return
                payload = {k: v for k, v in row.items() if not k.startswith("_")}
                path = interpolate_path(ops.update.path)

                # Three-way conflict detection (field-scoped)
                # Only runs when use_desired_state_table=True AND lookup is configured
                if (
                    writeback_cfg.use_desired_state_table
                    and ops.lookup is not None
                    and external_id
                ):
                    try:
                        lookup_path_3way = interpolate_path(ops.lookup.path)
                        preflight_resp = await transport._raw_request(
                            ops.lookup.method.upper(), lookup_path_3way
                        )
                        current_state: dict[str, Any] = {}
                        try:
                            current_state = orjson.loads(preflight_resp.content) if preflight_resp.content else {}
                        except Exception:
                            pass

                        # Fetch last-written state from lwstate table
                        async with self._pool.connection() as lw_conn_3way:
                            last_written_3way = await get_lwstate(
                                lw_conn_3way, connector.name, result.datatype, external_id
                            )

                        # Get base from row dict
                        base_3way: dict[str, Any] = row.get("_base") or row.get("base") or {}

                        # Field-scoped three-way comparison
                        payload_fields = set(payload.keys())
                        current_relevant = {k: v for k, v in current_state.items() if k in payload_fields}
                        base_relevant = {k: v for k, v in (base_3way or {}).items() if k in payload_fields}
                        lw_relevant = {k: v for k, v in (last_written_3way or {}).items() if k in payload_fields}

                        safe = (current_relevant == base_relevant) or (current_relevant == lw_relevant)

                        if not safe:
                            # Conflict detected — always update lwstate to current reality
                            _conflict_etag = preflight_resp.headers.get(writeback_cfg.etag_header) or None
                            async with self._pool.connection() as lw_update_conn:
                                await upsert_lwstate(
                                    lw_update_conn, connector.name, result.datatype,
                                    external_id, current_state,
                                    etag=_conflict_etag,
                                )
                                await lw_update_conn.commit()

                            try:
                                conflicts_detected_total.labels(
                                    connector=connector.name,
                                    datatype=result.datatype,
                                    resolution=writeback_cfg.conflict_resolution.value,
                                    namespace="public",
                                ).inc()
                            except Exception:
                                pass

                            resolution = writeback_cfg.conflict_resolution
                            if resolution == ConflictResolution.last_writer_wins:
                                logger.warning(
                                    "writeback_conflict_last_writer_wins",
                                    action=action, external_id=external_id,
                                )
                                result.conflicts += 1
                                # Fall through — proceed with write despite conflict
                            elif resolution == ConflictResolution.skip_and_warn:
                                logger.warning(
                                    "writeback_conflict_skip",
                                    action=action, external_id=external_id,
                                )
                                result.skipped += 1
                                result.conflicts += 1
                                return
                            elif resolution == ConflictResolution.dead_letter:
                                logger.warning(
                                    "writeback_conflict_dead_letter",
                                    action=action, external_id=external_id,
                                )
                                result.failed += 1
                                result.conflicts += 1
                                result._failed_external_ids.add(external_id)
                                result._failed_entries.append((external_id, action, "conflict:dead_letter"))
                                return
                            elif resolution == ConflictResolution.server_wins:
                                logger.warning(
                                    "writeback_conflict_server_wins",
                                    action=action, external_id=external_id,
                                )
                                result.skipped += 1
                                result.conflicts += 1
                                return
                            elif resolution == ConflictResolution.re_ingest_and_recompute:
                                logger.warning(
                                    "writeback_conflict_re_ingest",
                                    action=action, external_id=external_id,
                                )
                                try:
                                    async with self._pool.connection() as ctrl_conn:
                                        await ctrl_conn.execute(
                                            """
                                            INSERT INTO inout_ops_control
                                                (connector, datatype, command, payload, status)
                                            VALUES (%s, %s, 'resync', %s, 'pending')
                                            """,
                                            [
                                                connector.name,
                                                result.datatype,
                                                orjson.dumps({"external_id": external_id}).decode(),
                                            ],
                                        )
                                        await ctrl_conn.commit()
                                except Exception:
                                    pass
                                result.skipped += 1
                                result.conflicts += 1
                                return
                            # else: default last_writer_wins — continue

                        # Safe or last_writer_wins: carry ETag from pre-flight if configured
                        _3way_etag = preflight_resp.headers.get(writeback_cfg.etag_header, "")
                    except Exception:
                        _3way_etag = ""
                        # On error, fall through to normal write
                else:
                    _3way_etag = ""

                # CRDT merge: apply before diff_fields and protection-level branching.
                # GETs the current remote state and merges local payload according to
                # the configured strategy (lww_register, g_counter).
                if writeback_cfg.crdt_type and ops.lookup is not None and external_id:
                    try:
                        from inandout.writeback.crdt import crdt_merge
                        lookup_path_crdt = interpolate_path(ops.lookup.path)
                        crdt_resp = await transport._raw_request(
                            ops.lookup.method.upper(), lookup_path_crdt
                        )
                        remote_crdt_state: dict[str, Any] = {}
                        try:
                            remote_crdt_state = orjson.loads(crdt_resp.content) if crdt_resp.content else {}
                        except Exception:
                            pass
                        crdt_ts_field = getattr(writeback_cfg, "crdt_ts_field", "_updated_at")
                        merged_crdt = crdt_merge(
                            payload, remote_crdt_state, writeback_cfg.crdt_type, ts_field=crdt_ts_field
                        )
                        if merged_crdt is None:
                            logger.info(
                                "writeback_crdt_skip_remote_newer",
                                action=action, external_id=external_id,
                                crdt_type=writeback_cfg.crdt_type,
                            )
                            result.skipped += 1
                            return
                        payload = merged_crdt
                    except Exception as exc:
                        logger.warning("writeback_crdt_merge_failed", error=str(exc))
                        # Fall through to normal write

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
                    # Use base_version from the desired-state row as the ETag when available
                    # (avoids an extra GET round-trip when the MDM has already recorded it).
                    # Fall back to a fresh lookup GET when base_version is absent.
                    base_version: str = str(row.get("base_version") or row.get("_base_version") or "")
                    lookup_path = interpolate_path(ops.lookup.path)
                    if base_version:
                        etag = base_version
                        remote_data = {}  # skip GET — trust base_version as ETag
                    else:
                        try:
                            lookup_resp = await transport._raw_request(
                                ops.lookup.method.upper(), lookup_path
                            )
                            etag = lookup_resp.headers.get(writeback_cfg.etag_header, "")
                            remote_data = {}
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
                    # Prefer base_version from desired-state row as If-Match (avoids extra GET)
                    _row_base_version = str(row.get("base_version") or row.get("_base_version") or "")
                    _effective_etag = _row_base_version or _3way_etag
                    if _effective_etag and writeback_cfg.etag_header:
                        extra_headers[writeback_cfg.if_match_header] = _effective_etag

                    # B1: dry_run — log would-be write, skip actual HTTP call
                    if dry_run:
                        base_url = connector.connection.base_url.rstrip("/")
                        _log_dry_run(action, ops.update.method.upper(), f"{base_url}{path}", extra_headers, payload)
                        return

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

                # After successful write: update lwstate with payload + ETag from response
                if (
                    writeback_cfg.use_desired_state_table
                    and ops.lookup is not None
                    and external_id
                    and sent_payload is not None
                ):
                    try:
                        # Store the actual payload (without synthetic _etag key) and pass
                        # the ETag separately so it lands in its own column.
                        lw_etag = _3way_etag or None
                        async with self._pool.connection() as lw_post_conn:
                            await upsert_lwstate(
                                lw_post_conn, connector.name, result.datatype,
                                external_id, sent_payload,
                                etag=lw_etag,
                            )
                            await lw_post_conn.commit()
                    except Exception:
                        pass  # Non-critical

                result._audit_entries.append((external_id, action, sent_payload, sent_diff))

                # Level 3: post-write verification — GET and compare against what was sent
                if (
                    writeback_cfg.protection_level == ProtectionLevel.post_write_verify
                    and sent_payload is not None
                    and ops.lookup is not None
                ):
                    await self._post_write_verify(
                        transport, connector, writeback_cfg, ops,
                        action, external_id, sent_payload, result,
                    )

                result.processed += 1

            elif action == "delete":
                if ops.delete is None:
                    result.skipped += 1
                    return
                path = interpolate_path(ops.delete.path)
                extra_headers = _make_extra_headers({})

                # B1: dry_run
                if dry_run:
                    base_url = connector.connection.base_url.rstrip("/")
                    _log_dry_run(action, ops.delete.method.upper(), f"{base_url}{path}", extra_headers, {})
                    return

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

                # B1: dry_run
                if dry_run:
                    base_url = connector.connection.base_url.rstrip("/")
                    _log_dry_run(action, ops.archive.method.upper(), f"{base_url}{path}", extra_headers, payload)
                    return

                if extra_headers:
                    await transport._raw_request(ops.archive.method.upper(), path, json=payload, headers=extra_headers)
                else:
                    await transport._request(ops.archive.method.upper(), path, json=payload)
                result._audit_entries.append((external_id, action, payload, None))
                result.processed += 1

            elif action == "merge":
                # T2 #34: merge — update the surviving record, delete losers, update identity map.
                # Row shape expected from OSI-Mapping delta view:
                #   external_id       — surviving cluster_id / external_id
                #   data              — desired state for the survivor
                #   _losing_ids       — list[str] of external_ids to be deleted (losers)
                if ops.update is None:
                    logger.warning("merge_no_update_op_configured", connector=connector.name, datatype=result.datatype)
                    result.skipped += 1
                    return

                surviving_id = external_id
                losing_ids: list[str] = row.get("_losing_ids") or []
                payload = {k: v for k, v in row.items() if not k.startswith("_")}

                # Allow a plugin hook to modify the merged payload before writing
                hook = merge_hook_registry.get(connector.name, result.datatype)
                if hook is not None:
                    try:
                        payload = await hook(payload, {}, {})
                    except Exception as exc:
                        logger.warning("merge_hook_failed", error=str(exc))

                if dry_run:
                    base_url = connector.connection.base_url.rstrip("/")
                    update_path = interpolate_path(ops.update.path)
                    _log_dry_run("merge:update", ops.update.method.upper(), f"{base_url}{update_path}", {}, payload)
                    for loser_id in losing_ids:
                        if ops.delete is not None:
                            loser_path = ops.delete.path.replace("${external_id}", loser_id)
                            _log_dry_run("merge:delete", ops.delete.method.upper(), f"{base_url}{loser_path}", {}, {})
                    return

                # 1. Update the survivor
                update_path = interpolate_path(ops.update.path)
                extra_headers = _make_extra_headers(payload)
                if extra_headers:
                    await transport._raw_request(ops.update.method.upper(), update_path, json=payload, headers=extra_headers)
                else:
                    await transport._request(ops.update.method.upper(), update_path, json=payload)

                # 2. Delete the losers
                if ops.delete is not None:
                    for loser_id in losing_ids:
                        loser_path = ops.delete.path.replace("${external_id}", loser_id)
                        try:
                            await transport._request(ops.delete.method.upper(), loser_path)
                        except httpx.HTTPError as loser_exc:
                            logger.warning(
                                "merge_loser_delete_failed",
                                loser_id=loser_id, error=str(loser_exc),
                            )

                # 3. Update identity map: point all loser cluster_ids to survivor's external_id
                for loser_id in losing_ids:
                    await self._record_identity_map(
                        connector=result.connector,
                        datatype=result.datatype,
                        external_id=loser_id,
                        internal_id=surviving_id,
                    )

                result._audit_entries.append((surviving_id, action, payload, None))
                result.processed += 1

            elif action == "split":
                # T2 #34: split — create child records from a single source row.
                # Row shape expected from OSI-Mapping delta view:
                #   external_id   — source cluster_id being split
                #   _split_rows   — list[dict], each the desired-state payload for a new child
                if ops.insert is None:
                    logger.warning("split_no_insert_op_configured", connector=connector.name, datatype=result.datatype)
                    result.skipped += 1
                    return

                split_rows: list[dict[str, Any]] = row.get("_split_rows") or []
                if not split_rows:
                    logger.warning("split_no_rows", external_id=external_id)
                    result.skipped += 1
                    return

                if dry_run:
                    base_url = connector.connection.base_url.rstrip("/")
                    for split_payload in split_rows:
                        insert_path = interpolate_path(ops.insert.path)
                        _log_dry_run("split:insert", ops.insert.method.upper(), f"{base_url}{insert_path}", {}, split_payload)
                    return

                created_ids: list[str] = []
                for split_payload in split_rows:
                    insert_path = interpolate_path(ops.insert.path)
                    extra_headers = _make_extra_headers(split_payload)
                    if extra_headers:
                        resp = await transport._raw_request(ops.insert.method.upper(), insert_path, json=split_payload, headers=extra_headers)
                    else:
                        resp = await transport._raw_request(ops.insert.method.upper(), insert_path, json=split_payload)
                    resp.raise_for_status()
                    # Record identity for each created child
                    try:
                        resp_body: dict[str, Any] = orjson.loads(resp.content) if resp.content else {}
                        returned_id = next(
                            (str(resp_body[f]) for f in ("id", f"{result.datatype}_id", f"{result.connector}_id", "externalId") if f in resp_body),
                            None,
                        )
                        if returned_id:
                            created_ids.append(returned_id)
                            await self._record_identity_map(
                                connector=result.connector,
                                datatype=result.datatype,
                                external_id=external_id,
                                internal_id=returned_id,
                            )
                    except Exception:
                        pass

                logger.info("split_complete", source_id=external_id, child_count=len(created_ids))
                result._audit_entries.append((external_id, action, {"_split_rows": len(split_rows)}, None))
                result.processed += 1

            else:
                logger.warning("unsupported_writeback_action", action=action)
                result.skipped += 1

        except httpx.HTTPError as exc:
            logger.error("writeback_http_error", action=action, external_id=external_id, error=str(exc))
            result.failed += 1
            result._failed_external_ids.add(external_id)
            result._failed_entries.append((external_id, action, str(exc)))

    async def _post_write_verify(
        self,
        transport: HttpTransportAdapter,
        connector: ConnectorConfig,
        writeback_cfg: WritebackConfig,
        ops: Any,
        action: str,
        external_id: str,
        sent_payload: dict[str, Any],
        result: WritebackResult,
    ) -> None:
        """Level 3 protection: GET the record after a successful write and verify
        the remote state matches what was sent.  Discrepancies are routed through
        the configured conflict_resolution strategy (T2 #38).
        """
        if ops.lookup is None:
            return
        try:
            lookup_path = ops.lookup.path.replace("${external_id}", external_id or "")
            verify_resp = await transport._raw_request(
                ops.lookup.method.upper(), lookup_path
            )
            remote: dict[str, Any] = {}
            try:
                remote = orjson.loads(verify_resp.content) if verify_resp.content else {}
            except Exception:
                return

            # Compare only the fields we sent
            mismatch_fields: list[str] = [
                k for k, v in sent_payload.items()
                if k in remote and remote[k] != v
            ]
            if not mismatch_fields:
                return  # Verification passed

            logger.warning(
                "post_write_verification_mismatch",
                connector=connector.name,
                datatype=result.datatype,
                external_id=external_id,
                mismatch_fields=mismatch_fields,
            )
            try:
                conflicts_detected_total.labels(
                    connector=connector.name,
                    datatype=result.datatype,
                    resolution="post_write_verify",
                    namespace="public",
                ).inc()
            except Exception:
                pass

            resolution = writeback_cfg.conflict_resolution
            if resolution in (ConflictResolution.re_ingest_and_recompute,):
                # Signal ingestion to re-fetch this record
                try:
                    async with self._pool.connection() as ctrl_conn:
                        await ctrl_conn.execute(
                            """
                            INSERT INTO inout_ops_control
                                (target_tool, connector, datatype, command, payload, status)
                            VALUES ('ingestion', %s, %s, 'resync', %s, 'pending')
                            """,
                            [
                                connector.name,
                                result.datatype,
                                orjson.dumps({"external_id": external_id}).decode(),
                            ],
                        )
                        await ctrl_conn.commit()
                except Exception:
                    pass
            elif resolution == ConflictResolution.dead_letter:
                result.failed += 1
                result.processed -= 1  # undo the processed increment
                result._failed_external_ids.add(external_id)
        except Exception as exc:
            logger.warning("post_write_verify_failed", external_id=external_id, error=str(exc))

    async def _update_desired_state_statuses(
        self,
        rows: list[dict],
        result: WritebackResult,
        connector: str,
        datatype: str,
    ) -> None:
        """Batch-update _status on inout_dst_* rows after a writeback cycle.

        OSI-Mapping reads _status to distinguish pending rows from those that
        have been actioned.  Errors are swallowed — never block writeback.
        """
        # Build index of successfully processed external_ids from _audit_entries
        succeeded: set[str] = {str(ext_id) for ext_id, *_ in result._audit_entries}
        failed: set[str] = result._failed_external_ids

        for row in rows:
            ext_id = str(row.get("external_id") or row.get("_cluster_id") or "")
            if not ext_id:
                continue
            if ext_id in succeeded:
                status = "processed"
            elif ext_id in failed:
                status = "failed"
            else:
                status = "skipped"
            await update_desired_state_status(self._pool, connector, datatype, ext_id, status)

    async def _record_identity_map(
        self,
        connector: str,
        datatype: str,
        external_id: str,
        internal_id: str,
    ) -> None:
        """Upsert a cluster_id → target_external_id mapping in inout_ops_identity_map.

        Parameters
        ----------
        external_id:
            The MDM cluster_id (the canonical cross-system identifier).
        internal_id:
            The ID assigned by the target system after a successful insert.
            Stored in both ``internal_id`` (legacy) and ``target_external_id``
            (spec-aligned name from migration 021).
        """
        try:
            async with self._pool.connection() as conn:
                await conn.execute(
                    """
                    INSERT INTO inout_ops_identity_map
                        (connector, datatype, external_id, internal_id,
                         cluster_id, target_external_id,
                         created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (connector, datatype, external_id) DO UPDATE
                    SET internal_id = EXCLUDED.internal_id,
                        cluster_id = EXCLUDED.cluster_id,
                        target_external_id = EXCLUDED.target_external_id,
                        updated_at = NOW()
                    """,
                    [connector, datatype, external_id, internal_id,
                     external_id, internal_id],
                )
                await conn.commit()
        except psycopg.errors.UndefinedColumn:
            # Migration 021 not yet applied — fall back to legacy columns only
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
            except Exception as exc:
                logger.warning("identity_map_write_failed", error=str(exc))
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
        """Write per-row feedback to inout_ops_writeback_result.

        Records both successful writes (status='ok') and failed writes
        (status='failed') so operators can audit all outcomes and so
        crash-recovery deduplication is complete.
        """
        if not rows and not result._failed_entries:
            return
        # Build audit map from accumulated entries
        audit_map: dict[tuple[str, str], tuple[dict[str, Any] | None, dict[str, Any] | None]] = {}
        for ext_id, act, payload, diff in result._audit_entries:
            audit_map[(ext_id, act)] = (payload, diff)

        try:
            async with self._pool.connection() as conn:
                # Write successful rows
                for row in rows:
                    action = row.get("_action", "")
                    external_id = row.get("external_id") or row.get("_cluster_id", "")
                    payload_snap, field_diff = audit_map.get((external_id, action), (None, None))

                    # Try inserting with run_id + audit columns (migration 022 / 006)
                    try:
                        await conn.execute(
                            """
                            INSERT INTO inout_ops_writeback_result
                                (connector, datatype, delta_table, action, external_id, status,
                                 run_id, payload_snapshot, field_diff, processed_at)
                            VALUES (%s, %s, %s, %s, %s, 'ok', %s, %s, %s, NOW())
                            ON CONFLICT (connector, datatype, run_id, external_id, action)
                                WHERE run_id IS NOT NULL DO NOTHING
                            """,
                            [
                                result.connector,
                                result.datatype,
                                result.delta_table,
                                action,
                                external_id,
                                uuid.UUID(result.run_id),
                                orjson.dumps(payload_snap).decode() if payload_snap is not None else None,
                                orjson.dumps(field_diff).decode() if field_diff is not None else None,
                            ],
                        )
                    except Exception:
                        # Fall back: without run_id (pre-022 DB) but still with audit columns
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

                # Write failed rows so operators can see all outcomes
                for ext_id, act, error_msg in result._failed_entries:
                    error_detail = orjson.dumps({"error": error_msg}).decode()
                    try:
                        await conn.execute(
                            """
                            INSERT INTO inout_ops_writeback_result
                                (connector, datatype, delta_table, action, external_id, status,
                                 payload_snapshot, processed_at)
                            VALUES (%s, %s, %s, %s, %s, 'failed', %s, NOW())
                            ON CONFLICT DO NOTHING
                            """,
                            [
                                result.connector,
                                result.datatype,
                                result.delta_table,
                                act,
                                ext_id,
                                error_detail,
                            ],
                        )
                    except Exception:
                        try:
                            await conn.execute(
                                """
                                INSERT INTO inout_ops_writeback_result
                                    (connector, datatype, delta_table, action, external_id, status, processed_at)
                                VALUES (%s, %s, %s, %s, %s, 'failed', NOW())
                                ON CONFLICT DO NOTHING
                                """,
                                [
                                    result.connector,
                                    result.datatype,
                                    result.delta_table,
                                    act,
                                    ext_id,
                                ],
                            )
                        except Exception:
                            pass

                await conn.commit()
        except psycopg.errors.UndefinedTable:
            logger.debug("writeback_result_table_not_found_skipping_feedback")
        except Exception as exc:
            logger.warning("writeback_feedback_write_failed", error=str(exc))
