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


_JSON_SCHEMA_TYPE_MAP: dict[str, type | tuple[type, ...]] = {
    "string": str,
    "integer": int,
    "number": (int, float),
    "boolean": bool,
    "array": list,
    "object": dict,
    "null": type(None),
}


def _validate_payload_schema(
    payload: dict[str, Any],
    schema: dict[str, Any],
) -> list[str]:
    """T2 #23: validate *payload* against a JSON-Schema-subset *schema*.

    Supported keywords: ``required``, ``properties`` (with ``type``),
    ``additionalProperties`` (bool).  Returns a list of error strings (empty
    means valid).
    """
    errors: list[str] = []
    required: list[str] = schema.get("required") or []
    properties: dict[str, Any] = schema.get("properties") or {}
    additional_props: bool | None = schema.get("additionalProperties")

    for field in required:
        if field not in payload:
            errors.append(f"required field '{field}' is missing")

    for field, field_schema in properties.items():
        if field not in payload:
            continue
        expected_type = field_schema.get("type")
        if expected_type:
            py_type = _JSON_SCHEMA_TYPE_MAP.get(expected_type)
            if py_type is not None and not isinstance(payload[field], py_type):
                actual = type(payload[field]).__name__
                errors.append(
                    f"field '{field}' expected type '{expected_type}', got '{actual}'"
                )

    if additional_props is False:
        allowed = set(properties.keys())
        for field in payload:
            if field not in allowed:
                errors.append(f"field '{field}' is not allowed by payload_schema")

    return errors


def _apply_writeback_transforms(
    payload: dict[str, Any],
    row: dict[str, Any],
    writeback_cfg: Any,
) -> dict[str, Any]:
    """T2 #16/#17: apply pre-write transforms to *payload*.

    Steps (in order):
    1. Inject MDM ``cluster_id`` under ``external_reference_field`` (T2 #16).
    2. Apply declarative ``field_mappings`` — rename, cast, default (T2 #17).
    """
    # T2 #16: populate external reference field with MDM cluster_id
    ext_ref = getattr(writeback_cfg, "external_reference_field", None)
    if ext_ref:
        cluster_id = row.get("_cluster_id") or row.get("cluster_id")
        if cluster_id:
            payload = {**payload, ext_ref: cluster_id}

    # T2 #17: field mappings (rename / cast / default)
    mappings = getattr(writeback_cfg, "field_mappings", [])
    if mappings:
        from inandout.ingestion.field_mapper import apply_field_mappings
        strict = getattr(writeback_cfg, "field_mappings_strict", False)
        payload = apply_field_mappings(payload, mappings, strict=strict)

    return payload


def _check_batch_response(
    resp_content: bytes | None,
    external_id: str | None,
    writeback_cfg: Any,
    result: "WritebackResult",
    action: str,
) -> bool:
    """T2 #29: parse a batch HTTP response and check whether *external_id* succeeded.

    Returns True when the record succeeded (or when batch_response is not configured).
    Returns False and updates *result* when the record is reported as failed.
    """
    batch_cfg = getattr(writeback_cfg, "batch_response", None)
    if batch_cfg is None or external_id is None:
        return True
    try:
        from inandout.writeback.batch_response import extract_batch_errors, parse_batch_response
        body: dict[str, Any] = {}
        try:
            body = orjson.loads(resp_content) if resp_content else {}
        except Exception:
            return True  # Can't parse; assume success
        outcomes = parse_batch_response(body, batch_cfg)
        if external_id in outcomes and not outcomes[external_id]:
            errors = extract_batch_errors(body, batch_cfg)
            err_msg = errors.get(external_id, "batch_record_failed")
            logger.warning(
                "writeback_batch_record_failed",
                external_id=external_id,
                action=action,
                error=err_msg,
            )
            result.failed += 1
            result.processed = max(0, result.processed - 1)
            result._failed_external_ids.add(external_id)
            result._failed_entries.append((external_id, action, f"batch_response:{err_msg}"))
            return False
    except Exception:
        pass  # Parse failure must not block writeback
    return True


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
    # Accumulates (external_id, action, payload, diff, effective_protection_level) for audit trail
    _audit_entries: list[tuple[str, str, dict[str, Any] | None, dict[str, Any] | None, str]] = field(
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
                    delta_table, log, result, batch_size=writeback_cfg.batch_size,
                    batch_max_bytes=getattr(writeback_cfg, "batch_max_bytes", None),
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

                # T2 #31: delete safety guard — abort deletes when batch exceeds limit
                max_deletes = getattr(writeback_cfg, "max_deletes_per_batch", None)
                if max_deletes is not None and rows:
                    delete_count = sum(1 for r in rows if r.get("_action") == "delete")
                    if delete_count > max_deletes:
                        log.warning(
                            "writeback_delete_safety_guard_tripped",
                            delete_count=delete_count,
                            max_deletes_per_batch=max_deletes,
                            connector=connector.name,
                            datatype=datatype,
                        )
                        rows = [r for r in rows if r.get("_action") != "delete"]
                        result.skipped += delete_count

                async with HttpTransportAdapter(connector) as transport:
                    # Group rows by external_id to preserve per-id ordering
                    grouped: dict[str, list[dict[str, Any]]] = {}
                    for row_data in rows:
                        ext_id = row_data.get("external_id") or row_data.get("_cluster_id", "")
                        if ext_id not in grouped:
                            grouped[ext_id] = []
                        grouped[ext_id].append(row_data)

                    # T2 #21: group-abort tracking — shared across all concurrent
                    # _dispatch_group coroutines.  When any member of a _group_id
                    # group fails, remaining members in the same group are
                    # immediately moved to dead-letter and counted as failed.
                    _aborted_groups: dict[str, str] = {}  # group_id → abort reason

                    async def _dispatch_group(
                        group_rows: list[dict[str, Any]],
                    ) -> None:
                        async with semaphore:
                            for row_data in group_rows:
                                action = row_data.get("_action", "")
                                external_id = row_data.get("external_id") or row_data.get("_cluster_id", "")
                                gid = row_data.get("_group_id")

                                # T2 #21: abort this member if its group has already failed
                                if gid and gid in _aborted_groups:
                                    abort_reason = _aborted_groups[gid]
                                    log.warning(
                                        "writeback_group_member_aborted",
                                        external_id=external_id,
                                        group_id=gid,
                                        reason=abort_reason,
                                    )
                                    result.failed += 1
                                    result._failed_entries.append((
                                        external_id, action,
                                        f"group_partial_failure:{gid}:{abort_reason}",
                                    ))
                                    try:
                                        from inandout.deadletter.writeback import move_to_dead_letter
                                        await move_to_dead_letter(
                                            self._pool,
                                            connector.name,
                                            result.datatype,
                                            external_id=external_id,
                                            action="group_partial_failure",
                                            payload_snapshot={
                                                k: v for k, v in row_data.items()
                                                if not k.startswith("_")
                                            },
                                            error_message=(
                                                f"Group {gid} aborted: "
                                                f"member failed ({abort_reason})"
                                            ),
                                            delta_table=result.delta_table,
                                        )
                                    except Exception:
                                        pass
                                    continue

                                _failed_before = result.failed
                                await self._dispatch_row(
                                    transport, connector, writeback_cfg,
                                    action, external_id, row_data, log, result
                                )
                                # T2 #21: mark group aborted when a member fails
                                if (
                                    gid
                                    and result.failed > _failed_before
                                    and gid not in _aborted_groups
                                ):
                                    _aborted_groups[gid] = (
                                        f"member {external_id} action={action} failed"
                                    )

                    async with anyio.create_task_group() as tg:
                        for group_rows in grouped.values():
                            tg.start_soon(_dispatch_group, group_rows)

                await self._write_feedback(rows, result, log)
                await self._auto_dead_letter_exceeded_rows(result, writeback_cfg)
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
        batch_max_bytes: int | None = None,
    ) -> list[dict] | None:
        """Fetch up to *batch_size* non-noop rows from the delta table. Returns None if table doesn't exist.

        When *batch_max_bytes* is set (T2 #33), rows are accumulated until the
        cumulative uncompressed JSON payload size would exceed the limit, then
        the remainder is dropped from the in-memory batch (remaining rows stay
        in the delta table for the next cycle).
        """
        try:
            async with self._pool.connection() as fetch_conn:
                cur = await fetch_conn.execute(
                    f"SELECT * FROM {delta_table} WHERE _action != 'noop' LIMIT {batch_size}"
                )
                col_names = [desc[0] for desc in cur.description or []]
                rows_raw = await cur.fetchall()
                if not rows_raw:
                    return []
                rows = [dict(zip(col_names, row)) for row in rows_raw]

                # T2 #33: trim batch when cumulative payload exceeds batch_max_bytes
                if batch_max_bytes is not None:
                    trimmed: list[dict] = []
                    cumulative_bytes = 0
                    for row in rows:
                        row_bytes = len(orjson.dumps(
                            {k: v for k, v in row.items() if not k.startswith("_")}
                        ))
                        if trimmed and cumulative_bytes + row_bytes > batch_max_bytes:
                            logger.info(
                                "writeback_batch_max_bytes_reached",
                                delta_table=delta_table,
                                batch_bytes=cumulative_bytes,
                                batch_max_bytes=batch_max_bytes,
                                rows_in_batch=len(trimmed),
                                rows_deferred=len(rows) - len(trimmed),
                            )
                            break
                        trimmed.append(row)
                        cumulative_bytes += row_bytes
                    rows = trimmed

                return rows
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
        # T2 #25: check writeback circuit breaker before dispatching
        from inandout.transport.circuit_breaker import get_circuit_breaker, CircuitState
        _cb_cfg = getattr(connector, "circuit_breaker", None) or {}
        _cb = get_circuit_breaker(
            connector.name,
            result.datatype,
            failure_threshold=int(_cb_cfg.get("failure_threshold", 5)),
            recovery_timeout=float(_cb_cfg.get("recovery_timeout", 60.0)),
        )
        if not _cb.allow_request():
            log.warning(
                "writeback_circuit_breaker_open_skip",
                connector=connector.name,
                datatype=result.datatype,
                external_id=external_id,
                state=_cb.state,
            )
            result.skipped += 1
            return

        # T2 #35: required-fields guard — route to dead-letter when any configured field is absent
        _required_fields = getattr(writeback_cfg, "required_fields", [])
        if _required_fields:
            _payload_check = {k: v for k, v in row.items() if not k.startswith("_")}
            _missing = [f for f in _required_fields if f not in _payload_check]
            if _missing:
                logger.warning(
                    "writeback_required_fields_missing",
                    connector=connector.name,
                    datatype=result.datatype,
                    external_id=external_id,
                    missing_fields=_missing,
                )
                result.failed += 1
                result._failed_external_ids.add(external_id)
                result._failed_entries.append(
                    (external_id, action, f"required_fields_missing:{','.join(_missing)}")
                )
                return

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
            _http_write_ok = False  # T2 #25: set True when an HTTP write completes
            if action == "insert":
                if ops.insert is None:
                    result.skipped += 1
                    return
                payload = {k: v for k, v in row.items() if not k.startswith("_")}
                payload = _apply_writeback_transforms(payload, row, writeback_cfg)
                # T2 #23: pre-write payload validation
                _pw_schema = getattr(writeback_cfg, "payload_schema", None)
                if _pw_schema:
                    _pw_errors = _validate_payload_schema(payload, _pw_schema)
                    if _pw_errors:
                        logger.warning(
                            "writeback_payload_validation_failed",
                            external_id=external_id,
                            action=action,
                            errors=_pw_errors,
                        )
                        result.failed += 1
                        result._failed_external_ids.add(external_id)
                        result._failed_entries.append(
                            (external_id, action, f"payload_validation:{_pw_errors[0]}")
                        )
                        return
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
                _eff_pl = writeback_cfg.protection_level.value if writeback_cfg.protection_level else "none"
                result._audit_entries.append((external_id, action, payload, diff, _eff_pl))

                # Level 3: post-write verification — GET and compare against what was sent
                if writeback_cfg.protection_level == ProtectionLevel.post_write_verify:
                    await self._post_write_verify(
                        transport, connector, writeback_cfg, ops,
                        action, external_id, payload, result,
                    )

                _http_write_ok = True
                result.processed += 1
                # T2 #29: parse batch response to detect per-record failure
                _check_batch_response(insert_resp.content, external_id, writeback_cfg, result, action)

            elif action == "update":
                if ops.update is None:
                    result.skipped += 1
                    return
                payload = {k: v for k, v in row.items() if not k.startswith("_")}
                payload = _apply_writeback_transforms(payload, row, writeback_cfg)
                # T2 #23: pre-write payload validation
                _pw_schema_upd = getattr(writeback_cfg, "payload_schema", None)
                if _pw_schema_upd:
                    _pw_errors_upd = _validate_payload_schema(payload, _pw_schema_upd)
                    if _pw_errors_upd:
                        logger.warning(
                            "writeback_payload_validation_failed",
                            external_id=external_id,
                            action=action,
                            errors=_pw_errors_upd,
                        )
                        result.failed += 1
                        result._failed_external_ids.add(external_id)
                        result._failed_entries.append(
                            (external_id, action, f"payload_validation:{_pw_errors_upd[0]}")
                        )
                        return
                path = interpolate_path(ops.update.path)
                # T2 #29: initialise response content holder (set when _upd_resp is captured)
                _upd_resp_content: bytes | None = None

                # T2 #5: sentinel for remote data fetched during preflight
                # (set inside the three-way block, then reused for diff_fields)
                _preflight_remote_data: dict[str, Any] | None = None

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
                            if preflight_resp.is_success:
                                current_state = orjson.loads(preflight_resp.content) if preflight_resp.content else {}
                                # T2 #5: capture remote data so diff_fields can reuse this GET
                                _preflight_remote_data = current_state if current_state else None
                        except Exception:
                            pass

                        # Fetch last-written state from lwstate table
                        async with self._pool.connection() as lw_conn_3way:
                            last_written_3way = await get_lwstate(
                                lw_conn_3way, connector.name, result.datatype, external_id
                            )

                        # Get base from row dict
                        base_3way: dict[str, Any] = row.get("_base") or row.get("base") or {}

                        # T2 #12: normalize GET response field names to write-path names
                        _resp_map = getattr(writeback_cfg, "response_field_map", None) or {}
                        if _resp_map:
                            current_state = {_resp_map.get(k, k): v for k, v in current_state.items()}

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
                                # Also fire in-process bus for same-process ingestion daemons
                                try:
                                    from inandout.events import EventType, get_event_bus
                                    await get_event_bus().publish(
                                        EventType.REINGEST_SIGNAL,
                                        connector=connector.name,
                                        datatype=result.datatype,
                                        external_id=external_id,
                                        reason="three_way_conflict",
                                    )
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
                            if crdt_resp.is_success:
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
                    if _preflight_remote_data is not None:
                        # T2 #5: single GET serves both conflict detection and diff
                        # computation — no extra DB query needed.
                        payload = {k: v for k, v in payload.items() if _preflight_remote_data.get(k) != v}
                        if not payload:
                            result.skipped += 1
                            return
                        logger.debug(
                            "writeback_diff_via_preflight_get",
                            external_id=external_id,
                            fields_changed=len(payload),
                        )
                    else:
                        # Fall back: query DB for _last_written to compute diff
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
                            etag = lookup_resp.headers.get(writeback_cfg.etag_header, "") if lookup_resp.is_success else ""
                            remote_data = {}
                            try:
                                if lookup_resp.is_success:
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
                        _upd_resp = await transport._raw_request(ops.update.method.upper(), path, json=payload, headers=extra_headers)
                        _upd_resp.raise_for_status()
                        _upd_resp_content = getattr(_upd_resp, "content", None)
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

                _eff_pl_upd = writeback_cfg.protection_level.value if writeback_cfg.protection_level else "none"
                result._audit_entries.append((external_id, action, sent_payload, sent_diff, _eff_pl_upd))

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

                _http_write_ok = True
                result.processed += 1
                # T2 #29: parse batch response to detect per-record failure in update
                _check_batch_response(_upd_resp_content, external_id, writeback_cfg, result, action)

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
                    _del_resp = await transport._raw_request(ops.delete.method.upper(), path, headers=extra_headers)
                    _del_resp.raise_for_status()
                else:
                    await transport._request(ops.delete.method.upper(), path)
                _eff_pl_del = writeback_cfg.protection_level.value if writeback_cfg.protection_level else "none"
                result._audit_entries.append((external_id, action, None, None, _eff_pl_del))
                _http_write_ok = True
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
                    _arch_resp = await transport._raw_request(ops.archive.method.upper(), path, json=payload, headers=extra_headers)
                    _arch_resp.raise_for_status()
                else:
                    await transport._request(ops.archive.method.upper(), path, json=payload)
                _eff_pl_arch = writeback_cfg.protection_level.value if writeback_cfg.protection_level else "none"
                result._audit_entries.append((external_id, action, payload, None, _eff_pl_arch))
                _http_write_ok = True
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
                    _merge_upd_resp = await transport._raw_request(ops.update.method.upper(), update_path, json=payload, headers=extra_headers)
                    _merge_upd_resp.raise_for_status()
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

                _eff_pl_merge = writeback_cfg.protection_level.value if writeback_cfg.protection_level else "none"
                result._audit_entries.append((surviving_id, action, payload, None, _eff_pl_merge))
                _http_write_ok = True
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
                _eff_pl_split = writeback_cfg.protection_level.value if writeback_cfg.protection_level else "none"
                result._audit_entries.append((external_id, action, {"_split_rows": len(split_rows)}, None, _eff_pl_split))
                _http_write_ok = True
                result.processed += 1

            else:
                logger.warning("unsupported_writeback_action", action=action)
                result.skipped += 1

        except httpx.HTTPError as exc:
            logger.error("writeback_http_error", action=action, external_id=external_id, error=str(exc))
            _cb.record_failure()
            result.failed += 1
            result._failed_external_ids.add(external_id)
            result._failed_entries.append((external_id, action, str(exc)))
        else:
            # No HTTP exception — if an actual write was made, record success
            if _http_write_ok:
                _cb.record_success()

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

            # T2 #12: normalize GET response field names to write-path names before comparison
            _resp_map_v = getattr(writeback_cfg, "response_field_map", None) or {}
            if _resp_map_v:
                remote = {_resp_map_v.get(k, k): v for k, v in remote.items()}

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
                # Also fire in-process bus for same-process ingestion daemons
                try:
                    from inandout.events import EventType, get_event_bus
                    await get_event_bus().publish(
                        EventType.REINGEST_SIGNAL,
                        connector=connector.name,
                        datatype=result.datatype,
                        external_id=external_id,
                        reason="post_write_verify_conflict",
                    )
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
        audit_map: dict[tuple[str, str], tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]] = {}
        for ext_id, act, payload, diff, *rest in result._audit_entries:
            pl = rest[0] if rest else None
            audit_map[(ext_id, act)] = (payload, diff, pl)

        try:
            async with self._pool.connection() as conn:
                # Write successful rows
                for row in rows:
                    action = row.get("_action", "")
                    external_id = row.get("external_id") or row.get("_cluster_id", "")
                    audit_entry = audit_map.get((external_id, action), (None, None, None))
                    payload_snap, field_diff, effective_pl = audit_entry

                    # Try inserting with run_id + audit columns + protection_level (migration 022 / 006)
                    try:
                        await conn.execute(
                            """
                            INSERT INTO inout_ops_writeback_result
                                (connector, datatype, delta_table, action, external_id, status,
                                 run_id, payload_snapshot, field_diff, protection_level, processed_at)
                            VALUES (%s, %s, %s, %s, %s, 'ok', %s, %s, %s, %s, NOW())
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
                                effective_pl,
                            ],
                        )
                    except Exception:
                        # Fall back: without protection_level column (pre-T2#38 DB)
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
                                # Final fallback: bare insert
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

    async def _auto_dead_letter_exceeded_rows(
        self,
        result: WritebackResult,
        writeback_cfg: WritebackConfig,
    ) -> None:
        """T2 #24: Move failed rows that have exceeded max_retry_count to the dead-letter table."""
        max_retries = getattr(writeback_cfg, "max_retry_count", 3)
        if max_retries <= 0 or not result._failed_entries:
            return

        from inandout.deadletter.writeback import failure_count_for_row, move_to_dead_letter
        from inandout.postgres.schema import dead_letter_table_name

        # Get payload snapshots from audit entries indexed by (external_id, action)
        audit_payloads: dict[tuple[str, str], dict | None] = {
            (str(ext_id), str(act)): payload
            for ext_id, act, payload, *_ in result._audit_entries
        }

        for ext_id, act, error_msg in result._failed_entries:
            count = await failure_count_for_row(
                self._pool, result.connector, result.datatype, result.delta_table, ext_id
            )
            if count >= max_retries:
                payload_snap = audit_payloads.get((str(ext_id), str(act)))
                logger.warning(
                    "writeback_row_exceeded_max_retries",
                    connector=result.connector,
                    datatype=result.datatype,
                    external_id=ext_id,
                    action=act,
                    failure_count=count,
                    max_retry_count=max_retries,
                )
                await move_to_dead_letter(
                    self._pool,
                    result.connector,
                    result.datatype,
                    external_id=ext_id,
                    action=act,
                    payload_snapshot=payload_snap,
                    error_message=error_msg,
                    delta_table=result.delta_table,
                )
