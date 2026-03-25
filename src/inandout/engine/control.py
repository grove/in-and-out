"""Control table command dispatcher.

Polls inout_ops_control for pending commands and executes them.
Supported commands:
  force_full_sync      — clear watermark for (connector, datatype) so next poll is a full sync
  pause_connector      — add (connector, datatype) to the in-process pause set
  resume_connector     — remove (connector, datatype) from the pause set
  requeue_dead_letter  — re-dispatch rows from the dead-letter table (max 3 retries per row)
  rotate-credential    — invalidate cached auth tokens for a credential_ref
"""
from __future__ import annotations

import uuid
from typing import Any

import orjson
import psycopg
import structlog
from psycopg_pool import AsyncConnectionPool

logger = structlog.get_logger(__name__)

# Maximum number of times a DL row may be requeued before it is abandoned.
_MAX_REQUEUE_COUNT = 3


class ControlDispatcher:
    """Fetches and executes pending commands from inout_ops_control."""

    def __init__(
        self,
        pool: AsyncConnectionPool,
        paused_connectors: set[tuple[str, str]],
        target_tool: str | None = None,
        drain_callback: Any | None = None,
        reload_callback: Any | None = None,
    ) -> None:
        self._pool = pool
        self._paused = paused_connectors
        # When set, only dispatch commands addressed to this tool (or with no target_tool)
        self._target_tool = target_tool
        # Called when a 'drain' command is received; typically sets a module-level _draining flag
        self._drain_callback = drain_callback
        # Called when a 'reload-config' command is received; typically sets a threading.Event
        self._reload_callback = reload_callback

    # ------------------------------------------------------------------
    # Main entry point — called by the polling loop every N seconds
    # ------------------------------------------------------------------

    async def dispatch_pending(self, engine: Any | None = None) -> int:
        """Fetch and execute up to 20 pending commands. Returns count executed."""
        async with self._pool.connection() as conn:
            if self._target_tool is not None:
                rows = await (await conn.execute(
                    """
                    SELECT id, connector, datatype, command, payload
                    FROM inout_ops_control
                    WHERE status = 'pending'
                      AND (target_tool = %s OR target_tool IS NULL)
                    ORDER BY issued_at
                    LIMIT 20
                    """,
                    [self._target_tool],
                )).fetchall()
            else:
                rows = await (await conn.execute(
                    """
                    SELECT id, connector, datatype, command, payload
                    FROM inout_ops_control
                    WHERE status = 'pending'
                    ORDER BY issued_at
                    LIMIT 20
                    """
                )).fetchall()

        if not rows:
            return 0

        executed = 0
        for row in rows:
            cmd_id, connector, datatype, command, payload = row
            payload_dict: dict = {}
            if payload:
                try:
                    payload_dict = orjson.loads(payload) if isinstance(payload, (str, bytes)) else payload
                except Exception:
                    pass

            log = logger.bind(cmd_id=str(cmd_id), command=command, connector=connector, datatype=datatype)
            log.info("control_command_dispatching")

            await self._acknowledge(cmd_id)
            try:
                result = await self._execute(command, connector, datatype, payload_dict, engine)
                await self._complete(cmd_id, result)
                executed += 1
                log.info("control_command_completed", result=result)
            except Exception as exc:
                await self._fail(cmd_id, str(exc))
                log.error("control_command_failed", error=str(exc))

        return executed

    # ------------------------------------------------------------------
    # Command handlers
    # ------------------------------------------------------------------

    async def _execute(
        self,
        command: str,
        connector: str | None,
        datatype: str | None,
        payload: dict,
        engine: Any | None,
    ) -> dict:
        if command == "force_full_sync":
            return await self._cmd_force_full_sync(connector, datatype)
        elif command == "pause_connector":
            return self._cmd_pause_connector(connector, datatype)
        elif command == "resume_connector":
            return self._cmd_resume_connector(connector, datatype)
        elif command == "requeue_dead_letter":
            return await self._cmd_requeue_dead_letter(connector, datatype, payload, engine)
        elif command == "reset-watermark":
            return await self._cmd_force_full_sync(connector, datatype)
        elif command == "reload-config":
            return self._cmd_reload_config(connector, datatype, payload)
        elif command == "reset-circuit-breaker":
            return self._cmd_reset_circuit_breaker(connector, datatype)
        elif command == "resync":
            return await self._cmd_resync(connector, datatype, payload, engine)
        elif command == "trigger-writeback":
            return await self._cmd_trigger_writeback(connector, datatype, payload, engine)
        elif command == "validate":
            return await self._cmd_validate_writeback(connector, datatype, payload, engine)
        elif command == "drain":
            return self._cmd_drain(connector)
        elif command == "rotate-credential":
            return self._cmd_rotate_credential(connector, payload)
        else:
            raise ValueError(f"Unknown command: {command!r}")

    def _cmd_drain(self, connector: str | None) -> dict:
        """Initiate graceful drain: stop accepting new rows; finish in-flight ones.

        Sets the daemon-side _draining flag via the registered callback, causing
        all polling loops to exit cleanly after their current iteration.  The
        process will then terminate normally when its task group unwinds.
        """
        if self._drain_callback is not None:
            self._drain_callback()
        scope = connector or "all"
        logger.info("drain_initiated", scope=scope)
        return {"draining": scope}

    def _cmd_rotate_credential(
        self, connector: str | None, payload: dict
    ) -> dict:
        """Invalidate cached auth tokens for a credential, forcing re-acquisition.

        Payload keys:
          credential_ref (required) — the credential whose cached tokens to clear.
        """
        credential_ref: str | None = payload.get("credential_ref")
        if not credential_ref:
            raise ValueError(
                "rotate-credential requires 'credential_ref' in payload"
            )

        from inandout.transport.auth import invalidate_credential_cache

        invalidated = invalidate_credential_cache(credential_ref)
        logger.info(
            "credential_rotated",
            connector=connector,
            credential_ref=credential_ref,
            invalidated=invalidated,
        )
        return {
            "rotated": credential_ref,
            "cache_entries_invalidated": invalidated,
        }

    async def _cmd_force_full_sync(
        self, connector: str | None, datatype: str | None
    ) -> dict:
        """Clear watermark(s) so the next poll does a full sync."""
        if not connector:
            raise ValueError("force_full_sync requires 'connector'")

        async with self._pool.connection() as conn:
            if datatype:
                await conn.execute(
                    "DELETE FROM inout_ops_watermark WHERE connector = %s AND datatype = %s",
                    [connector, datatype],
                )
            else:
                await conn.execute(
                    "DELETE FROM inout_ops_watermark WHERE connector = %s",
                    [connector],
                )
            await conn.commit()

        scope = f"{connector}/{datatype or '*'}"
        logger.info("watermark_cleared_for_full_sync", scope=scope)
        return {"cleared": scope}

    def _cmd_pause_connector(
        self, connector: str | None, datatype: str | None
    ) -> dict:
        if not connector:
            raise ValueError("pause_connector requires 'connector'")
        key = (connector, datatype or "*")
        self._paused.add(key)
        logger.info("connector_paused", connector=connector, datatype=datatype)
        return {"paused": f"{connector}/{datatype or '*'}"}

    def _cmd_resume_connector(
        self, connector: str | None, datatype: str | None
    ) -> dict:
        if not connector:
            raise ValueError("resume_connector requires 'connector'")
        key = (connector, datatype or "*")
        self._paused.discard(key)
        logger.info("connector_resumed", connector=connector, datatype=datatype)
        return {"resumed": f"{connector}/{datatype or '*'}"}

    async def _cmd_requeue_dead_letter(
        self,
        connector: str | None,
        datatype: str | None,
        payload: dict,
        engine: Any | None,
    ) -> dict:
        """Re-dispatch rows from the dead-letter table via the ingestion engine."""
        if not connector or not datatype:
            raise ValueError("requeue_dead_letter requires 'connector' and 'datatype'")
        if engine is None:
            raise RuntimeError("requeue_dead_letter requires an active IngestionEngine")

        from inandout.postgres.schema import dead_letter_table_name
        dl_table = dead_letter_table_name("ingestion", connector, datatype)
        limit = int(payload.get("limit", 50))

        async with self._pool.connection() as conn:
            try:
                rows = await (await conn.execute(
                    f"""
                    SELECT id, external_id, raw
                    FROM {dl_table}
                    WHERE requeue_count < %s AND requeued_at IS NULL
                    ORDER BY failed_at
                    LIMIT %s
                    """,
                    [_MAX_REQUEUE_COUNT, limit],
                )).fetchall()
            except psycopg.errors.UndefinedTable:
                return {"requeued": 0, "reason": "dead-letter table not found"}

        if not rows:
            return {"requeued": 0}

        requeued = 0
        errors = 0
        for row in rows:
            dl_id, external_id, raw_json = row
            try:
                # psycopg3 auto-parses JSONB → dict; handle both dict and JSON string
                if isinstance(raw_json, dict):
                    raw: dict = raw_json
                elif raw_json:
                    raw = orjson.loads(raw_json)
                else:
                    raw = {}
                # Re-inject via a direct upsert — bypass HTTP layer
                await self._requeue_single(connector, datatype, dl_id, external_id, raw)
                requeued += 1
            except Exception as exc:
                errors += 1
                logger.warning("requeue_row_failed", dl_id=dl_id, error=str(exc))

        return {"requeued": requeued, "errors": errors}

    def _cmd_reload_config(
        self,
        connector: str | None,
        datatype: str | None,
        payload: dict,
    ) -> dict:
        """Signal that config should be reloaded at the start of the next poll cycle.

        Calls the registered reload_callback (typically threading.Event.set) which
        is monitored by _hot_reload_watcher in the daemon.  The hot-reload loop then
        re-reads connector YAML files and restarts affected polling tasks.
        """
        scope = f"{connector or '*'}/{datatype or '*'}"
        if self._reload_callback is not None:
            self._reload_callback()
            logger.info("reload_config_triggered", scope=scope)
        else:
            logger.info("reload_config_requested_no_callback", scope=scope)
        return {"reload_requested": scope}

    def _cmd_reset_circuit_breaker(
        self,
        connector: str | None,
        datatype: str | None,
    ) -> dict:
        """Force the circuit breaker for (connector, datatype) back to CLOSED."""
        if not connector:
            raise ValueError("reset-circuit-breaker requires 'connector'")

        from inandout.transport.circuit_breaker import _registry, get_circuit_breaker

        if datatype:
            # Reset specific (connector, datatype) pair
            cb = _registry.get((connector, datatype))
            if cb is not None:
                cb.reset()
            scope = f"{connector}/{datatype}"
        else:
            # Reset all circuit breakers for this connector
            to_reset = [cb for (c, _), cb in _registry.items() if c == connector]
            for cb in to_reset:
                cb.reset()
            scope = f"{connector}/*"

        logger.info("circuit_breaker_reset_via_control", scope=scope)
        return {"reset": scope}

    async def _cmd_resync(
        self,
        connector: str | None,
        datatype: str | None,
        payload: dict,
        engine: Any | None,
    ) -> dict:
        """Handle a resync command: targeted single-record fetch or full sync."""
        if not connector or not datatype:
            raise ValueError("resync requires 'connector' and 'datatype'")
        if engine is None:
            raise RuntimeError("resync requires an active IngestionEngine")

        external_id = payload.get("external_id")
        max_iterations = int(payload.get("max_iterations", 3))

        if external_id is not None:
            # Check how many times this record has been resynced recently
            async with self._pool.connection() as conn:
                try:
                    count_row = await (await conn.execute(
                        """
                        SELECT COUNT(*) FROM inout_ops_control
                        WHERE connector = %s AND datatype = %s
                          AND command = 'resync'
                          AND payload->>'external_id' = %s
                          AND issued_at > NOW() - INTERVAL '1 hour'
                        """,
                        [connector, datatype, str(external_id)],
                    )).fetchone()
                    resync_count = count_row[0] if count_row else 0
                except Exception:
                    resync_count = 0

            if resync_count >= max_iterations:
                logger.warning(
                    "resync_max_iterations_reached",
                    connector=connector,
                    datatype=datatype,
                    external_id=external_id,
                    count=resync_count,
                    max_iterations=max_iterations,
                )
                return {"status": "abandoned", "reason": "max_iterations", "external_id": external_id}

            # Find connector config and ingestion config from engine
            # engine.run_sync_single_record needs connector cfg — look it up
            try:
                connector_cfg = None
                ingestion_cfg = None
                if hasattr(engine, "_connector_configs"):
                    for cfg in engine._connector_configs:
                        if cfg.name == connector:
                            connector_cfg = cfg
                            dtype_cfg = cfg.datatypes.get(datatype)
                            if dtype_cfg:
                                ingestion_cfg = dtype_cfg.ingestion
                            break

                if connector_cfg is None or ingestion_cfg is None:
                    logger.warning(
                        "resync_connector_config_not_found",
                        connector=connector, datatype=datatype
                    )
                    return {"status": "skipped", "reason": "connector_config_not_found"}

                result = await engine.run_sync_single_record(
                    connector_cfg, datatype, ingestion_cfg, str(external_id)
                )
                return {
                    "status": result.status,
                    "external_id": external_id,
                    "inserted": result.records_inserted,
                    "updated": result.records_updated,
                }
            except Exception as exc:
                logger.error("resync_single_record_failed", error=str(exc))
                return {"status": "failed", "error": str(exc)}
        else:
            # No external_id → trigger full sync
            try:
                connector_cfg = None
                ingestion_cfg = None
                if hasattr(engine, "_connector_configs"):
                    for cfg in engine._connector_configs:
                        if cfg.name == connector:
                            connector_cfg = cfg
                            dtype_cfg = cfg.datatypes.get(datatype)
                            if dtype_cfg:
                                ingestion_cfg = dtype_cfg.ingestion
                            break

                if connector_cfg is None or ingestion_cfg is None:
                    return {"status": "skipped", "reason": "connector_config_not_found"}

                result = await engine.run_sync(connector_cfg, datatype, ingestion_cfg)
                return {"status": result.status, "mode": "full_sync"}
            except Exception as exc:
                logger.error("resync_full_sync_failed", error=str(exc))
                return {"status": "failed", "error": str(exc)}

    async def _requeue_single(
        self,
        connector: str,
        datatype: str,
        dl_id: int,
        external_id: str | None,
        raw: dict,
    ) -> None:
        from inandout.postgres.schema import source_table_name, dead_letter_table_name
        from inandout.ingestion.engine import _compute_raw_hash, _upsert_record

        if not external_id or not raw:
            raise ValueError("Cannot requeue row without external_id and raw data")

        src_table = source_table_name(connector, datatype)
        dl_table = dead_letter_table_name("ingestion", connector, datatype)
        raw_hash = _compute_raw_hash(raw)
        run_id = uuid.uuid4()

        async with self._pool.connection() as conn:
            async with conn.transaction():
                await _upsert_record(conn, src_table, external_id, raw, raw_hash, run_id)
                await conn.execute(
                    f"""
                    UPDATE {dl_table}
                    SET requeue_count = requeue_count + 1, requeued_at = NOW()
                    WHERE id = %s
                    """,
                    [dl_id],
                )

    async def _cmd_trigger_writeback(
        self,
        connector: str | None,
        datatype: str | None,
        payload: dict,
        engine: Any | None,
    ) -> dict:
        """B2: Trigger one writeback cycle immediately for (connector, datatype)."""
        if not connector or not datatype:
            raise ValueError("trigger-writeback requires 'connector' and 'datatype'")

        # engine here may be a WritebackEngine — look for run_writeback_cycle method
        if engine is None or not hasattr(engine, "run_writeback_cycle"):
            # Try to import and use WritebackEngine from the pool
            return {"status": "skipped", "reason": "no_writeback_engine_available"}

        delta_table = payload.get("delta_table", f"_delta_{connector}_{datatype}")

        # We need writeback_cfg — look it up from connected configs if available
        writeback_cfg = payload.get("_writeback_cfg")  # may be injected by daemon
        if writeback_cfg is None:
            return {"status": "skipped", "reason": "writeback_cfg_not_found"}

        try:
            from inandout.config.connector import ConnectorConfig
            result = await engine.run_writeback_cycle(
                payload.get("_connector_cfg"), datatype, writeback_cfg, delta_table
            )
            logger.info(
                "trigger_writeback_completed",
                connector=connector,
                datatype=datatype,
                processed=result.processed,
                skipped=result.skipped,
            )
            return {"status": "completed", "processed": result.processed, "skipped": result.skipped}
        except Exception as exc:
            logger.error("trigger_writeback_failed", connector=connector, datatype=datatype, error=str(exc))
            return {"status": "failed", "error": str(exc)}

    async def _cmd_validate_writeback(
        self,
        connector: str | None,
        datatype: str | None,
        payload: dict,
        engine: Any | None,
    ) -> dict:
        """B3: Validate writeback connectivity, auth, field mappings, and ETag support (T2 #37)."""
        if not connector:
            raise ValueError("validate requires 'connector'")

        # Try to load the connector config from the registry / payload path
        connector_cfg = None
        connector_path = payload.get("connector_path") or payload.get("connector")
        if connector_path:
            try:
                from pathlib import Path
                from inandout.config.loader import load_connector
                loaded = load_connector(Path(connector_path))
                connector_cfg = loaded.connector
            except Exception as load_exc:
                logger.warning("validate_connector_load_failed", path=connector_path, error=str(load_exc))

        # Fall back to engine's connector registry if available
        if connector_cfg is None and engine is not None:
            registry = getattr(engine, "_connector_registry", {})
            connector_cfg = registry.get(connector)

        if connector_cfg is not None:
            # Use the rich validation module
            from inandout.writeback.validate import validate_writeback_connector

            datatype_names = [datatype] if datatype else None
            vr = await validate_writeback_connector(connector_cfg, datatype_names=datatype_names)

            raw_datatypes = []
            for dt in vr.datatypes:
                raw_datatypes.append({
                    "datatype": dt.datatype,
                    "configured_protection_level": dt.configured_protection_level,
                    "effective_protection_level": dt.effective_protection_level,
                    "etag_support": dt.etag_support,
                    "if_match_support": dt.if_match_support,
                    "operations_ok": dt.operations_ok,
                    "errors": dt.errors,
                    "warnings": dt.warnings,
                })
            return {
                "connector": vr.connector,
                "connectivity": vr.connectivity,
                "auth": vr.auth,
                "ok": vr.ok,
                "datatypes": raw_datatypes,
                "errors": vr.errors,
            }

        # Fallback: shallow probe using base_url from payload
        result: dict = {
            "connectivity": "unknown",
            "auth": "unknown",
            "field_mappings": "unknown",
            "etag_support": False,
            "protection_level": "unknown",
            "errors": [],
        }

        base_url = payload.get("base_url", "")
        if not base_url:
            result["errors"].append(
                "connector config not found and base_url not provided — "
                "pass connector_path in payload or ensure connector is registered"
            )
            return result

        try:
            import httpx
            async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
                try:
                    resp = await client.get("/")
                    result["connectivity"] = "ok"
                    if resp.status_code == 401:
                        result["auth"] = "failed"
                        result["errors"].append(f"401 Unauthorized from {base_url}")
                    elif resp.status_code == 403:
                        result["auth"] = "failed"
                        result["errors"].append(f"403 Forbidden from {base_url}")
                    else:
                        result["auth"] = "ok"
                except Exception as conn_exc:
                    result["connectivity"] = "failed"
                    result["errors"].append(f"connection failed: {conn_exc}")

                if result["connectivity"] == "ok":
                    try:
                        etag_header = payload.get("etag_header", "ETag")
                        head_resp = await client.head("/")
                        etag = head_resp.headers.get(etag_header) or head_resp.headers.get(etag_header.lower())
                        result["etag_support"] = bool(etag)
                        result["protection_level"] = "optimistic" if etag else "none"
                    except Exception:
                        result["etag_support"] = False
                        result["protection_level"] = "none"

        except Exception as outer_exc:
            result["errors"].append(f"validate_error: {outer_exc}")

        # Field mapping / operation path validation from payload
        operations = payload.get("operations", {})
        field_errors = []
        for op_name, op_cfg in operations.items():
            if isinstance(op_cfg, dict) and not op_cfg.get("path"):
                field_errors.append(f"operation.{op_name}.path is empty")
        result["field_mappings"] = "ok" if not field_errors else f"errors: {field_errors}"
        if field_errors:
            result["errors"].extend(field_errors)

        logger.info("writeback_validate_complete", connector=connector, datatype=datatype, result=result)
        return result

    # ------------------------------------------------------------------
    # Status helpers
    # ------------------------------------------------------------------

    async def _acknowledge(self, cmd_id: uuid.UUID) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE inout_ops_control SET status='acknowledged', acknowledged_at=NOW() WHERE id=%s",
                [cmd_id],
            )
            await conn.commit()

    async def _complete(self, cmd_id: uuid.UUID, result: dict) -> None:
        raw = orjson.dumps(result).decode()
        # Guard against very large results bloating the control table
        if len(raw) > 8000:
            raw = orjson.dumps({"_truncated": True, "summary": raw[:500]}).decode()
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE inout_ops_control SET status='completed', completed_at=NOW(), result=%s WHERE id=%s",
                [raw, cmd_id],
            )
            await conn.commit()

    async def _fail(self, cmd_id: uuid.UUID, error: str) -> None:
        raw = orjson.dumps({"error": error[:2000]}).decode()
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE inout_ops_control SET status='failed', completed_at=NOW(), result=%s WHERE id=%s",
                [raw, cmd_id],
            )
            await conn.commit()


def is_paused(paused_set: set[tuple[str, str]], connector: str, datatype: str) -> bool:
    """Return True if this (connector, datatype) pair is currently paused."""
    return (connector, datatype) in paused_set or (connector, "*") in paused_set
