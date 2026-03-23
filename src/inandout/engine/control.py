"""Control table command dispatcher.

Polls inout_ops_control for pending commands and executes them.
Supported commands:
  force_full_sync      — clear watermark for (connector, datatype) so next poll is a full sync
  pause_connector      — add (connector, datatype) to the in-process pause set
  resume_connector     — remove (connector, datatype) from the pause set
  requeue_dead_letter  — re-dispatch rows from the dead-letter table (max 3 retries per row)
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
    ) -> None:
        self._pool = pool
        self._paused = paused_connectors

    # ------------------------------------------------------------------
    # Main entry point — called by the polling loop every N seconds
    # ------------------------------------------------------------------

    async def dispatch_pending(self, engine: Any | None = None) -> int:
        """Fetch and execute up to 20 pending commands. Returns count executed."""
        async with self._pool.connection() as conn:
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
        else:
            raise ValueError(f"Unknown command: {command!r}")

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
        """Signal that config should be reloaded on next poll cycle.

        The actual reload is performed by the daemon loop — this command
        just logs the intent. Hot-reload via plugin version watcher handles
        the real work asynchronously.
        """
        scope = f"{connector or '*'}/{datatype or '*'}"
        logger.info("reload_config_requested", scope=scope, payload=payload)
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
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE inout_ops_control SET status='completed', completed_at=NOW(), result=%s WHERE id=%s",
                [orjson.dumps(result).decode(), cmd_id],
            )
            await conn.commit()

    async def _fail(self, cmd_id: uuid.UUID, error: str) -> None:
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE inout_ops_control SET status='failed', completed_at=NOW(), result=%s WHERE id=%s",
                [orjson.dumps({"error": error}).decode(), cmd_id],
            )
            await conn.commit()


def is_paused(paused_set: set[tuple[str, str]], connector: str, datatype: str) -> bool:
    """Return True if this (connector, datatype) pair is currently paused."""
    return (connector, datatype) in paused_set or (connector, "*") in paused_set
