"""T2 #24: Writeback dead-letter queue helpers.

Permanently failed writeback rows (those that have exceeded *max_retry_count*
consecutive failures) are moved from the delta table to a per-datatype
dead-letter table ``inout_dl_writeback_{connector}_{datatype}``.

Operators can then inspect, replay, or drop these rows via the CLI.
"""
from __future__ import annotations

from typing import Any

import orjson
import structlog

logger = structlog.get_logger(__name__)

# Maximum requeue attempts before a dead-letter row is considered permanently failed
_MAX_DL_REQUEUE_COUNT = 3


async def failure_count_for_row(
    pool: Any,
    connector: str,
    datatype: str,
    delta_table: str,
    external_id: str,
) -> int:
    """Return the number of times *external_id* has failed in the audit table."""
    try:
        async with pool.connection() as conn:
            row = await (await conn.execute(
                """
                SELECT COUNT(*)
                FROM inout_ops_writeback_result
                WHERE connector = %s
                  AND datatype = %s
                  AND delta_table = %s
                  AND external_id = %s
                  AND status = 'failed'
                """,
                [connector, datatype, delta_table, external_id],
            )).fetchone()
            return int(row[0]) if row else 0
    except Exception:
        return 0


async def move_to_dead_letter(
    pool: Any,
    connector: str,
    datatype: str,
    external_id: str,
    action: str,
    payload_snapshot: dict | None,
    error_message: str,
    delta_table: str,
    namespace: str = "public",
) -> None:
    """Move a permanently failed writeback row to the dead-letter table.

    Also marks the delta row as ``_action='dead_lettered'`` to prevent
    further automatic retries.
    """
    from inandout.postgres.schema import dead_letter_table_name, ensure_dead_letter_table

    dl_table = dead_letter_table_name("writeback", connector, datatype, namespace)

    # Check dead-letter policy before accepting the row
    try:
        from inandout.deadletter.policy import get_policy
        _attempt = await failure_count_for_row(pool, connector, datatype, delta_table, external_id)
        if not get_policy(connector).should_dead_letter(
            connector, datatype, external_id, action, _attempt
        ):
            logger.info(
                "writeback_dead_letter_skipped_by_policy",
                connector=connector,
                datatype=datatype,
                external_id=external_id,
                action=action,
            )
            return
    except Exception:
        pass  # Policy check failure must not block the default DL path

    try:
        async with pool.connection() as conn:
            await ensure_dead_letter_table(conn, "writeback", connector, datatype, namespace)

            raw_bytes = orjson.dumps(payload_snapshot) if payload_snapshot else b"null"
            await conn.execute(
                f"""
                INSERT INTO {dl_table}
                    (external_id, raw, error_message, error_class)
                VALUES (%s, %s::jsonb, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                [external_id, raw_bytes.decode(), error_message, action],
            )

            # Mark the delta row as dead-lettered so it is no longer fetched
            try:
                await conn.execute(
                    f"""
                    UPDATE {delta_table}
                    SET _action = 'dead_lettered'
                    WHERE (external_id = %s OR _cluster_id = %s)
                      AND _action NOT IN ('noop', 'dead_lettered')
                    """,
                    [external_id, external_id],
                )
            except Exception as upd_exc:
                logger.debug(
                    "writeback_dl_mark_delta_failed",
                    external_id=external_id,
                    error=str(upd_exc),
                )

            await conn.commit()
            logger.info(
                "writeback_row_dead_lettered",
                connector=connector,
                datatype=datatype,
                external_id=external_id,
                action=action,
                dl_table=dl_table,
            )
    except Exception as exc:
        logger.warning(
            "writeback_dead_letter_write_failed",
            connector=connector,
            datatype=datatype,
            external_id=external_id,
            error=str(exc),
        )


async def fetch_writeback_dead_letter_rows(
    pool: Any,
    connector: str,
    datatype: str,
    limit: int = 20,
    namespace: str = "public",
) -> list[dict]:
    """Fetch rows from the writeback dead-letter table."""
    from inandout.postgres.schema import dead_letter_table_name
    table = dead_letter_table_name("writeback", connector, datatype, namespace)
    try:
        async with pool.connection() as conn:
            rows = await (await conn.execute(
                f"""
                SELECT id, external_id, raw, error_message, error_class,
                       failed_at, requeue_count
                FROM {table}
                ORDER BY failed_at DESC
                LIMIT %s
                """,
                [limit],
            )).fetchall()
        return [
            {
                "id": r[0],
                "external_id": r[1],
                "raw": r[2],
                "error_message": r[3],
                "error_class": r[4],   # stores the action
                "failed_at": r[5],
                "requeue_count": r[6],
            }
            for r in rows
        ]
    except Exception as exc:
        logger.warning(
            "writeback_dl_fetch_error",
            connector=connector,
            datatype=datatype,
            error=str(exc),
        )
        return []


async def replay_writeback_dead_letter_rows(
    pool: Any,
    connector: str,
    datatype: str,
    delta_table: str,
    limit: int = 50,
    namespace: str = "public",
) -> dict:
    """Re-insert dead-letter rows into the delta table for retry.

    Only rows with ``requeue_count < _MAX_DL_REQUEUE_COUNT`` are replayed.
    Returns a summary dict with ``replayed`` and ``errors`` counts.
    """
    from inandout.postgres.schema import dead_letter_table_name
    dl_table = dead_letter_table_name("writeback", connector, datatype, namespace)

    try:
        async with pool.connection() as conn:
            rows = await (await conn.execute(
                f"""
                SELECT id, external_id, raw, error_class, requeue_count, failed_at
                FROM {dl_table}
                WHERE requeue_count < %s AND requeued_at IS NULL
                ORDER BY failed_at
                LIMIT %s
                """,
                [_MAX_DL_REQUEUE_COUNT, limit],
            )).fetchall()
    except Exception as exc:
        return {"replayed": 0, "errors": 0, "reason": str(exc)}

    if not rows:
        return {"replayed": 0, "errors": 0}

    replayed = 0
    errors = 0

    for row in rows:
        dl_id, external_id, raw_json, action, requeue_count, failed_at = row
        # Check replay policy
        try:
            from inandout.deadletter.policy import get_policy
            import datetime as _dt_rp
            _now = _dt_rp.datetime.now(_dt_rp.timezone.utc)
            _fa = failed_at
            if _fa is not None and hasattr(_fa, "tzinfo") and _fa.tzinfo is None:
                _fa = _fa.replace(tzinfo=_dt_rp.timezone.utc)
            age_secs = (_now - _fa).total_seconds() if _fa else 0.0
            if not get_policy(connector).should_replay(
                connector, datatype, external_id or "",
                requeue_count or 0, age_secs,
            ):
                continue
        except Exception:
            pass  # Policy check failure falls back to default behaviour
        try:
            payload: dict = {}
            if isinstance(raw_json, dict):
                payload = raw_json
            elif raw_json:
                payload = orjson.loads(raw_json)

            action = action or "update"

            # Re-insert into delta table with original action
            async with pool.connection() as conn:
                cols = list(payload.keys()) if payload else []
                if "_action" not in cols:
                    cols = ["_action"] + cols
                    vals = [action] + list(payload.values())
                else:
                    vals = list(payload.values())

                await conn.execute(
                    f"""
                    UPDATE {dl_table}
                    SET requeued_at = NOW(),
                        requeue_count = requeue_count + 1
                    WHERE id = %s
                    """,
                    [dl_id],
                )
                # Un-dead-letter the delta row so it can be re-fetched
                await conn.execute(
                    f"""
                    UPDATE {delta_table}
                    SET _action = %s
                    WHERE (external_id = %s OR _cluster_id = %s)
                      AND _action = 'dead_lettered'
                    """,
                    [action, external_id, external_id],
                )
                await conn.commit()
            replayed += 1
            logger.info(
                "writeback_dl_row_replayed",
                dl_id=dl_id,
                connector=connector,
                datatype=datatype,
                external_id=external_id,
            )
        except Exception as exc:
            errors += 1
            logger.warning(
                "writeback_dl_replay_row_failed",
                dl_id=dl_id,
                external_id=external_id,
                error=str(exc),
            )

    return {"replayed": replayed, "errors": errors}
