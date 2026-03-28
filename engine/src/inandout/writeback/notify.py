"""PostgreSQL LISTEN/NOTIFY support for streaming writeback."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, AsyncIterator

import structlog

logger = structlog.get_logger(__name__)

_TRIGGER_FUNC_TEMPLATE = """\
CREATE OR REPLACE FUNCTION inandout_notify_{safe_name}()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_notify('inandout_delta', '{connector}:{datatype}');
  RETURN NEW;
END;
$$;
"""

_TRIGGER_DDL_TEMPLATE = """\
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger WHERE tgname = 'inandout_notify_{safe_name}_trigger'
  ) THEN
    CREATE TRIGGER inandout_notify_{safe_name}_trigger
    AFTER INSERT ON {delta_table}
    FOR EACH ROW EXECUTE FUNCTION inandout_notify_{safe_name}();
  END IF;
END;
$$;
"""


def _safe_name(delta_table: str) -> str:
    """Derive a safe identifier from a table name."""
    return delta_table.replace(".", "_").replace("-", "_")


async def create_delta_notify_trigger(conn: Any, delta_table: str) -> None:
    """Create a PostgreSQL trigger that fires NOTIFY inandout_delta on INSERT."""
    # Extract connector and datatype from delta_table name (_delta_<connector>_<datatype>)
    parts = delta_table.lstrip("_").split("_", 2)
    if len(parts) >= 3:
        connector = parts[1]
        datatype = parts[2]
    else:
        connector = delta_table
        datatype = "unknown"

    safe = _safe_name(delta_table)
    func_sql = _TRIGGER_FUNC_TEMPLATE.format(
        safe_name=safe,
        connector=connector,
        datatype=datatype,
    )
    trigger_sql = _TRIGGER_DDL_TEMPLATE.format(
        safe_name=safe,
        delta_table=delta_table,
    )
    await conn.execute(func_sql)
    await conn.execute(trigger_sql)


async def listen_for_deltas(
    pool: Any,
    channel: str = "inandout_delta",
    reconnect_delay_secs: float = 2.0,
    reconnect_max_secs: float = 60.0,
) -> AsyncIterator[str]:
    """Open a dedicated connection, LISTEN, and yield notification payloads.

    Automatically reconnects if the underlying PostgreSQL connection drops
    (e.g. due to a network hiccup, PgBouncer timeout, or server restart).
    Back-off starts at *reconnect_delay_secs* and doubles on each consecutive
    failure up to *reconnect_max_secs*, then resets to the base value after a
    successful reconnect.
    """
    import anyio

    delay = reconnect_delay_secs
    while True:
        try:
            async with pool.connection() as conn:
                await conn.set_autocommit(True)
                await conn.execute(f"LISTEN {channel}")
                logger.info("listening_for_delta_notifications", channel=channel)
                delay = reconnect_delay_secs  # reset back-off on successful connect
                async for notification in conn.notifies():
                    payload: str = notification.payload or ""
                    yield payload
        except Exception as exc:
            logger.warning(
                "delta_notify_connection_lost",
                error=str(exc),
                reconnect_in_secs=delay,
            )
            await anyio.sleep(delay)
            delay = min(delay * 2, reconnect_max_secs)
