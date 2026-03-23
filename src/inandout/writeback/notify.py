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
) -> AsyncIterator[str]:
    """Open a dedicated connection, LISTEN, and yield notification payloads."""
    async with pool.connection() as conn:
        await conn.set_autocommit(True)
        await conn.execute(f"LISTEN {channel}")
        logger.info("listening_for_delta_notifications", channel=channel)
        async for notification in conn.notifies():
            payload: str = notification.payload or ""
            yield payload
