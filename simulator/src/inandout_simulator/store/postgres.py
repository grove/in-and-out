"""PostgreSQL-backed persistent store for the demo simulator."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

from inandout_simulator.store import MutationEvent, _new_id, _next_id, _now_iso


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


_DDL = """\
CREATE TABLE IF NOT EXISTS sim_records (
    connector   TEXT NOT NULL,
    datatype    TEXT NOT NULL,
    record_id   TEXT NOT NULL,
    data        JSONB NOT NULL,
    created_at  TEXT NOT NULL,
    modified_at TEXT NOT NULL,
    source      TEXT NOT NULL DEFAULT 'seed',
    deleted_at  TEXT,
    PRIMARY KEY (connector, datatype, record_id)
);

CREATE TABLE IF NOT EXISTS sim_mutations (
    event_id    TEXT PRIMARY KEY,
    connector   TEXT NOT NULL,
    datatype    TEXT NOT NULL,
    operation   TEXT NOT NULL,
    record_id   TEXT NOT NULL,
    record      JSONB,
    before      JSONB,
    source      TEXT NOT NULL,
    timestamp   TEXT NOT NULL
);
"""


class PostgresStore:
    """Async PostgreSQL store using psycopg3's native async API.

    The DSN may be any libpq connection string accepted by psycopg, e.g.
    ``postgresql://user:pass@host:5432/dbname``.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: Any = None
        self._init_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _get_pool(self) -> Any:
        """Return the connection pool, creating and initialising it on first call."""
        if self._pool is not None:
            return self._pool
        async with self._init_lock:
            if self._pool is not None:
                return self._pool
            from psycopg_pool import AsyncConnectionPool

            pool: Any = AsyncConnectionPool(
                conninfo=self._dsn,
                min_size=1,
                max_size=10,
                open=False,
            )
            await pool.open()
            async with pool.connection() as conn:
                # DDL is idempotent; run with autocommit so it can't dead-lock
                # with itself when two processes start simultaneously.
                await conn.set_autocommit(True)
                await conn.execute(_DDL)
            self._pool = pool
        return self._pool

    # ------------------------------------------------------------------
    # Protocol implementation
    # ------------------------------------------------------------------

    async def seed(
        self,
        connector: str,
        datatype: str,
        records: list[dict[str, Any]],
        pk_field: str = "id",
        cursor_field: str | None = None,
    ) -> None:
        from psycopg.types.json import Jsonb

        pool = await self._get_pool()
        async with pool.connection() as conn:
            for rec in records:
                rid = str(rec.get(pk_field, _new_id()))
                ts = _ts()
                if cursor_field and cursor_field in rec:
                    ts = str(rec[cursor_field])
                await conn.execute(
                    """
                    INSERT INTO sim_records
                        (connector, datatype, record_id, data, created_at, modified_at, source)
                    VALUES (%s, %s, %s, %s, %s, %s, 'seed')
                    ON CONFLICT DO NOTHING
                    """,
                    (connector, datatype, rid, Jsonb(rec), ts, ts),
                )

    async def list_all(
        self,
        connector: str,
        datatype: str,
        *,
        cursor_field: str | None = None,
        watermark: str | None = None,
        include_deleted: bool = False,
    ) -> list[dict[str, Any]]:
        pool = await self._get_pool()
        async with pool.connection() as conn:
            if include_deleted:
                rows = await conn.execute(
                    "SELECT data, deleted_at, modified_at, created_at "
                    "FROM sim_records WHERE connector=%s AND datatype=%s "
                    "ORDER BY modified_at ASC",
                    (connector, datatype),
                )
            else:
                rows = await conn.execute(
                    "SELECT data, deleted_at, modified_at, created_at "
                    "FROM sim_records WHERE connector=%s AND datatype=%s AND deleted_at IS NULL "
                    "ORDER BY modified_at ASC",
                    (connector, datatype),
                )
            result = []
            async for row in rows:
                d = dict(row[0])  # psycopg3 returns JSONB as dict
                d["__modified_at__"] = row[2]
                d["__created_at__"] = row[3]
                if row[1] is not None:
                    d["__deleted_at__"] = row[1]
                result.append(d)

        if cursor_field and watermark:
            result = [r for r in result if str(r.get(cursor_field, "")) > watermark]
        return result

    async def get_by_id(
        self,
        connector: str,
        datatype: str,
        record_id: str,
    ) -> dict[str, Any] | None:
        pool = await self._get_pool()
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT data, deleted_at, modified_at, created_at "
                "FROM sim_records WHERE connector=%s AND datatype=%s AND record_id=%s",
                (connector, datatype, record_id),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        result = dict(row[0])
        result["__modified_at__"] = row[2]
        result["__created_at__"] = row[3]
        if row[1] is not None:
            result["__deleted_at__"] = row[1]
        return result

    async def create(
        self,
        connector: str,
        datatype: str,
        data: dict[str, Any],
        pk_field: str = "id",
        source: str = "engine",
    ) -> dict[str, Any]:
        from psycopg.types.json import Jsonb

        pool = await self._get_pool()
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT record_id FROM sim_records WHERE connector=%s AND datatype=%s",
                (connector, datatype),
            )
            existing = [row[0] async for row in cur]
            rid = str(data.get(pk_field) or _next_id(existing))
            data = dict(data)
            data[pk_field] = rid
            ts = _ts()
            await conn.execute(
                """
                INSERT INTO sim_records
                    (connector, datatype, record_id, data, created_at, modified_at, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (connector, datatype, record_id)
                DO UPDATE SET data=%s, modified_at=%s, source=%s, deleted_at=NULL
                """,
                (
                    connector, datatype, rid, Jsonb(data), ts, ts, source,
                    Jsonb(data), ts, source,
                ),
            )
            await conn.execute(
                "INSERT INTO sim_mutations "
                "    (event_id, connector, datatype, operation, record_id, record, before, source, timestamp) "
                "VALUES (%s, %s, %s, 'create', %s, %s, NULL, %s, %s)",
                (_new_id(), connector, datatype, rid, Jsonb(data), source, ts),
            )
        return data

    async def update(
        self,
        connector: str,
        datatype: str,
        record_id: str,
        data: dict[str, Any],
        source: str = "engine",
    ) -> dict[str, Any] | None:
        from psycopg.types.json import Jsonb

        pool = await self._get_pool()
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT data FROM sim_records "
                "WHERE connector=%s AND datatype=%s AND record_id=%s",
                (connector, datatype, record_id),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            existing: dict[str, Any] = dict(row[0])
            merged = {**existing, **data}
            ts = _ts()
            await conn.execute(
                "UPDATE sim_records SET data=%s, modified_at=%s, source=%s "
                "WHERE connector=%s AND datatype=%s AND record_id=%s",
                (Jsonb(merged), ts, source, connector, datatype, record_id),
            )
            await conn.execute(
                "INSERT INTO sim_mutations "
                "    (event_id, connector, datatype, operation, record_id, record, before, source, timestamp) "
                "VALUES (%s, %s, %s, 'update', %s, %s, %s, %s, %s)",
                (_new_id(), connector, datatype, record_id, Jsonb(merged), Jsonb(existing), source, ts),
            )
        return merged

    async def delete(
        self,
        connector: str,
        datatype: str,
        record_id: str,
        source: str = "engine",
    ) -> bool:
        from psycopg.types.json import Jsonb

        pool = await self._get_pool()
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT data, deleted_at FROM sim_records "
                "WHERE connector=%s AND datatype=%s AND record_id=%s",
                (connector, datatype, record_id),
            )
            row = await cur.fetchone()
            if row is None or row[1] is not None:
                return False  # not found or already deleted
            before: dict[str, Any] = dict(row[0])
            ts = _ts()
            await conn.execute(
                "UPDATE sim_records SET deleted_at=%s, modified_at=%s, source=%s "
                "WHERE connector=%s AND datatype=%s AND record_id=%s",
                (ts, ts, source, connector, datatype, record_id),
            )
            await conn.execute(
                "INSERT INTO sim_mutations "
                "    (event_id, connector, datatype, operation, record_id, record, before, source, timestamp) "
                "VALUES (%s, %s, %s, 'delete', %s, NULL, %s, %s, %s)",
                (_new_id(), connector, datatype, record_id, Jsonb(before), source, ts),
            )
        return True

    async def restore(
        self,
        connector: str,
        datatype: str,
        record_id: str,
        source: str = "ui",
    ) -> dict[str, Any] | None:
        from psycopg.types.json import Jsonb

        pool = await self._get_pool()
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT data, deleted_at FROM sim_records "
                "WHERE connector=%s AND datatype=%s AND record_id=%s",
                (connector, datatype, record_id),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            data: dict[str, Any] = dict(row[0])
            if row[1] is None:
                return data  # not deleted, nothing to do
            ts = _ts()
            await conn.execute(
                "UPDATE sim_records SET deleted_at=NULL, modified_at=%s, source=%s "
                "WHERE connector=%s AND datatype=%s AND record_id=%s",
                (ts, source, connector, datatype, record_id),
            )
            await conn.execute(
                "INSERT INTO sim_mutations "
                "    (event_id, connector, datatype, operation, record_id, record, before, source, timestamp) "
                "VALUES (%s, %s, %s, 'create', %s, %s, NULL, %s, %s)",
                (_new_id(), connector, datatype, record_id, Jsonb(data), source, ts),
            )
        return data

    async def count(self, connector: str, datatype: str) -> int:
        pool = await self._get_pool()
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT COUNT(*) FROM sim_records "
                "WHERE connector=%s AND datatype=%s AND deleted_at IS NULL",
                (connector, datatype),
            )
            row = await cur.fetchone()
        return row[0] if row else 0

    async def recent_mutations(
        self,
        connector: str | None = None,
        datatype: str | None = None,
        limit: int = 100,
    ) -> list[MutationEvent]:
        pool = await self._get_pool()
        async with pool.connection() as conn:
            if connector and datatype:
                cur = await conn.execute(
                    "SELECT event_id, connector, datatype, operation, record_id, "
                    "       record, before, source, timestamp "
                    "FROM sim_mutations WHERE connector=%s AND datatype=%s "
                    "ORDER BY timestamp DESC LIMIT %s",
                    (connector, datatype, limit),
                )
            elif connector:
                cur = await conn.execute(
                    "SELECT event_id, connector, datatype, operation, record_id, "
                    "       record, before, source, timestamp "
                    "FROM sim_mutations WHERE connector=%s "
                    "ORDER BY timestamp DESC LIMIT %s",
                    (connector, limit),
                )
            else:
                cur = await conn.execute(
                    "SELECT event_id, connector, datatype, operation, record_id, "
                    "       record, before, source, timestamp "
                    "FROM sim_mutations ORDER BY timestamp DESC LIMIT %s",
                    (limit,),
                )
            rows = await cur.fetchall()
        return [
            MutationEvent(
                event_id=r[0],
                connector=r[1],
                datatype=r[2],
                operation=r[3],
                record_id=r[4],
                record=dict(r[5]) if r[5] is not None else None,
                before=dict(r[6]) if r[6] is not None else None,
                source=r[7],
                timestamp=r[8],
            )
            for r in rows
        ]
