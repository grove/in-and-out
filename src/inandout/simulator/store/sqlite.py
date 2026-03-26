"""SQLite-backed persistent store for the demo simulator."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from inandout.simulator.store import MutationEvent, StoredRecord, _new_id, _now_iso


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


_DDL = """
CREATE TABLE IF NOT EXISTS sim_records (
    connector   TEXT NOT NULL,
    datatype    TEXT NOT NULL,
    record_id   TEXT NOT NULL,
    data        TEXT NOT NULL,
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
    record      TEXT,
    before      TEXT,
    source      TEXT NOT NULL,
    timestamp   TEXT NOT NULL
);
"""


class SQLiteStore:
    """SQLite-backed store.  Uses ``asyncio.to_thread`` for all I/O so the
    event loop is never blocked.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = str(path)
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _open(self) -> sqlite3.Connection:
        if self._conn is None:
            conn = sqlite3.connect(self._path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.executescript(_DDL)
            conn.commit()
            self._conn = conn
        return self._conn

    def _sync(self, fn, *args):
        with self._lock:
            return fn(self._open(), *args)

    async def _run(self, fn, *args):
        return await asyncio.to_thread(self._sync, fn, *args)

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
        def _do(conn, connector, datatype, records, pk_field, cursor_field):
            for rec in records:
                rid = str(rec.get(pk_field, _new_id()))
                ts = _ts()
                if cursor_field and cursor_field in rec:
                    ts = str(rec[cursor_field])
                conn.execute(
                    "INSERT OR IGNORE INTO sim_records "
                    "(connector, datatype, record_id, data, created_at, modified_at, source) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (connector, datatype, rid, json.dumps(rec), ts, ts, "seed"),
                )
            conn.commit()

        await self._run(_do, connector, datatype, records, pk_field, cursor_field)

    async def list_all(
        self,
        connector: str,
        datatype: str,
        *,
        cursor_field: str | None = None,
        watermark: str | None = None,
        include_deleted: bool = False,
    ) -> list[dict[str, Any]]:
        def _do(conn, connector, datatype, cursor_field, watermark, include_deleted):
            if include_deleted:
                rows = conn.execute(
                    "SELECT data, deleted_at FROM sim_records WHERE connector=? AND datatype=? ORDER BY modified_at ASC",
                    (connector, datatype),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT data, deleted_at FROM sim_records WHERE connector=? AND datatype=? AND deleted_at IS NULL ORDER BY modified_at ASC",
                    (connector, datatype),
                ).fetchall()
            result = []
            for r in rows:
                d = json.loads(r["data"])
                if r["deleted_at"] is not None:
                    d["__deleted_at__"] = r["deleted_at"]
                result.append(d)
            if cursor_field and watermark:
                result = [r for r in result if str(r.get(cursor_field, "")) > watermark]
            return result

        return await self._run(_do, connector, datatype, cursor_field, watermark, include_deleted)

    async def get_by_id(
        self,
        connector: str,
        datatype: str,
        record_id: str,
    ) -> dict[str, Any] | None:
        def _do(conn, connector, datatype, record_id):
            row = conn.execute(
                "SELECT data, deleted_at FROM sim_records WHERE connector=? AND datatype=? AND record_id=?",
                (connector, datatype, record_id),
            ).fetchone()
            if row is None:
                return None
            result = json.loads(row["data"])
            if row["deleted_at"] is not None:
                result["__deleted_at__"] = row["deleted_at"]
            return result

        return await self._run(_do, connector, datatype, record_id)

    async def create(
        self,
        connector: str,
        datatype: str,
        data: dict[str, Any],
        pk_field: str = "id",
        source: str = "engine",
    ) -> dict[str, Any]:
        def _do(conn, connector, datatype, data, pk_field, source):
            rid = str(data.get(pk_field) or _new_id())
            data = dict(data)
            data[pk_field] = rid
            ts = _ts()
            conn.execute(
                "INSERT OR REPLACE INTO sim_records "
                "(connector, datatype, record_id, data, created_at, modified_at, source) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (connector, datatype, rid, json.dumps(data), ts, ts, source),
            )
            event_id = _new_id()
            conn.execute(
                "INSERT INTO sim_mutations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (event_id, connector, datatype, "create", rid, json.dumps(data), None, source, ts),
            )
            conn.commit()
            return data

        return await self._run(_do, connector, datatype, data, pk_field, source)

    async def update(
        self,
        connector: str,
        datatype: str,
        record_id: str,
        data: dict[str, Any],
        source: str = "engine",
    ) -> dict[str, Any] | None:
        def _do(conn, connector, datatype, record_id, data, source):
            row = conn.execute(
                "SELECT data, created_at FROM sim_records WHERE connector=? AND datatype=? AND record_id=?",
                (connector, datatype, record_id),
            ).fetchone()
            if row is None:
                return None
            existing = json.loads(row["data"])
            merged = {**existing, **data}
            ts = _ts()
            conn.execute(
                "UPDATE sim_records SET data=?, modified_at=?, source=? "
                "WHERE connector=? AND datatype=? AND record_id=?",
                (json.dumps(merged), ts, source, connector, datatype, record_id),
            )
            conn.execute(
                "INSERT INTO sim_mutations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    _new_id(),
                    connector,
                    datatype,
                    "update",
                    record_id,
                    json.dumps(merged),
                    json.dumps(existing),
                    source,
                    ts,
                ),
            )
            conn.commit()
            return merged

        return await self._run(_do, connector, datatype, record_id, data, source)

    async def delete(
        self,
        connector: str,
        datatype: str,
        record_id: str,
        source: str = "engine",
    ) -> bool:
        def _do(conn, connector, datatype, record_id, source):
            row = conn.execute(
                "SELECT data FROM sim_records WHERE connector=? AND datatype=? AND record_id=?",
                (connector, datatype, record_id),
            ).fetchone()
            before_json = row["data"] if row else None
            cur = conn.execute(
                "DELETE FROM sim_records WHERE connector=? AND datatype=? AND record_id=?",
                (connector, datatype, record_id),
            )
            if cur.rowcount == 0:
                conn.rollback()
                return False
            ts = _ts()
            conn.execute(
                "INSERT INTO sim_mutations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    _new_id(),
                    connector,
                    datatype,
                    "delete",
                    record_id,
                    None,
                    before_json,
                    source,
                    ts,
                ),
            )
            conn.commit()
            return True

        return await self._run(_do, connector, datatype, record_id, source)

    async def count(self, connector: str, datatype: str) -> int:
        def _do(conn, connector, datatype):
            row = conn.execute(
                "SELECT COUNT(*) FROM sim_records WHERE connector=? AND datatype=? AND deleted_at IS NULL",
                (connector, datatype),
            ).fetchone()
            return row[0]

        return await self._run(_do, connector, datatype)

    async def recent_mutations(
        self,
        connector: str | None = None,
        datatype: str | None = None,
        limit: int = 100,
    ) -> list[MutationEvent]:
        def _do(conn, connector, datatype, limit):
            if connector and datatype:
                rows = conn.execute(
                    "SELECT * FROM sim_mutations WHERE connector=? AND datatype=? ORDER BY timestamp DESC LIMIT ?",
                    (connector, datatype, limit),
                ).fetchall()
            elif connector:
                rows = conn.execute(
                    "SELECT * FROM sim_mutations WHERE connector=? ORDER BY timestamp DESC LIMIT ?",
                    (connector, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM sim_mutations ORDER BY timestamp DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [
                MutationEvent(
                    event_id=r["event_id"],
                    connector=r["connector"],
                    datatype=r["datatype"],
                    operation=r["operation"],
                    record_id=r["record_id"],
                    record=json.loads(r["record"]) if r["record"] else None,
                    before=json.loads(r["before"]) if r["before"] else None,
                    source=r["source"],
                    timestamp=r["timestamp"],
                )
                for r in rows
            ]

        return await self._run(_do, connector, datatype, limit)
