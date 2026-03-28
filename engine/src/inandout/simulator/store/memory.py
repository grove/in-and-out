"""In-memory record store for the demo simulator."""

from __future__ import annotations

import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

from inandout.simulator.store import MutationEvent, StoredRecord, _new_id, _next_id, _now_iso


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


class MemoryStore:
    """Thread-safe (asyncio-safe) in-memory store.

    Records are kept as ``StoredRecord`` instances in an ordered list per
    ``(connector, datatype)`` namespace.  Lookup by ID uses a secondary dict
    index for O(1) access.
    """

    def __init__(self) -> None:
        # (connector, datatype) -> list[StoredRecord]
        self._records: dict[tuple[str, str], list[StoredRecord]] = defaultdict(list)
        # (connector, datatype) -> {record_id: index in list}
        self._index: dict[tuple[str, str], dict[str, int]] = defaultdict(dict)
        # bounded mutation log shared across all (connector, datatype) namespaces
        self._mutations: deque[MutationEvent] = deque(maxlen=500)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _key(self, connector: str, datatype: str) -> tuple[str, str]:
        return (connector, datatype)

    def _rebuild_index(self, key: tuple[str, str]) -> None:
        self._index[key] = {r.record_id: i for i, r in enumerate(self._records[key])}

    def _emit(self, event: MutationEvent) -> None:
        self._mutations.appendleft(event)

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
        key = self._key(connector, datatype)
        existing = self._index[key]
        for rec in records:
            rid = str(rec.get(pk_field, _new_id()))
            if rid in existing:
                continue  # idempotent — skip if already seeded
            # Use the cursor_field timestamp as modified_at so incremental
            # sync filters work correctly against seed data.
            ts = _ts()
            if cursor_field and cursor_field in rec:
                ts = str(rec[cursor_field])
            stored = StoredRecord(
                record_id=rid,
                data=dict(rec),
                created_at=ts,
                modified_at=ts,
                source="seed",
            )
            self._records[key].append(stored)
            existing[rid] = len(self._records[key]) - 1

    async def list_all(
        self,
        connector: str,
        datatype: str,
        *,
        cursor_field: str | None = None,
        watermark: str | None = None,
        include_deleted: bool = False,
    ) -> list[dict[str, Any]]:
        key = self._key(connector, datatype)
        records = self._records[key]
        if not include_deleted:
            records = [r for r in records if r.deleted_at is None]
        if cursor_field and watermark:
            records = [r for r in records if str(r.data.get(cursor_field, "")) > watermark]
        result = []
        for r in records:
            d = dict(r.data)
            d["__modified_at__"] = r.modified_at
            d["__created_at__"] = r.created_at
            if r.deleted_at is not None:
                d["__deleted_at__"] = r.deleted_at
            result.append(d)
        return result

    async def get_by_id(
        self,
        connector: str,
        datatype: str,
        record_id: str,
    ) -> dict[str, Any] | None:
        key = self._key(connector, datatype)
        idx = self._index[key].get(record_id)
        if idx is None:
            return None
        stored = self._records[key][idx]
        result = dict(stored.data)
        result["__modified_at__"] = stored.modified_at
        result["__created_at__"] = stored.created_at
        if stored.deleted_at is not None:
            result["__deleted_at__"] = stored.deleted_at
        return result

    async def create(
        self,
        connector: str,
        datatype: str,
        data: dict[str, Any],
        pk_field: str = "id",
        source: str = "engine",
    ) -> dict[str, Any]:
        key = self._key(connector, datatype)
        existing_ids = [r.record_id for r in self._records[key]]
        rid = str(data.get(pk_field) or _next_id(existing_ids))
        # Ensure the pk field is in the data
        data = dict(data)
        data[pk_field] = rid
        ts = _ts()
        stored = StoredRecord(
            record_id=rid,
            data=data,
            created_at=ts,
            modified_at=ts,
            source=source,
        )
        self._records[key].append(stored)
        self._index[key][rid] = len(self._records[key]) - 1
        self._emit(
            MutationEvent(
                connector=connector,
                datatype=datatype,
                operation="create",
                record_id=rid,
                record=data,
                source=source,
            )
        )
        return dict(data)

    async def update(
        self,
        connector: str,
        datatype: str,
        record_id: str,
        data: dict[str, Any],
        source: str = "engine",
    ) -> dict[str, Any] | None:
        key = self._key(connector, datatype)
        idx = self._index[key].get(record_id)
        if idx is None:
            return None
        stored = self._records[key][idx]
        before = dict(stored.data)
        merged = {**stored.data, **data}
        self._records[key][idx] = StoredRecord(
            record_id=record_id,
            data=merged,
            created_at=stored.created_at,
            modified_at=_ts(),
            source=source,
        )
        self._emit(
            MutationEvent(
                connector=connector,
                datatype=datatype,
                operation="update",
                record_id=record_id,
                record=merged,
                before=before,
                source=source,
            )
        )
        return dict(merged)

    async def delete(
        self,
        connector: str,
        datatype: str,
        record_id: str,
        source: str = "engine",
    ) -> bool:
        key = self._key(connector, datatype)
        idx = self._index[key].get(record_id)
        if idx is None:
            return False
        stored = self._records[key][idx]
        if stored.deleted_at is not None:
            return False  # already deleted
        before = dict(stored.data)
        ts = _ts()
        self._records[key][idx] = StoredRecord(
            record_id=record_id,
            data=stored.data,
            created_at=stored.created_at,
            modified_at=ts,
            source=source,
            deleted_at=ts,
        )
        self._emit(
            MutationEvent(
                connector=connector,
                datatype=datatype,
                operation="delete",
                record_id=record_id,
                record=None,
                before=before,
                source=source,
            )
        )
        return True

    async def restore(
        self,
        connector: str,
        datatype: str,
        record_id: str,
        source: str = "ui",
    ) -> dict[str, Any] | None:
        key = self._key(connector, datatype)
        idx = self._index[key].get(record_id)
        if idx is None:
            return None
        stored = self._records[key][idx]
        if stored.deleted_at is None:
            return dict(stored.data)  # not deleted, nothing to do
        ts = _ts()
        self._records[key][idx] = StoredRecord(
            record_id=record_id,
            data=stored.data,
            created_at=stored.created_at,
            modified_at=ts,
            source=source,
            deleted_at=None,
        )
        self._emit(
            MutationEvent(
                connector=connector,
                datatype=datatype,
                operation="create",  # resurrect looks like a create to the engine
                record_id=record_id,
                record=dict(stored.data),
                before=None,
                source=source,
            )
        )
        return dict(stored.data)

    async def count(self, connector: str, datatype: str) -> int:
        return sum(1 for r in self._records[self._key(connector, datatype)] if r.deleted_at is None)

    async def recent_mutations(
        self,
        connector: str | None = None,
        datatype: str | None = None,
        limit: int = 100,
    ) -> list[MutationEvent]:
        events = list(self._mutations)
        if connector is not None:
            events = [e for e in events if e.connector == connector]
        if datatype is not None:
            events = [e for e in events if e.datatype == datatype]
        return events[:limit]
