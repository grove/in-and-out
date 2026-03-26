"""Record store protocol and shared types for the demo simulator."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------


@dataclass
class StoredRecord:
    """A record held in the simulator store."""

    record_id: str
    """Value of the primary-key field, cast to string."""

    data: dict[str, Any]
    """The application-level record (what the engine sees)."""

    created_at: str
    """ISO-8601 UTC timestamp."""

    modified_at: str
    """ISO-8601 UTC timestamp; updated on every write."""

    source: str = "seed"
    """Who last wrote this record: 'seed' | 'engine' | 'ui'."""

    deleted_at: str | None = None
    """Set when the record is soft-deleted; None while alive."""


@dataclass
class MutationEvent:
    """An event emitted when a record is created, updated, or deleted."""

    event_id: str = field(default_factory=_new_id)
    connector: str = ""
    datatype: str = ""
    operation: str = ""  # "create" | "update" | "delete"
    record_id: str = ""
    record: dict[str, Any] | None = None  # None for delete; the AFTER state
    before: dict[str, Any] | None = None  # the BEFORE state for update/delete
    source: str = "engine"  # "engine" | "ui"
    timestamp: str = field(default_factory=_now_iso)


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class RecordStore(Protocol):
    """Minimal async interface every store backend must satisfy."""

    async def seed(
        self,
        connector: str,
        datatype: str,
        records: list[dict[str, Any]],
        pk_field: str = "id",
        cursor_field: str | None = None,
    ) -> None:
        """Bulk-load records at startup (idempotent — skips existing IDs)."""
        ...

    async def list_all(
        self,
        connector: str,
        datatype: str,
        *,
        cursor_field: str | None = None,
        watermark: str | None = None,
        include_deleted: bool = False,
    ) -> list[dict[str, Any]]:
        """Return all records, optionally filtered by cursor_field > watermark.

        When *include_deleted* is True, soft-deleted records are included and
        their data dict will contain a ``__deleted_at__`` key.
        """
        ...

    async def get_by_id(
        self,
        connector: str,
        datatype: str,
        record_id: str,
    ) -> dict[str, Any] | None:
        """Return a single record by its string primary-key value, or None."""
        ...

    async def create(
        self,
        connector: str,
        datatype: str,
        data: dict[str, Any],
        pk_field: str = "id",
        source: str = "engine",
    ) -> dict[str, Any]:
        """Persist a new record, auto-generating its ID if absent."""
        ...

    async def update(
        self,
        connector: str,
        datatype: str,
        record_id: str,
        data: dict[str, Any],
        source: str = "engine",
    ) -> dict[str, Any] | None:
        """Merge *data* into an existing record.  Returns updated record or None."""
        ...

    async def delete(
        self,
        connector: str,
        datatype: str,
        record_id: str,
        source: str = "engine",
    ) -> bool:
        """Soft-delete a record.  Returns True if it existed and was not already deleted."""
        ...

    async def restore(
        self,
        connector: str,
        datatype: str,
        record_id: str,
        source: str = "ui",
    ) -> dict[str, Any] | None:
        """Un-delete a soft-deleted record.  Returns the record or None if not found."""
        ...

    async def count(self, connector: str, datatype: str) -> int:
        """Return the number of records for this (connector, datatype) pair."""
        ...

    async def recent_mutations(
        self,
        connector: str | None = None,
        datatype: str | None = None,
        limit: int = 100,
    ) -> list[MutationEvent]:
        """Return recent mutation events, newest first."""
        ...
