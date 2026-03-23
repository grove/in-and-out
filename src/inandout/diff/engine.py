"""Sync run diffing — compare records between two sync runs."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class SyncRunDiff:
    """Result of comparing two sync runs for a connector/datatype."""

    run_a: str
    run_b: str
    added: list[str] = field(default_factory=list)
    """external_ids present in run_b but not run_a."""
    removed: list[str] = field(default_factory=list)
    """external_ids present in run_a but not run_b."""
    changed: list[dict] = field(default_factory=list)
    """Records present in both runs but with different raw data.
    Each entry: {"external_id": ..., "fields_changed": [...], "before": {...}, "after": {...}}
    """
    unchanged_count: int = 0
    """Number of records with identical raw data in both runs."""


def compute_field_diff(before: dict[str, Any], after: dict[str, Any]) -> list[str]:
    """Return a list of field names that differ between *before* and *after*.

    Covers: fields added in *after*, fields removed from *after*, and
    fields whose value changed.
    """
    changed: list[str] = []
    all_keys = set(before.keys()) | set(after.keys())
    for key in sorted(all_keys):
        if key not in before:
            changed.append(key)  # added
        elif key not in after:
            changed.append(key)  # removed
        elif before[key] != after[key]:
            changed.append(key)  # changed
    return changed


async def diff_sync_runs(
    pool: Any,
    connector: str,
    datatype: str,
    run_a_id: str,
    run_b_id: str,
) -> SyncRunDiff:
    """Compare records touched in two sync runs.

    Queries the history table ``inout_src_{connector}_{datatype}_history``
    for rows from each run and produces a SyncRunDiff.

    Parameters
    ----------
    pool:
        AsyncConnectionPool.
    connector:
        Connector name.
    datatype:
        Datatype name.
    run_a_id:
        UUID string for the first (older) run.
    run_b_id:
        UUID string for the second (newer) run.
    """
    from inandout.postgres.schema import source_history_table_name

    hist_table = source_history_table_name(connector, datatype)
    diff = SyncRunDiff(run_a=run_a_id, run_b=run_b_id)

    import orjson

    async with pool.connection() as conn:
        # Fetch all records touched in run_a
        rows_a_raw = await (await conn.execute(
            f"""
            SELECT external_id, raw
            FROM {hist_table}
            WHERE _sync_run_id = %s::uuid
            """,
            [run_a_id],
        )).fetchall()

        rows_b_raw = await (await conn.execute(
            f"""
            SELECT external_id, raw
            FROM {hist_table}
            WHERE _sync_run_id = %s::uuid
            """,
            [run_b_id],
        )).fetchall()

    # Build dicts: external_id → raw JSONB
    def _to_dict(raw_val: Any) -> dict[str, Any]:
        if raw_val is None:
            return {}
        if isinstance(raw_val, dict):
            return raw_val
        if isinstance(raw_val, (str, bytes)):
            return orjson.loads(raw_val)
        return {}

    map_a: dict[str, dict[str, Any]] = {str(r[0]): _to_dict(r[1]) for r in rows_a_raw}
    map_b: dict[str, dict[str, Any]] = {str(r[0]): _to_dict(r[1]) for r in rows_b_raw}

    ids_a = set(map_a.keys())
    ids_b = set(map_b.keys())

    # Records only in run_b → added
    diff.added = sorted(ids_b - ids_a)

    # Records only in run_a → removed
    diff.removed = sorted(ids_a - ids_b)

    # Records in both → compare raw
    for ext_id in sorted(ids_a & ids_b):
        before = map_a[ext_id]
        after = map_b[ext_id]
        if before == after:
            diff.unchanged_count += 1
        else:
            fields_changed = compute_field_diff(before, after)
            diff.changed.append({
                "external_id": ext_id,
                "fields_changed": fields_changed,
                "before": before,
                "after": after,
            })

    logger.info(
        "diff_sync_runs_complete",
        connector=connector,
        datatype=datatype,
        run_a=run_a_id,
        run_b=run_b_id,
        added=len(diff.added),
        removed=len(diff.removed),
        changed=len(diff.changed),
        unchanged=diff.unchanged_count,
    )
    return diff
