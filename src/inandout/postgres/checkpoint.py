"""Intra-sync checkpoint helpers.

Provides save/load/clear operations for inout_ops_sync_checkpoint.
Used by IngestionEngine to enable crash-safe sync resumption.
"""
from __future__ import annotations

import uuid
from typing import Any


async def save_checkpoint(
    pool: Any,
    run_id: uuid.UUID,
    connector: str,
    datatype: str,
    page_number: int,
    cursor_value: str | None,
    records_committed: int,
) -> None:
    """Upsert a checkpoint row for the given run_id."""
    async with pool.connection() as conn:
        try:
            await conn.execute(
                """
                INSERT INTO inout_ops_sync_checkpoint
                    (run_id, connector, datatype, page_number, cursor_value,
                     records_committed, checkpointed_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (run_id) DO UPDATE SET
                    page_number       = EXCLUDED.page_number,
                    cursor_value      = EXCLUDED.cursor_value,
                    records_committed = EXCLUDED.records_committed,
                    checkpointed_at   = NOW()
                """,
                [
                    str(run_id), connector, datatype, page_number,
                    cursor_value, records_committed,
                ],
            )
            await conn.commit()
        except Exception:
            pass  # Checkpoint failure must never block sync


async def load_checkpoint(
    pool: Any,
    run_id: uuid.UUID,
) -> dict | None:
    """Return the checkpoint row for run_id as a dict, or None if not found."""
    async with pool.connection() as conn:
        try:
            row = await (await conn.execute(
                """
                SELECT run_id, connector, datatype, page_number, cursor_value,
                       records_committed, checkpointed_at
                FROM inout_ops_sync_checkpoint
                WHERE run_id = %s
                """,
                [str(run_id)],
            )).fetchone()
        except Exception:
            return None

    if row is None:
        return None

    return {
        "run_id": str(row[0]),
        "connector": row[1],
        "datatype": row[2],
        "page_number": row[3],
        "cursor_value": row[4],
        "records_committed": row[5],
        "checkpointed_at": row[6],
    }


async def clear_checkpoint(
    pool: Any,
    run_id: uuid.UUID,
) -> None:
    """Delete the checkpoint row for run_id (called after successful sync completion)."""
    async with pool.connection() as conn:
        try:
            await conn.execute(
                "DELETE FROM inout_ops_sync_checkpoint WHERE run_id = %s",
                [str(run_id)],
            )
            await conn.commit()
        except Exception:
            pass  # Non-critical
