"""Create inout_ops_sync_checkpoint table for intra-sync checkpointing.

Revision ID: 014_20260323
Revises: 013_20260323
Create Date: 2026-03-23 00:00:00.000000

Provides crash-safe resume capability for long-running syncs.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "014_20260323"
down_revision: Union[str, None] = "013_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS inout_ops_sync_checkpoint (
            run_id              UUID NOT NULL REFERENCES inout_ops_sync_run(id) ON DELETE CASCADE,
            connector           TEXT NOT NULL,
            datatype            TEXT NOT NULL,
            page_number         INTEGER NOT NULL DEFAULT 0,
            cursor_value        TEXT,
            records_committed   INTEGER NOT NULL DEFAULT 0,
            checkpointed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (run_id)
        )
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE IF EXISTS inout_ops_sync_checkpoint"))
