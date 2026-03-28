"""Add locked_until / locked_by columns to inout_ops_sync_lock.

Revision ID: 019_20260323
Revises: 018_20260323
Create Date: 2026-03-23 00:00:00.000000

Enables stale-lock expiry: a lock row whose locked_until timestamp is in
the past can be forcibly released, removing the need for an operator to
manually clear orphaned rows when a worker crashes without yielding the lock.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

revision: str = "024_20260323"
down_revision: Union[str, None] = "023_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        ALTER TABLE inout_ops_sync_lock
            ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS locked_by    TEXT NOT NULL DEFAULT ''
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS inout_ops_sync_lock_locked_until
            ON inout_ops_sync_lock (locked_until)
            WHERE locked_until IS NOT NULL
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP INDEX IF EXISTS inout_ops_sync_lock_locked_until"))
    conn.execute(text("""
        ALTER TABLE inout_ops_sync_lock
            DROP COLUMN IF EXISTS locked_until,
            DROP COLUMN IF EXISTS locked_by
    """))
