"""Add watermark and writeback tracking columns to inout_ops_sync_run.

Revision ID: 009_20260323
Revises: 008_20260323
Create Date: 2026-03-23 00:00:00.000000

Adds:
  high_water_mark_before TEXT  — watermark value at start of sync run
  high_water_mark_after  TEXT  — watermark value at end of sync run
  records_written        INT   — records successfully written back
  records_skipped        INT   — records skipped (no-op / filtered)
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "009_20260323"
down_revision: Union[str, None] = "008_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run "
        "ADD COLUMN IF NOT EXISTS high_water_mark_before TEXT"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run "
        "ADD COLUMN IF NOT EXISTS high_water_mark_after TEXT"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run "
        "ADD COLUMN IF NOT EXISTS records_written INT NOT NULL DEFAULT 0"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run "
        "ADD COLUMN IF NOT EXISTS records_skipped INT NOT NULL DEFAULT 0"
    ))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run DROP COLUMN IF EXISTS high_water_mark_before"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run DROP COLUMN IF EXISTS high_water_mark_after"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run DROP COLUMN IF EXISTS records_written"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run DROP COLUMN IF EXISTS records_skipped"
    ))
