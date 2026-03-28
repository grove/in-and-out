"""Ensure inout_ops_watermark has watermark_type and updated_by_run_id.

Revision ID: 010_20260323
Revises: 009_20260323
Create Date: 2026-03-23 00:00:00.000000

The initial DDL already includes these columns. This migration is a
belt-and-suspenders guard for deployments that ran before the columns
were added to the schema definition.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "010_20260323"
down_revision: Union[str, None] = "009_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "ALTER TABLE inout_ops_watermark "
        "ADD COLUMN IF NOT EXISTS watermark_type TEXT NOT NULL DEFAULT 'cursor'"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_watermark "
        "ADD COLUMN IF NOT EXISTS updated_by_run_id UUID "
        "REFERENCES inout_ops_sync_run(id)"
    ))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "ALTER TABLE inout_ops_watermark DROP COLUMN IF EXISTS watermark_type"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_watermark DROP COLUMN IF EXISTS updated_by_run_id"
    ))
