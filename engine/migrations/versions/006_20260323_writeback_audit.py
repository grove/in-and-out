"""Add payload_snapshot and field_diff columns to inout_ops_writeback_result.

Revision ID: 006_20260323
Revises: 005_20260323
Create Date: 2026-03-23 00:00:00.000000

"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "006_20260323"
down_revision: Union[str, None] = "005_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        ALTER TABLE inout_ops_writeback_result
        ADD COLUMN IF NOT EXISTS payload_snapshot JSONB
    """))
    conn.execute(text("""
        ALTER TABLE inout_ops_writeback_result
        ADD COLUMN IF NOT EXISTS field_diff JSONB
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        ALTER TABLE inout_ops_writeback_result
        DROP COLUMN IF EXISTS payload_snapshot
    """))
    conn.execute(text("""
        ALTER TABLE inout_ops_writeback_result
        DROP COLUMN IF EXISTS field_diff
    """))
