"""Ensure inout_ops_control has target_tool and issued_by columns.

Revision ID: 011_20260323
Revises: 010_20260323
Create Date: 2026-03-23 00:00:00.000000

The initial DDL already includes issued_by. This migration is a
belt-and-suspenders guard, and also adds target_tool which identifies
which tool instance (ingestion/writeback) should process the command.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "011_20260323"
down_revision: Union[str, None] = "010_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "ALTER TABLE inout_ops_control "
        "ADD COLUMN IF NOT EXISTS target_tool TEXT"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_control "
        "ADD COLUMN IF NOT EXISTS issued_by TEXT"
    ))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "ALTER TABLE inout_ops_control DROP COLUMN IF EXISTS target_tool"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_control DROP COLUMN IF EXISTS issued_by"
    ))
