"""MDM Contract schema fixes: desired-state columns, lwstate etag, control target_tool, sync_run.

Revision ID: 020_20260323
Revises: 019_20260323
Create Date: 2026-03-23 00:00:00.000000

Fixes several gaps between the implementation and the MDM Contract specification:

1. inout_ops_control: add target_tool column so ingestion and writeback can each
   filter for their own commands without picking up each other's.

2. inout_ops_sync_run: add error_detail JSONB (structured errors alongside the
   legacy error_message TEXT); add 'aborted' to the valid_status check constraint
   so graceful drain termination is recorded accurately.

3. Note: per-datatype desired-state tables (inout_dst_*) and lwstate tables
   (inout_dst_*_lwstate) are created dynamically at runtime when a connector is
   loaded, so DDL changes to those tables are handled via ALTER TABLE ADD COLUMN
   IF NOT EXISTS in ensure_desired_state_table() / ensure_lwstate_table() rather
   than in Alembic migrations.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "020_20260323"
down_revision: Union[str, None] = "019_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()

    # 1. inout_ops_control: add target_tool
    conn.execute(text(
        "ALTER TABLE inout_ops_control "
        "ADD COLUMN IF NOT EXISTS target_tool TEXT"
    ))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS inout_ops_control_target_tool_status "
        "ON inout_ops_control (target_tool, status) WHERE status = 'pending'"
    ))

    # 2. inout_ops_sync_run: structured error detail + aborted status
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run "
        "ADD COLUMN IF NOT EXISTS error_detail JSONB"
    ))
    # Drop and recreate the status CHECK to include 'aborted'
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run "
        "DROP CONSTRAINT IF EXISTS valid_status"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run "
        "ADD CONSTRAINT valid_status "
        "CHECK (status IN ('running', 'completed', 'failed', 'skipped', 'aborted'))"
    ))


def downgrade() -> None:
    conn = op.get_bind()

    conn.execute(text(
        "DROP INDEX IF EXISTS inout_ops_control_target_tool_status"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_control DROP COLUMN IF EXISTS target_tool"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run DROP COLUMN IF EXISTS error_detail"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run DROP CONSTRAINT IF EXISTS valid_status"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run "
        "ADD CONSTRAINT valid_status "
        "CHECK (status IN ('running', 'completed', 'failed', 'skipped'))"
    ))
