"""Add run_id to inout_ops_writeback_result for per-cycle deduplication.

Revision ID: 022_20260323
Revises: 021_20260323
Create Date: 2026-03-23 00:00:00.000000

Within a single writeback cycle, the engine writes one audit row per
(connector, datatype, external_id, action).  Without a stable key, two
concurrent calls to _write_feedback (e.g. after a crash mid-cycle) could
insert duplicate rows, confusing the crash-recovery deduplication logic.

Adding a `run_id UUID` column—populated once per run_writeback_cycle() call
and shared across all rows in that batch—lets us define a partial unique
index on (connector, datatype, run_id, external_id, action) WHERE run_id IS
NOT NULL.  The ON CONFLICT ... DO NOTHING in _write_feedback then has a
concrete conflict target and will silently ignore duplicate insertions.

Rows written by older engine versions (before this migration) retain
run_id = NULL and are unaffected by the partial index.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "022_20260323"
down_revision: Union[str, None] = "021_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        ALTER TABLE inout_ops_writeback_result
            ADD COLUMN IF NOT EXISTS run_id UUID
    """))
    conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS uix_writeback_result_run_id
            ON inout_ops_writeback_result (connector, datatype, run_id, external_id, action)
            WHERE run_id IS NOT NULL
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "DROP INDEX IF EXISTS uix_writeback_result_run_id"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_writeback_result DROP COLUMN IF EXISTS run_id"
    ))
