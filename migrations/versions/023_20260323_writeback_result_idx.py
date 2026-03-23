"""Add processed_at index on inout_ops_writeback_result for housekeeping efficiency.

Revision ID: 023_20260323
Revises: 022_20260323
Create Date: 2026-03-23 00:00:00.000000

The housekeeping DELETE against inout_ops_writeback_result filters purely on
``processed_at``:

    DELETE FROM inout_ops_writeback_result
    WHERE processed_at < NOW() - INTERVAL '<retention>'
      AND processed_at < NOW() - INTERVAL '1 day'

Without a plain B-tree index on ``processed_at`` Postgres must perform a
sequential scan of the entire table every time housekeeping runs.  On tables
with millions of audit rows this can take seconds and lock rows unnecessarily.

Adding a dedicated ``(processed_at)`` index lets Postgres use an index range
scan and delete only the qualifying rows with minimal I/O.

The existing partial unique index
``uix_writeback_result_run_id (connector, datatype, run_id, external_id, action)``
is unaffected.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "023_20260323"
down_revision: Union[str, None] = "022_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA_VERSION = 23


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_writeback_result_processed_at
            ON inout_ops_writeback_result (processed_at)
    """))
    conn.execute(text(f"UPDATE inout_ops_meta SET schema_version = {SCHEMA_VERSION}"))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        DROP INDEX IF EXISTS idx_writeback_result_processed_at
    """))
    conn.execute(text(f"UPDATE inout_ops_meta SET schema_version = {SCHEMA_VERSION - 1}"))
