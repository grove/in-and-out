"""Add response_status, response_body, response_headers to inout_ops_writeback_result.

Revision ID: 018_20260323
Revises: 017_20260323
Create Date: 2026-03-23 00:00:00.000000

Captures the full HTTP response for every write operation so operators can
audit what each target API returned (T2 #13).
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "018_20260323"
down_revision: Union[str, None] = "017_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "ALTER TABLE inout_ops_writeback_result "
        "ADD COLUMN IF NOT EXISTS response_status INTEGER"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_writeback_result "
        "ADD COLUMN IF NOT EXISTS response_body JSONB"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_writeback_result "
        "ADD COLUMN IF NOT EXISTS response_headers JSONB"
    ))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "ALTER TABLE inout_ops_writeback_result "
        "DROP COLUMN IF EXISTS response_status"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_writeback_result "
        "DROP COLUMN IF EXISTS response_body"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_writeback_result "
        "DROP COLUMN IF EXISTS response_headers"
    ))
