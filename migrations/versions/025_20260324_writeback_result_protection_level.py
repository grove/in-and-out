"""Add protection_level column to inout_ops_writeback_result.

Revision ID: 025_20260324
Revises: 024_20260323
Create Date: 2026-03-24 00:00:00.000000

Stores the protection_level value that was active for the row at the time
it was written back, enabling post-hoc auditing of which rows were treated
as PII/sensitive without re-joining to the connector config.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

revision: str = "025_20260324"
down_revision: Union[str, None] = "024_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        ALTER TABLE inout_ops_writeback_result
            ADD COLUMN IF NOT EXISTS protection_level TEXT
    """))
    # schema version: 25
    conn.execute(text("UPDATE inout_ops_meta SET value = '25' WHERE key = 'schema_version'"))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        ALTER TABLE inout_ops_writeback_result
            DROP COLUMN IF EXISTS protection_level
    """))
