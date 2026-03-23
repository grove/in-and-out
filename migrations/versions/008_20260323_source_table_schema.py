"""Ensure source tables have data, _schema_version, _source_version columns.

Revision ID: 008_20260323
Revises: 007_20260323
Create Date: 2026-03-23 00:00:00.000000

These columns are already in the DDL (source_table_ddl) so new tables will
have them automatically. This migration adds them to any tables that were
created before these columns existed.

Since source tables are per-connector/datatype and named dynamically, this
migration only handles the known operational columns — new source tables are
handled by ensure_source_table() at runtime.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "008_20260323"
down_revision: Union[str, None] = "007_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    # Discover all inout_src_* tables and add missing columns
    rows = conn.execute(text(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name LIKE 'inout_src_%' "
        "AND table_name NOT LIKE '%_history'"
    )).fetchall()
    for (table_name,) in rows:
        # data column (JSONB NOT NULL DEFAULT '{}')
        conn.execute(text(
            f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "
            "data JSONB NOT NULL DEFAULT '{}'"
        ))
        # _schema_version column
        conn.execute(text(
            f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "
            "_schema_version INTEGER NOT NULL DEFAULT 1"
        ))
        # _source_version column
        conn.execute(text(
            f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "
            "_source_version TEXT"
        ))
        # _lineage column (also added by step 86 ensure_source_table, belt+suspenders)
        conn.execute(text(
            f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "
            "_lineage JSONB"
        ))


def downgrade() -> None:
    # These columns are additive and safe to leave; downgrade is a no-op
    pass
