"""Add records_fetched and records_errored columns to inout_ops_sync_run.

Revision ID: 013_20260323
Revises: 012_20260323
Create Date: 2026-03-23 00:00:00.000000

Adds:
  records_fetched INTEGER NOT NULL DEFAULT 0  — total records received from the API across all pages
  records_errored INTEGER NOT NULL DEFAULT 0  — records that failed upsert (dead-letter or quality)

Note: records_fetched already existed in the CREATE TABLE DDL (schema.py) but may be missing
on older databases. This migration ensures both columns exist unconditionally.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "013_20260323"
down_revision: Union[str, None] = "012_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run "
        "ADD COLUMN IF NOT EXISTS records_fetched INTEGER NOT NULL DEFAULT 0"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run "
        "ADD COLUMN IF NOT EXISTS records_errored INTEGER NOT NULL DEFAULT 0"
    ))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run DROP COLUMN IF EXISTS records_fetched"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_sync_run DROP COLUMN IF EXISTS records_errored"
    ))
