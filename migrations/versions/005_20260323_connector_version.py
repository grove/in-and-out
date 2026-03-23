"""Add inout_ops_connector_version table for connector versioning.

Revision ID: 005_20260323
Revises: 004_20260323
Create Date: 2026-03-23 00:00:00.000000

"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "005_20260323"
down_revision: Union[str, None] = "004_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS inout_ops_connector_version (
            connector        TEXT PRIMARY KEY,
            deployed_version TEXT NOT NULL,
            updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE IF EXISTS inout_ops_connector_version"))
