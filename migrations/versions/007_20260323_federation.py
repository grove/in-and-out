"""Add inout_ops_federation table for multi-instance coordination.

Revision ID: 007_20260323
Revises: 006_20260323
Create Date: 2026-03-23 00:00:00.000000

"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "007_20260323"
down_revision: Union[str, None] = "006_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS inout_ops_federation (
            instance_id           TEXT NOT NULL,
            namespace             TEXT NOT NULL DEFAULT 'public',
            connector             TEXT NOT NULL,
            datatype              TEXT NOT NULL,
            health_score          FLOAT,
            last_sync_at          TIMESTAMPTZ,
            circuit_breaker_state TEXT,
            dead_letter_depth     INT DEFAULT 0,
            reported_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (instance_id, connector, datatype)
        )
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS inout_ops_federation_reported_at_idx
        ON inout_ops_federation (reported_at)
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP INDEX IF EXISTS inout_ops_federation_reported_at_idx"))
    conn.execute(text("DROP TABLE IF EXISTS inout_ops_federation"))
