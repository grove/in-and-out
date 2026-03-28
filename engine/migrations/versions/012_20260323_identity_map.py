"""Create inout_ops_identity_map table for external-id → internal-id mapping.

Revision ID: 012_20260323
Revises: 011_20260323
Create Date: 2026-03-23 00:00:00.000000

The identity map records the internal ID assigned by the target system
after a successful insert writeback, keyed by (connector, datatype, external_id).
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "012_20260323"
down_revision: Union[str, None] = "011_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS inout_ops_identity_map (
            connector       TEXT NOT NULL,
            datatype        TEXT NOT NULL,
            external_id     TEXT NOT NULL,
            internal_id     TEXT NOT NULL,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            sync_run_id     UUID REFERENCES inout_ops_sync_run(id),
            PRIMARY KEY (connector, datatype, external_id)
        )
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS inout_ops_identity_map_internal_id_idx
        ON inout_ops_identity_map (connector, datatype, internal_id)
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "DROP INDEX IF EXISTS inout_ops_identity_map_internal_id_idx"
    ))
    conn.execute(text("DROP TABLE IF EXISTS inout_ops_identity_map"))
