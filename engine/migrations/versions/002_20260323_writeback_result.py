"""Add inout_ops_writeback_result table and _deleted_at column on source tables.

Revision ID: 002_20260323
Revises: 001_20260323
Create Date: 2026-03-23 00:00:00.000000

"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "002_20260323"
down_revision: Union[str, None] = "001_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS inout_ops_writeback_result (
            id            BIGSERIAL PRIMARY KEY,
            connector     TEXT        NOT NULL,
            datatype      TEXT        NOT NULL,
            delta_table   TEXT        NOT NULL,
            action        TEXT        NOT NULL,
            external_id   TEXT,
            status        TEXT        NOT NULL DEFAULT 'ok',
            error_message TEXT,
            processed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS inout_ops_writeback_result_connector_idx
        ON inout_ops_writeback_result (connector, datatype, processed_at DESC)
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE IF EXISTS inout_ops_writeback_result"))
