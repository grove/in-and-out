"""Add inout_ops_webhook_log table for webhook audit trail.

Revision ID: 003_20260323
Revises: 002_20260323
Create Date: 2026-03-23 00:00:00.000000

"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "003_20260323"
down_revision: Union[str, None] = "002_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS inout_ops_webhook_log (
            id           BIGSERIAL PRIMARY KEY,
            connector    TEXT NOT NULL,
            datatype     TEXT,
            external_id  TEXT,
            received_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            payload_hash TEXT,
            action       TEXT,
            status       TEXT NOT NULL DEFAULT 'processed'
        )
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS inout_ops_webhook_log_connector_idx
        ON inout_ops_webhook_log (connector, datatype, received_at DESC)
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE IF EXISTS inout_ops_webhook_log"))
