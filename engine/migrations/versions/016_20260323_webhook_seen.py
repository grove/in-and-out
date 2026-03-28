"""Create inout_ops_webhook_seen table for event deduplication.

Revision ID: 016_20260323
Revises: 015_20260323
Create Date: 2026-03-23 00:00:00.000000

Tracks processed webhook event IDs to prevent duplicate processing (T1 #25).
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "016_20260323"
down_revision: Union[str, None] = "015_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS inout_ops_webhook_seen (
            connector       TEXT NOT NULL,
            event_id        TEXT NOT NULL,
            received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (connector, event_id)
        )
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS inout_ops_webhook_seen_received_at
            ON inout_ops_webhook_seen (received_at)
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP INDEX IF EXISTS inout_ops_webhook_seen_received_at"))
    conn.execute(text("DROP TABLE IF EXISTS inout_ops_webhook_seen"))
