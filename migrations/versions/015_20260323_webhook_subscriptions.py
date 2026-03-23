"""Create inout_ops_webhook_subscriptions table for lifecycle management.

Revision ID: 015_20260323
Revises: 014_20260323
Create Date: 2026-03-23 00:00:00.000000

Tracks webhook subscriptions registered by the ingestion daemon so that
lifecycle operations (renew, deregister, health-check) can be scoped to
subscriptions we own (T1 #7, T1 #26).
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "015_20260323"
down_revision: Union[str, None] = "014_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS inout_ops_webhook_subscriptions (
            id                  BIGSERIAL PRIMARY KEY,
            connector           TEXT NOT NULL,
            datatype            TEXT,
            webhook_id          TEXT NOT NULL,
            callback_url        TEXT NOT NULL,
            registered_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_renewed_at     TIMESTAMPTZ,
            last_health_check_at TIMESTAMPTZ,
            status              TEXT NOT NULL DEFAULT 'active',
            UNIQUE (connector, datatype, webhook_id)
        )
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("DROP TABLE IF EXISTS inout_ops_webhook_subscriptions"))
