"""Create inout_ops_webhook_route_seq for per-route fan-out event ordering.

Revision ID: 019_20260323
Revises: 018_20260323
Create Date: 2026-03-23 00:00:00.000000

Tracks the last-seen sequence value per (connector, datatype, route, external_id)
so that fan-out webhook routes delivering events for the same external_id can be
ordered independently.  Without this, out-of-order events from different routes
could corrupt the source table with stale data (T1 #35).
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "019_20260323"
down_revision: Union[str, None] = "018_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS inout_ops_webhook_route_seq (
            connector       TEXT NOT NULL,
            datatype        TEXT NOT NULL,
            route           TEXT NOT NULL,
            external_id     TEXT NOT NULL,
            last_seq        TEXT NOT NULL,
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (connector, datatype, route, external_id)
        )
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS inout_ops_webhook_route_seq_updated_at
            ON inout_ops_webhook_route_seq (updated_at)
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "DROP INDEX IF EXISTS inout_ops_webhook_route_seq_updated_at"
    ))
    conn.execute(text(
        "DROP TABLE IF EXISTS inout_ops_webhook_route_seq"
    ))
