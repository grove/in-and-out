"""Initial schema: operational tables for inandout.

Revision ID: 001_20260323
Revises:
Create Date: 2026-03-23 00:00:00.000000

"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "001_20260323"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    from inandout.postgres.schema import OPERATIONAL_TABLES_DDL

    conn = op.get_bind()
    conn.execute(text(OPERATIONAL_TABLES_DDL))


def downgrade() -> None:
    conn = op.get_bind()
    # Drop in reverse dependency order (watermark references sync_run)
    conn.execute(text("DROP TABLE IF EXISTS inout_ops_control"))
    conn.execute(text("DROP TABLE IF EXISTS inout_ops_watermark"))
    conn.execute(text("DROP TABLE IF EXISTS inout_ops_sync_run"))
