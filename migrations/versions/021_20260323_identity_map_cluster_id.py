"""Add cluster_id column to inout_ops_identity_map to align with MDM Contract.

Revision ID: 021_20260323
Revises: 020_20260323
Create Date: 2026-03-23 00:00:00.000000

The MDM Contract specifies the identity map as:
  (cluster_id, connector, datatype) → external_id   [the target system's assigned ID]

The existing table was structured as (connector, datatype, external_id) → internal_id
where external_id held the cluster_id and internal_id held the target-assigned ID.
The column names were inverted relative to the spec.

This migration:
1. Adds cluster_id TEXT (populated from the existing external_id column, which held
   MDM cluster_id values — the naming was the problem, not the data).
2. Adds a UNIQUE constraint on (cluster_id, connector, datatype) as the spec-compliant
   primary lookup direction.
3. The existing PRIMARY KEY (connector, datatype, external_id) becomes the secondary
   index for reverse lookups: given a connector+datatype+target-external-id, find the
   cluster_id.

No data loss — all existing rows are migrated by copying external_id → cluster_id.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "021_20260323"
down_revision: Union[str, None] = "020_20260323"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    # Add cluster_id column (initially nullable so existing rows can be backfilled)
    conn.execute(text(
        "ALTER TABLE inout_ops_identity_map ADD COLUMN IF NOT EXISTS cluster_id TEXT"
    ))
    # Backfill: cluster_id = external_id for all existing rows
    conn.execute(text(
        "UPDATE inout_ops_identity_map SET cluster_id = external_id WHERE cluster_id IS NULL"
    ))
    # Make cluster_id NOT NULL now that all rows are populated
    conn.execute(text(
        "ALTER TABLE inout_ops_identity_map ALTER COLUMN cluster_id SET NOT NULL"
    ))
    # Add unique constraint for the spec-compliant lookup direction
    conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS inout_ops_identity_map_cluster_pk
        ON inout_ops_identity_map (cluster_id, connector, datatype)
    """))
    # Rename the internal_id → target_external_id to clarify semantics.
    # We keep the old column name as an alias column to avoid breaking callers.
    conn.execute(text(
        "ALTER TABLE inout_ops_identity_map "
        "ADD COLUMN IF NOT EXISTS target_external_id TEXT"
    ))
    conn.execute(text(
        "UPDATE inout_ops_identity_map "
        "SET target_external_id = internal_id WHERE target_external_id IS NULL"
    ))
    # Add reverse-lookup index: given connector+datatype+target_external_id → cluster_id
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS inout_ops_identity_map_reverse_lookup
        ON inout_ops_identity_map (connector, datatype, target_external_id)
        WHERE target_external_id IS NOT NULL
    """))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(text(
        "DROP INDEX IF EXISTS inout_ops_identity_map_reverse_lookup"
    ))
    conn.execute(text(
        "DROP INDEX IF EXISTS inout_ops_identity_map_cluster_pk"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_identity_map DROP COLUMN IF EXISTS target_external_id"
    ))
    conn.execute(text(
        "ALTER TABLE inout_ops_identity_map DROP COLUMN IF EXISTS cluster_id"
    ))
