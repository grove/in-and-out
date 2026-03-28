"""Unit tests for source table schema fixes (Priority 1 — Phase 2)."""
from __future__ import annotations


# ---------------------------------------------------------------------------
# source_table_ddl column tests
# ---------------------------------------------------------------------------


def test_source_table_ddl_has_data_column():
    """source_table_ddl should include a 'data JSONB NOT NULL' column."""
    from inandout.postgres.schema import source_table_ddl

    ddl = source_table_ddl("myconn", "contacts")
    assert "data" in ddl
    assert "JSONB" in ddl


def test_source_table_ddl_has_schema_version_column():
    """source_table_ddl should include _schema_version INTEGER column."""
    from inandout.postgres.schema import source_table_ddl

    ddl = source_table_ddl("myconn", "contacts")
    assert "_schema_version" in ddl
    assert "INTEGER" in ddl


def test_source_table_ddl_has_source_version_column():
    """source_table_ddl should include _source_version TEXT column."""
    from inandout.postgres.schema import source_table_ddl

    ddl = source_table_ddl("myconn", "contacts")
    assert "_source_version" in ddl
    assert "TEXT" in ddl


def test_source_table_ddl_has_lineage_column():
    """source_table_ddl should include _lineage JSONB column."""
    from inandout.postgres.schema import source_table_ddl

    ddl = source_table_ddl("myconn", "contacts")
    assert "_lineage" in ddl


def test_source_table_ddl_has_all_required_columns():
    """source_table_ddl should include all GOAL.md required columns."""
    from inandout.postgres.schema import source_table_ddl

    ddl = source_table_ddl("testconn", "orders")
    for col in ("external_id", "data", "raw", "_ingested_at", "_sync_run_id",
                "_raw_hash", "_deleted", "_schema_version", "_source_version",
                "_lineage", "_last_written"):
        assert col in ddl, f"Missing column: {col}"


# ---------------------------------------------------------------------------
# Migration 008 structure tests (no DB required)
# ---------------------------------------------------------------------------


def test_migration_008_exists():
    """Migration 008_source_table_schema.py should exist and be importable."""
    import importlib
    mod = importlib.import_module(
        "migrations.versions.008_20260323_source_table_schema"
    )
    assert hasattr(mod, "upgrade")
    assert hasattr(mod, "downgrade")
    assert mod.revision == "008_20260323"
    assert mod.down_revision == "007_20260323"
