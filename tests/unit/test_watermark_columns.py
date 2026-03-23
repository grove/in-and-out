"""Unit tests for watermark schema columns (Priority 3 — Phase 2)."""
from __future__ import annotations


def test_migration_010_exists():
    """Migration 010_watermark_columns.py should exist and be importable."""
    import importlib
    mod = importlib.import_module(
        "migrations.versions.010_20260323_watermark_columns"
    )
    assert hasattr(mod, "upgrade")
    assert hasattr(mod, "downgrade")
    assert mod.revision == "010_20260323"
    assert mod.down_revision == "009_20260323"


def test_operational_tables_ddl_has_watermark_type():
    """OPERATIONAL_TABLES_DDL should include watermark_type column."""
    from inandout.postgres.schema import OPERATIONAL_TABLES_DDL

    assert "watermark_type" in OPERATIONAL_TABLES_DDL


def test_operational_tables_ddl_has_updated_by_run_id():
    """OPERATIONAL_TABLES_DDL should include updated_by_run_id column."""
    from inandout.postgres.schema import OPERATIONAL_TABLES_DDL

    assert "updated_by_run_id" in OPERATIONAL_TABLES_DDL


def test_set_watermark_uses_watermark_type():
    """set_watermark should pass watermark_type to the INSERT."""
    import inspect
    from inandout.postgres.watermark import set_watermark

    source = inspect.getsource(set_watermark)
    assert "watermark_type" in source
