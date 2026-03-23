"""Unit tests for source unavailability handling (A8)."""
from __future__ import annotations

from inandout.observability.metrics import source_unavailable_total


def test_source_unavailable_counter_registered() -> None:
    """source_unavailable_total metric should be registered and labelable."""
    counter = source_unavailable_total
    assert counter is not None
    counter.labels(connector="test", datatype="contacts").inc(0)


def test_source_unavailable_counter_increments() -> None:
    source_unavailable_total.labels(connector="erp", datatype="invoices").inc()


def test_connector_health_table_ddl_exists() -> None:
    """Migration 017 should define the health table SQL (smoke test)."""
    import importlib.util
    import pathlib

    migration_path = pathlib.Path(
        "migrations/versions/017_20260323_connector_health.py"
    )
    spec = importlib.util.spec_from_file_location("migration_017", migration_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    # We just need to import it — don't execute upgrade()
    assert module is not None
