"""Unit tests for soft-delete resurrection (A6)."""
from __future__ import annotations

from inandout.observability.metrics import records_resurrected_total


def test_resurrection_counter_registered() -> None:
    """records_resurrected_total metric should be registered and labelable."""
    counter = records_resurrected_total
    assert counter is not None
    # Try to use the counter with the table label
    counter.labels(table="inout_src_test_contacts").inc(0)


def test_resurrection_counter_increments() -> None:
    """Incrementing the counter should not raise."""
    from inandout.observability.metrics import records_resurrected_total
    records_resurrected_total.labels(table="inout_src_crm_accounts").inc()
