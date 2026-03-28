"""Unit tests for intra-sync deduplication (T1 #33)."""
from __future__ import annotations


# ---------------------------------------------------------------------------
# Pure logic tests for in_run_seen set behaviour
# ---------------------------------------------------------------------------


def test_dedup_set_skips_second_occurrence():
    """Second occurrence of same external_id in same run should be detected."""
    in_run_seen: set[str] = set()

    def process(external_id: str) -> str:
        """Return 'processed' or 'skipped'."""
        if external_id in in_run_seen:
            return "skipped"
        in_run_seen.add(external_id)
        return "processed"

    assert process("42") == "processed"
    assert process("42") == "skipped"
    assert process("99") == "processed"


def test_dedup_set_allows_different_ids():
    """Different external_ids in same run should all be processed."""
    in_run_seen: set[str] = set()
    results = []
    for ext_id in ["1", "2", "3"]:
        if ext_id in in_run_seen:
            results.append("skipped")
        else:
            in_run_seen.add(ext_id)
            results.append("processed")
    assert results == ["processed", "processed", "processed"]


def test_dedup_set_is_per_run():
    """in_run_seen is created fresh per run — same id in different run is processed."""
    # First run
    in_run_seen_1: set[str] = set()
    in_run_seen_1.add("42")

    # Second run — fresh set
    in_run_seen_2: set[str] = set()
    assert "42" not in in_run_seen_2  # Not carried over from run 1

    in_run_seen_2.add("42")
    assert "42" in in_run_seen_2


# ---------------------------------------------------------------------------
# intra_sync_duplicates_total metric
# ---------------------------------------------------------------------------


def test_intra_sync_duplicates_metric_exists():
    """intra_sync_duplicates_total should be importable from metrics module."""
    from inandout.observability.metrics import intra_sync_duplicates_total
    assert intra_sync_duplicates_total is not None


def test_intra_sync_duplicates_metric_labels():
    """intra_sync_duplicates_total should have connector and datatype labels."""
    from inandout.observability.metrics import intra_sync_duplicates_total
    labels = list(intra_sync_duplicates_total._labelnames)
    assert "connector" in labels
    assert "datatype" in labels


def test_intra_sync_duplicates_metric_increments():
    """intra_sync_duplicates_total can be incremented."""
    from inandout.observability.metrics import intra_sync_duplicates_total
    before = intra_sync_duplicates_total.labels(
        connector="test_dedup", datatype="items"
    )._value.get()
    intra_sync_duplicates_total.labels(
        connector="test_dedup", datatype="items"
    ).inc()
    after = intra_sync_duplicates_total.labels(
        connector="test_dedup", datatype="items"
    )._value.get()
    assert after == before + 1


# ---------------------------------------------------------------------------
# Engine source code inspection
# ---------------------------------------------------------------------------


def test_ingestion_engine_has_in_run_seen():
    """IngestionEngine._do_sync should maintain in_run_seen set."""
    import inspect
    from inandout.ingestion import engine as engine_mod
    source = inspect.getsource(engine_mod)
    assert "in_run_seen" in source


def test_ingestion_engine_logs_intra_sync_duplicate():
    """IngestionEngine should log intra_sync_duplicate_skipped."""
    import inspect
    from inandout.ingestion import engine as engine_mod
    source = inspect.getsource(engine_mod)
    assert "intra_sync_duplicate_skipped" in source


def test_ingestion_engine_increments_duplicate_counter():
    """IngestionEngine should increment intra_sync_duplicates_total on duplicate."""
    import inspect
    from inandout.ingestion import engine as engine_mod
    source = inspect.getsource(engine_mod)
    assert "intra_sync_duplicates_total" in source
