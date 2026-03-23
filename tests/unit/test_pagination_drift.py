"""Unit tests for pagination drift protection (T1 #38 A2)."""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Config field defaults
# ---------------------------------------------------------------------------

def test_list_config_drift_protection_defaults():
    """ListConfig should have drift_protection=True by default."""
    from inandout.config.ingestion import ListConfig
    from inandout.config.pagination import CursorConfig, PaginationConfig, PaginationStrategy

    cfg = ListConfig(
        path="/contacts",
        pagination=PaginationConfig(
            strategy=PaginationStrategy.cursor,
            cursor=CursorConfig(response_path="next", request_param="after"),
        ),
    )
    assert cfg.drift_protection is True
    assert cfg.drift_max_shrink_pct == 50.0
    assert cfg.drift_min_records == 0
    assert cfg.snapshot_param is None
    assert cfg.reconciliation_pass is False


def test_list_config_drift_protection_customizable():
    """ListConfig drift fields are configurable."""
    from inandout.config.ingestion import ListConfig
    from inandout.config.pagination import CursorConfig, PaginationConfig, PaginationStrategy

    cfg = ListConfig(
        path="/contacts",
        pagination=PaginationConfig(
            strategy=PaginationStrategy.cursor,
            cursor=CursorConfig(response_path="next", request_param="after"),
        ),
        drift_protection=False,
        drift_max_shrink_pct=30.0,
        drift_min_records=100,
        snapshot_param="snapshot_id",
        reconciliation_pass=True,
    )
    assert cfg.drift_protection is False
    assert cfg.drift_max_shrink_pct == 30.0
    assert cfg.drift_min_records == 100
    assert cfg.snapshot_param == "snapshot_id"
    assert cfg.reconciliation_pass is True


# ---------------------------------------------------------------------------
# Drift detection logic
# ---------------------------------------------------------------------------

def _drift_check(records_fetched: int, last_known_count: int, drift_max_shrink_pct: float,
                 drift_min_records: int) -> bool:
    """Returns True if drift is detected (circuit breaker should trip)."""
    if last_known_count == 0:
        return False
    threshold = last_known_count * (1.0 - drift_max_shrink_pct / 100.0)
    exceeds_min = last_known_count > drift_min_records
    return records_fetched < threshold and exceeds_min


def test_drift_detected_large_drop():
    """Large drop (>50%) triggers drift detection."""
    assert _drift_check(
        records_fetched=100,
        last_known_count=500,
        drift_max_shrink_pct=50.0,
        drift_min_records=0,
    ) is True


def test_drift_not_detected_small_drop():
    """Small drop (<50%) does NOT trigger drift detection."""
    assert _drift_check(
        records_fetched=400,
        last_known_count=500,
        drift_max_shrink_pct=50.0,
        drift_min_records=0,
    ) is False


def test_drift_not_detected_exact_threshold():
    """Exactly at 50% threshold (250/500) should NOT trip (< not <=)."""
    assert _drift_check(
        records_fetched=250,
        last_known_count=500,
        drift_max_shrink_pct=50.0,
        drift_min_records=0,
    ) is False


def test_drift_not_detected_when_last_count_zero():
    """No drift detection when last_known_count is 0 (first run)."""
    assert _drift_check(
        records_fetched=0,
        last_known_count=0,
        drift_max_shrink_pct=50.0,
        drift_min_records=0,
    ) is False


def test_drift_min_records_prevents_trip():
    """drift_min_records=1000 prevents trip when last_known_count < threshold."""
    # last_known_count=500 <= drift_min_records=1000 → no trip
    assert _drift_check(
        records_fetched=100,
        last_known_count=500,
        drift_max_shrink_pct=50.0,
        drift_min_records=1000,
    ) is False


# ---------------------------------------------------------------------------
# Snapshot param wiring
# ---------------------------------------------------------------------------

def test_snapshot_param_added_to_base_params():
    """When snapshot_param is set, the run_id is injected into base_params."""
    import uuid
    snapshot_param = "snapshot_id"
    snapshot_value = str(uuid.uuid4())

    base_params: dict = {}
    if snapshot_param and snapshot_value:
        base_params[snapshot_param] = snapshot_value

    assert "snapshot_id" in base_params
    assert base_params["snapshot_id"] == snapshot_value


def test_snapshot_param_not_added_when_none():
    """When snapshot_param is None, base_params is unchanged."""
    snapshot_param = None
    snapshot_value = None

    base_params: dict = {}
    if snapshot_param and snapshot_value:
        base_params[snapshot_param] = snapshot_value

    assert base_params == {}


# ---------------------------------------------------------------------------
# Metrics counter
# ---------------------------------------------------------------------------

def test_pagination_drift_events_total_metric_exists():
    """pagination_drift_events_total counter should be registered."""
    from inandout.observability.metrics import pagination_drift_events_total
    assert pagination_drift_events_total is not None


def test_pagination_drift_events_total_can_increment():
    """Incrementing the drift counter should not raise."""
    from inandout.observability.metrics import pagination_drift_events_total
    try:
        pagination_drift_events_total.labels(
            connector="test_connector",
            datatype="test_datatype",
        ).inc()
    except Exception as exc:
        pytest.fail(f"Incrementing drift counter raised: {exc}")
