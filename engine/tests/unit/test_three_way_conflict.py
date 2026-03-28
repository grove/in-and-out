"""Unit tests for three-way conflict detection (Priority 7 — Phase 2)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# conflicts_detected_total metric tests
# ---------------------------------------------------------------------------


def test_conflicts_detected_total_metric_exists():
    """conflicts_detected_total should be importable from metrics module."""
    from inandout.observability.metrics import conflicts_detected_total
    assert conflicts_detected_total is not None


def test_conflicts_detected_total_metric_has_correct_labels():
    """conflicts_detected_total should have connector, datatype, resolution, namespace labels."""
    from inandout.observability.metrics import conflicts_detected_total
    # Access label names from the metric descriptor
    label_names = list(conflicts_detected_total._labelnames)
    assert "connector" in label_names
    assert "datatype" in label_names
    assert "resolution" in label_names
    assert "namespace" in label_names


# ---------------------------------------------------------------------------
# WritebackEngine conflict detection tests
# ---------------------------------------------------------------------------


def test_writeback_engine_imports_conflicts_detected_total():
    """writeback engine module should import conflicts_detected_total."""
    import inspect
    import inandout.writeback.engine as engine_mod
    source = inspect.getsource(engine_mod)
    assert "conflicts_detected_total" in source


def test_writeback_engine_emits_conflicts_metric_on_server_wins():
    """writeback engine source should emit conflicts_detected_total for server_wins."""
    import inspect
    import inandout.writeback.engine as engine_mod
    source = inspect.getsource(engine_mod)
    # Should have conflicts_detected_total.labels(...).inc() for server_wins
    assert "server_wins" in source
    assert "conflicts_detected_total" in source


def test_writeback_engine_emits_conflicts_metric_on_412():
    """writeback engine source should emit conflicts_detected_total for 412."""
    import inspect
    import inandout.writeback.engine as engine_mod
    source = inspect.getsource(engine_mod)
    assert "412_precondition_failed" in source
    assert "conflicts_detected_total" in source


def test_writeback_engine_emits_conflicts_metric_on_merge_fields():
    """writeback engine source should emit conflicts_detected_total for merge_fields."""
    import inspect
    import inandout.writeback.engine as engine_mod
    source = inspect.getsource(engine_mod)
    assert "merge_fields" in source
    assert "conflicts_detected_total" in source


# ---------------------------------------------------------------------------
# _compute_field_diff used in three-way conflict detection
# ---------------------------------------------------------------------------


def test_compute_field_diff_detects_changed_fields():
    """_compute_field_diff should detect fields changed between base and current."""
    from inandout.writeback.engine import _compute_field_diff

    base = {"name": "Alice", "email": "alice@old.com"}
    current = {"name": "Alice", "email": "alice@new.com"}
    diff = _compute_field_diff(base, current)

    assert "email" in diff.get("changed", {})
    assert diff["changed"]["email"]["from"] == "alice@old.com"
    assert diff["changed"]["email"]["to"] == "alice@new.com"


def test_compute_field_diff_detects_added_fields():
    """_compute_field_diff should detect new fields added."""
    from inandout.writeback.engine import _compute_field_diff

    base = {"name": "Alice"}
    current = {"name": "Alice", "phone": "555-1234"}
    diff = _compute_field_diff(base, current)

    assert "phone" in diff.get("added", [])


def test_compute_field_diff_detects_removed_fields():
    """_compute_field_diff should detect fields removed."""
    from inandout.writeback.engine import _compute_field_diff

    base = {"name": "Alice", "phone": "555-1234"}
    current = {"name": "Alice"}
    diff = _compute_field_diff(base, current)

    assert "phone" in diff.get("removed", [])


def test_compute_field_diff_empty_for_identical_payloads():
    """_compute_field_diff should return empty diff for identical payloads."""
    from inandout.writeback.engine import _compute_field_diff

    payload = {"name": "Alice", "email": "alice@example.com"}
    diff = _compute_field_diff(payload, payload)

    assert not diff.get("added")
    assert not diff.get("removed")
    assert not diff.get("changed")


# ---------------------------------------------------------------------------
# Three-way safety logic unit tests (pure logic, no DB/HTTP)
# ---------------------------------------------------------------------------


def _three_way_safe(current: dict, base: dict | None, last_written: dict | None, payload: dict) -> bool:
    """Replicate the field-scoped three-way comparison from WritebackEngine."""
    payload_fields = set(payload.keys())
    current_relevant = {k: v for k, v in current.items() if k in payload_fields}
    base_relevant = {k: v for k, v in (base or {}).items() if k in payload_fields}
    lw_relevant = {k: v for k, v in (last_written or {}).items() if k in payload_fields}
    return (current_relevant == base_relevant) or (current_relevant == lw_relevant)


def test_three_way_current_equals_base_is_safe():
    """current == base → safe, write should proceed."""
    payload = {"name": "Alice", "email": "a@b.com"}
    base = {"name": "Alice", "email": "a@b.com", "extra": "x"}
    current = {"name": "Alice", "email": "a@b.com", "extra": "y"}  # extra differs but not in payload
    last_written = {"name": "Old"}

    assert _three_way_safe(current, base, last_written, payload) is True


def test_three_way_current_equals_last_written_is_safe():
    """current == last_written → safe (own prior write), write should proceed."""
    payload = {"name": "Alice", "email": "a@b.com"}
    base = {"name": "Old"}
    current = {"name": "Alice", "email": "a@b.com"}
    last_written = {"name": "Alice", "email": "a@b.com"}

    assert _three_way_safe(current, base, last_written, payload) is True


def test_three_way_conflict_when_current_differs_from_both():
    """current != base AND current != last_written → conflict detected."""
    payload = {"name": "Alice", "email": "a@b.com"}
    base = {"name": "Alice", "email": "a@b.com"}
    current = {"name": "Bob", "email": "b@b.com"}  # someone else changed it
    last_written = {"name": "Alice", "email": "a@b.com"}

    assert _three_way_safe(current, base, last_written, payload) is False


def test_three_way_safe_with_none_base_and_none_last_written():
    """None base and None last_written: only safe if current has all-empty payload fields."""
    payload = {"name": "Alice"}
    current = {}  # current has no name field → current_relevant == {} == base_relevant {}
    assert _three_way_safe(current, None, None, payload) is True


def test_three_way_conflict_with_none_base_and_different_current():
    """None base but current has different data → conflict."""
    payload = {"name": "Alice"}
    current = {"name": "Bob"}  # current_relevant = {"name": "Bob"}, base_relevant = {} → not equal
    # also last_written=None so lw_relevant={} ≠ {"name": "Bob"}
    assert _three_way_safe(current, None, None, payload) is False


# ---------------------------------------------------------------------------
# WritebackEngine._dispatch_row three-way conflict integration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_row_safe_current_equals_base_proceeds(monkeypatch):
    """current == base (field-scoped) → write proceeds, no conflict counter."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from inandout.writeback.engine import WritebackEngine, WritebackResult
    from inandout.config.writeback import (
        ConflictResolution, OperationConfig, OperationsConfig, ProtectionLevel, WritebackConfig,
        UpdateOperationConfig, ConditionalWrite,
    )
    from inandout.config.connector import ConnectorConfig

    pool = MagicMock()
    pool.connection = MagicMock()

    # Mock connection context manager for lwstate reads
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=AsyncMock(fetchone=AsyncMock(return_value=None)))
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    engine = WritebackEngine(pool)

    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/contacts/${external_id}"),
        update=UpdateOperationConfig(
            method="PATCH",
            path="/contacts/${external_id}",
            conditional_write=ConditionalWrite(enabled=True),
        ),
    )
    writeback_cfg = WritebackConfig(
        protection_level=ProtectionLevel.conditional_write_required,
        conflict_resolution=ConflictResolution.skip_and_warn,
        supported_actions=["update"],
        operations=ops,
        use_desired_state_table=True,
    )

    connector = MagicMock(spec=ConnectorConfig)
    connector.name = "hubspot"

    result = WritebackResult(connector="hubspot", datatype="contacts", delta_table="_delta_hubspot_contacts")

    # row has _base matching current state → safe
    row = {
        "name": "Alice",
        "email": "a@b.com",
        "_action": "update",
        "_base": {"name": "Alice", "email": "a@b.com"},  # base == current → safe
    }

    # Mock transport: preflight GET returns current == base (no conflict)
    transport = AsyncMock()
    preflight_response = MagicMock()
    preflight_response.content = b'{"name": "Alice", "email": "a@b.com"}'
    preflight_response.headers = {}
    update_response = MagicMock()
    update_response.status_code = 200
    update_response.headers = {}
    transport._raw_request = AsyncMock(side_effect=[preflight_response, update_response])
    transport._request = AsyncMock()

    # Mock get_lwstate to return None (no prior write)
    with patch("inandout.writeback.engine.get_lwstate", new_callable=AsyncMock, return_value=None), \
         patch("inandout.writeback.engine.upsert_lwstate", new_callable=AsyncMock):
        # Should NOT conflict since current == base (within payload fields)
        await engine._dispatch_row(
            transport, connector, writeback_cfg,
            "update", "42", row, MagicMock(), result
        )

    # Write should have proceeded (skipped=0) since current == base
    assert result.skipped == 0
    assert result.conflicts == 0


@pytest.mark.asyncio
async def test_dispatch_row_skip_and_warn_on_conflict(monkeypatch):
    """conflict detected with skip_and_warn → skipped incremented, conflict counter fired."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from inandout.writeback.engine import WritebackEngine, WritebackResult
    from inandout.config.writeback import (
        ConflictResolution, OperationConfig, OperationsConfig, ProtectionLevel, WritebackConfig,
        UpdateOperationConfig, ConditionalWrite,
    )
    from inandout.config.connector import ConnectorConfig

    pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=AsyncMock(fetchone=AsyncMock(return_value=None)))
    pool.connection = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    engine = WritebackEngine(pool)

    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/contacts/${external_id}"),
        update=UpdateOperationConfig(
            method="PATCH",
            path="/contacts/${external_id}",
            conditional_write=ConditionalWrite(enabled=True),
        ),
    )
    writeback_cfg = WritebackConfig(
        protection_level=ProtectionLevel.conditional_write_required,
        conflict_resolution=ConflictResolution.skip_and_warn,
        supported_actions=["update"],
        operations=ops,
        use_desired_state_table=True,
    )

    connector = MagicMock(spec=ConnectorConfig)
    connector.name = "hubspot"

    result = WritebackResult(connector="hubspot", datatype="contacts", delta_table="_delta_hubspot_contacts")

    row = {"name": "Alice", "email": "a@b.com", "_action": "update"}

    transport = AsyncMock()
    # Current state differs from both base (row has no _base) and last_written (None)
    preflight_response = MagicMock()
    preflight_response.content = b'{"name": "Bob", "email": "b@b.com"}'  # conflict!
    preflight_response.headers = {}
    transport._raw_request = AsyncMock(return_value=preflight_response)

    with patch("inandout.writeback.engine.get_lwstate", new_callable=AsyncMock, return_value=None), \
         patch("inandout.writeback.engine.upsert_lwstate", new_callable=AsyncMock) as mock_upsert:
        await engine._dispatch_row(
            transport, connector, writeback_cfg,
            "update", "42", row, MagicMock(), result
        )

    assert result.skipped == 1
    assert result.conflicts == 1
    # lwstate should have been updated to current_state
    mock_upsert.assert_called_once()


@pytest.mark.asyncio
async def test_dispatch_row_last_writer_wins_proceeds_on_conflict(monkeypatch):
    """last_writer_wins → write proceeds despite conflict, counter incremented."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from inandout.writeback.engine import WritebackEngine, WritebackResult
    from inandout.config.writeback import (
        ConflictResolution, OperationConfig, OperationsConfig, ProtectionLevel, WritebackConfig,
        UpdateOperationConfig, ConditionalWrite,
    )
    from inandout.config.connector import ConnectorConfig

    pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=AsyncMock(fetchone=AsyncMock(return_value=None)))
    pool.connection = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    engine = WritebackEngine(pool)

    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/contacts/${external_id}"),
        update=UpdateOperationConfig(
            method="PATCH",
            path="/contacts/${external_id}",
            conditional_write=ConditionalWrite(enabled=True),
        ),
    )
    writeback_cfg = WritebackConfig(
        protection_level=ProtectionLevel.conditional_write_required,
        conflict_resolution=ConflictResolution.last_writer_wins,
        supported_actions=["update"],
        operations=ops,
        use_desired_state_table=True,
    )

    connector = MagicMock(spec=ConnectorConfig)
    connector.name = "hubspot"

    result = WritebackResult(connector="hubspot", datatype="contacts", delta_table="_delta_hubspot_contacts")

    row = {"name": "Alice", "email": "a@b.com", "_action": "update"}

    transport = AsyncMock()
    # current state conflicts
    preflight_response = MagicMock()
    preflight_response.content = b'{"name": "Bob", "email": "b@b.com"}'
    preflight_response.headers = {}
    update_response = MagicMock()
    update_response.status_code = 200
    update_response.headers = {}
    transport._raw_request = AsyncMock(side_effect=[preflight_response, update_response])
    transport._request = AsyncMock()

    with patch("inandout.writeback.engine.get_lwstate", new_callable=AsyncMock, return_value=None), \
         patch("inandout.writeback.engine.upsert_lwstate", new_callable=AsyncMock):
        await engine._dispatch_row(
            transport, connector, writeback_cfg,
            "update", "42", row, MagicMock(), result
        )

    # last_writer_wins: conflict counter but write proceeds (processed or skipped=0 from conflict path)
    assert result.conflicts == 1
    assert result.skipped == 0  # write proceeded


@pytest.mark.asyncio
async def test_dispatch_row_re_ingest_inserts_control_row(monkeypatch):
    """re_ingest_and_recompute → control row inserted with command 'resync'."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from inandout.writeback.engine import WritebackEngine, WritebackResult
    from inandout.config.writeback import (
        ConflictResolution, OperationConfig, OperationsConfig, ProtectionLevel, WritebackConfig,
        UpdateOperationConfig, ConditionalWrite,
    )
    from inandout.config.connector import ConnectorConfig

    # Track INSERT calls
    executed_sqls = []

    pool = MagicMock()
    mock_conn = AsyncMock()

    async def capture_execute(sql, params=None):
        executed_sqls.append((sql, params))
        return AsyncMock(fetchone=AsyncMock(return_value=None))

    mock_conn.execute = AsyncMock(side_effect=capture_execute)
    mock_conn.commit = AsyncMock()
    pool.connection = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    engine = WritebackEngine(pool)

    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/contacts/${external_id}"),
        update=UpdateOperationConfig(
            method="PATCH",
            path="/contacts/${external_id}",
            conditional_write=ConditionalWrite(enabled=True),
        ),
    )
    writeback_cfg = WritebackConfig(
        protection_level=ProtectionLevel.conditional_write_required,
        conflict_resolution=ConflictResolution.re_ingest_and_recompute,
        supported_actions=["update"],
        operations=ops,
        use_desired_state_table=True,
    )

    connector = MagicMock(spec=ConnectorConfig)
    connector.name = "hubspot"

    result = WritebackResult(connector="hubspot", datatype="contacts", delta_table="_delta_hubspot_contacts")
    row = {"name": "Alice", "_action": "update"}

    transport = AsyncMock()
    preflight_response = MagicMock()
    preflight_response.content = b'{"name": "Bob"}'  # conflict
    preflight_response.headers = {}
    transport._raw_request = AsyncMock(return_value=preflight_response)

    with patch("inandout.writeback.engine.get_lwstate", new_callable=AsyncMock, return_value=None), \
         patch("inandout.writeback.engine.upsert_lwstate", new_callable=AsyncMock):
        await engine._dispatch_row(
            transport, connector, writeback_cfg,
            "update", "42", row, MagicMock(), result
        )

    assert result.skipped == 1
    assert result.conflicts == 1
    # At least one INSERT into inout_ops_control
    inserts = [s for s, _ in executed_sqls if "inout_ops_control" in s]
    assert len(inserts) >= 1


@pytest.mark.asyncio
async def test_dispatch_row_dead_letter_on_conflict(monkeypatch):
    """dead_letter conflict resolution → failed counter incremented."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from inandout.writeback.engine import WritebackEngine, WritebackResult
    from inandout.config.writeback import (
        ConflictResolution, OperationConfig, OperationsConfig, ProtectionLevel, WritebackConfig,
        UpdateOperationConfig, ConditionalWrite,
    )
    from inandout.config.connector import ConnectorConfig

    pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=AsyncMock(fetchone=AsyncMock(return_value=None)))
    pool.connection = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    engine = WritebackEngine(pool)

    ops = OperationsConfig(
        lookup=OperationConfig(method="GET", path="/contacts/${external_id}"),
        update=UpdateOperationConfig(
            method="PATCH",
            path="/contacts/${external_id}",
            conditional_write=ConditionalWrite(enabled=True),
        ),
    )
    writeback_cfg = WritebackConfig(
        protection_level=ProtectionLevel.conditional_write_required,
        conflict_resolution=ConflictResolution.dead_letter,
        supported_actions=["update"],
        operations=ops,
        use_desired_state_table=True,
    )

    connector = MagicMock(spec=ConnectorConfig)
    connector.name = "hubspot"

    result = WritebackResult(connector="hubspot", datatype="contacts", delta_table="_delta_hubspot_contacts")
    row = {"name": "Alice", "_action": "update"}

    transport = AsyncMock()
    preflight_response = MagicMock()
    preflight_response.content = b'{"name": "Bob"}'  # conflict
    preflight_response.headers = {}
    transport._raw_request = AsyncMock(return_value=preflight_response)

    with patch("inandout.writeback.engine.get_lwstate", new_callable=AsyncMock, return_value=None), \
         patch("inandout.writeback.engine.upsert_lwstate", new_callable=AsyncMock):
        await engine._dispatch_row(
            transport, connector, writeback_cfg,
            "update", "42", row, MagicMock(), result
        )

    assert result.failed == 1
    assert result.conflicts == 1
