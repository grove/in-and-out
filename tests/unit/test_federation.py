"""Unit tests for federation reporter (Step 85)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# FederationReporter tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_federation_reporter_report_calls_pool():
    """report() should upsert into inout_ops_federation via pool."""
    from inandout.federation.reporter import FederationReporter

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.execute = AsyncMock()
    mock_conn.commit = AsyncMock()

    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)

    reporter = FederationReporter(mock_pool, "test-instance", "public")
    await reporter.report(
        connector="myconn",
        datatype="contacts",
        health_score=0.9,
        last_sync_at=None,
        circuit_state="closed",
        dl_depth=0,
    )

    mock_conn.execute.assert_called_once()
    mock_conn.commit.assert_called_once()

    # Check that the SQL contains the expected table name
    call_args = mock_conn.execute.call_args
    sql = call_args[0][0]
    assert "inout_ops_federation" in sql


@pytest.mark.anyio
async def test_federation_reporter_report_handles_error_gracefully():
    """report() should swallow errors without raising."""
    from inandout.federation.reporter import FederationReporter

    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(side_effect=Exception("DB connection failed"))

    reporter = FederationReporter(mock_pool, "test-instance", "public")
    # Should not raise
    await reporter.report(
        connector="myconn",
        datatype="contacts",
        health_score=0.5,
        last_sync_at=None,
        circuit_state="open",
        dl_depth=5,
    )


@pytest.mark.anyio
async def test_federation_reporter_cleanup_stale():
    """cleanup_stale() should execute a DELETE and return row count."""
    from inandout.federation.reporter import FederationReporter

    mock_cur = MagicMock()
    mock_cur.rowcount = 3

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.execute = AsyncMock(return_value=mock_cur)
    mock_conn.commit = AsyncMock()

    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=mock_conn)

    reporter = FederationReporter(mock_pool, "test-instance", "public")
    deleted = await reporter.cleanup_stale(max_age_secs=300.0)

    assert deleted == 3
    mock_conn.execute.assert_called_once()


@pytest.mark.anyio
async def test_federation_reporter_cleanup_stale_returns_zero_on_error():
    """cleanup_stale() should return 0 on error without raising."""
    from inandout.federation.reporter import FederationReporter

    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(side_effect=Exception("DB error"))

    reporter = FederationReporter(mock_pool, "test-instance", "public")
    result = await reporter.cleanup_stale()
    assert result == 0


# ---------------------------------------------------------------------------
# default_instance_id tests
# ---------------------------------------------------------------------------

def test_default_instance_id_is_string():
    """_default_instance_id should return a non-empty string."""
    from inandout.federation.reporter import _default_instance_id

    iid = _default_instance_id()
    assert isinstance(iid, str)
    assert len(iid) > 0
    # Should contain hostname:pid format
    assert ":" in iid


# ---------------------------------------------------------------------------
# FederationConfig tests
# ---------------------------------------------------------------------------

def test_federation_config_defaults():
    """FederationConfig should have correct defaults."""
    from inandout.config.tool import FederationConfig

    cfg = FederationConfig()
    assert cfg.enabled is False
    assert cfg.report_interval_secs == 30.0
    assert cfg.stale_threshold_secs == 300.0


def test_federation_config_enabled():
    """FederationConfig with enabled=True should be valid."""
    from inandout.config.tool import FederationConfig

    cfg = FederationConfig(enabled=True, report_interval_secs=10.0)
    assert cfg.enabled is True
    assert cfg.report_interval_secs == 10.0


# ---------------------------------------------------------------------------
# IngestionToolConfig federation field tests
# ---------------------------------------------------------------------------

def test_ingestion_tool_config_has_federation_field():
    """IngestionToolConfig should have a federation field."""
    from inandout.config.tool import IngestionToolConfig, DatabaseConfig

    cfg = IngestionToolConfig(database=DatabaseConfig(dsn="postgresql://localhost/test"))
    assert hasattr(cfg, "federation")
    assert cfg.federation.enabled is False


def test_ingestion_tool_config_federation_enabled():
    """IngestionToolConfig with federation.enabled=True should be valid."""
    from inandout.config.tool import IngestionToolConfig, DatabaseConfig, FederationConfig

    cfg = IngestionToolConfig(
        database=DatabaseConfig(dsn="postgresql://localhost/test"),
        federation=FederationConfig(enabled=True, report_interval_secs=15.0),
    )
    assert cfg.federation.enabled is True
    assert cfg.federation.report_interval_secs == 15.0


# ---------------------------------------------------------------------------
# federation_routed_total counter tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_report_increments_federation_routed_counter():
    """report() should increment federation_routed_total on success."""
    from unittest.mock import patch as _patch, MagicMock as _MM, AsyncMock as _AM
    from inandout.federation.reporter import FederationReporter

    mock_conn = _AM()
    mock_conn.__aenter__ = _AM(return_value=mock_conn)
    mock_conn.__aexit__ = _AM(return_value=False)
    mock_conn.execute = _AM()
    mock_conn.commit = _AM()

    mock_pool = _MM()
    mock_pool.connection = _MM(return_value=mock_conn)

    mock_label_set = _MM()
    mock_counter = _MM()
    mock_counter.labels = _MM(return_value=mock_label_set)

    reporter = FederationReporter(mock_pool, "inst-1", "ns1")
    with _patch("inandout.federation.reporter.federation_routed_total", mock_counter):
        await reporter.report(
            connector="myconn",
            datatype="contacts",
            health_score=1.0,
            last_sync_at=None,
            circuit_state="closed",
            dl_depth=0,
        )

    mock_counter.labels.assert_called_once_with(
        connector="myconn",
        datatype="contacts",
        destination="ns1/myconn/contacts",
    )
    mock_label_set.inc.assert_called_once()


@pytest.mark.anyio
async def test_report_does_not_increment_counter_on_error():
    """report() should NOT increment the counter when the DB insert fails."""
    from unittest.mock import patch as _patch, MagicMock as _MM
    from inandout.federation.reporter import FederationReporter

    mock_pool = _MM()
    mock_pool.connection = _MM(side_effect=Exception("DB down"))

    mock_label_set = _MM()
    mock_counter = _MM()
    mock_counter.labels = _MM(return_value=mock_label_set)

    reporter = FederationReporter(mock_pool, "inst-1", "ns1")
    with _patch("inandout.federation.reporter.federation_routed_total", mock_counter):
        await reporter.report(
            connector="myconn",
            datatype="contacts",
            health_score=0.0,
            last_sync_at=None,
            circuit_state="open",
            dl_depth=10,
        )

    mock_label_set.inc.assert_not_called()
