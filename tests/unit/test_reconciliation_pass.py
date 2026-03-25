"""Unit tests for reconciliation pass functionality (T1 #38)."""
from __future__ import annotations

import pytest


def test_reconciliation_pass_config_defaults_to_false():
    """reconciliation_pass should default to False."""
    from inandout.config.ingestion import ListConfig
    
    cfg = ListConfig(
        method="GET",
        path="/records",
        record_selector="items",
        pagination={"strategy": "offset", "offset": {"page_size": 100}},
    )
    assert cfg.reconciliation_pass is False


def test_reconciliation_pass_can_be_enabled():
    """reconciliation_pass flag should be configurable."""
    from inandout.config.ingestion import ListConfig
    
    cfg = ListConfig(
        method="GET",
        path="/records",
        record_selector="items",
        pagination={"strategy": "offset", "offset": {"page_size": 100}},
        detail_path="/records/${id}",
        reconciliation_pass=True,
    )
    assert cfg.reconciliation_pass is True


def test_reconciliation_requires_detail_path():
    """Reconciliation pass implementation requires detail_path to re-fetch records."""
    # This is a documentation test - the implementation logs and skips if no detail_path
    from inandout.config.ingestion import ListConfig
    
    cfg = ListConfig(
        method="GET",
        path="/records",
        record_selector="items",
        pagination={"strategy": "offset", "offset": {"page_size": 100}},
        reconciliation_pass=True,
        # No detail_path - reconciliation will be skipped
    )
    assert cfg.reconciliation_pass is True
    assert cfg.detail_path is None


def test_reconciliation_method_exists():
    """IngestionEngine should have _reconciliation_pass method."""
    from inandout.ingestion.engine import IngestionEngine
    from unittest.mock import MagicMock
    from psycopg_pool import AsyncConnectionPool
    
    pool = MagicMock(spec=AsyncConnectionPool)
    engine = IngestionEngine(pool)
    
    assert hasattr(engine, '_reconciliation_pass')
    assert callable(engine._reconciliation_pass)
