"""Unit tests for multi-region / read replica support (Step 68)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# DatabaseConfig
# ---------------------------------------------------------------------------

def test_database_config_read_replica_dsn_defaults_none():
    from inandout.config.tool import DatabaseConfig

    cfg = DatabaseConfig(dsn="postgresql://localhost/test")
    assert cfg.read_replica_dsn is None


def test_database_config_read_replica_dsn_settable():
    from inandout.config.tool import DatabaseConfig

    cfg = DatabaseConfig(
        dsn="postgresql://primary/test",
        read_replica_dsn="postgresql://replica/test",
    )
    assert cfg.read_replica_dsn == "postgresql://replica/test"


# ---------------------------------------------------------------------------
# create_read_pool
# ---------------------------------------------------------------------------

async def test_create_read_pool_returns_none_when_not_configured():
    from inandout.config.tool import DatabaseConfig
    from inandout.postgres.pool import create_read_pool

    cfg = DatabaseConfig(dsn="postgresql://localhost/test")
    pool = await create_read_pool(cfg)
    assert pool is None


async def test_create_read_pool_returns_pool_when_configured():
    from inandout.config.tool import DatabaseConfig
    from inandout.postgres.pool import create_read_pool

    cfg = DatabaseConfig(
        dsn="postgresql://primary/test",
        read_replica_dsn="postgresql://replica/test",
    )

    mock_pool = AsyncMock()

    with patch("inandout.postgres.pool.AsyncConnectionPool") as mock_cls:
        mock_cls.return_value = mock_pool
        mock_pool.open = AsyncMock()

        pool = await create_read_pool(cfg)

    assert pool is not None
    mock_cls.assert_called_once()
    # Verify it used the replica DSN
    call_kwargs = mock_cls.call_args
    assert "postgresql://replica/test" in str(call_kwargs)


# ---------------------------------------------------------------------------
# IngestionEngine._read_conn_pool
# ---------------------------------------------------------------------------

def test_engine_read_conn_pool_returns_primary_when_no_replica():
    from inandout.ingestion.engine import IngestionEngine

    primary = MagicMock()
    engine = IngestionEngine(pool=primary)
    assert engine._read_conn_pool() is primary


def test_engine_read_conn_pool_returns_read_pool_when_set():
    from inandout.ingestion.engine import IngestionEngine

    primary = MagicMock()
    replica = MagicMock()
    engine = IngestionEngine(pool=primary, read_pool=replica)
    assert engine._read_conn_pool() is replica


def test_engine_read_conn_pool_fallback_when_none():
    from inandout.ingestion.engine import IngestionEngine

    primary = MagicMock()
    engine = IngestionEngine(pool=primary, read_pool=None)
    assert engine._read_conn_pool() is primary


# ---------------------------------------------------------------------------
# Verify read pool is stored
# ---------------------------------------------------------------------------

def test_engine_stores_read_pool():
    from inandout.ingestion.engine import IngestionEngine

    primary = MagicMock()
    replica = MagicMock()
    engine = IngestionEngine(pool=primary, read_pool=replica)
    assert engine._read_pool is replica
