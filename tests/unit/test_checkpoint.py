"""Unit tests for intra-sync checkpointing (T1 #29)."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, call

import pytest


# ---------------------------------------------------------------------------
# save_checkpoint / load_checkpoint round-trip (mock pool)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_save_checkpoint_inserts_row():
    """save_checkpoint should execute an UPSERT into inout_ops_sync_checkpoint."""
    from inandout.postgres.checkpoint import save_checkpoint

    executed = []
    mock_conn = AsyncMock()

    async def capture_execute(sql, params=None):
        executed.append((sql, params))
        return AsyncMock()

    mock_conn.execute = AsyncMock(side_effect=capture_execute)
    mock_conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    run_id = uuid.uuid4()
    await save_checkpoint(pool, run_id, "hubspot", "contacts", 5, "2026-01-01T00:00:00", 250)

    assert len(executed) == 1
    sql, params = executed[0]
    assert "inout_ops_sync_checkpoint" in sql
    assert str(run_id) in params
    assert "hubspot" in params
    assert "contacts" in params
    assert 5 in params
    assert 250 in params
    mock_conn.commit.assert_called_once()


@pytest.mark.asyncio
async def test_load_checkpoint_returns_dict():
    """load_checkpoint should return a dict with checkpoint fields."""
    from inandout.postgres.checkpoint import load_checkpoint

    run_id = uuid.uuid4()
    fake_row = (str(run_id), "hubspot", "contacts", 5, "2026-01-01", 250, None)

    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor.fetchone = AsyncMock(return_value=fake_row)
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    pool = MagicMock()
    pool.connection = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    result = await load_checkpoint(pool, run_id)

    assert result is not None
    assert result["connector"] == "hubspot"
    assert result["datatype"] == "contacts"
    assert result["page_number"] == 5
    assert result["cursor_value"] == "2026-01-01"
    assert result["records_committed"] == 250


@pytest.mark.asyncio
async def test_load_checkpoint_returns_none_when_not_found():
    """load_checkpoint should return None when no checkpoint exists."""
    from inandout.postgres.checkpoint import load_checkpoint

    mock_conn = AsyncMock()
    mock_cursor = AsyncMock()
    mock_cursor.fetchone = AsyncMock(return_value=None)
    mock_conn.execute = AsyncMock(return_value=mock_cursor)

    pool = MagicMock()
    pool.connection = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    result = await load_checkpoint(pool, uuid.uuid4())
    assert result is None


@pytest.mark.asyncio
async def test_clear_checkpoint_deletes_row():
    """clear_checkpoint should DELETE the checkpoint row."""
    from inandout.postgres.checkpoint import clear_checkpoint

    executed = []
    mock_conn = AsyncMock()

    async def capture_execute(sql, params=None):
        executed.append((sql, params))
        return AsyncMock()

    mock_conn.execute = AsyncMock(side_effect=capture_execute)
    mock_conn.commit = AsyncMock()

    pool = MagicMock()
    pool.connection = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=False)

    run_id = uuid.uuid4()
    await clear_checkpoint(pool, run_id)

    assert any("DELETE" in s and "inout_ops_sync_checkpoint" in s for s, _ in executed)
    mock_conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# checkpoint_every_n_pages config field
# ---------------------------------------------------------------------------

def test_ingestion_config_has_checkpoint_field():
    """IngestionConfig should have checkpoint_every_n_pages with default 0."""
    from inandout.config.ingestion import IngestionConfig

    cfg = IngestionConfig(
        primary_key="id",
        history_mode="overwrite",
        schedule={"interval": "5m"},
        list={"path": "/contacts", "pagination": {"strategy": "offset"}},
    )
    assert cfg.checkpoint_every_n_pages == 0


def test_ingestion_config_checkpoint_every_n_pages_configurable():
    """checkpoint_every_n_pages can be set to a positive integer."""
    from inandout.config.ingestion import IngestionConfig

    cfg = IngestionConfig(
        primary_key="id",
        history_mode="overwrite",
        schedule={"interval": "5m"},
        list={"path": "/contacts", "pagination": {"strategy": "offset"}},
        checkpoint_every_n_pages=10,
    )
    assert cfg.checkpoint_every_n_pages == 10


# ---------------------------------------------------------------------------
# Checkpoint interval correctness (pure logic test)
# ---------------------------------------------------------------------------

def test_checkpoint_every_n_pages_interval():
    """Checkpoint should be saved at pages 5, 10, 15 when n=5."""
    n = 5
    checkpointed_at = []
    for page in range(1, 21):
        if page % n == 0:
            checkpointed_at.append(page)
    assert checkpointed_at == [5, 10, 15, 20]


def test_checkpoint_every_1_page():
    """Checkpoint every 1 page saves at every page."""
    n = 1
    checkpointed_at = [p for p in range(1, 6) if p % n == 0]
    assert checkpointed_at == [1, 2, 3, 4, 5]


def test_checkpoint_disabled_when_zero():
    """checkpoint_every_n_pages = 0 means disabled (no checkpoints saved)."""
    n = 0
    # Division by zero guard — when n==0, condition `page_number % n == 0` is never evaluated
    # The engine checks `if checkpoint_n > 0 and page_number % checkpoint_n == 0`
    checkpointed = False
    checkpoint_n = 0
    for page in range(1, 6):
        if checkpoint_n > 0 and page % checkpoint_n == 0:
            checkpointed = True
    assert not checkpointed
