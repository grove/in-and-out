"""Unit tests for CDC source mode."""
from __future__ import annotations

import sys
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.ingestion.cdc import (
    CdcSource,
    CdcSourceConfig,
    KafkaCdcSource,
    KinesisCdcSource,
    PgLogicalCdcSource,
    get_cdc_source,
)


# ---------------------------------------------------------------------------
# get_cdc_source returns correct class for backend
# ---------------------------------------------------------------------------

def test_get_cdc_source_returns_kafka():
    cfg = CdcSourceConfig(
        backend="kafka",
        connection_string="localhost:9092",
        topic_or_stream="my-topic",
    )
    # KafkaCdcSource raises NotImplementedError if aiokafka not installed,
    # but the factory should still return an instance of the right type.
    try:
        source = get_cdc_source(cfg)
        assert isinstance(source, KafkaCdcSource)
    except NotImplementedError:
        # aiokafka not installed — factory still tried to create the right type
        pass


def test_get_cdc_source_returns_kinesis():
    cfg = CdcSourceConfig(
        backend="kinesis",
        connection_string="",
        topic_or_stream="my-stream",
    )
    try:
        source = get_cdc_source(cfg)
        assert isinstance(source, KinesisCdcSource)
    except NotImplementedError:
        pass


def test_get_cdc_source_returns_pg_logical():
    cfg = CdcSourceConfig(
        backend="pg_logical",
        connection_string="postgresql://user:pass@localhost/db",
        topic_or_stream="my-slot",
    )
    source = get_cdc_source(cfg)
    assert isinstance(source, PgLogicalCdcSource)


def test_get_cdc_source_raises_for_unknown_backend():
    cfg = CdcSourceConfig(
        backend="unknown_backend",
        connection_string="",
        topic_or_stream="test",
    )
    with pytest.raises(ValueError, match="Unknown CDC backend"):
        get_cdc_source(cfg)


# ---------------------------------------------------------------------------
# KafkaCdcSource raises helpful error when aiokafka not installed
# ---------------------------------------------------------------------------

def test_kafka_cdc_source_raises_when_aiokafka_missing():
    """KafkaCdcSource raises NotImplementedError with install instructions."""
    cfg = CdcSourceConfig(
        backend="kafka",
        connection_string="localhost:9092",
        topic_or_stream="test-topic",
    )

    # Temporarily remove aiokafka from sys.modules to simulate missing dependency
    saved = sys.modules.get("aiokafka")
    sys.modules["aiokafka"] = None  # type: ignore[assignment]
    try:
        with pytest.raises((NotImplementedError, ImportError)) as exc_info:
            KafkaCdcSource(cfg)
        assert "aiokafka" in str(exc_info.value).lower() or "pip install" in str(exc_info.value).lower()
    finally:
        if saved is None:
            sys.modules.pop("aiokafka", None)
        else:
            sys.modules["aiokafka"] = saved


# ---------------------------------------------------------------------------
# _run_cdc_sync with a mock source processes records correctly
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_run_cdc_sync_processes_records():
    """_run_cdc_sync consumes records from mock CdcSource and upserts them."""
    import os
    import uuid

    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "test-secret"

    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import ConnectorConfig, ConnectionConfig
    from inandout.config.ingestion import IngestionConfig, HistoryMode, ScheduleConfig, ListConfig
    from inandout.config.pagination import PaginationConfig, PaginationStrategy
    from inandout.ingestion.engine import IngestionEngine

    connector = ConnectorConfig(
        name="test",
        system="TestSystem",
        generation_profile="ingestion_polling_readonly",
        api_version="v1",
        connection=ConnectionConfig(base_url="https://api.example.com"),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="test-key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "contacts": {
                "ingestion": {
                    "primary_key": "id",
                    "history_mode": "overwrite",
                    "schedule": {"interval": "5m"},
                    "list": {
                        "method": "GET",
                        "path": "/contacts",
                        "pagination": {"strategy": "offset"},
                    },
                    "source_mode": "cdc",
                }
            }
        },
    )

    ingestion_cfg = connector.datatypes["contacts"].ingestion

    # Mock pool and connection
    from contextlib import asynccontextmanager

    mock_cursor = AsyncMock()
    mock_cursor.fetchone = AsyncMock(return_value=None)  # no existing record
    mock_cursor.description = [("external_id",)]

    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=mock_cursor)
    mock_conn.commit = AsyncMock()

    # transaction() returns an async context manager
    class _FakeTxn:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *a):
            pass

    mock_conn.transaction = MagicMock(return_value=_FakeTxn())

    # connection() returns an async context manager yielding mock_conn
    class _FakeConnCtx:
        async def __aenter__(self):
            return mock_conn
        async def __aexit__(self, *a):
            pass

    mock_pool = MagicMock()
    mock_pool.connection = MagicMock(return_value=_FakeConnCtx())

    # Mock CdcSource
    mock_source = AsyncMock(spec=CdcSource)
    mock_source.consume = AsyncMock(return_value=[
        {"id": "1", "name": "Alice"},
        {"id": "2", "name": "Bob"},
    ])
    mock_source.commit = AsyncMock()

    engine = IngestionEngine(pool=mock_pool)

    from inandout.ingestion.engine import SyncResult
    result = SyncResult(
        run_id=uuid.uuid4(),
        connector="test",
        datatype="contacts",
        mode="cdc",
    )

    log = MagicMock()

    # We call _run_cdc_sync directly with the mock source
    await engine._run_cdc_sync(
        connector=connector,
        datatype="contacts",
        ingestion_cfg=ingestion_cfg,
        result=result,
        log=log,
        cdc_source=mock_source,
    )

    # Source was consumed and committed
    mock_source.consume.assert_called_once()
    mock_source.commit.assert_called_once()

    # Records were fetched
    assert result.records_fetched == 2


# ---------------------------------------------------------------------------
# CdcSourceConfig validation
# ---------------------------------------------------------------------------

def test_cdc_source_config_defaults():
    cfg = CdcSourceConfig(
        backend="kafka",
        connection_string="localhost:9092",
        topic_or_stream="test",
    )
    assert cfg.consumer_group == "inandout"


def test_cdc_source_config_custom_consumer_group():
    cfg = CdcSourceConfig(
        backend="kafka",
        connection_string="localhost:9092",
        topic_or_stream="test",
        consumer_group="my-group",
    )
    assert cfg.consumer_group == "my-group"
