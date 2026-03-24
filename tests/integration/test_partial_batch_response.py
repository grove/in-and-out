"""Integration tests for T2 #29: Partial-success batch response handling.

Some external APIs accept batch write requests and return mixed success/failure
responses — for example, an HTTP 207 Multi-Status with a body enumerating
per-record outcomes.  The writeback engine must parse these to classify each
record individually: retry only the failed records, not the whole batch.

GOAL.md T2 #29: "Some external APIs accept batch write requests and return mixed
success/failure responses — for example, an HTTP 200 or 207 with a body
enumerating per-record outcomes. The writeback tool must declaratively parse
such responses to correctly classify per-record success and failure, retry only
the failed records, and record confirmed successes. Retrying the entire batch
on a mixed response is not acceptable."
"""
from __future__ import annotations

import os

import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import (
    ConnectorConfig,
    ConnectionConfig,
    DatatypeConfig,
    GenerationProfile,
)
from inandout.config.writeback import (
    BatchResponseConfig,
    ConflictResolution,
    OperationConfig,
    OperationsConfig,
    ProtectionLevel,
    UpdateOperationConfig,
    WritebackConfig,
)
from inandout.writeback.batch_response import extract_batch_errors, parse_batch_response
from inandout.writeback.engine import WritebackEngine


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_CONNECTOR = "batch_resp_test"
_DATATYPE = "orders"
_BASE_URL = "https://api.batch-resp-test.example.com"
_DELTA_TABLE = f"inout_delta_{_CONNECTOR}_{_DATATYPE}"
os.environ["INOUT_CREDENTIAL_BATCH_RESP_TEST_KEY"] = "dummy"

# BatchResponseConfig: results array in response body at "results",
# each item has "id" (record_id) and "status" ("ok" = success, else failure).
_BATCH_CFG = BatchResponseConfig(
    success_path="results",
    record_id_path="id",
    status_path="status",
    success_statuses=["ok"],
    error_path="message",
)


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="BatchRespSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="batch_resp_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["update"],
                    enable_crash_recovery=False,
                    max_retry_count=0,
                    batch_response=_BATCH_CFG,
                    # idempotency_key_header forces the engine to use _raw_request()
                    # (not _request()) so the response body is captured for batch parsing.
                    idempotency_key_header="Idempotency-Key",
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/{_DATATYPE}/${{external_id}}"),
                        insert=OperationConfig(method="POST", path=f"/v1/{_DATATYPE}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/{_DATATYPE}/${{external_id}}"),
                    ),
                )
            )
        },
    )


async def _setup(pool) -> None:
    async with pool.connection() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {_DELTA_TABLE} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'update'
            )
        """)
        await conn.commit()


async def _clear(pool) -> None:
    async with pool.connection() as conn:
        try:
            await conn.execute(f"DELETE FROM {_DELTA_TABLE}")
        except Exception:
            pass
        await conn.commit()


async def _insert(pool, external_id: str, name: str, action: str = "update") -> None:
    async with pool.connection() as conn:
        await conn.execute(
            f"INSERT INTO {_DELTA_TABLE} (external_id, name, _action) VALUES (%s, %s, %s)",
            [external_id, name, action],
        )
        await conn.commit()


# ---------------------------------------------------------------------------
# Unit-style tests for the parse_batch_response / extract_batch_errors helpers
# ---------------------------------------------------------------------------


def test_parse_batch_response_success_and_failure():
    """T2 #29: parse_batch_response correctly classifies per-record outcomes."""
    response_body = {
        "results": [
            {"id": "order-001", "status": "ok"},
            {"id": "order-002", "status": "error", "message": "Validation failed"},
            {"id": "order-003", "status": "ok"},
        ]
    }

    outcomes = parse_batch_response(response_body, _BATCH_CFG)
    assert outcomes["order-001"] is True
    assert outcomes["order-002"] is False
    assert outcomes["order-003"] is True


def test_extract_batch_errors_returns_failed_with_message():
    """T2 #29: extract_batch_errors returns {id: error_message} for failed records."""
    response_body = {
        "results": [
            {"id": "order-004", "status": "ok"},
            {"id": "order-005", "status": "error", "message": "Quota exceeded"},
        ]
    }

    cfg = BatchResponseConfig(
        success_path="results",
        record_id_path="id",
        status_path="status",
        success_statuses=["ok"],
        error_path="message",
    )
    errors = extract_batch_errors(response_body, cfg)
    assert "order-005" in errors
    assert "quota" in errors["order-005"].lower() or "exceeded" in errors["order-005"].lower()


def test_parse_batch_response_empty_success_path_returns_empty():
    """T2 #29: when the success_path points to a non-array, result is empty dict."""
    response_body = {"status": "ok"}  # no results array

    outcomes = parse_batch_response(response_body, _BATCH_CFG)
    assert outcomes == {}


# ---------------------------------------------------------------------------
# Integration-level tests: batch response wired into the writeback engine
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_batch_response_failed_record_counted_as_failed(pool, run_migrations):
    """T2 #29: when the batch response marks a record as failed, the engine
    increments result.failed and does NOT count it as processed.

    The API returns HTTP 200 but the body's 'results' array reports the
    record as status='error'.
    """
    await _setup(pool)
    await _clear(pool)

    external_id = "order-batch-fail-1"
    await _insert(pool, external_id, "Important Order")

    connector = _make_connector()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    # 200 OK but per-record failure in the batch body
    batch_body = {
        "results": [
            {"id": external_id, "status": "error", "message": "Stock depleted"}
        ]
    }

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch(f"/v1/{_DATATYPE}/{external_id}").mock(
            return_value=httpx.Response(200, json=batch_body)
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, _DELTA_TABLE)

    # The write itself succeeded HTTP-wise but the batch result marks it failed
    assert result.failed >= 1, (
        f"Batch-reported failure must increment result.failed; got failed={result.failed}"
    )


@pytest.mark.anyio
async def test_batch_response_successful_record_counted_as_processed(pool, run_migrations):
    """T2 #29: when the batch response marks a record as successful, the engine
    counts it as processed (not failed).
    """
    await _setup(pool)
    await _clear(pool)

    external_id = "order-batch-ok-1"
    await _insert(pool, external_id, "OK Order")

    connector = _make_connector()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    batch_body = {
        "results": [
            {"id": external_id, "status": "ok"}
        ]
    }

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.patch(f"/v1/{_DATATYPE}/{external_id}").mock(
            return_value=httpx.Response(200, json=batch_body)
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, _DELTA_TABLE)

    assert result.failed == 0, (
        f"Batch-reported success must not increment result.failed; got failed={result.failed}"
    )
    assert result.processed >= 1, (
        f"Successful batch record must increment result.processed; got processed={result.processed}"
    )
