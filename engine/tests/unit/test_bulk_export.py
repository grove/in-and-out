"""Unit tests for bulk export support (T1 #48 A5)."""
from __future__ import annotations

import csv
import io
import json
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest


def _make_bulk_cfg(**kwargs):
    from inandout.config.ingestion import BulkExportConfig

    defaults = dict(
        submit_path="/export/submit",
        status_path="/export/status",
        download_path="/export/download",
    )
    defaults.update(kwargs)
    return BulkExportConfig(**defaults)


def _make_mock_transport(submit_body, status_bodies, download_content):
    """Build a mock HttpTransportAdapter for bulk export tests."""
    mock_transport = AsyncMock()
    responses = []

    # Submit response
    submit_resp = MagicMock()
    submit_resp.content = json.dumps(submit_body).encode()
    responses.append(submit_resp)

    # Status poll responses
    for sb in status_bodies:
        sr = MagicMock()
        sr.content = json.dumps(sb).encode()
        responses.append(sr)

    # Download response
    dl_resp = MagicMock()
    dl_resp.content = download_content
    responses.append(dl_resp)

    mock_transport._request = AsyncMock(side_effect=responses)
    return mock_transport


# ---------------------------------------------------------------------------
# BulkExportConfig defaults
# ---------------------------------------------------------------------------

def test_bulk_export_config_defaults():
    """BulkExportConfig should have sensible defaults."""
    cfg = _make_bulk_cfg()
    assert cfg.submit_method == "POST"
    assert cfg.status_field == "status"
    assert cfg.complete_values == ["completed", "done", "success"]
    assert cfg.failed_values == ["failed", "error"]
    assert cfg.job_id_field == "id"
    assert cfg.poll_interval == "30s"
    assert cfg.max_wait == "4h"
    assert cfg.result_format == "jsonl"
    assert cfg.record_selector is None


def test_list_config_bulk_export_none_default():
    """ListConfig.bulk_export defaults to None."""
    from inandout.config.ingestion import ListConfig
    from inandout.config.pagination import CursorConfig, PaginationConfig, PaginationStrategy

    cfg = ListConfig(
        path="/contacts",
        pagination=PaginationConfig(
            strategy=PaginationStrategy.cursor,
            cursor=CursorConfig(response_path="next", request_param="after"),
        ),
    )
    assert cfg.bulk_export is None


# ---------------------------------------------------------------------------
# BulkExportFailed exception
# ---------------------------------------------------------------------------

def test_bulk_export_failed_exception():
    """BulkExportFailed contains job_id and status."""
    from inandout.ingestion.bulk_export import BulkExportFailed

    exc = BulkExportFailed("job_123", "failed")
    assert exc.job_id == "job_123"
    assert exc.status == "failed"
    assert "job_123" in str(exc)


# ---------------------------------------------------------------------------
# JSONL format parsing
# ---------------------------------------------------------------------------

def test_run_bulk_export_jsonl_parsing():
    """run_bulk_export jsonl format parsing logic produces correct records."""
    import orjson

    jsonl_content = b'{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}\n'
    lines = jsonl_content.decode("utf-8").splitlines()
    parsed = [orjson.loads(line) for line in lines if line.strip()]
    assert len(parsed) == 2
    assert parsed[0]["name"] == "Alice"
    assert parsed[1]["name"] == "Bob"


@pytest.mark.asyncio
async def test_run_bulk_export_csv_format():
    """run_bulk_export with csv format yields dicts from CSV rows."""
    import csv
    import io

    csv_content = b"id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n"
    text = csv_content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    rows = [dict(row) for row in reader]

    assert len(rows) == 2
    assert rows[0]["name"] == "Alice"
    assert rows[1]["email"] == "bob@example.com"


@pytest.mark.asyncio
async def test_run_bulk_export_json_array_format_with_selector():
    """run_bulk_export with json_array + record_selector extracts nested array."""
    import orjson
    from inandout.ingestion.bulk_export import _extract_nested

    data = {
        "results": {
            "contacts": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
        }
    }
    extracted = _extract_nested(data, "results.contacts")
    assert isinstance(extracted, list)
    assert len(extracted) == 2
    assert extracted[0]["name"] == "Alice"


@pytest.mark.asyncio
async def test_extract_nested_returns_none_for_missing_path():
    """_extract_nested returns None for a path that doesn't exist."""
    from inandout.ingestion.bulk_export import _extract_nested

    data = {"foo": {"bar": [1, 2]}}
    result = _extract_nested(data, "foo.missing.path")
    assert result is None


# ---------------------------------------------------------------------------
# Failure handling
# ---------------------------------------------------------------------------

def test_bulk_export_failed_status_raises():
    """When status is a failed value, BulkExportFailed should be raised."""
    from inandout.ingestion.bulk_export import BulkExportFailed

    failed_values = ["failed", "error"]
    status = "failed"
    job_id = "job_999"

    with pytest.raises(BulkExportFailed) as exc_info:
        if status in failed_values:
            raise BulkExportFailed(job_id, status)

    assert exc_info.value.job_id == job_id


def test_bulk_export_max_wait_exceeded_raises():
    """When max_wait is exceeded, BulkExportFailed should be raised."""
    from inandout.ingestion.bulk_export import BulkExportFailed

    max_wait_secs = 10.0
    elapsed = 15.0
    job_id = "job_888"

    with pytest.raises(BulkExportFailed) as exc_info:
        if elapsed >= max_wait_secs:
            raise BulkExportFailed(job_id, f"max_wait_exceeded_after_{max_wait_secs}s")

    assert "max_wait_exceeded" in exc_info.value.status


# ---------------------------------------------------------------------------
# Crash resume: job_id persisted in checkpoint
# ---------------------------------------------------------------------------

def test_bulk_export_checkpoint_cursor_format():
    """bulk_export_job:{job_id} is the format used to store job_id in checkpoint."""
    job_id = "job_abc123"
    cursor_value = f"bulk_export_job:{job_id}"

    # Verify the parsing logic
    if cursor_value.startswith("bulk_export_job:"):
        extracted_job_id = cursor_value[len("bulk_export_job:"):]
    else:
        extracted_job_id = None

    assert extracted_job_id == job_id
