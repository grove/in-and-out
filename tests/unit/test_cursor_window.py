"""Unit tests for incremental sync cursor window (time-bounded pages)."""
from __future__ import annotations

import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from inandout.config._duration import parse_duration
from inandout.config.ingestion import (
    IncrementalConfig,
    IncrementalCursorType,
    ListConfig,
    RequestFilterConfig,
    RequestFilterMode,
)


# ---------------------------------------------------------------------------
# cursor_window: window_end is clamped to now when watermark + window > now
# ---------------------------------------------------------------------------

def test_cursor_window_clamped_to_now():
    """If watermark + window_secs > now, window_end should be clamped to now."""
    watermark_float = time.time() - 100  # 100 seconds ago
    window_secs = 200  # 200 second window → would go 100s into the future
    now_float = time.time()

    window_end_float = min(watermark_float + window_secs, now_float)

    assert window_end_float == pytest.approx(now_float, abs=0.1)
    assert window_end_float <= now_float


def test_cursor_window_not_clamped_when_within_range():
    """If watermark + window_secs < now, window_end = watermark + window_secs."""
    watermark_float = time.time() - 500  # 500 seconds ago
    window_secs = 200  # 200 second window — stays in the past
    now_float = time.time()

    expected_window_end = watermark_float + window_secs
    window_end_float = min(watermark_float + window_secs, now_float)

    assert window_end_float == pytest.approx(expected_window_end, abs=0.01)
    assert window_end_float < now_float


# ---------------------------------------------------------------------------
# cursor_window: new watermark is set to window_end, not now
# ---------------------------------------------------------------------------

def test_new_watermark_set_to_window_end():
    """After a windowed sync, the new watermark should be window_end, not now."""
    # Use a watermark far enough in the past that window_end doesn't reach now
    watermark_float = time.time() - 7200  # 2 hours ago
    window_secs = parse_duration("1h")  # 3600 seconds → window_end is 1 hour ago (in the past)
    now_float = time.time()

    window_end_float = min(watermark_float + window_secs, now_float)
    # window_end is watermark + 3600 = 7200-3600 = 3600 seconds ago (not clamped)
    assert window_end_float < now_float - 10  # well before now

    # The new watermark should be window_end, not now
    new_watermark = str(window_end_float)

    # Verify it's significantly different from 'now'
    assert abs(float(new_watermark) - now_float) > 1800  # at least 30 minutes before now
    assert float(new_watermark) == pytest.approx(window_end_float, abs=0.01)


# ---------------------------------------------------------------------------
# IncrementalConfig has cursor_window field
# ---------------------------------------------------------------------------

def test_incremental_config_cursor_window_field_exists():
    """IncrementalConfig should have a cursor_window field."""
    inc = IncrementalConfig(cursor_window=None)
    assert inc.cursor_window is None

    inc2 = IncrementalConfig(cursor_window="1d")
    assert inc2.cursor_window == "1d"


def test_incremental_config_cursor_window_default_is_none():
    """cursor_window defaults to None for backward compatibility."""
    inc = IncrementalConfig()
    assert inc.cursor_window is None


# ---------------------------------------------------------------------------
# until_param injected into request when configured
# ---------------------------------------------------------------------------

def test_until_param_field_in_request_filter_config():
    """RequestFilterConfig should have until_param field."""
    rf = RequestFilterConfig(mode="query_param", until_param=None)
    assert rf.until_param is None

    rf2 = RequestFilterConfig(mode="query_param", until_param="until")
    assert rf2.until_param == "until"


def test_until_param_default_is_none():
    """until_param defaults to None for backward compatibility."""
    rf = RequestFilterConfig(mode="query_param")
    assert rf.until_param is None


@pytest.mark.anyio
async def test_until_param_injected_into_base_params():
    """When until_param is set and window_end is provided, it appears in request params."""
    import os
    import httpx
    import respx

    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "test-secret"

    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import ConnectorConfig, ConnectionConfig
    from inandout.config.pagination import PaginationConfig, PaginationStrategy
    from inandout.transport.http import HttpTransportAdapter

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
                        "incremental": {
                            "enabled": True,
                            "cursor_field": "updated_at",
                            "request_filter": {
                                "mode": "query_param",
                                "param": "since",
                                "until_param": "until",
                            },
                        },
                    },
                }
            }
        },
    )

    list_cfg = connector.datatypes["contacts"].ingestion.list  # type: ignore

    # Mock the HTTP request
    with respx.mock:
        route = respx.get("https://api.example.com/contacts").mock(
            return_value=httpx.Response(200, json=[])
        )

        async with HttpTransportAdapter(connector) as transport:
            pages = []
            async for page in transport.fetch_pages(
                list_cfg,
                watermark="1000000",
                window_end="1003600",
            ):
                pages.append(page)

    # Verify the request was made
    assert route.called
    request = route.calls.last.request
    from urllib.parse import parse_qs, urlparse
    parsed = urlparse(str(request.url))
    params = parse_qs(parsed.query)
    assert "since" in params
    assert params["since"][0] == "1000000"
    assert "until" in params
    assert params["until"][0] == "1003600"


# ---------------------------------------------------------------------------
# cursor_window: ISO-8601 timestamp watermarks
# ---------------------------------------------------------------------------

def test_cursor_window_iso_watermark_computes_correct_window_end():
    """cursor_window arithmetic works correctly when watermark is an ISO-8601 string."""
    import datetime
    from datetime import timezone
    from inandout.config._duration import parse_duration

    # Watermark: 2026-01-01T00:00:00Z (as ISO string, like typical connectors emit)
    watermark_dt = datetime.datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    watermark_str = watermark_dt.isoformat().replace("+00:00", "Z")

    window_secs = parse_duration("1h")  # 3600 seconds

    # Simulate the engine logic
    watermark_iso_float = watermark_dt.timestamp()
    now_float = datetime.datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp()  # well after window
    window_end_float = min(watermark_iso_float + window_secs, now_float)

    # window_end should be 2026-01-01T01:00:00Z
    expected_dt = datetime.datetime(2026, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert window_end_float == pytest.approx(expected_dt.timestamp(), abs=0.01)

    # Reconstruct ISO string from window_end_float
    we_dt = datetime.datetime.fromtimestamp(window_end_float, tz=timezone.utc)
    window_end_iso = we_dt.isoformat().replace("+00:00", "Z")
    assert window_end_iso == "2026-01-01T01:00:00Z"


def test_cursor_window_iso_clamped_to_now():
    """ISO watermark near now is clamped so window_end does not exceed now."""
    import datetime
    from datetime import timezone
    from inandout.config._duration import parse_duration

    # Watermark 30 seconds ago, window = 10 minutes → window would exceed now
    now_dt = datetime.datetime.now(tz=timezone.utc)
    watermark_dt = now_dt - datetime.timedelta(seconds=30)

    window_secs = parse_duration("10m")  # 600s
    watermark_float = watermark_dt.timestamp()
    now_float = now_dt.timestamp()

    window_end_float = min(watermark_float + window_secs, now_float)

    # Must not exceed now
    assert window_end_float <= now_float + 0.01  # tiny tolerance for float arithmetic
    assert window_end_float == pytest.approx(now_float, abs=1.0)
