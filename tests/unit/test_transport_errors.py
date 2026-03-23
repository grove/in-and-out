"""Unit tests for HTTP error classification and retry logic."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest
import respx

from inandout.transport.errors import (
    ErrorClass,
    classify_http_error,
    classify_request_error,
    is_retryable,
    retry_after_seconds,
)


def _make_status_error(status_code: int, headers: dict | None = None) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://api.example.com/v1/test")
    response = httpx.Response(status_code, headers=headers or {}, request=request)
    return httpx.HTTPStatusError(str(status_code), request=request, response=response)


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------

def test_429_is_rate_limit():
    exc = _make_status_error(429)
    assert classify_http_error(exc) == ErrorClass.rate_limit


def test_401_is_auth():
    exc = _make_status_error(401)
    assert classify_http_error(exc) == ErrorClass.auth


def test_403_is_auth():
    exc = _make_status_error(403)
    assert classify_http_error(exc) == ErrorClass.auth


def test_422_is_data_error():
    exc = _make_status_error(422)
    assert classify_http_error(exc) == ErrorClass.data_error


def test_400_is_data_error():
    exc = _make_status_error(400)
    assert classify_http_error(exc) == ErrorClass.data_error


def test_404_is_data_error():
    exc = _make_status_error(404)
    assert classify_http_error(exc) == ErrorClass.data_error


def test_409_is_data_error():
    exc = _make_status_error(409)
    assert classify_http_error(exc) == ErrorClass.data_error


def test_500_is_transient():
    exc = _make_status_error(500)
    assert classify_http_error(exc) == ErrorClass.transient


def test_503_is_transient():
    exc = _make_status_error(503)
    assert classify_http_error(exc) == ErrorClass.transient


def test_connect_timeout_is_transient():
    request = httpx.Request("GET", "https://api.example.com/")
    exc = httpx.ConnectTimeout("timed out", request=request)
    assert classify_request_error(exc) == ErrorClass.transient


def test_is_retryable_connect_timeout():
    request = httpx.Request("GET", "https://api.example.com/")
    exc = httpx.ConnectTimeout("timed out", request=request)
    assert is_retryable(exc) is True


def test_is_not_retryable_422():
    exc = _make_status_error(422)
    assert is_retryable(exc) is False


def test_is_not_retryable_401():
    exc = _make_status_error(401)
    assert is_retryable(exc) is False


def test_retry_after_seconds_numeric():
    exc = _make_status_error(429, headers={"Retry-After": "30"})
    assert retry_after_seconds(exc) == 30.0


def test_retry_after_seconds_date_fallback():
    exc = _make_status_error(429, headers={"Retry-After": "Mon, 01 Jan 2026 00:00:00 GMT"})
    assert retry_after_seconds(exc) == 60.0


def test_retry_after_seconds_missing():
    exc = _make_status_error(429)
    assert retry_after_seconds(exc) is None


# ---------------------------------------------------------------------------
# Retry logic in HttpTransportAdapter
# ---------------------------------------------------------------------------

import os

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
from inandout.transport.http import HttpTransportAdapter


def _make_test_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name="test",
        system="TestSystem",
        generation_profile=GenerationProfile.ingestion_polling_readonly,
        api_version="v1",
        connection=ConnectionConfig(base_url="https://api.example.com"),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            "items": DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="5m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/v1/items",
                            record_selector="results",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.cursor,
                                cursor=CursorConfig(
                                    request_param="cursor",
                                    response_path="next_cursor",
                                ),
                            ),
                        )
                    },
                )
            )
        },
    )


@pytest.mark.anyio
async def test_retry_on_503_then_200():
    """Mock httpx to return 503 twice then 200 — verify 200 response is returned."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"
    connector = _make_test_connector()

    call_count = 0

    def side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return httpx.Response(503, json={"error": "service unavailable"}, request=request)
        return httpx.Response(200, json={"results": [], "next_cursor": None})

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.get("/v1/test").mock(side_effect=side_effect)

        with patch("anyio.sleep", new_callable=AsyncMock) as mock_sleep:
            adapter = HttpTransportAdapter(connector, max_retries=5)
            async with adapter:
                resp = await adapter._request("GET", "/v1/test")

    assert resp.status_code == 200
    assert call_count == 3
    assert mock_sleep.call_count == 2


@pytest.mark.anyio
async def test_retry_on_429_with_retry_after():
    """Mock httpx to return 429 with Retry-After: 1 — verify it waits and retries."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"
    connector = _make_test_connector()

    call_count = 0

    def side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(
                429,
                headers={"Retry-After": "1"},
                json={"error": "rate limited"},
                request=request,
            )
        return httpx.Response(200, json={"results": []})

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.get("/v1/test").mock(side_effect=side_effect)

        with patch("anyio.sleep", new_callable=AsyncMock) as mock_sleep:
            adapter = HttpTransportAdapter(connector, max_retries=5)
            async with adapter:
                resp = await adapter._request("GET", "/v1/test")

    assert resp.status_code == 200
    assert call_count == 2
    # Sleep was called once with the Retry-After value (1 second, capped at 60)
    assert mock_sleep.call_count == 1
    assert mock_sleep.call_args[0][0] == 1.0


@pytest.mark.anyio
async def test_no_retry_on_422():
    """422 is a data error and should not be retried."""
    os.environ["INOUT_CREDENTIAL_TEST_KEY"] = "dummy"
    connector = _make_test_connector()

    call_count = 0

    def side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(422, json={"error": "unprocessable"}, request=request)

    with respx.mock(base_url="https://api.example.com", assert_all_called=False) as mock:
        mock.get("/v1/test").mock(side_effect=side_effect)

        adapter = HttpTransportAdapter(connector, max_retries=5)
        async with adapter:
            with pytest.raises(httpx.HTTPStatusError) as exc_info:
                await adapter._request("GET", "/v1/test")

    assert exc_info.value.response.status_code == 422
    assert call_count == 1  # no retries
