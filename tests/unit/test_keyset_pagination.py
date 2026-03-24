"""Unit tests for T2 #12 — keyset / seek pagination strategy."""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Config model tests
# ---------------------------------------------------------------------------

def test_keyset_strategy_in_enum():
    """PaginationStrategy includes 'keyset'."""
    from inandout.config.pagination import PaginationStrategy
    assert PaginationStrategy.keyset == "keyset"


def test_keyset_config_defaults():
    """KeysetConfig has correct defaults."""
    from inandout.config.pagination import KeysetConfig
    cfg = KeysetConfig(keyset_field="id", request_param="after")
    assert cfg.page_size == 100
    assert cfg.page_size_param == "limit"


def test_keyset_config_custom_values():
    """KeysetConfig accepts custom page_size and page_size_param."""
    from inandout.config.pagination import KeysetConfig
    cfg = KeysetConfig(keyset_field="created_at", request_param="since", page_size=50, page_size_param="page_size")
    assert cfg.keyset_field == "created_at"
    assert cfg.request_param == "since"
    assert cfg.page_size == 50
    assert cfg.page_size_param == "page_size"


def test_keyset_config_extra_fields_forbidden():
    """KeysetConfig extra fields are forbidden."""
    from pydantic import ValidationError
    from inandout.config.pagination import KeysetConfig
    with pytest.raises(ValidationError):
        KeysetConfig(keyset_field="id", request_param="after", unknown="bad")


def test_pagination_config_keyset_field_is_none_by_default():
    """PaginationConfig.keyset defaults to None for non-keyset strategies."""
    from inandout.config.pagination import PaginationConfig, PaginationStrategy, CursorConfig
    cfg = PaginationConfig(
        strategy=PaginationStrategy.cursor,
        cursor=CursorConfig(response_path="next_cursor", request_param="cursor"),
    )
    assert cfg.keyset is None


def test_pagination_config_accepts_keyset():
    """PaginationConfig can hold a KeysetConfig."""
    from inandout.config.pagination import PaginationConfig, PaginationStrategy, KeysetConfig
    cfg = PaginationConfig(
        strategy=PaginationStrategy.keyset,
        keyset=KeysetConfig(keyset_field="id", request_param="after"),
    )
    assert cfg.keyset is not None
    assert cfg.keyset.keyset_field == "id"


# ---------------------------------------------------------------------------
# Transport: fetch_pages with keyset strategy
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_keyset_single_full_page_then_partial():
    """Keyset: fetches page 1 (full), then page 2 (partial), then stops."""
    from unittest.mock import AsyncMock, MagicMock, patch
    import orjson

    page1 = [{"id": str(i), "name": f"rec-{i}"} for i in range(1, 11)]   # 10 records (full)
    page2 = [{"id": "11", "name": "rec-11"}, {"id": "12", "name": "rec-12"}]  # 2 records (partial)

    call_count = 0
    expected_params: list[dict] = []

    async def _mock_request(method, path, params=None, **kwargs):
        nonlocal call_count
        call_count += 1
        expected_params.append(dict(params or {}))
        resp = MagicMock()
        resp.content = orjson.dumps(page1 if call_count == 1 else page2)
        return resp

    from inandout.config.pagination import PaginationConfig, PaginationStrategy, KeysetConfig
    from inandout.config.ingestion import ListConfig

    list_cfg = ListConfig(
        path="/records",
        record_selector=None,
        pagination=PaginationConfig(
            strategy=PaginationStrategy.keyset,
            keyset=KeysetConfig(keyset_field="id", request_param="after", page_size=10),
        ),
    )

    with patch("inandout.transport.http.HttpTransportAdapter.__init__", return_value=None):
        from inandout.transport.http import HttpTransportAdapter
        transport = HttpTransportAdapter.__new__(HttpTransportAdapter)
        transport._client = None
        transport._request = _mock_request

        pages = []
        async for page in transport.fetch_pages(list_cfg):
            pages.append(page)

    assert len(pages) == 2
    assert pages[0] == page1
    assert pages[1] == page2
    # First call has no "after" param; second call has after="10" (last id in page1)
    assert "after" not in expected_params[0]
    assert expected_params[1]["after"] == "10"


@pytest.mark.asyncio
async def test_keyset_stops_when_keyset_field_missing():
    """Keyset loop stops if records in the page don't contain the keyset field."""
    from unittest.mock import MagicMock, patch
    import orjson

    page1 = [{"name": "rec-1"}, {"name": "rec-2"}]  # no "id" field → can't advance

    call_count = 0

    async def _mock_request(method, path, params=None, **kwargs):
        nonlocal call_count
        call_count += 1
        resp = MagicMock()
        resp.content = orjson.dumps(page1)
        return resp

    from inandout.config.pagination import PaginationConfig, PaginationStrategy, KeysetConfig
    from inandout.config.ingestion import ListConfig

    list_cfg = ListConfig(
        path="/records",
        record_selector=None,
        pagination=PaginationConfig(
            strategy=PaginationStrategy.keyset,
            keyset=KeysetConfig(keyset_field="id", request_param="after", page_size=10),
        ),
    )

    with patch("inandout.transport.http.HttpTransportAdapter.__init__", return_value=None):
        from inandout.transport.http import HttpTransportAdapter
        transport = HttpTransportAdapter.__new__(HttpTransportAdapter)
        transport._client = None
        transport._request = _mock_request

        pages = []
        async for page in transport.fetch_pages(list_cfg):
            pages.append(page)

    # Only 1 page fetched; loop stops because last record has no keyset_field
    assert len(pages) == 1
    assert call_count == 1


@pytest.mark.asyncio
async def test_keyset_full_three_pages():
    """Keyset: three full pages followed by empty termination."""
    from unittest.mock import MagicMock, patch
    import orjson

    pages_data = [
        [{"id": str(i)} for i in range(1, 6)],   # 5 records (full, page_size=5)
        [{"id": str(i)} for i in range(6, 11)],  # 5 records (full)
        [{"id": str(i)} for i in range(11, 14)], # 3 records (partial → stop)
    ]
    call_count = 0

    async def _mock_request(method, path, params=None, **kwargs):
        nonlocal call_count
        resp = MagicMock()
        resp.content = orjson.dumps(pages_data[call_count])
        call_count += 1
        return resp

    from inandout.config.pagination import PaginationConfig, PaginationStrategy, KeysetConfig
    from inandout.config.ingestion import ListConfig

    list_cfg = ListConfig(
        path="/records",
        record_selector=None,
        pagination=PaginationConfig(
            strategy=PaginationStrategy.keyset,
            keyset=KeysetConfig(keyset_field="id", request_param="after", page_size=5),
        ),
    )

    with patch("inandout.transport.http.HttpTransportAdapter.__init__", return_value=None):
        from inandout.transport.http import HttpTransportAdapter
        transport = HttpTransportAdapter.__new__(HttpTransportAdapter)
        transport._client = None
        transport._request = _mock_request

        all_pages = []
        async for page in transport.fetch_pages(list_cfg):
            all_pages.append(page)

    assert len(all_pages) == 3
    assert all_pages[0] == pages_data[0]
    assert all_pages[1] == pages_data[1]
    assert all_pages[2] == pages_data[2]
