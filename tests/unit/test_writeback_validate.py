"""Unit tests for writeback validate control command (T2 #37 B3)."""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# validate command exists
# ---------------------------------------------------------------------------

def test_validate_cmd_method_exists():
    """ControlDispatcher has _cmd_validate_writeback method."""
    from inandout.engine.control import ControlDispatcher

    assert hasattr(ControlDispatcher, "_cmd_validate_writeback")
    assert callable(ControlDispatcher._cmd_validate_writeback)


@pytest.mark.asyncio
async def test_validate_requires_connector():
    """validate raises ValueError when connector is None."""
    from unittest.mock import MagicMock
    from inandout.engine.control import ControlDispatcher

    pool = MagicMock()
    paused: set = set()
    dispatcher = ControlDispatcher(pool, paused)

    with pytest.raises(ValueError, match="validate requires"):
        await dispatcher._cmd_validate_writeback(None, None, {}, engine=None)


@pytest.mark.asyncio
async def test_validate_returns_result_dict_structure():
    """validate returns a dict with expected keys."""
    from unittest.mock import MagicMock, AsyncMock, patch
    from inandout.engine.control import ControlDispatcher

    pool = MagicMock()
    paused: set = set()
    dispatcher = ControlDispatcher(pool, paused)

    # Payload has no base_url → connectivity unknown, errors populated
    result = await dispatcher._cmd_validate_writeback(
        "hubspot", "contacts",
        {},  # no base_url
        engine=None,
    )

    assert "connectivity" in result
    assert "auth" in result
    assert "field_mappings" in result
    assert "etag_support" in result
    assert "protection_level" in result
    assert "errors" in result
    assert isinstance(result["errors"], list)


@pytest.mark.asyncio
async def test_validate_no_base_url_returns_error_in_errors():
    """validate without base_url populates errors list."""
    from unittest.mock import MagicMock
    from inandout.engine.control import ControlDispatcher

    pool = MagicMock()
    paused: set = set()
    dispatcher = ControlDispatcher(pool, paused)

    result = await dispatcher._cmd_validate_writeback(
        "hubspot", "contacts", {}, engine=None
    )

    assert len(result["errors"]) > 0


@pytest.mark.asyncio
async def test_validate_connectivity_ok_with_200():
    """validate returns connectivity=ok when server returns 200."""
    from unittest.mock import MagicMock, AsyncMock, patch
    import httpx
    from inandout.engine.control import ControlDispatcher

    pool = MagicMock()
    paused: set = set()
    dispatcher = ControlDispatcher(pool, paused)

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {}

    mock_head_response = MagicMock()
    mock_head_response.headers = {"ETag": '"abc123"'}

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_response)
    mock_client.head = AsyncMock(return_value=mock_head_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("httpx.AsyncClient", return_value=mock_client):
        result = await dispatcher._cmd_validate_writeback(
            "hubspot", "contacts",
            {"base_url": "https://api.hubspot.com"},
            engine=None,
        )

    assert result["connectivity"] == "ok"
    assert result["auth"] == "ok"


@pytest.mark.asyncio
async def test_validate_etag_support_true_when_etag_header_present():
    """validate returns etag_support=True when ETag header is in response."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from inandout.engine.control import ControlDispatcher

    pool = MagicMock()
    paused: set = set()
    dispatcher = ControlDispatcher(pool, paused)

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {}

    mock_head_response = MagicMock()
    mock_head_response.headers = {"ETag": '"strongetag"'}

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_response)
    mock_client.head = AsyncMock(return_value=mock_head_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("httpx.AsyncClient", return_value=mock_client):
        result = await dispatcher._cmd_validate_writeback(
            "hubspot", "contacts",
            {"base_url": "https://api.hubspot.com"},
            engine=None,
        )

    assert result["etag_support"] is True
    assert result["protection_level"] == "full"


@pytest.mark.asyncio
async def test_validate_etag_support_false_when_no_etag():
    """validate returns etag_support=False when no ETag in response."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from inandout.engine.control import ControlDispatcher

    pool = MagicMock()
    paused: set = set()
    dispatcher = ControlDispatcher(pool, paused)

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.headers = {}

    mock_head_response = MagicMock()
    mock_head_response.headers = {}  # No ETag

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_response)
    mock_client.head = AsyncMock(return_value=mock_head_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("httpx.AsyncClient", return_value=mock_client):
        result = await dispatcher._cmd_validate_writeback(
            "hubspot", "contacts",
            {"base_url": "https://api.hubspot.com"},
            engine=None,
        )

    assert result["etag_support"] is False
    assert result["protection_level"] == "practical"


@pytest.mark.asyncio
async def test_validate_auth_failed_on_401():
    """validate returns auth=failed when server returns 401."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from inandout.engine.control import ControlDispatcher

    pool = MagicMock()
    paused: set = set()
    dispatcher = ControlDispatcher(pool, paused)

    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_response.headers = {}

    mock_head_response = MagicMock()
    mock_head_response.headers = {}

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_response)
    mock_client.head = AsyncMock(return_value=mock_head_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("httpx.AsyncClient", return_value=mock_client):
        result = await dispatcher._cmd_validate_writeback(
            "hubspot", "contacts",
            {"base_url": "https://api.hubspot.com"},
            engine=None,
        )

    assert result["auth"] == "failed"
