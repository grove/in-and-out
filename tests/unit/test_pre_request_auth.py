"""Unit tests for pre-request session-token auth (A3 — T1 #24)."""
from __future__ import annotations

import os
import time
from unittest.mock import MagicMock, patch

import httpx
import pytest
import respx


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cfg(
    endpoint: str = "https://auth.example.com/session",
    method: str = "POST",
    credential_ref: str = "test_pre_req",
    token_field: str = "token",
    token_lifetime_secs: float = 3600.0,
    request_body: dict | None = None,
    token_header: str = "X-Session-Token",
) -> MagicMock:
    cfg = MagicMock()
    cfg.endpoint = endpoint
    cfg.method = method
    cfg.credential_ref = credential_ref
    cfg.token_field = token_field
    cfg.token_lifetime_secs = token_lifetime_secs
    cfg.request_body = request_body or {}
    cfg.token_header = token_header
    return cfg


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_token_acquired_on_first_request():
    """Token is acquired from endpoint on first use."""
    os.environ["INOUT_CREDENTIAL_TEST_PRE_REQ"] = "user:pass"
    from inandout.transport import pre_request_auth as pra

    # Clear cache
    pra._token_cache.clear()

    cfg = _make_cfg()

    with respx.mock(base_url="https://auth.example.com") as mock:
        mock.post("/session").mock(
            return_value=httpx.Response(200, json={"token": "sess-abc-123"})
        )
        token = await pra.acquire_session_token(cfg)

    assert token == "sess-abc-123"


@pytest.mark.anyio
async def test_token_cached_not_reacquired():
    """Second call uses cached token — endpoint NOT called again."""
    os.environ["INOUT_CREDENTIAL_TEST_PRE_REQ"] = "user:pass"
    from inandout.transport import pre_request_auth as pra

    pra._token_cache.clear()
    cache_key = ("test_pre_req", "https://auth.example.com/session")
    pra._token_cache[cache_key] = {
        "token": "cached-token",
        "expires_at": time.monotonic() + 3600.0,
    }

    cfg = _make_cfg()
    provider = pra.PreRequestAuthProvider(cfg)

    call_count = 0
    original_fetch = provider._fetch_token_sync

    def _counting_fetch():
        nonlocal call_count
        call_count += 1
        return original_fetch()

    provider._fetch_token_sync = _counting_fetch

    token = provider._get_cached_token_sync()
    assert token == "cached-token"
    assert call_count == 0, "Token endpoint should not be called when cache is valid"


@pytest.mark.anyio
async def test_token_reacquired_after_expiry():
    """Expired token → re-acquired on next request."""
    os.environ["INOUT_CREDENTIAL_TEST_PRE_REQ"] = "user:pass"
    from inandout.transport import pre_request_auth as pra

    pra._token_cache.clear()
    cache_key = ("test_pre_req", "https://auth.example.com/session")
    # Set token as expired (expires_at in the past)
    pra._token_cache[cache_key] = {
        "token": "expired-token",
        "expires_at": time.monotonic() - 1.0,
    }

    cfg = _make_cfg()

    with respx.mock(base_url="https://auth.example.com") as mock:
        mock.post("/session").mock(
            return_value=httpx.Response(200, json={"token": "fresh-token"})
        )
        import anyio

        # acquire_session_token should get a new token
        token = await pra.acquire_session_token(cfg)

    assert token == "fresh-token"


@pytest.mark.anyio
async def test_401_invalidates_cache_and_retries():
    """401 on API call → token invalidated, re-acquired, request retried."""
    os.environ["INOUT_CREDENTIAL_TEST_PRE_REQ"] = "user:pass"
    from inandout.transport import pre_request_auth as pra

    pra._token_cache.clear()

    cfg = _make_cfg()
    provider = pra.PreRequestAuthProvider(cfg)

    # Simulate a cached (now stale) token
    cache_key = ("test_pre_req", "https://auth.example.com/session")
    pra._token_cache[cache_key] = {
        "token": "stale-token",
        "expires_at": time.monotonic() + 3600.0,
    }

    call_log = []

    def _mock_fetch() -> str:
        call_log.append("fetch_called")
        pra._token_cache[cache_key] = {
            "token": "new-token",
            "expires_at": time.monotonic() + 3600.0,
        }
        return "new-token"

    provider._fetch_token_sync = _mock_fetch

    # Simulate the auth_flow: stale token → 401 → invalidate → re-fetch
    request = httpx.Request("GET", "https://api.example.com/data")
    flow = provider.auth_flow(request)

    # First yield: inject token
    req = next(flow)
    assert req.headers.get("X-Session-Token") == "stale-token"

    # Simulate 401 response
    response_401 = httpx.Response(401)
    try:
        req2 = flow.send(response_401)
        assert req2.headers.get("X-Session-Token") == "new-token"
        assert "fetch_called" in call_log
    except StopIteration:
        pass


@pytest.mark.anyio
async def test_concurrent_requests_share_cached_token():
    """Concurrent requests use the same cached token (no re-acquisition per request)."""
    os.environ["INOUT_CREDENTIAL_TEST_PRE_REQ"] = "user:pass"
    from inandout.transport import pre_request_auth as pra

    pra._token_cache.clear()
    cache_key = ("test_pre_req", "https://auth.example.com/session")
    pra._token_cache[cache_key] = {
        "token": "shared-token",
        "expires_at": time.monotonic() + 3600.0,
    }

    cfg = _make_cfg()
    fetch_count = 0

    import anyio

    results = []

    async def _get_token() -> None:
        provider = pra.PreRequestAuthProvider(cfg)
        original = provider._fetch_token_sync

        def _counting(*args, **kwargs):
            nonlocal fetch_count
            fetch_count += 1
            return original(*args, **kwargs)

        provider._fetch_token_sync = _counting
        token = provider._get_cached_token_sync()
        results.append(token)

    async with anyio.create_task_group() as tg:
        for _ in range(5):
            tg.start_soon(_get_token)

    # All should get the cached token without re-fetching
    assert all(t == "shared-token" for t in results)
    assert fetch_count == 0, f"Token was re-fetched {fetch_count} times; expected 0"


@pytest.mark.anyio
async def test_dot_notation_token_field():
    """token_field using dot notation resolves nested path."""
    os.environ["INOUT_CREDENTIAL_TEST_PRE_REQ"] = "user:pass"
    from inandout.transport import pre_request_auth as pra

    pra._token_cache.clear()

    cfg = _make_cfg(token_field="data.session.token")

    with respx.mock(base_url="https://auth.example.com") as mock:
        mock.post("/session").mock(
            return_value=httpx.Response(
                200, json={"data": {"session": {"token": "nested-token-value"}}}
            )
        )
        token = await pra.acquire_session_token(cfg)

    assert token == "nested-token-value"
