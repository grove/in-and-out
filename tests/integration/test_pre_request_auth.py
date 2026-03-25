"""Integration tests for pre-request session-token authentication (T1 #24).

Covers:
- Session token is acquired before the first API call and injected as X-Session-Token header
- Cached token is reused across multiple requests within a cycle (no re-acquisition)
- 401 response invalidates the cache, re-acquires a new token, and retries once
"""
from __future__ import annotations

import os
import time
import unittest.mock

import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.auth import PreRequestAuthConfig
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.writeback import (
    WritebackConfig,
    ProtectionLevel,
    ConflictResolution,
    OperationsConfig,
    OperationConfig,
    UpdateOperationConfig,
)
from inandout.transport.pre_request_auth import PreRequestAuthProvider, _token_cache
from inandout.writeback.engine import WritebackEngine

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_BASE_URL = "https://api.pre-req-test.example.com"
_AUTH_URL = "https://auth.pre-req-test.example.com/session"
_CONNECTOR = "pre_req_test"
_DATATYPE = "contacts"


@pytest.fixture(autouse=True)
def _clear_pre_request_cache():
    """Ensure pre-request token cache is clean before and after each test."""
    _token_cache.clear()
    yield
    _token_cache.clear()


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_PRE_REQ_TEST_CREDS"] = "testuser:testpass"
    yield
    os.environ.pop("INOUT_CREDENTIAL_PRE_REQ_TEST_CREDS", None)


def _make_pre_request_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="PreReqSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(
            base_url=_BASE_URL,
            pre_request=PreRequestAuthConfig(
                endpoint=_AUTH_URL,
                method="POST",
                credential_ref="pre_req_test_creds",
                token_field="token",
                token_header="X-Session-Token",
                token_lifetime_secs=3600.0,
            ),
        ),
        # auth field is required but bypassed when pre_request is configured
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="pre_req_test_creds",
            api_key=ApiKeyConfig(location="header", name="X-Unused"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["update"],
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/contacts/${{external_id}}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/contacts/${{external_id}}"),
                    ),
                ),
            ),
        },
    )


async def _setup_delta(pool, table: str, rows: list[dict]) -> None:
    cols = list(rows[0].keys())
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {table}")
        col_defs = ", ".join(
            f"{c} TEXT" + (" NOT NULL DEFAULT 'update'" if c == "_action" else "") for c in cols
        )
        await conn.execute(f"CREATE TABLE {table} ({col_defs})")
        for row in rows:
            placeholders = ", ".join(["%s"] * len(cols))
            await conn.execute(
                f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})",
                [row[c] for c in cols],
            )
        await conn.commit()


@pytest.mark.anyio
async def test_pre_request_token_fetched_and_injected(pool, run_migrations):
    """T1 #24: session token acquired via POST to auth endpoint and injected as X-Session-Token header."""
    delta_table = f"inout_delta_{_CONNECTOR}_inject"
    await _setup_delta(pool, delta_table, [
        {"external_id": "contact-pr-1", "name": "Alice", "_action": "update"},
    ])

    connector = _make_pre_request_connector()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    captured_session_headers: list[str] = []

    def _patch_handler(request: httpx.Request) -> httpx.Response:
        captured_session_headers.append(request.headers.get("X-Session-Token", ""))
        return httpx.Response(200, json={"id": "contact-pr-1"})

    with respx.mock(assert_all_called=False) as mock:
        # Token endpoint — synchronous httpx.Client call inside PreRequestAuthProvider
        mock.post(_AUTH_URL).mock(
            return_value=httpx.Response(200, json={"token": "sess-abc123"})
        )
        mock.patch(f"{_BASE_URL}/v1/contacts/contact-pr-1").mock(side_effect=_patch_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert result.failed == 0
    assert len(captured_session_headers) == 1
    assert captured_session_headers[0] == "sess-abc123", (
        f"Expected X-Session-Token: sess-abc123, got {captured_session_headers[0]!r}"
    )


@pytest.mark.anyio
async def test_pre_request_token_cached_not_re_fetched(pool, run_migrations):
    """T1 #24: cached token is reused across multiple requests — endpoint not called again."""
    delta_table = f"inout_delta_{_CONNECTOR}_caching"
    await _setup_delta(pool, delta_table, [
        {"external_id": "contact-pr-2", "name": "Bob", "_action": "update"},
        {"external_id": "contact-pr-3", "name": "Carol", "_action": "update"},
        {"external_id": "contact-pr-4", "name": "Dave", "_action": "update"},
    ])

    connector = _make_pre_request_connector()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    # Pre-populate the token cache so no token fetch is needed
    cache_key = ("pre_req_test_creds", _AUTH_URL)
    _token_cache[cache_key] = {
        "token": "cached-token-xyz",
        "expires_at": time.monotonic() + 3600.0,
    }

    token_fetch_count = [0]
    session_headers_seen: list[str] = []

    def _count_token_fetch(request: httpx.Request) -> httpx.Response:
        token_fetch_count[0] += 1
        return httpx.Response(200, json={"token": "should-not-be-called"})

    def _patch_handler(request: httpx.Request) -> httpx.Response:
        session_headers_seen.append(request.headers.get("X-Session-Token", ""))
        return httpx.Response(200, json={"id": "ok"})

    with respx.mock(assert_all_called=False) as mock:
        mock.post(_AUTH_URL).mock(side_effect=_count_token_fetch)
        mock.patch(url__regex=r"/v1/contacts/contact-pr-[234]").mock(side_effect=_patch_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 3
    assert token_fetch_count[0] == 0, (
        f"Token endpoint should not be called when cache is valid; called {token_fetch_count[0]} times"
    )
    assert all(h == "cached-token-xyz" for h in session_headers_seen), (
        f"All requests should use cached token; got headers: {session_headers_seen}"
    )


@pytest.mark.anyio
async def test_pre_request_401_invalidates_cache_and_retries(pool, run_migrations):
    """T1 #24: 401 response invalidates token cache, re-acquires a new token, and retries the request."""
    delta_table = f"inout_delta_{_CONNECTOR}_retry401"
    await _setup_delta(pool, delta_table, [
        {"external_id": "contact-pr-5", "name": "Eve", "_action": "update"},
    ])

    connector = _make_pre_request_connector()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    # Pre-populate with a stale token
    cache_key = ("pre_req_test_creds", _AUTH_URL)
    _token_cache[cache_key] = {
        "token": "stale-token",
        "expires_at": time.monotonic() + 3600.0,
    }

    refresh_count = [0]

    def _mock_fetch_token(self: PreRequestAuthProvider) -> str:
        refresh_count[0] += 1
        refreshed = f"fresh-token-{refresh_count[0]}"
        _token_cache[(self._cfg.credential_ref, self._cfg.endpoint)] = {
            "token": refreshed,
            "expires_at": time.monotonic() + 3600.0,
        }
        return refreshed

    patch_count = [0]
    session_headers_seen: list[str] = []

    def _patch_handler(request: httpx.Request) -> httpx.Response:
        patch_count[0] += 1
        session_headers_seen.append(request.headers.get("X-Session-Token", ""))
        if patch_count[0] == 1:
            return httpx.Response(401, json={"error": "session_expired"})
        return httpx.Response(200, json={"id": "contact-pr-5"})

    with unittest.mock.patch.object(PreRequestAuthProvider, "_fetch_token_sync", _mock_fetch_token):
        with respx.mock(assert_all_called=False) as mock:
            mock.patch(f"{_BASE_URL}/v1/contacts/contact-pr-5").mock(side_effect=_patch_handler)

            engine = WritebackEngine(pool)
            result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert patch_count[0] == 2, f"Expected 2 PATCH attempts (initial 401 + retry), got {patch_count[0]}"
    assert refresh_count[0] == 1, f"Expected 1 token refresh (triggered by 401), got {refresh_count[0]}"
    assert session_headers_seen[0] == "stale-token"
    assert session_headers_seen[1] == "fresh-token-1"
    assert result.processed == 1
