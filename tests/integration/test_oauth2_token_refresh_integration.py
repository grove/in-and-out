"""Integration tests for OAuth2 client_credentials token refresh (T1 #11)."""
from __future__ import annotations

import os
import re
import time
import unittest.mock

import httpx
import orjson
import pytest
import respx

from inandout.config.auth import OAuth2Auth, OAuth2Config
from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
from inandout.config.writeback import (
    WritebackConfig, ProtectionLevel, ConflictResolution,
    OperationsConfig, OperationConfig, UpdateOperationConfig,
)
from inandout.transport.auth import OAuth2ClientCredentialsAuth
from inandout.writeback.engine import WritebackEngine

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_BASE_URL = "https://api.oauth2-test.example.com"
_TOKEN_URL = "https://auth.oauth2-test.example.com/token"
_CONNECTOR = "oauth2_test"
_DATATYPE = "records"
_DELTA_TABLE = f"inout_delta_{_CONNECTOR}_{_DATATYPE}"


@pytest.fixture(autouse=True)
def _clear_oauth2_cache():
    """Ensure OAuth2 token cache is clean before and after each test."""
    OAuth2ClientCredentialsAuth._cache.clear()
    OAuth2ClientCredentialsAuth._locks.clear()
    yield
    OAuth2ClientCredentialsAuth._cache.clear()
    OAuth2ClientCredentialsAuth._locks.clear()


@pytest.fixture(autouse=True)
def _set_credential():
    os.environ["INOUT_CREDENTIAL_OAUTH2_TEST_CREDS"] = "myclientid:myclientsecret"
    yield
    os.environ.pop("INOUT_CREDENTIAL_OAUTH2_TEST_CREDS", None)


def _make_oauth2_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="OAuth2System",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=OAuth2Auth(
            type="oauth2",
            credential_ref="oauth2_test_creds",
            oauth2=OAuth2Config(
                grant_type="client_credentials",
                token_url=_TOKEN_URL,
                scopes=["write"],
            ),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["update"],
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/records/${{external_id}}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/records/${{external_id}}"),
                    ),
                ),
            ),
        },
    )


async def _setup_delta_table(pool, table_name: str) -> None:
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        await conn.execute(f"""
            CREATE TABLE {table_name} (
                external_id TEXT,
                status      TEXT,
                _action     TEXT NOT NULL DEFAULT 'update'
            )
        """)
        await conn.execute(
            f"INSERT INTO {table_name} (external_id, status, _action) VALUES (%s, %s, %s)",
            ["rec-1", "active", "update"],
        )
        await conn.commit()


@pytest.mark.anyio
async def test_oauth2_token_injected_in_authorization_header(pool):
    """Engine fetches an OAuth2 token and injects it as Bearer in the PATCH request."""
    delta_table = f"inout_delta_{_CONNECTOR}_inject"
    await _setup_delta_table(pool, delta_table)

    connector = _make_oauth2_connector()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    patched_headers: list[str] = []

    def _patch_handler(request: httpx.Request) -> httpx.Response:
        patched_headers.append(request.headers.get("Authorization", ""))
        return httpx.Response(200, json={"id": "rec-1"})

    with respx.mock(assert_all_called=False) as mock:
        # Token endpoint (called synchronously by OAuth2ClientCredentialsAuth)
        mock.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json={"access_token": "test-token-123", "expires_in": 3600})
        )
        mock.patch(f"{_BASE_URL}/v1/records/rec-1").mock(side_effect=_patch_handler)

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 1
    assert result.failed == 0
    assert len(patched_headers) == 1
    assert patched_headers[0] == "Bearer test-token-123"


@pytest.mark.anyio
async def test_oauth2_401_triggers_cache_invalidation_and_retry(pool):
    """A 401 from the API causes the OAuth2 auth to invalidate the cache and retry.

    Strategy: pre-populate cache with a valid "initial-token", patch
    _fetch_token_sync so the refresh is synchronous and controllable, confirm
    the PATCH is attempted twice (initial 401 + retry) and the retry uses the
    refreshed token.
    """
    delta_table = f"inout_delta_{_CONNECTOR}_retry401"
    await _setup_delta_table(pool, delta_table)

    connector = _make_oauth2_connector()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    cache_key = ("oauth2_test_creds", _TOKEN_URL)
    # Pre-populate with a valid token so the initial request doesn't trigger a fetch
    OAuth2ClientCredentialsAuth._cache[cache_key] = {
        "access_token": "initial-token",
        "expires_at": time.monotonic() + 3600,
    }

    refresh_count: list[int] = [0]

    def _mock_fetch_token(self: OAuth2ClientCredentialsAuth) -> str:
        """Replacement for _fetch_token_sync that tracks invocations."""
        refresh_count[0] += 1
        refreshed = f"refreshed-token-{refresh_count[0]}"
        self.__class__._cache[self._cache_key] = {
            "access_token": refreshed,
            "expires_at": time.monotonic() + 3600,
        }
        return refreshed

    patch_count: list[int] = [0]
    auth_headers_seen: list[str] = []

    def _patch_handler(request: httpx.Request) -> httpx.Response:
        patch_count[0] += 1
        auth_headers_seen.append(request.headers.get("Authorization", ""))
        if patch_count[0] == 1:
            return httpx.Response(401, json={"error": "token_expired"})
        return httpx.Response(200, json={"id": "rec-1"})

    with unittest.mock.patch.object(OAuth2ClientCredentialsAuth, "_fetch_token_sync", _mock_fetch_token):
        with respx.mock(assert_all_called=False) as mock:
            mock.patch(f"{_BASE_URL}/v1/records/rec-1").mock(side_effect=_patch_handler)

            engine = WritebackEngine(pool)
            result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert patch_count[0] == 2, f"Expected 2 PATCH attempts (initial 401 + retry), got {patch_count[0]}"
    assert refresh_count[0] == 1, f"Expected 1 token refresh (triggered by 401), got {refresh_count[0]}"
    assert auth_headers_seen[0] == "Bearer initial-token"
    assert auth_headers_seen[1] == "Bearer refreshed-token-1"


@pytest.mark.anyio
async def test_oauth2_token_cached_across_multiple_requests(pool):
    """Token is reused across multiple PATCH requests within the same writeback cycle.

    Strategy: pre-populate the cache with a known token, run 3 rows, capture
    the Authorization header from each PATCH, verify they are all identical.
    """
    delta_table = f"inout_delta_{_CONNECTOR}_caching"
    async with pool.connection() as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {delta_table}")
        await conn.execute(f"""
            CREATE TABLE {delta_table} (
                external_id TEXT,
                status      TEXT,
                _action     TEXT NOT NULL DEFAULT 'update'
            )
        """)
        for i in range(3):
            await conn.execute(
                f"INSERT INTO {delta_table} (external_id, status, _action) VALUES (%s, %s, %s)",
                [f"reccache-{i}", "active", "update"],
            )
        await conn.commit()

    connector = _make_oauth2_connector()
    writeback_cfg = connector.datatypes[_DATATYPE].writeback

    # Pre-populate cache with a long-lived token — no token fetch should happen
    cache_key = ("oauth2_test_creds", _TOKEN_URL)
    OAuth2ClientCredentialsAuth._cache[cache_key] = {
        "access_token": "pre-cached-token",
        "expires_at": time.monotonic() + 3600,
    }

    auth_headers_seen: list[str] = []

    def _patch_handler(request: httpx.Request) -> httpx.Response:
        auth_headers_seen.append(request.headers.get("Authorization", ""))
        return httpx.Response(200, json={"id": request.url.path.split("/")[-1]})

    with respx.mock(assert_all_called=False) as mock:
        mock.patch(re.compile(r"https://api\.oauth2-test\.example\.com/v1/records/reccache-\d")).mock(
            side_effect=_patch_handler
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, writeback_cfg, delta_table)

    assert result.processed == 3
    assert len(auth_headers_seen) == 3
    # All requests used the same cached token — no refetch occurred
    assert all(h == "Bearer pre-cached-token" for h in auth_headers_seen), (
        f"Not all requests used cached token: {auth_headers_seen}"
    )
