"""Unit tests for rotate-credential control command and cache invalidation."""
from __future__ import annotations

import time

import pytest

from inandout.transport.auth import (
    OAuth2ClientCredentialsAuth,
    OAuth2RefreshTokenAuth,
    CustomAuthProvider,
    invalidate_credential_cache,
)


@pytest.fixture(autouse=True)
def clear_all_caches():
    for cls in (OAuth2ClientCredentialsAuth, OAuth2RefreshTokenAuth, CustomAuthProvider):
        cls._cache.clear()
        cls._locks.clear()
    yield
    for cls in (OAuth2ClientCredentialsAuth, OAuth2RefreshTokenAuth, CustomAuthProvider):
        cls._cache.clear()
        cls._locks.clear()


def _populate_caches() -> None:
    """Populate caches for two different credential_refs."""
    now = time.monotonic()
    OAuth2ClientCredentialsAuth._cache[("cred_a", "https://tok")] = {
        "access_token": "a1", "expires_at": now + 3600,
    }
    OAuth2ClientCredentialsAuth._cache[("cred_b", "https://tok")] = {
        "access_token": "b1", "expires_at": now + 3600,
    }
    OAuth2RefreshTokenAuth._cache[("cred_a", "https://tok")] = {
        "access_token": "a2", "expires_at": now + 3600,
    }
    CustomAuthProvider._cache[("cred_a", "https://api/login")] = {
        "token": "a3", "expires_at": now + 3600,
    }


def test_invalidate_only_matching_credential():
    _populate_caches()
    count = invalidate_credential_cache("cred_a")
    assert count == 3  # a1, a2, a3 removed

    # cred_b still present
    assert ("cred_b", "https://tok") in OAuth2ClientCredentialsAuth._cache


def test_invalidate_returns_zero_for_unknown():
    _populate_caches()
    count = invalidate_credential_cache("no_such_cred")
    assert count == 0


def test_invalidate_all_classes():
    _populate_caches()
    invalidate_credential_cache("cred_a")
    for cls in (OAuth2ClientCredentialsAuth, OAuth2RefreshTokenAuth, CustomAuthProvider):
        for k in cls._cache:
            assert k[0] != "cred_a"


# ---------------------------------------------------------------
# ControlDispatcher._cmd_rotate_credential (inline / lightweight)
# ---------------------------------------------------------------

@pytest.mark.anyio
async def test_control_dispatch_rotate_credential():
    """Verify the ControlDispatcher routes rotate-credential correctly."""
    from unittest.mock import AsyncMock, MagicMock

    pool = MagicMock()
    pool.connection = AsyncMock()

    from inandout.engine.control import ControlDispatcher
    disp = ControlDispatcher(pool, paused_connectors=set())

    _populate_caches()
    result = await disp._execute(
        command="rotate-credential",
        connector="hubspot",
        datatype=None,
        payload={"credential_ref": "cred_a"},
        engine=None,
    )
    assert result["rotated"] == "cred_a"
    assert result["cache_entries_invalidated"] == 3


@pytest.mark.anyio
async def test_control_dispatch_rotate_credential_requires_ref():
    from unittest.mock import AsyncMock, MagicMock

    pool = MagicMock()
    pool.connection = AsyncMock()

    from inandout.engine.control import ControlDispatcher
    disp = ControlDispatcher(pool, paused_connectors=set())

    with pytest.raises(ValueError, match="credential_ref"):
        await disp._execute(
            command="rotate-credential",
            connector="hubspot",
            datatype=None,
            payload={},
            engine=None,
        )
