"""Unit tests for secret backend implementations."""
from __future__ import annotations

import os

import httpx
import pytest
import respx

from inandout.secrets.backend import EnvSecretBackend, VaultSecretBackend
from inandout.secrets import configure_backend, get_credential, EnvSecretBackend as _EnvBackend


# ---------------------------------------------------------------------------
# EnvSecretBackend
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_env_backend_reads_correct_env_var(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_API_KEY", "secret-value-123")
    backend = EnvSecretBackend()
    result = await backend.get_secret("my-api-key")
    assert result == "secret-value-123"


@pytest.mark.anyio
async def test_env_backend_uppercases_and_replaces_hyphens(monkeypatch):
    monkeypatch.setenv("INOUT_CREDENTIAL_MY_OAUTH_SECRET", "oauth-value")
    backend = EnvSecretBackend()
    result = await backend.get_secret("my-oauth-secret")
    assert result == "oauth-value"


@pytest.mark.anyio
async def test_env_backend_custom_prefix(monkeypatch):
    monkeypatch.setenv("MYAPP_FOO_BAR", "custom-value")
    backend = EnvSecretBackend(prefix="MYAPP_")
    result = await backend.get_secret("foo-bar")
    assert result == "custom-value"


@pytest.mark.anyio
async def test_env_backend_missing_var_raises_key_error(monkeypatch):
    monkeypatch.delenv("INOUT_CREDENTIAL_NONEXISTENT", raising=False)
    backend = EnvSecretBackend()
    with pytest.raises(KeyError, match="NONEXISTENT"):
        await backend.get_secret("nonexistent")


# ---------------------------------------------------------------------------
# VaultSecretBackend
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@respx.mock
async def test_vault_backend_makes_correct_http_request():
    """VaultSecretBackend fetches from correct Vault KV v2 URL."""
    vault_response = {
        "data": {
            "data": {
                "value": "vault-secret-value"
            }
        }
    }
    respx.get("https://vault.example.com/v1/secret/data/my-cred").mock(
        return_value=httpx.Response(200, json=vault_response)
    )

    backend = VaultSecretBackend(
        addr="https://vault.example.com",
        token="my-vault-token",
        mount="secret",
    )
    result = await backend.get_secret("my-cred")
    assert result == "vault-secret-value"


@pytest.mark.anyio
@respx.mock
async def test_vault_backend_sends_token_header():
    """VaultSecretBackend includes X-Vault-Token header."""
    route = respx.get("https://vault.example.com/v1/kv/data/test-ref").mock(
        return_value=httpx.Response(200, json={"data": {"data": {"value": "v"}}})
    )

    backend = VaultSecretBackend(
        addr="https://vault.example.com",
        token="super-secret-token",
        mount="kv",
    )
    await backend.get_secret("test-ref")

    last_request = route.calls.last.request
    assert last_request.headers.get("X-Vault-Token") == "super-secret-token"


@pytest.mark.anyio
@respx.mock
async def test_vault_backend_custom_mount():
    """VaultSecretBackend uses the configured mount path."""
    route = respx.get("https://vault.example.com/v1/my-mount/data/cred").mock(
        return_value=httpx.Response(200, json={"data": {"data": {"value": "x"}}})
    )

    backend = VaultSecretBackend(
        addr="https://vault.example.com",
        token="tok",
        mount="my-mount",
    )
    result = await backend.get_secret("cred")
    assert result == "x"


# ---------------------------------------------------------------------------
# configure_backend + get_credential round-trip
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_configure_backend_round_trip(monkeypatch):
    """configure_backend changes the global backend used by get_credential."""
    import inandout.secrets as secrets_module

    monkeypatch.setenv("INOUT_CREDENTIAL_TEST_CRED", "round-trip-value")

    # Reset to env backend
    configure_backend(EnvSecretBackend())
    result = await get_credential("test-cred")
    assert result == "round-trip-value"

    # Restore default
    configure_backend(EnvSecretBackend())


@pytest.mark.anyio
async def test_configure_backend_replaces_global():
    """configure_backend replaces the global _backend."""
    import inandout.secrets as secrets_module

    class FakeBackend(EnvSecretBackend):
        async def get_secret(self, ref: str) -> str:
            return f"fake-{ref}"

    configure_backend(FakeBackend())
    result = await get_credential("anything")
    assert result == "fake-anything"

    # Restore
    configure_backend(EnvSecretBackend())


# ---------------------------------------------------------------------------
# Missing env var raises KeyError with helpful message
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_missing_env_var_error_message_includes_var_name(monkeypatch):
    monkeypatch.delenv("INOUT_CREDENTIAL_MISSING_ONE", raising=False)
    backend = EnvSecretBackend()
    with pytest.raises(KeyError) as exc_info:
        await backend.get_secret("missing-one")
    # Message should include the env var name
    error_str = str(exc_info.value)
    assert "MISSING_ONE" in error_str
