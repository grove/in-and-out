"""Pluggable credential provider protocol.

Third-party packages (or application code) can register custom providers to
resolve credentials from sources other than environment variables — e.g.,
HashiCorp Vault, AWS Secrets Manager, Azure Key Vault, or an encrypted
configuration store.

Usage::

    from inandout.secrets.provider import register_provider, CredentialProvider

    class VaultProvider:
        def resolve(self, credential_ref: str) -> str | None:
            secret = vault_client.read_secret(credential_ref)
            return secret.get("value")

    register_provider(VaultProvider())

Providers are consulted in LIFO order (most-recently registered first), then
the built-in environment-variable fallback is tried.  The first non-``None``
return value wins.

Entry-point auto-registration (optional)::

    [project.entry-points."inandout.credential_providers"]
    my_vault = "my_package.secrets:get_provider"

The factory function must return a single ``CredentialProvider`` instance.
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

import structlog

logger = structlog.get_logger(__name__)

_ENTRY_POINT_GROUP = "inandout.credential_providers"


@runtime_checkable
class CredentialProvider(Protocol):
    """Protocol for custom credential backends."""

    def resolve(self, credential_ref: str) -> str | None:
        """Return the credential value for *credential_ref*, or ``None`` if not found.

        Parameters
        ----------
        credential_ref:
            The ``credential_ref`` value declared in the connector YAML
            (e.g. ``"hubspot_api_key"``).

        Returns
        -------
        str | None
            The resolved secret value, or ``None`` to signal that this
            provider cannot satisfy the request (the next provider or the
            env-var fallback will be tried).
        """
        ...


# Ordered list of providers.  Each is tried in turn (most-recently registered
# first) until one returns a non-None value.
_providers: list[CredentialProvider] = []


def register_provider(provider: CredentialProvider) -> None:
    """Add *provider* to the front of the resolution chain."""
    _providers.insert(0, provider)


def resolve_via_providers(credential_ref: str) -> str | None:
    """Attempt to resolve *credential_ref* through all registered providers.

    Returns the first non-None value, or ``None`` if all providers return
    ``None`` (meaning the env-var fallback should be used).
    """
    for provider in _providers:
        try:
            value = provider.resolve(credential_ref)
            if value is not None:
                return value
        except Exception as exc:
            logger.warning(
                "credential_provider_error",
                credential_ref=credential_ref,
                provider=type(provider).__name__,
                error=str(exc),
            )
    return None


def discover_and_register_providers() -> int:
    """Discover and register credential providers via entry points.

    Scans ``importlib.metadata.entry_points(group="inandout.credential_providers")``
    for all registered provider factories.  Each entry point must point to a
    callable that returns a single ``CredentialProvider`` instance.

    Returns the number of providers registered.
    """
    from importlib.metadata import entry_points

    registered = 0
    try:
        eps = entry_points(group=_ENTRY_POINT_GROUP)
    except Exception as exc:
        logger.warning("credential_provider_discovery_failed", error=str(exc))
        return 0

    for ep in eps:
        try:
            factory = ep.load()
            provider = factory()
            register_provider(provider)
            registered += 1
            logger.info(
                "credential_provider_registered",
                entry_point=ep.name,
                provider=type(provider).__name__,
            )
        except Exception as exc:
            logger.warning(
                "credential_provider_load_failed",
                entry_point=ep.name,
                error=str(exc),
            )

    return registered


def clear_providers() -> None:
    """Remove all registered providers.  Intended for use in tests only."""
    _providers.clear()
