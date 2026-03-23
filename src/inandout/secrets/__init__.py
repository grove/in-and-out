"""Secret management package.

Module-level singleton backend defaults to EnvSecretBackend.
Call configure_backend() to switch to a different backend.
"""
from __future__ import annotations

from inandout.secrets.backend import (
    AwsSecretsManagerBackend,
    EnvSecretBackend,
    GcpSecretManagerBackend,
    SecretBackend,
    VaultSecretBackend,
)

_backend: SecretBackend = EnvSecretBackend()


def configure_backend(backend: SecretBackend) -> None:
    """Replace the global secret backend."""
    global _backend
    _backend = backend


async def get_credential(ref: str) -> str:
    """Retrieve the credential identified by *ref* from the configured backend."""
    return await _backend.get_secret(ref)


__all__ = [
    "SecretBackend",
    "EnvSecretBackend",
    "VaultSecretBackend",
    "AwsSecretsManagerBackend",
    "GcpSecretManagerBackend",
    "configure_backend",
    "get_credential",
]
