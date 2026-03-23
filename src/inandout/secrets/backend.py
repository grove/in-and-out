"""Secret backend implementations."""
from __future__ import annotations

import os
from abc import ABC, abstractmethod


class SecretBackend(ABC):
    """Abstract base for secret backends."""

    @abstractmethod
    async def get_secret(self, ref: str) -> str:
        """Retrieve the secret value identified by *ref*."""
        ...


class EnvSecretBackend(SecretBackend):
    """Reads secrets from environment variables.

    The env var name is: ``{prefix}{ref.upper().replace('-', '_')}``
    """

    def __init__(self, prefix: str = "INOUT_CREDENTIAL_") -> None:
        self._prefix = prefix

    async def get_secret(self, ref: str) -> str:
        env_var = self._prefix + ref.upper().replace("-", "_")
        value = os.environ.get(env_var)
        if value is None:
            raise KeyError(
                f"Secret '{ref}' not found. "
                f"Set env var {env_var} or configure a different credential backend."
            )
        return value


class VaultSecretBackend(SecretBackend):
    """Reads secrets from HashiCorp Vault KV v2."""

    def __init__(self, addr: str, token: str, mount: str = "secret") -> None:
        self._addr = addr.rstrip("/")
        self._token = token
        self._mount = mount

    async def get_secret(self, ref: str) -> str:
        import httpx

        url = f"{self._addr}/v1/{self._mount}/data/{ref}"
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                url,
                headers={"X-Vault-Token": self._token},
                timeout=10.0,
            )
            resp.raise_for_status()
            body = resp.json()
            # KV v2 path: data.data.value
            return str(body["data"]["data"]["value"])


class AwsSecretsManagerBackend(SecretBackend):
    """Reads secrets from AWS Secrets Manager using aioboto3."""

    def __init__(self, region: str) -> None:
        try:
            import aioboto3  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "aioboto3 is required for AwsSecretsManagerBackend. "
                "Install it with: pip install aioboto3"
            ) from exc
        self._region = region

    async def get_secret(self, ref: str) -> str:
        import aioboto3

        session = aioboto3.Session()
        async with session.client("secretsmanager", region_name=self._region) as client:
            resp = await client.get_secret_value(SecretId=ref)
            return str(resp["SecretString"])


class GcpSecretManagerBackend(SecretBackend):
    """Reads secrets from GCP Secret Manager."""

    def __init__(self, project: str) -> None:
        try:
            from google.cloud import secretmanager  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "google-cloud-secret-manager is required for GcpSecretManagerBackend. "
                "Install it with: pip install google-cloud-secret-manager"
            ) from exc
        self._project = project

    async def get_secret(self, ref: str) -> str:
        from google.cloud import secretmanager

        client = secretmanager.SecretManagerServiceAsyncClient()
        name = f"projects/{self._project}/secrets/{ref}/versions/latest"
        response = await client.access_secret_version(request={"name": name})
        return response.payload.data.decode("utf-8")
