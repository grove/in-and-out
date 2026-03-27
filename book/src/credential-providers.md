# Credential Providers

By default, in-and-out resolves connector credentials from environment variables named `INOUT_CREDENTIAL_<REF>` (upper-cased). The **credential provider** extension point lets you swap that built-in resolver for any secret store — HashiCorp Vault, AWS Secrets Manager, Azure Key Vault, an encrypted config file, or your own backend — without touching the core engine.

---

## How it works

When the transport layer needs to resolve a `credential_ref` string from a connector YAML:

1. Each registered `CredentialProvider` is tried in **LIFO** order (most-recently-registered first).
2. The first provider that returns a non-`None` value wins.
3. If no provider resolves it, the built-in `INOUT_CREDENTIAL_<REF>` environment-variable fallback is used.
4. If the env var is also absent a `RuntimeError` is raised at request time.

This means you can layer providers: add a Vault provider that handles production secrets while the env-var fallback still works for local development.

---

## The protocol

A credential provider is any object that satisfies:

```python
class CredentialProvider(Protocol):
    def resolve(self, credential_ref: str) -> str | None:
        ...
```

- `credential_ref` is the exact string from the connector YAML (e.g. `hubspot_api_key`).
- Return the secret value as a string, or `None` to pass to the next provider in the chain.

---

## Registering a provider

### Programmatically

```python
from inandout.secrets.provider import register_provider, CredentialProvider

class VaultProvider:
    def __init__(self, client):
        self._client = client

    def resolve(self, credential_ref: str) -> str | None:
        data = self._client.read_secret(f"inandout/{credential_ref}")
        return data.get("value") if data else None

register_provider(VaultProvider(vault_client))
```

Call `register_provider()` before the daemon starts accepting requests (e.g., in a startup hook or an entry-point factory).

### Via entry points (recommended for packages)

Create a factory function that returns a single `CredentialProvider` instance:

```python
# my_vault_plugin/secrets.py
from inandout.secrets.provider import CredentialProvider
import hvac

def get_provider() -> CredentialProvider:
    client = hvac.Client(url="https://vault.example.com")
    return VaultProvider(client)
```

Register it in `pyproject.toml`:

```toml
[project.entry-points."inandout.credential_providers"]
my_vault = "my_vault_plugin.secrets:get_provider"
```

Providers registered via entry points are discovered automatically at daemon startup via `importlib.metadata`. The package must be installed into the **same Python environment** as in-and-out.

---

## Multiple providers

Providers form a chain. Register them in order from lowest to highest priority — the last-registered provider is tried first:

```python
register_provider(EnvFileProvider())   # tried second
register_provider(VaultProvider())     # tried first
```

---

## Clearing providers (testing)

```python
from inandout.secrets.provider import clear_providers
clear_providers()
```

Useful in test teardown to reset the chain between tests.
