"""Connector marketplace / registry index fetcher and installer."""
from __future__ import annotations

from pathlib import Path

import httpx
from pydantic import BaseModel, ConfigDict

DEFAULT_INDEX_URL = (
    "https://raw.githubusercontent.com/grove/in-and-out-connectors/main/index.json"
)


class ConnectorIndexEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    version: str
    description: str
    yaml_url: str
    hooks_url: str | None = None


class ConnectorIndex(BaseModel):
    model_config = ConfigDict(extra="forbid")

    connectors: list[ConnectorIndexEntry]


async def fetch_index(index_url: str = DEFAULT_INDEX_URL) -> ConnectorIndex:
    """Fetch and parse the connector index from *index_url*."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(index_url, timeout=30.0)
        resp.raise_for_status()
        return ConnectorIndex.model_validate(resp.json())


async def install_connector(entry: ConnectorIndexEntry, dest_dir: Path) -> Path:
    """Download the connector YAML (and optional hooks .py) to *dest_dir*.

    Returns the path to the saved YAML file.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    async with httpx.AsyncClient() as client:
        # Download YAML
        yaml_resp = await client.get(entry.yaml_url, timeout=30.0)
        yaml_resp.raise_for_status()
        yaml_path = dest_dir / f"{entry.name}.yaml"
        yaml_path.write_bytes(yaml_resp.content)

        # Download optional hooks .py
        if entry.hooks_url:
            hooks_resp = await client.get(entry.hooks_url, timeout=30.0)
            hooks_resp.raise_for_status()
            hooks_path = dest_dir / f"{entry.name}_hooks.py"
            hooks_path.write_bytes(hooks_resp.content)

    return yaml_path


def search_connectors(index: ConnectorIndex, query: str) -> list[ConnectorIndexEntry]:
    """Filter connectors by substring match on name or description (case-insensitive)."""
    q = query.lower()
    return [
        entry
        for entry in index.connectors
        if q in entry.name.lower() or q in entry.description.lower()
    ]
