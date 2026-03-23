"""Connector marketplace / registry package."""
from inandout.registry.index import (
    DEFAULT_INDEX_URL,
    ConnectorIndex,
    ConnectorIndexEntry,
    fetch_index,
    install_connector,
    search_connectors,
)

__all__ = [
    "DEFAULT_INDEX_URL",
    "ConnectorIndex",
    "ConnectorIndexEntry",
    "fetch_index",
    "install_connector",
    "search_connectors",
]
