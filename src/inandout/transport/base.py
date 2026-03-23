"""Transport adapter protocol (abstract interface)."""
from __future__ import annotations

from typing import Any, AsyncGenerator, Protocol, runtime_checkable

from inandout.config.ingestion import ListConfig


@runtime_checkable
class TransportAdapter(Protocol):
    async def fetch_pages(
        self,
        list_config: ListConfig,
        watermark: str | None = None,
    ) -> AsyncGenerator[list[dict[str, Any]], None]: ...

    async def close(self) -> None: ...
