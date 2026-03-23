"""Runtime management API package."""
from __future__ import annotations

from typing import Any


def build_api_router(pool: Any = None) -> Any:
    """Build and return the FastAPI APIRouter, wiring up the pool."""
    from inandout.api.routes import _set_pool, router

    if pool is not None:
        _set_pool(pool)
    return router


__all__ = ["build_api_router"]
