"""Web UI module for in-and-out."""
from __future__ import annotations

from pathlib import Path

from starlette.routing import Mount
from starlette.staticfiles import StaticFiles


def build_ui_router() -> Mount:
    """Return a Starlette Mount that serves the SPA under /ui."""
    static_dir = Path(__file__).parent / "static"
    return Mount("/ui", app=StaticFiles(directory=str(static_dir), html=True))
