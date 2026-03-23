"""Unit tests for the Web UI (Step 71)."""
from __future__ import annotations

import pytest
from pathlib import Path


# ---------------------------------------------------------------------------
# Static files exist
# ---------------------------------------------------------------------------

def test_index_html_exists():
    """The index.html file must exist in the ui/static directory."""
    from inandout.ui import build_ui_router
    import inandout.ui as ui_module

    static_dir = Path(ui_module.__file__).parent / "static"
    index = static_dir / "index.html"
    assert index.exists(), f"index.html not found at {index}"


def test_index_html_contains_title():
    import inandout.ui as ui_module

    static_dir = Path(ui_module.__file__).parent / "static"
    content = (static_dir / "index.html").read_text()
    assert "in-and-out" in content


def test_index_html_contains_htmx_script():
    import inandout.ui as ui_module

    static_dir = Path(ui_module.__file__).parent / "static"
    content = (static_dir / "index.html").read_text()
    assert "htmx" in content.lower()


# ---------------------------------------------------------------------------
# build_ui_router
# ---------------------------------------------------------------------------

def test_build_ui_router_returns_mount():
    from inandout.ui import build_ui_router
    from starlette.routing import Mount

    mount = build_ui_router()
    assert isinstance(mount, Mount)
    assert mount.path == "/ui"


# ---------------------------------------------------------------------------
# Starlette test client — /ui/ serves HTML
# ---------------------------------------------------------------------------

def test_ui_root_returns_200():
    from starlette.testclient import TestClient
    from starlette.applications import Starlette
    from inandout.ui import build_ui_router

    app = Starlette(routes=[build_ui_router()])
    client = TestClient(app, raise_server_exceptions=False)

    resp = client.get("/ui/")
    assert resp.status_code == 200
    assert "html" in resp.headers.get("content-type", "").lower()


def test_ui_returns_html_content():
    from starlette.testclient import TestClient
    from starlette.applications import Starlette
    from inandout.ui import build_ui_router

    app = Starlette(routes=[build_ui_router()])
    client = TestClient(app, raise_server_exceptions=False)

    resp = client.get("/ui/")
    assert resp.status_code == 200
    assert "in-and-out" in resp.text
