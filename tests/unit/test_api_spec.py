"""Unit tests for Step 54 — OpenAPI spec generation and client SDK codegen."""
from __future__ import annotations

import json
from typing import Any

import pytest
from typer.testing import CliRunner


runner = CliRunner()


def _build_spec_app() -> Any:
    """Build the FastAPI app the same way api spec does."""
    from fastapi import FastAPI
    from inandout.api import build_api_router

    app = FastAPI(title="in-and-out management API", version="0.1.0")
    router = build_api_router(pool=None)
    app.include_router(router, prefix="/api")
    return app


# ---------------------------------------------------------------------------
# test_api_spec_outputs_valid_json
# ---------------------------------------------------------------------------

def test_api_spec_outputs_valid_json(tmp_path):
    """api spec command outputs valid JSON with 'openapi' key."""
    from inandout.cli.main import app

    out_file = tmp_path / "spec.json"
    result = runner.invoke(app, ["api", "spec", "--output", str(out_file)])

    assert result.exit_code == 0, result.output
    assert out_file.exists()

    spec = json.loads(out_file.read_text())
    assert "openapi" in spec


def test_api_spec_stdout_valid_json():
    """api spec without --output writes valid JSON to stdout."""
    from inandout.cli.main import app

    result = runner.invoke(app, ["api", "spec"])
    assert result.exit_code == 0, result.output

    spec = json.loads(result.output)
    assert "openapi" in spec


def test_api_spec_contains_expected_paths():
    """api spec JSON contains /api/connectors and /api/health paths."""
    spec_app = _build_spec_app()
    spec = spec_app.openapi()

    paths = spec.get("paths", {})
    assert "/api/health" in paths, f"Missing /api/health. Got: {list(paths.keys())[:10]}"
    assert "/api/connectors" in paths, f"Missing /api/connectors. Got: {list(paths.keys())[:10]}"


def test_api_spec_contains_sync_runs_path():
    """api spec JSON contains /api/sync-runs path."""
    spec_app = _build_spec_app()
    spec = spec_app.openapi()

    paths = spec.get("paths", {})
    assert "/api/sync-runs" in paths


def test_api_spec_contains_dead_letter_path():
    """api spec JSON contains dead-letter path."""
    spec_app = _build_spec_app()
    spec = spec_app.openapi()

    paths = spec.get("paths", {})
    dead_letter_paths = [p for p in paths if "dead-letter" in p]
    assert dead_letter_paths, f"No dead-letter paths found. Got: {list(paths.keys())}"


# ---------------------------------------------------------------------------
# test_generate_sdk_without_generator_exits_1
# ---------------------------------------------------------------------------

def test_generate_sdk_without_openapi_generator_exits_1(monkeypatch):
    """generate-sdk without openapi-generator-cli on PATH exits with code 1."""
    import shutil
    from inandout.cli.main import app

    # Make shutil.which return None for openapi-generator-cli
    original_which = shutil.which

    def mock_which(name, *args, **kwargs):
        if name == "openapi-generator-cli":
            return None
        return original_which(name, *args, **kwargs)

    monkeypatch.setattr(shutil, "which", mock_which)

    result = runner.invoke(app, ["api", "generate-sdk", "--lang", "python", "--output", "/tmp/sdk"])
    assert result.exit_code == 1
    # Should print helpful message
    assert "openapi-generator-cli" in result.output.lower() or "not found" in result.output.lower()
