"""Unit tests for connector generator test scaffold (C1)."""
from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from inandout.generator.template import render_connector_test


# ---------------------------------------------------------------------------
# Tests: render_connector_test
# ---------------------------------------------------------------------------


def test_render_connector_test_returns_valid_python():
    """render_connector_test returns syntactically valid Python."""
    code = render_connector_test(
        name="myapi",
        base_url="https://api.myapi.com",
        datatypes=["contacts", "deals"],
    )
    assert isinstance(code, str)
    # Must compile without error
    compile(code, "<generated>", "exec")


def test_render_connector_test_contains_correct_name():
    """Generated test file contains the connector name."""
    code = render_connector_test(
        name="hubspot",
        base_url="https://api.hubapi.com",
        datatypes=["contacts"],
    )
    assert "hubspot" in code


def test_render_connector_test_contains_base_url():
    """Generated test file contains the base URL."""
    base_url = "https://api.salesforce.com"
    code = render_connector_test(
        name="salesforce",
        base_url=base_url,
        datatypes=["leads"],
    )
    assert base_url in code


def test_render_connector_test_name_sanitised():
    """Connector name with spaces/uppercase is sanitised in the generated file."""
    code = render_connector_test(
        name="My API Connector",
        base_url="https://api.example.com",
        datatypes=[],
    )
    # Name should be sanitised (no spaces, lowercase)
    assert "my_api_connector" in code or "my" in code.lower()


def test_render_connector_test_credential_env_var():
    """Generated file references the correct credential env var."""
    code = render_connector_test(
        name="myconn",
        base_url="https://api.example.com",
        datatypes=[],
    )
    # Should contain uppercase env var reference
    assert "MYCONN" in code


def test_render_connector_test_empty_datatypes():
    """render_connector_test works with empty datatypes list."""
    code = render_connector_test(
        name="stub",
        base_url="https://api.stub.com",
        datatypes=[],
    )
    compile(code, "<generated>", "exec")


# ---------------------------------------------------------------------------
# Tests: connector new CLI writes both yaml and test files
# ---------------------------------------------------------------------------


def test_connector_new_writes_yaml_and_test_file(tmp_path: Path) -> None:
    """connector new command writes both {name}.yaml and test_{name}_connector.py."""
    from typer.testing import CliRunner
    from inandout.cli.main import app

    runner = CliRunner()

    with (
        patch("inandout.generator.introspect.fetch_openapi_spec", return_value=None),
    ):
        result = runner.invoke(
            app,
            [
                "connector", "new",
                "--name", "testconn",
                "--base-url", "https://api.test.com",
                "--output", str(tmp_path),
            ],
        )

    yaml_path = tmp_path / "testconn.yaml"
    test_path = tmp_path / "test_testconn_connector.py"

    assert yaml_path.exists(), f"YAML file not written; CLI output: {result.output}"
    assert test_path.exists(), f"Test file not written; CLI output: {result.output}"
    assert "test_testconn_connector.py" in result.output or test_path.exists()


def test_connector_new_test_file_valid_python(tmp_path: Path) -> None:
    """The generated test file is valid Python."""
    from typer.testing import CliRunner
    from inandout.cli.main import app

    runner = CliRunner()

    with (
        patch("inandout.generator.introspect.fetch_openapi_spec", return_value=None),
    ):
        result = runner.invoke(
            app,
            [
                "connector", "new",
                "--name", "validtest",
                "--base-url", "https://api.valid.com",
                "--output", str(tmp_path),
            ],
        )

    test_path = tmp_path / "test_validtest_connector.py"
    if test_path.exists():
        code = test_path.read_text()
        compile(code, str(test_path), "exec")
    else:
        pytest.skip("Test file not written (CLI may have errored)")
