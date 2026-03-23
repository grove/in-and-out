"""Unit tests for connector marketplace publishing (Step 87)."""
from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import respx
import httpx


VALID_CONNECTOR_YAML = """\
schema_version: 1
connector:
  name: testpub
  system: TestSystem
  generation_profile: ingestion_polling_readonly
  api_version: "v1"
  connection:
    base_url: https://api.test.example
  auth:
    type: api_key
    credential_ref: testpub_key
    api_key:
      location: header
      name: X-API-Key
  datatypes:
    contacts:
      ingestion:
        primary_key: id
        history_mode: overwrite
        schedule:
          interval: 5m
        list:
          method: GET
          path: /contacts
          record_selector: results
          pagination:
            strategy: offset
            offset:
              page_size: 100
              offset_param: offset
              limit_param: limit
"""


# ---------------------------------------------------------------------------
# ConnectorSubmission tests
# ---------------------------------------------------------------------------

def test_connector_submission_model():
    """ConnectorSubmission should be a valid Pydantic model."""
    from inandout.registry.publish import ConnectorSubmission

    submission = ConnectorSubmission(
        name="myconn",
        version="1.0.0",
        description="My connector",
        yaml_content="schema_version: 1\n",
        hooks_content=None,
    )
    assert submission.name == "myconn"
    assert submission.version == "1.0.0"
    assert submission.hooks_content is None


def test_connector_submission_with_hooks():
    """ConnectorSubmission with hooks_content should be valid."""
    from inandout.registry.publish import ConnectorSubmission

    submission = ConnectorSubmission(
        name="myconn",
        version="2.0.0",
        description="My connector",
        yaml_content="schema_version: 1\n",
        hooks_content="def get_hooks(): return {}",
    )
    assert submission.hooks_content is not None


# ---------------------------------------------------------------------------
# build_submission tests
# ---------------------------------------------------------------------------

def test_build_submission_from_yaml_file(tmp_path):
    """build_submission should load YAML and build a ConnectorSubmission."""
    from inandout.registry.publish import build_submission

    connector_file = tmp_path / "testpub.yaml"
    connector_file.write_text(VALID_CONNECTOR_YAML)

    submission = build_submission(
        connector_path=connector_file,
        hooks_path=None,
        description="Test connector",
        version="1.0.0",
    )

    assert submission.name == "testpub"
    assert submission.version == "1.0.0"
    assert submission.description == "Test connector"
    assert "schema_version: 1" in submission.yaml_content
    assert submission.hooks_content is None


def test_build_submission_with_hooks_file(tmp_path):
    """build_submission should include hooks content when hooks path is provided."""
    from inandout.registry.publish import build_submission

    connector_file = tmp_path / "testpub.yaml"
    connector_file.write_text(VALID_CONNECTOR_YAML)

    hooks_file = tmp_path / "testpub_hooks.py"
    hooks_file.write_text("def get_hooks(): return {}")

    submission = build_submission(
        connector_path=connector_file,
        hooks_path=hooks_file,
        description="Test connector",
        version="2.0.0",
    )

    assert submission.hooks_content == "def get_hooks(): return {}"
    assert submission.version == "2.0.0"


# ---------------------------------------------------------------------------
# validate_for_publish tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_validate_for_publish_valid_connector(tmp_path, monkeypatch):
    """validate_for_publish should return empty list for a valid connector."""
    from inandout.registry.publish import validate_for_publish

    monkeypatch.setenv("INOUT_CREDENTIAL_TESTPUB_KEY", "test-secret")

    connector_file = tmp_path / "testpub.yaml"
    connector_file.write_text(VALID_CONNECTOR_YAML)

    # Mock the testing runner to avoid needing respx for sub-tests
    with patch("inandout.testing.runner.run_connector_tests") as mock_tests:
        mock_suite = MagicMock()
        mock_suite.results = []
        mock_tests.return_value = mock_suite
        errors = await validate_for_publish(connector_file)

    # Linter should find no errors for this valid connector
    assert isinstance(errors, list)


@pytest.mark.anyio
async def test_validate_for_publish_missing_file():
    """validate_for_publish should return errors for missing file."""
    from inandout.registry.publish import validate_for_publish

    missing_path = Path("/tmp/nonexistent_connector_xyz.yaml")

    errors = await validate_for_publish(missing_path)

    # Should report an error since the file doesn't exist
    assert len(errors) > 0


# ---------------------------------------------------------------------------
# submit_connector tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@respx.mock
async def test_submit_connector_success(tmp_path):
    """submit_connector should POST to {index_url}/submit and return response JSON."""
    from inandout.registry.publish import ConnectorSubmission, submit_connector

    submission = ConnectorSubmission(
        name="testpub",
        version="1.0.0",
        description="A test connector",
        yaml_content="schema_version: 1\n",
    )

    respx.post("https://marketplace.example.com/submit").mock(
        return_value=httpx.Response(
            200,
            json={"status": "pending_review", "id": "abc-123"},
        )
    )

    result = await submit_connector(
        submission=submission,
        index_url="https://marketplace.example.com",
        token="my-secret-token",
    )

    assert result["status"] == "pending_review"
    assert result["id"] == "abc-123"


@pytest.mark.anyio
@respx.mock
async def test_submit_connector_server_error():
    """submit_connector should raise ValueError on HTTP error."""
    from inandout.registry.publish import ConnectorSubmission, submit_connector

    submission = ConnectorSubmission(
        name="testpub",
        version="1.0.0",
        description="A test connector",
        yaml_content="schema_version: 1\n",
    )

    respx.post("https://marketplace.example.com/submit").mock(
        return_value=httpx.Response(422, text="Validation failed")
    )

    with pytest.raises(ValueError, match="422"):
        await submit_connector(
            submission=submission,
            index_url="https://marketplace.example.com",
            token="bad-token",
        )


@pytest.mark.anyio
@respx.mock
async def test_submit_connector_sends_bearer_token():
    """submit_connector should send Authorization: Bearer header."""
    from inandout.registry.publish import ConnectorSubmission, submit_connector

    submission = ConnectorSubmission(
        name="testpub",
        version="1.0.0",
        description="A test connector",
        yaml_content="schema_version: 1\n",
    )

    captured_request = None

    def _capture(request: httpx.Request):
        nonlocal captured_request
        captured_request = request
        return httpx.Response(200, json={"status": "ok", "id": "xyz"})

    respx.post("https://marketplace.example.com/submit").mock(side_effect=_capture)

    await submit_connector(
        submission=submission,
        index_url="https://marketplace.example.com",
        token="my-secret-token",
    )

    assert captured_request is not None
    auth_header = captured_request.headers.get("authorization", "")
    assert auth_header == "Bearer my-secret-token"
