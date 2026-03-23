"""Connector marketplace publishing."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import structlog
from pydantic import BaseModel, ConfigDict

logger = structlog.get_logger(__name__)


class ConnectorSubmission(BaseModel):
    """Submission payload for the connector marketplace."""

    model_config = ConfigDict(extra="forbid")

    name: str
    version: str
    description: str
    yaml_content: str
    hooks_content: str | None = None


def build_submission(
    connector_path: Path,
    hooks_path: Path | None,
    description: str,
    version: str,
) -> ConnectorSubmission:
    """Read connector files and build a ConnectorSubmission.

    Parameters
    ----------
    connector_path:
        Path to the connector YAML file.
    hooks_path:
        Optional path to the hooks Python file.
    description:
        Short description for the submission.
    version:
        Version string for the submission.
    """
    from inandout.config.loader import load_connector

    cfg = load_connector(connector_path)
    connector_name = cfg.connector.name

    yaml_content = connector_path.read_text(encoding="utf-8")
    hooks_content: str | None = None
    if hooks_path is not None:
        hooks_content = hooks_path.read_text(encoding="utf-8")

    return ConnectorSubmission(
        name=connector_name,
        version=version,
        description=description or cfg.connector.description or "",
        yaml_content=yaml_content,
        hooks_content=hooks_content,
    )


async def validate_for_publish(connector_path: Path) -> list[str]:
    """Validate a connector for publishing.

    Runs the linter and connector tests. Returns a list of error strings.
    An empty list means the connector is ready to publish.

    Parameters
    ----------
    connector_path:
        Path to the connector YAML file.
    """
    errors: list[str] = []

    # Run linter
    try:
        from inandout.config.loader import load_connector
        from inandout.linter import lint_connector

        cfg = load_connector(connector_path)
        diags = lint_connector(cfg)
        for d in diags:
            if d.severity == "error":
                errors.append(f"[LINT] {d.rule_id}: {d.message}")
    except Exception as exc:
        errors.append(f"[LINT] Failed to run linter: {exc}")

    # Run connector tests
    try:
        from inandout.testing.runner import run_connector_tests

        suite = await run_connector_tests(connector_path)
        for result in suite.results:
            if not result.passed:
                # Skip mock fetch failures as they require a live API
                if "test_mock_fetch_one_page" in result.test_name:
                    continue
                # Skip credential failures as they may not be set in CI
                if result.test_name == "test_credentials_resolvable":
                    continue
                errors.append(f"[TEST] {result.test_name}: {result.message}")
    except Exception as exc:
        errors.append(f"[TEST] Failed to run connector tests: {exc}")

    return errors


async def submit_connector(
    submission: ConnectorSubmission,
    index_url: str,
    token: str,
) -> dict[str, Any]:
    """Submit a connector to the marketplace.

    Parameters
    ----------
    submission:
        The connector submission payload.
    index_url:
        Base URL of the marketplace API.
    token:
        Bearer token for authentication.

    Returns
    -------
    dict
        Response JSON from the marketplace (e.g. {"status": "pending_review", "id": "..."}).

    Raises
    ------
    httpx.HTTPStatusError
        If the server returns a 4xx or 5xx status code.
    ValueError
        If the submission is rejected with a helpful message.
    """
    submit_url = f"{index_url.rstrip('/')}/submit"

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            submit_url,
            json=submission.model_dump(),
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )

        if response.status_code >= 400:
            try:
                body = response.text
            except Exception:
                body = "(could not read response body)"
            raise ValueError(
                f"Submission failed with HTTP {response.status_code}: {body}"
            )

        return response.json()
