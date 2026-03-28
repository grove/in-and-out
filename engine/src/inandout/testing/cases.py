"""Built-in connector test cases."""
from __future__ import annotations

# Prevent pytest from collecting test_* functions in this module as test cases.
# These are connector test case *implementations*, not pytest tests.
__test__ = False

import time
from pathlib import Path
from typing import Any

from inandout.testing.framework import ConnectorTestResult


def _make_result(name: str, passed: bool, message: str, duration_ms: float) -> ConnectorTestResult:
    return ConnectorTestResult(
        test_name=name,
        passed=passed,
        message=message,
        duration_ms=duration_ms,
    )


def test_yaml_valid(cfg: Any) -> ConnectorTestResult:
    """Test that the connector YAML passes Pydantic schema validation."""
    start = time.monotonic()
    try:
        # cfg is already a ConnectorFileConfig — just access connector to trigger validation
        _ = cfg.connector
        duration = (time.monotonic() - start) * 1000
        return _make_result(
            "test_yaml_valid",
            passed=True,
            message=f"Connector '{cfg.connector.name}' is valid.",
            duration_ms=duration,
        )
    except Exception as exc:
        duration = (time.monotonic() - start) * 1000
        return _make_result(
            "test_yaml_valid",
            passed=False,
            message=f"YAML validation failed: {exc}",
            duration_ms=duration,
        )


def test_credentials_resolvable(cfg: Any) -> ConnectorTestResult:
    """Test that all credential_ref fields have corresponding environment variables."""
    import os
    start = time.monotonic()
    try:
        connector = cfg.connector
        auth_dict = connector.auth.model_dump() if hasattr(connector.auth, "model_dump") else {}
        credential_ref = auth_dict.get("credential_ref")

        if not credential_ref:
            duration = (time.monotonic() - start) * 1000
            return _make_result(
                "test_credentials_resolvable",
                passed=True,
                message="No credential_ref configured.",
                duration_ms=duration,
            )

        # Check with INOUT_CREDENTIAL_ prefix (EnvSecretBackend convention)
        prefix = "INOUT_CREDENTIAL_"
        env_var = prefix + str(credential_ref).upper().replace("-", "_").replace(".", "_")
        if os.environ.get(env_var) or os.environ.get(str(credential_ref)):
            duration = (time.monotonic() - start) * 1000
            return _make_result(
                "test_credentials_resolvable",
                passed=True,
                message=f"Credential '{credential_ref}' is resolvable via {env_var}.",
                duration_ms=duration,
            )
        else:
            duration = (time.monotonic() - start) * 1000
            return _make_result(
                "test_credentials_resolvable",
                passed=False,
                message=(
                    f"Credential '{credential_ref}' not resolvable. "
                    f"Set env var {env_var}."
                ),
                duration_ms=duration,
            )
    except Exception as exc:
        duration = (time.monotonic() - start) * 1000
        return _make_result(
            "test_credentials_resolvable",
            passed=False,
            message=f"Error checking credentials: {exc}",
            duration_ms=duration,
        )


def test_pagination_config_complete(cfg: Any) -> ConnectorTestResult:
    """Test that pagination configuration is complete for each datatype."""
    start = time.monotonic()
    try:
        connector = cfg.connector
        issues = []

        for dtype_name, dtype_cfg in connector.datatypes.items():
            if dtype_cfg.ingestion is None:
                continue

            pagination = dtype_cfg.ingestion.list.pagination
            pag_dict = pagination.model_dump() if hasattr(pagination, "model_dump") else {}
            strategy = str(pag_dict.get("strategy", ""))

            if strategy == "cursor":
                cursor_sub = pag_dict.get("cursor") or {}
                if not cursor_sub.get("response_path"):
                    issues.append(
                        f"Datatype '{dtype_name}': cursor pagination missing 'response_path'"
                    )
            elif strategy == "offset":
                offset_sub = pag_dict.get("offset") or {}
                has_total = bool(offset_sub.get("total_path"))
                has_has_more = bool(offset_sub.get("has_more_path"))
                if not has_total and not has_has_more:
                    # This is a warning-level issue; not critical if page_size is set
                    pass  # Allow offset pagination without total_path/has_more_path

        duration = (time.monotonic() - start) * 1000
        if issues:
            return _make_result(
                "test_pagination_config_complete",
                passed=False,
                message="; ".join(issues),
                duration_ms=duration,
            )
        return _make_result(
            "test_pagination_config_complete",
            passed=True,
            message="Pagination configuration is complete.",
            duration_ms=duration,
        )
    except Exception as exc:
        duration = (time.monotonic() - start) * 1000
        return _make_result(
            "test_pagination_config_complete",
            passed=False,
            message=f"Error checking pagination: {exc}",
            duration_ms=duration,
        )


def test_field_mappings_valid(cfg: Any) -> ConnectorTestResult:
    """Test that field mappings have no duplicates or circular references."""
    start = time.monotonic()
    try:
        connector = cfg.connector
        issues = []

        for dtype_name, dtype_cfg in connector.datatypes.items():
            mappings = dtype_cfg.field_mappings
            if not mappings:
                continue

            # Check for duplicate target names
            targets = [m.target for m in mappings]
            seen_targets: set[str] = set()
            for target in targets:
                if target in seen_targets:
                    issues.append(
                        f"Datatype '{dtype_name}': duplicate target field '{target}'"
                    )
                seen_targets.add(target)

            # Check for circular mappings (source == target)
            for m in mappings:
                if m.source == m.target:
                    issues.append(
                        f"Datatype '{dtype_name}': circular mapping '{m.source}' → '{m.target}'"
                    )

        duration = (time.monotonic() - start) * 1000
        if issues:
            return _make_result(
                "test_field_mappings_valid",
                passed=False,
                message="; ".join(issues),
                duration_ms=duration,
            )
        return _make_result(
            "test_field_mappings_valid",
            passed=True,
            message="Field mappings are valid.",
            duration_ms=duration,
        )
    except Exception as exc:
        duration = (time.monotonic() - start) * 1000
        return _make_result(
            "test_field_mappings_valid",
            passed=False,
            message=f"Error checking field mappings: {exc}",
            duration_ms=duration,
        )


def test_writeback_operations_complete(cfg: Any) -> ConnectorTestResult:
    """Test that writeback supported_actions have corresponding operation configs."""
    start = time.monotonic()
    try:
        connector = cfg.connector
        issues = []

        for dtype_name, dtype_cfg in connector.datatypes.items():
            if dtype_cfg.writeback is None:
                continue

            writeback = dtype_cfg.writeback
            wb_dict = writeback.model_dump() if hasattr(writeback, "model_dump") else {}
            supported_actions = wb_dict.get("supported_actions") or []
            operations = wb_dict.get("operations") or {}

            for action in supported_actions:
                # Check that a corresponding operation config exists
                if action in ("create", "update", "delete", "upsert"):
                    op_cfg = operations.get(action)
                    if not op_cfg:
                        issues.append(
                            f"Datatype '{dtype_name}': writeback supports '{action}' "
                            f"but operations.{action} is not configured"
                        )

        duration = (time.monotonic() - start) * 1000
        if issues:
            return _make_result(
                "test_writeback_operations_complete",
                passed=False,
                message="; ".join(issues),
                duration_ms=duration,
            )
        return _make_result(
            "test_writeback_operations_complete",
            passed=True,
            message="Writeback operations are complete.",
            duration_ms=duration,
        )
    except Exception as exc:
        duration = (time.monotonic() - start) * 1000
        return _make_result(
            "test_writeback_operations_complete",
            passed=False,
            message=f"Error checking writeback operations: {exc}",
            duration_ms=duration,
        )


async def test_mock_fetch_one_page(cfg: Any, datatype: str) -> ConnectorTestResult:
    """Test that one page can be fetched using a mock API response."""
    import respx
    import httpx

    start = time.monotonic()
    try:
        connector = cfg.connector
        dtype_cfg = connector.datatypes.get(datatype)
        if dtype_cfg is None or dtype_cfg.ingestion is None:
            duration = (time.monotonic() - start) * 1000
            return _make_result(
                "test_mock_fetch_one_page",
                passed=False,
                message=f"Datatype '{datatype}' has no ingestion config.",
                duration_ms=duration,
            )

        ingestion_cfg = dtype_cfg.ingestion
        base_url = connector.connection.base_url.rstrip("/")
        list_path = ingestion_cfg.list.path

        # Use respx to mock the API response
        mock_response = {
            "results": [{"id": "test-1", "name": "Test"}],
            "next_cursor": None,
        }

        records_fetched = []
        with respx.mock(assert_all_called=False):
            # Mock all GET requests to the list path
            url_pattern = f"{base_url}{list_path}"
            respx.get(url_pattern).mock(
                return_value=httpx.Response(200, json=mock_response)
            )

            from inandout.transport.http import HttpTransportAdapter
            async with HttpTransportAdapter(connector) as transport:
                async for page in transport.fetch_pages(ingestion_cfg.list, watermark=None):
                    records_fetched.extend(page)
                    break  # Only one page

        duration = (time.monotonic() - start) * 1000
        if records_fetched:
            return _make_result(
                "test_mock_fetch_one_page",
                passed=True,
                message=f"Fetched {len(records_fetched)} record(s) from mock.",
                duration_ms=duration,
            )
        else:
            return _make_result(
                "test_mock_fetch_one_page",
                passed=False,
                message="No records returned from mock fetch.",
                duration_ms=duration,
            )
    except Exception as exc:
        duration = (time.monotonic() - start) * 1000
        return _make_result(
            "test_mock_fetch_one_page",
            passed=False,
            message=f"Mock fetch failed: {exc}",
            duration_ms=duration,
        )


def test_lint_rules_pass(cfg: Any) -> ConnectorTestResult:
    """Test that the linter reports no errors (warnings are allowed)."""
    start = time.monotonic()
    try:
        from inandout.linter import lint_connector
        diags = lint_connector(cfg)
        errors = [d for d in diags if d.severity == "error"]

        duration = (time.monotonic() - start) * 1000
        if errors:
            error_msgs = [f"{d.rule_id}: {d.message}" for d in errors]
            return _make_result(
                "test_lint_rules_pass",
                passed=False,
                message=f"Linter errors: {'; '.join(error_msgs)}",
                duration_ms=duration,
            )
        return _make_result(
            "test_lint_rules_pass",
            passed=True,
            message=f"No linter errors ({len(diags)} warning(s)/info).",
            duration_ms=duration,
        )
    except Exception as exc:
        duration = (time.monotonic() - start) * 1000
        return _make_result(
            "test_lint_rules_pass",
            passed=False,
            message=f"Linter failed: {exc}",
            duration_ms=duration,
        )
