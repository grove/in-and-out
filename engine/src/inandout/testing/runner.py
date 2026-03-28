"""Connector test runner — loads connector and runs all test cases."""
from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

from inandout.testing.framework import ConnectorTestSuite


async def run_connector_tests(connector_path: Path) -> ConnectorTestSuite:
    """Load a connector YAML and run all built-in test cases.

    Parameters
    ----------
    connector_path:
        Path to the connector YAML file.

    Returns
    -------
    ConnectorTestSuite
    """
    from inandout.config.loader import load_connector
    from inandout.testing.cases import (
        test_yaml_valid,
        test_credentials_resolvable,
        test_pagination_config_complete,
        test_field_mappings_valid,
        test_writeback_operations_complete,
        test_mock_fetch_one_page,
        test_lint_rules_pass,
    )

    # Load connector — if this fails, return a suite with one failing result
    try:
        cfg = load_connector(connector_path)
    except Exception as exc:
        suite = ConnectorTestSuite(connector=connector_path.stem)
        from inandout.testing.framework import ConnectorTestResult
        suite.results.append(ConnectorTestResult(
            test_name="test_yaml_valid",
            passed=False,
            message=f"Failed to load connector: {exc}",
            duration_ms=0.0,
        ))
        return suite

    connector_name = cfg.connector.name
    suite = ConnectorTestSuite(connector=connector_name)

    # Run synchronous tests
    suite.results.append(test_yaml_valid(cfg))
    suite.results.append(test_credentials_resolvable(cfg))
    suite.results.append(test_pagination_config_complete(cfg))
    suite.results.append(test_field_mappings_valid(cfg))
    suite.results.append(test_writeback_operations_complete(cfg))
    suite.results.append(test_lint_rules_pass(cfg))

    # Run async mock-fetch tests for each ingestion datatype
    for dtype_name, dtype_cfg in cfg.connector.datatypes.items():
        if dtype_cfg.ingestion is not None:
            result = await test_mock_fetch_one_page(cfg, dtype_name)
            result.test_name = f"test_mock_fetch_one_page[{dtype_name}]"
            suite.results.append(result)

    return suite


def format_junit_xml(suite: ConnectorTestSuite) -> str:
    """Format a ConnectorTestSuite as JUnit XML string."""
    testsuite = ET.Element("testsuite")
    testsuite.set("name", suite.connector)
    testsuite.set("tests", str(suite.total))
    testsuite.set("failures", str(suite.failed))
    testsuite.set("errors", "0")

    for result in suite.results:
        tc = ET.SubElement(testsuite, "testcase")
        tc.set("name", result.test_name)
        tc.set("classname", f"inandout.testing.{suite.connector}")
        tc.set("time", f"{result.duration_ms / 1000:.3f}")

        if not result.passed:
            failure = ET.SubElement(tc, "failure")
            failure.set("message", result.message)
            failure.text = result.message

    tree = ET.ElementTree(testsuite)
    ET.indent(tree, space="  ")
    import io
    buf = io.StringIO()
    tree.write(buf, encoding="unicode", xml_declaration=True)
    return buf.getvalue()


def format_text_report(suite: ConnectorTestSuite) -> str:
    """Format a ConnectorTestSuite as a human-readable text report."""
    lines = [
        f"Connector Test Suite: {suite.connector}",
        "=" * 50,
    ]
    for result in suite.results:
        status = "PASS" if result.passed else "FAIL"
        icon = "✓" if result.passed else "✗"
        lines.append(f"  {icon} [{status}] {result.test_name} ({result.duration_ms:.1f}ms)")
        if not result.passed:
            lines.append(f"         {result.message}")
    lines.append("")
    lines.append(f"Results: {suite.passed}/{suite.total} passed, {suite.failed} failed")
    return "\n".join(lines)
