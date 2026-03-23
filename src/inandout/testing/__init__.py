"""Connector testing framework."""
from __future__ import annotations

from inandout.testing.framework import (
    ConnectorTestCase,
    ConnectorTestResult,
    ConnectorTestSuite,
)
from inandout.testing.runner import (
    format_junit_xml,
    format_text_report,
    run_connector_tests,
)

__all__ = [
    "ConnectorTestCase",
    "ConnectorTestResult",
    "ConnectorTestSuite",
    "format_junit_xml",
    "format_text_report",
    "run_connector_tests",
]
