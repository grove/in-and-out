"""Connector testing framework data models."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ConnectorTestCase:
    """Metadata for a connector test case."""

    name: str
    description: str


@dataclass
class ConnectorTestResult:
    """Result of running a single test case."""

    test_name: str
    passed: bool
    message: str
    duration_ms: float


@dataclass
class ConnectorTestSuite:
    """Collection of test results for a connector."""

    connector: str
    results: list[ConnectorTestResult] = field(default_factory=list)

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    @property
    def total(self) -> int:
        return len(self.results)
