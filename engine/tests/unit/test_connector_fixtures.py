"""Parametrized tests for connector fixture files.

Valid fixtures must produce no rule violations.
Invalid fixtures must produce exactly the rule IDs declared in their .errors.json manifest.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import yaml

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "connectors"
VALID_DIR = FIXTURES_DIR / "valid"
INVALID_DIR = FIXTURES_DIR / "invalid"

# ---------------------------------------------------------------------------
# Interpolation allow-lists
# ---------------------------------------------------------------------------

ALLOWED_INTERPOLATION_PREFIXES = (
    "runtime.",
    "credential.",
    "auth.",
    "record.",
    "data.",
    "pre_flight.",
    "subscription.",
)

ALLOWED_INTERPOLATION_EXACT = {
    "connection.base_url",
    "watermark",
    "external_id",
    "child.id",
    "job.id",
    "ingestion.field_selection",
    "pre_flight.etag",
    "pre_flight.version",
    "cluster_id",
}

ENV_VAR_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
INTERP_RE = re.compile(r"\$\{([^}]+)\}")


# ---------------------------------------------------------------------------
# Rule evaluation
# ---------------------------------------------------------------------------


@dataclass
class CheckIssue:
    rule_id: str
    message: str
    path: str = "$."


def _iter_strings(obj: Any) -> list[str]:
    out: list[str] = []
    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, str):
            out.append(cur)
        elif isinstance(cur, dict):
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)
    return out


def _is_allowed_interpolation(token: str) -> bool:
    if token in ALLOWED_INTERPOLATION_EXACT:
        return True
    if ENV_VAR_RE.match(token):
        return True
    return any(token.startswith(p) for p in ALLOWED_INTERPOLATION_PREFIXES)


def _detect_cycles(graph: dict[str, list[str]]) -> bool:
    visiting: set[str] = set()
    visited: set[str] = set()

    def dfs(node: str) -> bool:
        if node in visiting:
            return True
        if node in visited:
            return False
        visiting.add(node)
        for nxt in graph.get(node, []):
            if nxt in graph and dfs(nxt):
                return True
        visiting.remove(node)
        visited.add(node)
        return False

    return any(dfs(node) for node in graph)


def evaluate_rules(doc: dict[str, Any]) -> list[CheckIssue]:
    issues: list[CheckIssue] = []

    # CFG-005 missing required top-level keys
    if not isinstance(doc, dict) or "schema_version" not in doc or "connector" not in doc:
        issues.append(CheckIssue("CFG-005", "Missing required top-level keys"))
        return issues

    # CFG-006 unsupported schema version
    if doc.get("schema_version") != 1:
        issues.append(CheckIssue("CFG-006", "Unsupported schema_version", "$.schema_version"))

    connector = doc.get("connector", {})
    if not isinstance(connector, dict):
        issues.append(CheckIssue("CFG-005", "connector must be an object", "$.connector"))
        return issues

    # CFG-002 unknown interpolation namespace
    for s in _iter_strings(doc):
        for token in INTERP_RE.findall(s):
            if not _is_allowed_interpolation(token):
                issues.append(
                    CheckIssue("CFG-002", f"Unknown interpolation namespace: ${{{token}}}")
                )

    datatypes = connector.get("datatypes", {})
    if isinstance(datatypes, dict):
        for dtype_name, dtype_cfg in datatypes.items():
            if not isinstance(dtype_cfg, dict):
                continue

            ingestion = dtype_cfg.get("ingestion")
            if isinstance(ingestion, dict):
                list_cfg = ingestion.get("list")
                if isinstance(list_cfg, dict):
                    pagination = list_cfg.get("pagination")
                    if isinstance(pagination, dict) and pagination.get("strategy") == "cursor":
                        cursor = pagination.get("cursor")
                        if not (
                            isinstance(cursor, dict)
                            and "response_path" in cursor
                            and "request_param" in cursor
                        ):
                            issues.append(
                                CheckIssue(
                                    "CFG-001",
                                    "cursor strategy requires cursor.response_path and cursor.request_param",
                                    f"$.connector.datatypes.{dtype_name}.ingestion.list.pagination",
                                )
                            )

            writeback = dtype_cfg.get("writeback")
            if isinstance(writeback, dict):
                # CFG-010 protection-level / conditional-write pairing
                if writeback.get("protection_level") == 1:
                    enabled = (
                        writeback.get("operations", {})
                        .get("update", {})
                        .get("conditional_write", {})
                        .get("enabled")
                    )
                    if enabled is not True:
                        issues.append(
                            CheckIssue(
                                "CFG-010",
                                "protection_level=1 requires operations.update.conditional_write.enabled=true",
                                f"$.connector.datatypes.{dtype_name}.writeback.operations.update.conditional_write.enabled",
                            )
                        )

        # CFG-011 cyclic dependency graph
        dep_graph: dict[str, list[str]] = {}
        for dtype_name, dtype_cfg in datatypes.items():
            if not isinstance(dtype_cfg, dict):
                continue
            deps = dtype_cfg.get("writeback", {}).get("dependencies", [])
            if isinstance(deps, list):
                dep_graph[dtype_name] = [
                    d.get("depends_on") for d in deps if isinstance(d, dict) and d.get("depends_on")
                ]
            else:
                dep_graph[dtype_name] = []
        if _detect_cycles(dep_graph):
            issues.append(CheckIssue("CFG-011", "Cyclic datatype dependency graph"))

    return issues


# ---------------------------------------------------------------------------
# Parametrized tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", sorted(VALID_DIR.glob("*.yaml")), ids=lambda p: p.stem)
def test_valid_fixture(path: Path) -> None:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(doc, dict), "fixture must be a YAML object"
    issues = evaluate_rules(doc)
    assert issues == [], f"unexpected rule violations: {[i.rule_id for i in issues]}"


@pytest.mark.parametrize("path", sorted(INVALID_DIR.glob("*.yaml")), ids=lambda p: p.stem)
def test_invalid_fixture(path: Path) -> None:
    err_manifest = path.with_suffix(".errors.json")
    assert err_manifest.exists(), f"missing error manifest: {err_manifest.name}"

    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(doc, dict), "fixture must be a YAML object"

    manifest = json.loads(err_manifest.read_text(encoding="utf-8"))
    expected = set(manifest.get("expected_rule_ids", []))
    found = {issue.rule_id for issue in evaluate_rules(doc)}

    assert found == expected, (
        f"rule ID mismatch — missing: {sorted(expected - found)}, extra: {sorted(found - expected)}"
    )
