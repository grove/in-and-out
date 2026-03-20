#!/usr/bin/env python3
"""Validate connector fixture files used by agent workflows.

This script validates two things:
1) Valid fixtures satisfy required paths for their generation profile.
2) Invalid fixtures declare expected rule IDs that match detected rule IDs.

The checks are intentionally aligned with CONFIG_DESIGN.md §10.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import yaml  # type: ignore
except ImportError:
    print("ERROR: Missing dependency: pyyaml")
    print("Install with: pip install pyyaml")
    sys.exit(2)


ROOT = Path(__file__).resolve().parents[1]
VALID_DIR = ROOT / "fixtures" / "connectors" / "valid"
INVALID_DIR = ROOT / "fixtures" / "connectors" / "invalid"
SCHEMAS_DIR = ROOT / "schemas"


PROFILE_REQUIRED_PATHS: dict[str, list[str]] = {
    "ingestion_polling_readonly": [
        "schema_version",
        "connector.name",
        "connector.system",
        "connector.generation_profile",
        "connector.api_version",
        "connector.connection.base_url",
        "connector.auth",
        "connector.datatypes.{name}.ingestion.primary_key",
        "connector.datatypes.{name}.ingestion.history_mode",
        "connector.datatypes.{name}.ingestion.schedule",
        "connector.datatypes.{name}.ingestion.list.method",
        "connector.datatypes.{name}.ingestion.list.path",
        "connector.datatypes.{name}.ingestion.list.record_selector",
        "connector.datatypes.{name}.ingestion.list.pagination",
    ],
    "ingestion_webhook_incremental": [
        "schema_version",
        "connector.name",
        "connector.system",
        "connector.generation_profile",
        "connector.api_version",
        "connector.connection.base_url",
        "connector.auth",
        "connector.webhooks.path",
        "connector.webhooks.signature",
        "connector.webhooks.fan_out",
        "connector.datatypes.{name}.ingestion.primary_key",
        "connector.datatypes.{name}.ingestion.history_mode",
        "connector.datatypes.{name}.ingestion.schedule",
        "connector.datatypes.{name}.ingestion.list.method",
        "connector.datatypes.{name}.ingestion.list.path",
        "connector.datatypes.{name}.ingestion.list.record_selector",
        "connector.datatypes.{name}.ingestion.list.pagination",
        "connector.datatypes.{name}.ingestion.list.incremental",
        "connector.datatypes.{name}.ingestion.webhook_events",
    ],
    "writeback_patch": [
        "schema_version",
        "connector.name",
        "connector.system",
        "connector.generation_profile",
        "connector.api_version",
        "connector.connection.base_url",
        "connector.auth",
        "connector.datatypes.{name}.writeback.protection_level",
        "connector.datatypes.{name}.writeback.conflict_resolution",
        "connector.datatypes.{name}.writeback.supported_actions",
        "connector.datatypes.{name}.writeback.operations.lookup",
        "connector.datatypes.{name}.writeback.operations.update",
    ],
    "full_duplex": [
        "schema_version",
        "connector.name",
        "connector.system",
        "connector.generation_profile",
        "connector.api_version",
        "connector.connection.base_url",
        "connector.auth",
        "connector.webhooks.path",
        "connector.webhooks.signature",
        "connector.webhooks.fan_out",
        "connector.datatypes.{name}.ingestion.primary_key",
        "connector.datatypes.{name}.ingestion.history_mode",
        "connector.datatypes.{name}.ingestion.schedule",
        "connector.datatypes.{name}.ingestion.list.method",
        "connector.datatypes.{name}.ingestion.list.path",
        "connector.datatypes.{name}.ingestion.list.record_selector",
        "connector.datatypes.{name}.ingestion.list.pagination",
        "connector.datatypes.{name}.ingestion.list.incremental",
        "connector.datatypes.{name}.ingestion.webhook_events",
        "connector.datatypes.{name}.writeback.protection_level",
        "connector.datatypes.{name}.writeback.conflict_resolution",
        "connector.datatypes.{name}.writeback.supported_actions",
        "connector.datatypes.{name}.writeback.operations.lookup",
        "connector.datatypes.{name}.writeback.operations.update",
    ],
}


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


@dataclass
class CheckIssue:
    rule_id: str
    message: str
    path: str = "$."


def load_yaml(path: Path) -> Any:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def get_path(obj: Any, path: str) -> tuple[bool, Any]:
    parts = path.split(".")
    cur = obj
    for part in parts:
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return False, None
    return True, cur


def path_exists_for_all_datatypes(doc: dict[str, Any], path: str) -> bool:
    if "{name}" not in path:
        ok, _ = get_path(doc, path)
        return ok

    prefix, suffix = path.split("{name}")
    prefix = prefix.rstrip(".")
    suffix = suffix.lstrip(".")
    ok, datatypes = get_path(doc, prefix)
    if not ok or not isinstance(datatypes, dict) or not datatypes:
        return False

    for dtype_cfg in datatypes.values():
        target = dtype_cfg
        if suffix:
            for part in suffix.split("."):
                if isinstance(target, dict) and part in target:
                    target = target[part]
                else:
                    return False
    return True


def iter_strings(obj: Any) -> list[str]:
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


def is_allowed_interpolation(token: str) -> bool:
    if token in ALLOWED_INTERPOLATION_EXACT:
        return True
    if ENV_VAR_RE.match(token):
        return True
    return any(token.startswith(prefix) for prefix in ALLOWED_INTERPOLATION_PREFIXES)


def detect_cycles(graph: dict[str, list[str]]) -> bool:
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
    for s in iter_strings(doc):
        for token in INTERP_RE.findall(s):
            if not is_allowed_interpolation(token):
                issues.append(CheckIssue("CFG-002", f"Unknown interpolation namespace: ${{{token}}}"))

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
                dep_graph[dtype_name] = [d.get("depends_on") for d in deps if isinstance(d, dict) and d.get("depends_on")]
            else:
                dep_graph[dtype_name] = []
        if detect_cycles(dep_graph):
            issues.append(CheckIssue("CFG-011", "Cyclic datatype dependency graph"))

    return issues


def validate_valid_fixture(path: Path) -> list[str]:
    problems: list[str] = []
    doc = load_yaml(path)
    if not isinstance(doc, dict):
        return [f"{path}: not a YAML object"]

    issues = evaluate_rules(doc)
    if issues:
        problems.append(f"{path}: unexpected rule IDs in valid fixture: {sorted({i.rule_id for i in issues})}")

    profile = doc.get("connector", {}).get("generation_profile")
    if profile not in PROFILE_REQUIRED_PATHS:
        problems.append(f"{path}: unknown generation_profile: {profile}")
        return problems

    for req_path in PROFILE_REQUIRED_PATHS[profile]:
        if not path_exists_for_all_datatypes(doc, req_path):
            problems.append(f"{path}: missing required path for profile {profile}: {req_path}")

    return problems


def validate_invalid_fixture(path: Path) -> list[str]:
    problems: list[str] = []
    err_manifest = path.with_suffix(".errors.json")
    if not err_manifest.exists():
        return [f"{path}: missing expected error manifest {err_manifest.name}"]

    doc = load_yaml(path)
    if not isinstance(doc, dict):
        return [f"{path}: not a YAML object"]

    manifest = load_json(err_manifest)
    expected = set(manifest.get("expected_rule_ids", []))
    found = {issue.rule_id for issue in evaluate_rules(doc)}

    missing = sorted(expected - found)
    extra = sorted(found - expected)

    if missing:
        problems.append(f"{path}: expected rule IDs not found: {missing}")
    if extra:
        problems.append(f"{path}: unexpected additional rule IDs: {extra}")

    return problems


def parse_all_json_schemas() -> list[str]:
    problems: list[str] = []
    for json_file in sorted(SCHEMAS_DIR.rglob("*.json")):
        try:
            load_json(json_file)
        except Exception as exc:  # pylint: disable=broad-except
            problems.append(f"{json_file}: invalid JSON: {exc}")
    return problems


def main() -> int:
    problems: list[str] = []

    if not VALID_DIR.exists() or not INVALID_DIR.exists() or not SCHEMAS_DIR.exists():
        print("ERROR: expected directories are missing (schemas/ and fixtures/connectors/)")
        return 2

    problems.extend(parse_all_json_schemas())

    for path in sorted(VALID_DIR.glob("*.yaml")):
        problems.extend(validate_valid_fixture(path))

    for path in sorted(INVALID_DIR.glob("*.yaml")):
        problems.extend(validate_invalid_fixture(path))

    if problems:
        print("Fixture validation FAILED")
        for p in problems:
            print(f"- {p}")
        return 1

    print("Fixture validation PASSED")
    print(f"- Valid fixtures: {len(list(VALID_DIR.glob('*.yaml')))}")
    print(f"- Invalid fixtures: {len(list(INVALID_DIR.glob('*.yaml')))}")
    print(f"- Schemas parsed: {len(list(SCHEMAS_DIR.rglob('*.json')))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
