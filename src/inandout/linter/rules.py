"""Connector YAML lint rules."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Literal


@dataclass
class LintDiagnostic:
    severity: Literal["error", "warning", "info"]
    rule_id: str
    message: str
    path: str  # dot-notation config path


def _lint001(cfg: Any) -> list[LintDiagnostic]:
    """LINT001: cursor pagination config has no response_path → error."""
    diags: list[LintDiagnostic] = []
    connector = cfg.connector
    for dtype_name, dtype_cfg in connector.datatypes.items():
        if dtype_cfg.ingestion is None:
            continue
        pagination = dtype_cfg.ingestion.list.pagination
        pag_dict = pagination.model_dump() if hasattr(pagination, "model_dump") else {}
        strategy = str(pag_dict.get("strategy", ""))
        # Check for cursor strategy: cursor sub-config must have response_path
        cursor_sub = pag_dict.get("cursor") or {}
        if strategy == "cursor" and not cursor_sub.get("response_path"):
            diags.append(LintDiagnostic(
                severity="error",
                rule_id="LINT001",
                message=f"cursor pagination in datatype '{dtype_name}' has no 'response_path'",
                path=f"connector.datatypes.{dtype_name}.ingestion.list.pagination",
            ))
    return diags


def _lint002(cfg: Any) -> list[LintDiagnostic]:
    """LINT002: credential_ref not resolvable in current env → warning."""
    diags: list[LintDiagnostic] = []
    connector = cfg.connector
    auth = connector.auth
    # Check for env-var style credential refs
    auth_dict = auth.model_dump() if hasattr(auth, "model_dump") else {}
    credential_ref = auth_dict.get("credential_ref")
    if credential_ref:
        # credential_ref is expected to be an env-var name
        env_var = str(credential_ref).upper().replace("-", "_").replace(".", "_")
        if not os.environ.get(env_var) and not os.environ.get(credential_ref):
            diags.append(LintDiagnostic(
                severity="warning",
                rule_id="LINT002",
                message=f"credential_ref '{credential_ref}' not found in environment",
                path="connector.auth.credential_ref",
            ))
    return diags


def _lint003(cfg: Any) -> list[LintDiagnostic]:
    """LINT003: cron fires more frequently than once per minute → warning."""
    diags: list[LintDiagnostic] = []
    connector = cfg.connector
    for dtype_name, dtype_cfg in connector.datatypes.items():
        if dtype_cfg.ingestion is None:
            continue
        schedule = dtype_cfg.ingestion.schedule
        if not schedule.cron:
            continue
        try:
            from croniter import croniter
            import datetime
            now = datetime.datetime.now(datetime.timezone.utc)
            cron = croniter(schedule.cron, now)
            next1 = cron.get_next(datetime.datetime)
            next2 = cron.get_next(datetime.datetime)
            diff_secs = (next2 - next1).total_seconds()
            if diff_secs < 60:
                diags.append(LintDiagnostic(
                    severity="warning",
                    rule_id="LINT003",
                    message=(
                        f"cron '{schedule.cron}' in datatype '{dtype_name}' "
                        f"fires every {diff_secs:.0f}s — more frequently than once per minute"
                    ),
                    path=f"connector.datatypes.{dtype_name}.ingestion.schedule",
                ))
        except Exception:
            pass
    return diags


def _lint004(cfg: Any) -> list[LintDiagnostic]:
    """LINT004: field mapping source uses dot-notation but no record_selector → info."""
    diags: list[LintDiagnostic] = []
    connector = cfg.connector
    for dtype_name, dtype_cfg in connector.datatypes.items():
        if dtype_cfg.ingestion is None or not dtype_cfg.field_mappings:
            continue
        record_selector = dtype_cfg.ingestion.list.record_selector
        for fm in dtype_cfg.field_mappings:
            source = getattr(fm, "source_field", None) or getattr(fm, "source", None)
            if source and "." in str(source) and not record_selector:
                diags.append(LintDiagnostic(
                    severity="info",
                    rule_id="LINT004",
                    message=(
                        f"field mapping source '{source}' uses dot-notation but "
                        f"datatype '{dtype_name}' has no record_selector producing nested records"
                    ),
                    path=f"connector.datatypes.{dtype_name}.field_mappings",
                ))
    return diags


def _lint005(cfg: Any) -> list[LintDiagnostic]:
    """LINT005: writeback operations.lookup missing when protection_level=optimistic → error."""
    diags: list[LintDiagnostic] = []
    connector = cfg.connector
    for dtype_name, dtype_cfg in connector.datatypes.items():
        if dtype_cfg.writeback is None:
            continue
        wb = dtype_cfg.writeback
        from inandout.config.writeback import ProtectionLevel
        if wb.protection_level == ProtectionLevel.optimistic:
            ops = wb.operations
            if ops.lookup is None:
                diags.append(LintDiagnostic(
                    severity="error",
                    rule_id="LINT005",
                    message=(
                        f"datatype '{dtype_name}' has protection_level=optimistic "
                        "but operations.lookup is missing"
                    ),
                    path=f"connector.datatypes.{dtype_name}.writeback.operations.lookup",
                ))
    return diags


def _lint006(cfg: Any, known_connector_names: list[str]) -> list[LintDiagnostic]:
    """LINT006: depends_on references unknown connector → warning."""
    diags: list[LintDiagnostic] = []
    connector = cfg.connector
    known = set(known_connector_names)
    for dep in getattr(connector, "depends_on", []):
        if dep not in known:
            diags.append(LintDiagnostic(
                severity="warning",
                rule_id="LINT006",
                message=(
                    f"connector '{connector.name}' depends_on '{dep}' "
                    "which is not in the known connectors directory"
                ),
                path="connector.depends_on",
            ))
    return diags


def _lint007(cfg: Any) -> list[LintDiagnostic]:
    """LINT007: max_lag_seconds set but no alerting channel configured → info."""
    diags: list[LintDiagnostic] = []
    connector = cfg.connector
    for dtype_name, dtype_cfg in connector.datatypes.items():
        if dtype_cfg.ingestion is None:
            continue
        schedule = dtype_cfg.ingestion.schedule
        if schedule.max_lag_seconds is not None:
            # Check if the connector file has alerting configured
            # We check the top-level cfg (ConnectorFileConfig) for an alerting key
            cfg_dict = cfg.model_dump() if hasattr(cfg, "model_dump") else {}
            alerting = cfg_dict.get("alerting") or cfg_dict.get("connector", {}).get("alerting")
            if not alerting:
                diags.append(LintDiagnostic(
                    severity="info",
                    rule_id="LINT007",
                    message=(
                        f"datatype '{dtype_name}' has max_lag_seconds="
                        f"{schedule.max_lag_seconds} but no alerting channel is configured — "
                        "SLA violations will be silent"
                    ),
                    path=f"connector.datatypes.{dtype_name}.ingestion.schedule.max_lag_seconds",
                ))
    return diags


def _lint008(cfg: Any) -> list[LintDiagnostic]:
    """LINT008: writeback protection_level=none — no write-anomaly guard active → warning."""
    from inandout.config.writeback import ProtectionLevel

    diags: list[LintDiagnostic] = []
    connector = cfg.connector
    for dtype_name, dtype_cfg in connector.datatypes.items():
        if dtype_cfg.writeback is None:
            continue
        wb = dtype_cfg.writeback
        if wb.protection_level == ProtectionLevel.none:
            diags.append(LintDiagnostic(
                severity="warning",
                rule_id="LINT008",
                message=(
                    f"datatype '{dtype_name}' writeback has protection_level=none — "
                    "no write-anomaly guard is active; external modifications may be "
                    "silently overwritten"
                ),
                path=f"connector.datatypes.{dtype_name}.writeback.protection_level",
            ))
    return diags


def _lint010(cfg: Any) -> list[LintDiagnostic]:
    """LINT010: 'merge'/'split' in supported_actions but required operations not configured → error."""
    diags: list[LintDiagnostic] = []
    connector = cfg.connector
    for dtype_name, dtype_cfg in connector.datatypes.items():
        if dtype_cfg.writeback is None:
            continue
        wb = dtype_cfg.writeback
        actions = [str(a).lower() for a in (getattr(wb, "supported_actions", None) or [])]
        ops = wb.operations

        if "merge" in actions and ops.update is None:
            diags.append(LintDiagnostic(
                severity="error",
                rule_id="LINT010",
                message=(
                    f"datatype '{dtype_name}' lists 'merge' in supported_actions "
                    "but operations.update is not configured — merge requires an update operation"
                ),
                path=f"connector.datatypes.{dtype_name}.writeback.operations.update",
            ))
        if "split" in actions and ops.insert is None:
            diags.append(LintDiagnostic(
                severity="error",
                rule_id="LINT010",
                message=(
                    f"datatype '{dtype_name}' lists 'split' in supported_actions "
                    "but operations.insert is not configured — split requires an insert operation"
                ),
                path=f"connector.datatypes.{dtype_name}.writeback.operations.insert",
            ))
    return diags


def _lint011(cfg: Any) -> list[LintDiagnostic]:
    """LINT011: PII fields declared on datatype that also has writeback → info."""
    diags: list[LintDiagnostic] = []
    connector = cfg.connector
    for dtype_name, dtype_cfg in connector.datatypes.items():
        pii = list(getattr(dtype_cfg, "pii_fields", None) or [])
        if not pii:
            continue
        if dtype_cfg.writeback is not None:
            sample = ", ".join(pii[:3])
            ellipsis_mark = "\u2026" if len(pii) > 3 else ""
            diags.append(LintDiagnostic(
                severity="info",
                rule_id="LINT011",
                message=(
                    f"datatype '{dtype_name}' declares {len(pii)} PII field(s) "
                    f"({sample}{ellipsis_mark}) and also has a writeback config — "
                    "verify that PII fields are excluded from writeback payloads "
                    "or that the target system is authorised to receive them"
                ),
                path=f"connector.datatypes.{dtype_name}.pii_fields",
            ))
    return diags
