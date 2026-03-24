"""Connector YAML static analysis / linter."""
from __future__ import annotations

from typing import Any

from inandout.linter.rules import (
    LintDiagnostic,
    _lint001,
    _lint002,
    _lint003,
    _lint004,
    _lint005,
    _lint006,
    _lint007,
    _lint008,
    _lint009,
    _lint010,
    _lint011,
)

__all__ = ["lint_connector", "LintDiagnostic"]


def lint_connector(
    cfg: Any,
    known_connector_names: list[str] | None = None,
) -> list[LintDiagnostic]:
    """Run all lint rules against a ConnectorFileConfig.

    Parameters
    ----------
    cfg:
        A ``ConnectorFileConfig`` object.
    known_connector_names:
        Names of all connectors visible in the connectors directory
        (used for LINT006 dependency validation).

    Returns
    -------
    list[LintDiagnostic]
        All diagnostics found (any severity).
    """
    known = known_connector_names or []
    diags: list[LintDiagnostic] = []
    diags.extend(_lint001(cfg))
    diags.extend(_lint002(cfg))
    diags.extend(_lint003(cfg))
    diags.extend(_lint004(cfg))
    diags.extend(_lint005(cfg))
    diags.extend(_lint006(cfg, known))
    diags.extend(_lint007(cfg))
    diags.extend(_lint008(cfg))
    diags.extend(_lint009(cfg))
    diags.extend(_lint010(cfg))
    diags.extend(_lint011(cfg))
    return diags
