"""CI boundary check — inandout_simulator must have zero inandout.* imports.

This test walks every .py file under simulator/src/inandout_simulator/ AND
simulator/tests/ with the AST parser and asserts that none of them import
from the engine's ``inandout`` namespace at all.  The only allowed coupling
is to the JSON schemas in ``schemas/`` (loaded via yaml + jsonschema at
runtime) — no Python code from the engine may be referenced.
"""

import ast
import pathlib

_SIMULATOR_ROOT = pathlib.Path(__file__).parent.parent  # simulator/

_SCAN_DIRS = [
    _SIMULATOR_ROOT / "src" / "inandout_simulator",
    _SIMULATOR_ROOT / "tests",
]


def _collect_import_modules(source: str) -> list[str]:
    """Return all module names referenced by import statements in *source*."""
    tree = ast.parse(source)
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                modules.append(node.module)
    return modules


def test_simulator_has_zero_inandout_engine_imports() -> None:
    """No file in the simulator package or its tests may import from the inandout engine."""
    violations: list[str] = []
    for scan_dir in _SCAN_DIRS:
        for py_file in sorted(scan_dir.rglob("*.py")):
            source = py_file.read_text(encoding="utf-8")
            for module in _collect_import_modules(source):
                # Allow intra-simulator imports (inandout_simulator.*).
                # Reject anything from the engine namespace (inandout.*).
                if module.startswith("inandout."):
                    violations.append(
                        f"{py_file.relative_to(_SIMULATOR_ROOT)}: {module!r}"
                    )

    assert not violations, (
        "The simulator package and its tests must not import from the inandout engine package.\n"
        "Use inandout_simulator.loader for YAML loading and plain dicts for data.\n"
        "Violations:\n" + "\n".join(f"  {v}" for v in violations)
    )
