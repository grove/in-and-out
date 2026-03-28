"""CI boundary check — the simulator package must not import inandout.config.

This test walks every .py file under src/inandout/simulator/ with the AST
parser and asserts that none of them contain a direct import from the engine's
internal ``inandout.config`` namespace.  The simulator must only reference the
public schema contract at ``inandout.schema``.
"""

import ast
import pathlib


_SIMULATOR_DIR = pathlib.Path(__file__).parent.parent.parent / "src" / "inandout" / "simulator"


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


def test_simulator_does_not_import_inandout_config() -> None:
    """No simulator source file may import from inandout.config.*."""
    violations: list[str] = []
    for py_file in sorted(_SIMULATOR_DIR.rglob("*.py")):
        source = py_file.read_text(encoding="utf-8")
        for module in _collect_import_modules(source):
            if module.startswith("inandout.config"):
                violations.append(f"{py_file.relative_to(_SIMULATOR_DIR.parent.parent.parent)}: {module!r}")

    assert not violations, (
        "Simulator files must import from inandout.schema, not inandout.config.\n"
        "Violations:\n" + "\n".join(f"  {v}" for v in violations)
    )
