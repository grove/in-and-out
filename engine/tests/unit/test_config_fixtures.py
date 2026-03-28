"""Validate connector YAML fixtures against the Pydantic config models.

Valid fixtures must parse without error.
Invalid fixtures must raise ValidationError and the error message must
reference the expected CFG-XXX rule ID.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from inandout.config import load_connector

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "connectors"
VALID_DIR = FIXTURES / "valid"
INVALID_DIR = FIXTURES / "invalid"


# ---------------------------------------------------------------------------
# Valid fixtures — must parse cleanly
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", sorted(VALID_DIR.glob("*.yaml")))
def test_valid_fixture_parses(path: Path) -> None:
    config = load_connector(path)
    assert config.schema_version == 1
    assert config.connector.name  # non-empty


# ---------------------------------------------------------------------------
# Invalid fixtures — must raise ValidationError with the expected rule ID
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename, expected_rule_id",
    [
        ("invalid_pagination_shape.yaml", "CFG-001"),
        ("unknown_namespace.yaml", "CFG-002"),
        ("missing_schema_version.yaml", "CFG-005"),
        ("invalid_protection_level_pairing.yaml", "CFG-010"),
        ("cyclic_dependencies.yaml", "CFG-011"),
    ],
)
def test_invalid_fixture_raises(filename: str, expected_rule_id: str) -> None:
    path = INVALID_DIR / filename
    with pytest.raises(ValidationError) as exc_info:
        load_connector(path)

    errors_text = str(exc_info.value)
    assert expected_rule_id in errors_text, (
        f"Expected rule ID {expected_rule_id!r} not found in validation error.\n"
        f"Actual errors:\n{errors_text}"
    )
