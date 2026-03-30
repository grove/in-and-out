"""Schema-native YAML loader for connector files.

Validates the raw YAML against ``schemas/connector.schema.json`` using
jsonschema — no Pydantic dependency.  Returns the ``connector`` sub-dict
directly so callers can use plain dict access.
"""

from __future__ import annotations

import json
import os
import pathlib

import jsonschema
import jsonschema.validators
import referencing
import referencing.jsonschema
import yaml

_SCHEMAS_DIR = pathlib.Path(
    os.environ.get("INANDOUT_SCHEMAS_DIR")
    or str(pathlib.Path(__file__).parent.parent.parent.parent / "schemas")
)


def _build_registry() -> referencing.Registry:
    """Build a local referencing registry from all schemas in *_SCHEMAS_DIR*."""
    resources: list[referencing.Resource] = []
    for schema_path in [
        _SCHEMAS_DIR / "connector.schema.json",
        *(_SCHEMAS_DIR / "defs").glob("*.json"),
    ]:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        resources.append(
            referencing.Resource.from_contents(
                schema,
                default_specification=referencing.jsonschema.DRAFT202012,
            )
        )
    return referencing.Registry().with_resources(
        (r.id(), r)
        for r in resources  # type: ignore[union-attr]
    )


_REGISTRY = _build_registry()
_CONNECTOR_SCHEMA = json.loads((_SCHEMAS_DIR / "connector.schema.json").read_text(encoding="utf-8"))
_VALIDATOR_CLS = jsonschema.validators.validator_for(_CONNECTOR_SCHEMA)


def load_connector(path: str | pathlib.Path) -> dict:
    """Load and validate a connector YAML file.

    Reads *path*, validates its top-level structure against the JSON schema,
    and returns the ``connector`` dict (i.e. ``raw["connector"]``).
    """
    raw = yaml.safe_load(pathlib.Path(path).read_text(encoding="utf-8"))
    validator = _VALIDATOR_CLS(_CONNECTOR_SCHEMA, registry=_REGISTRY)
    errors = list(validator.iter_errors(raw))
    if errors:
        raise jsonschema.ValidationError(
            f"Connector YAML validation failed ({len(errors)} error(s)):\n"
            + "\n".join(f"  - {e.message} (at {list(e.absolute_path)})" for e in errors)
        )
    return raw["connector"]
