"""Local filesystem-based schema registry."""
from __future__ import annotations

import json
from pathlib import Path

from inandout.schema_registry.types import ColumnSchema, ConnectorSchema


class LocalSchemaRegistry:
    """Reads and writes ConnectorSchema files to/from a local directory.

    Files are named ``{connector}_{datatype}.json``.
    """

    def __init__(self, directory: str | Path) -> None:
        self._dir = Path(directory)
        self._dir.mkdir(parents=True, exist_ok=True)

    def _file_path(self, connector: str, datatype: str) -> Path:
        return self._dir / f"{connector}_{datatype}.json"

    async def get_schema(self, connector: str, datatype: str) -> ConnectorSchema | None:
        path = self._file_path(connector, datatype)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text())
            return ConnectorSchema.model_validate(data)
        except Exception:
            return None

    async def put_schema(self, schema: ConnectorSchema) -> None:
        path = self._file_path(schema.connector, schema.datatype)
        path.write_text(schema.model_dump_json(indent=2))

    @staticmethod
    def diff_schemas(old: ConnectorSchema, new: ConnectorSchema) -> list[str]:
        """Return a list of human-readable change descriptions between two schemas."""
        diffs: list[str] = []

        old_cols = {c.name: c for c in old.columns}
        new_cols = {c.name: c for c in new.columns}

        # Added columns
        for name in new_cols:
            if name not in old_cols:
                diffs.append(f"added column '{name}' ({new_cols[name].pg_type})")

        # Removed columns
        for name in old_cols:
            if name not in new_cols:
                diffs.append(f"removed column '{name}'")

        # Type changes
        for name in old_cols:
            if name in new_cols:
                oc = old_cols[name]
                nc = new_cols[name]
                if oc.pg_type != nc.pg_type:
                    diffs.append(
                        f"column '{name}' type changed: {oc.pg_type!r} → {nc.pg_type!r}"
                    )
                if oc.nullable != nc.nullable:
                    diffs.append(
                        f"column '{name}' nullable changed: {oc.nullable} → {nc.nullable}"
                    )

        return diffs


# ---------------------------------------------------------------------------
# Schema inference helpers
# ---------------------------------------------------------------------------

_FIELD_MAPPING_TYPE_MAP: dict[str, str] = {
    "str": "TEXT",
    "int": "INTEGER",
    "float": "FLOAT",
    "bool": "BOOLEAN",
    "datetime": "TIMESTAMPTZ",
    "date": "DATE",
    "uuid": "UUID",
    "json": "JSONB",
}


def infer_schema_from_record(
    connector: str,
    datatype: str,
    version: str,
    sample_record: dict,
    field_mappings: list | None = None,
) -> ConnectorSchema:
    """Infer a ConnectorSchema from a sample record and optional field_mappings."""
    # Build type map from field_mappings casts
    cast_map: dict[str, str] = {}
    if field_mappings:
        for fm in field_mappings:
            target = getattr(fm, "target_field", None) or getattr(fm, "source_field", None)
            cast = getattr(fm, "cast", None)
            if target and cast:
                pg_type = _FIELD_MAPPING_TYPE_MAP.get(str(cast).lower(), "TEXT")
                cast_map[target] = pg_type

    columns: list[ColumnSchema] = []
    for key, value in sample_record.items():
        if key in cast_map:
            pg_type = cast_map[key]
        elif isinstance(value, bool):
            pg_type = "BOOLEAN"
        elif isinstance(value, int):
            pg_type = "INTEGER"
        elif isinstance(value, float):
            pg_type = "FLOAT"
        elif isinstance(value, dict):
            pg_type = "JSONB"
        elif isinstance(value, list):
            pg_type = "JSONB"
        else:
            pg_type = "TEXT"
        columns.append(ColumnSchema(name=key, pg_type=pg_type, nullable=True))

    return ConnectorSchema(
        connector=connector,
        datatype=datatype,
        version=version,
        columns=columns,
    )
