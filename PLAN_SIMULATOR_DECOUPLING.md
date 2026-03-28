# Plan: Decouple the simulator from the engine package

> Status: **Revised — JSON-schema-native approach adopted**
> Goal: make the simulator installable and runnable with **zero Python code
> coupling to the engine**, so any alternative engine implementation (Go, Rust,
> Node, a third-party tool) can point at the same simulator by reading the same
> YAML connector files.  The contract is the **JSON schemas in `schemas/`**,
> not the Python models in `inandout.config`.

---

## Final architecture

```
connector.yaml
    │
    ▼
yaml.safe_load()
    │
    ├─► jsonschema.validate(schemas/connector.schema.json)  ← contract
    │
    ▼
  raw dict
    │
    ├─► simulator/app.py          (startup, route registration)
    ├─► simulator/route_builder.py (FastAPI routes, dict navigation)
    ├─► simulator/webhooks.py      (fan-out dispatch)
    └─► simulator/seed.py          (store pre-population)
```

The simulator imports **nothing from `inandout.config` or `inandout.schema`**.
Dependencies are `pyyaml`, `jsonschema`, `fastapi`, `uvicorn`, `httpx`, `jinja2` —
and nothing else from this repo.

---

## Current state (post shim phase)

The simulator was moved from `inandout.config.*` → `inandout.schema.*` imports
as a first step.  `inandout.schema` is a thin re-export shim over
`inandout.config` (Pydantic models).  This achieves namespace separation but
**not code decoupling** — the simulator still runs Python Pydantic models at
runtime.

| Simulator file      | Current import                      | Target                  |
|---------------------|--------------------------------------|-------------------------|
| `app.py`            | `inandout.schema.connector`, `loader`| None — raw dict         |
| `route_builder.py`  | `inandout.schema.connector`, `pagination` | None — string compare  |
| `webhooks.py`       | `inandout.schema.connector`          | None — raw dict         |
| `seed.py`           | `inandout.schema.connector`          | None — raw dict         |

---

## The JSON schemas (the real contract)

`schemas/connector.schema.json` + `schemas/defs/` already cover every field
the simulator needs:

| Field path | JSON schema location | Complete? |
|---|---|---|
| `connector.name`, `connector.auth.*` | `connector.schema.json` | ✓ |
| `connector.datatypes[*].ingestion.*` | `defs/ingestion.schema.json` | ✓ |
| `connector.datatypes[*].ingestion.list.pagination.*` | `defs/pagination.schema.json` | ✓ |
| `connector.datatypes[*].writeback.operations.*` | `defs/writeback.schema.json` | ✓ |
| `connector.datatypes[*].simulator.*` | `defs/simulator.schema.json` | ✓ |
| `connector.webhooks.*`, `fan_out.*`, `is_delete`, `array_unwrap` | `defs/webhooks.schema.json` | ✓ |
| `connector.datatypes[*].required_scopes` | ❌ Not yet added | Needs adding |

The schemas must be kept in sync with the YAML connector files as the
authoritative contract.  The Pydantic models in `inandout.config` remain the
engine's typed representation but are no longer the simulator's source of truth.

---

## Steps

### S1 — Add `required_scopes` to the JSON schema

`schemas/connector.schema.json` (or a new `defs/` entry) needs:

```json
"required_scopes": {
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "read":  {"type": "array", "items": {"type": "string"}, "default": []},
    "write": {"type": "array", "items": {"type": "string"}, "default": []}
  }
}
```

Add it to the datatype properties block (alongside `ingestion`, `writeback`,
`simulator`).

### S2 — New `simulator/loader.py` (schema-native YAML loading)

Replace the `load_connector()` Pydantic-based loader with a standalone module
the simulator owns:

```python
# src/inandout/simulator/loader.py
import pathlib
import json
import yaml
import jsonschema

_SCHEMA_DIR = pathlib.Path(__file__).parent.parent.parent.parent / "schemas"
_CONNECTOR_SCHEMA = json.loads((_SCHEMA_DIR / "connector.schema.json").read_text())

def load_connector(path: str | pathlib.Path) -> dict:
    """Load and validate a connector YAML file.  Returns the raw dict."""
    raw = yaml.safe_load(pathlib.Path(path).read_text())
    jsonschema.validate(raw, _CONNECTOR_SCHEMA)
    return raw["connector"]
```

No Pydantic.  Validation errors surface as `jsonschema.ValidationError` with
the field path clearly identified.

### S3 — Migrate simulator internals to dict access

Replace every typed attribute access with dict `[]` / `.get()` navigation:

```python
# Before (Pydantic)
connector.auth.type
dt_cfg.ingestion.list.path
pagination.strategy == PaginationStrategy.cursor
connector.webhooks.fan_out.routes

# After (dict)
connector["auth"]["type"]
dt_cfg["ingestion"]["list"]["path"]
pagination["strategy"] == "cursor"
connector.get("webhooks", {}).get("fan_out", {}).get("routes", [])
```

`PaginationStrategy` enum import is removed; comparisons become plain string
equality.  `ConnectorConfig` type hints become `dict`.

### S4 — Remove `inandout.schema` imports from simulator

Once S2 and S3 are complete, all four `from inandout.schema.*` import lines are
deleted.  The CI boundary test (`test_simulator_schema_boundary.py`) is updated
to also assert no `inandout.schema` imports:

```python
if module.startswith("inandout.config") or module.startswith("inandout.schema"):
    violations.append(...)
```

### S5 — `pyproject.toml` extras

```toml
[project.optional-dependencies]
engine    = ["inandout", "psycopg[binary]>=3", "httpx", "structlog",
             "alembic", "orjson", "asyncpg"]
simulator = ["pyyaml", "jsonschema", "fastapi", "uvicorn[standard]",
             "jinja2", "httpx", "aiofiles"]
```

The simulator `Dockerfile` installs only `.[simulator]` — no Pydantic,
no psycopg, no alembic, no orjson.

---

## What the `inandout.schema` shim becomes

After S4 the `src/inandout/schema/` package is no longer needed by the
simulator.  It can be:
- **Kept** as a stable public API for external Python consumers who want typed
  access to connector configs (e.g. tooling, validation scripts), or
- **Removed** when S1/S2 of the original plan (full `inandout.config` →
  `inandout.schema` rename) is done.

Either way it has no bearing on the simulator's runtime.

---

## What stays the same

The simulator's store (SQLite/memory), `route_builder` route logic, UI/HTMX
templates, SSE push, and `WebhookDispatcher` are all already fully internal.
The refactor is purely a change of *how the config dict is obtained* — from a
Pydantic object to a raw dict.  All the route-building logic, pagination
handling, seed expansion, and webhook dispatch are unchanged.

---

## What this enables

- The simulator can be shipped as a **standalone Docker image** with a
  sub-100 MB footprint (no DB drivers, no Pydantic, no async HTTP clients
  beyond the outbound webhook dispatcher).
- Alternative engine implementations in any language can be tested against the
  same simulator simply by pointing at the right URL and reading the same YAML
  connector files validated against the same JSON schemas.
- The JSON schemas in `schemas/` become the **language-neutral contract**.
  Other language SDKs can generate type-safe clients from them without ever
  touching the Python models.
- Linting rule: CI asserts the simulator directory has zero imports from
  `inandout.*` — the complete boundary.

---

## Implementation notes (history)

### Shim phase (completed — transitional only)

1. `src/inandout/schema/` created as a re-export shim over `inandout.config`
2. 4 simulator files migrated from `inandout.config.*` → `inandout.schema.*`
3. CI boundary test added (`test_simulator_schema_boundary.py`)

This gave namespace separation but not true decoupling.  It is superseded by
the JSON-schema-native approach above.  The shim files remain harmless until
S4 is complete.


---

## Current state

The simulator (`src/inandout/simulator/`) has **no imports from
`inandout.ingestion.*`**.  All coupling is into `inandout.config.*`:

| Simulator file      | Imported symbol                    | What it is used for                                                         |
|---------------------|------------------------------------|-----------------------------------------------------------------------------|
| `app.py`            | `load_connector`, `ConnectorConfig` | Parse YAML at startup; type hint on the connectors list                    |
| `route_builder.py`  | `ConnectorConfig`, `PaginationStrategy` | Build FastAPI routes from config fields; enum switch for pagination       |
| `webhooks.py`       | `ConnectorConfig`                  | Read webhook / fan-out config to build outbound POST to the engine          |
| `seed.py`           | `ConnectorConfig`                  | Read seed data + primary key to pre-populate the in-process store           |

`inandout.config` is pure schema — Pydantic models + a YAML loader.  The
engine imports the same package but adds all processing on top.  **That seam
is the one to cut.**

---

## Proposed package split

```
inandout-schema/          (new sub-package; no engine deps)
  connector.py            ← ConnectorConfig and the full model tree
  pagination.py           ← PaginationStrategy enum
  webhooks.py             ← WebhookConfig, FanOutConfig, FanOutRoute, …
  ingestion.py            ← IngestionConfig, incremental, out-of-order, …
  writeback.py            ← WritebackConfig, operations, …
  loader.py               ← load_connector(), load_connector_dir()
  profiles.py             ← generation profiles

inandout-engine/          depends on inandout-schema
  ingestion/…
  writeback/…
  cli.py

inandout-simulator/       depends on inandout-schema ONLY
  app.py
  route_builder.py
  webhooks.py
  seed.py
  …
```

Both engine and simulator live in the same repo.  This is a **sub-package
rename**, not a monorepo split.  It is consistent with the broader
modularisation described in `PLAN_MODULARIZATION.md`.

---

## Steps

### S1 — Create `src/inandout/schema/`

Move all files from `src/inandout/config/` to `src/inandout/schema/`.

Keep `src/inandout/config/` as a **thin re-export shim** for one release
cycle so that existing engine code, migrations, and tests still import without
changes:

```python
# src/inandout/config/connector.py  (shim — delete after S2)
from inandout.schema.connector import *  # noqa: F401, F403
```

### S2 — Migrate engine imports

Global-replace `from inandout.config.` → `from inandout.schema.` across:

- `src/inandout/ingestion/`
- `src/inandout/writeback/`
- `src/inandout/cli.py`
- `migrations/`
- `tests/`

Delete the config shims once the PR is green.

### S3 — Migrate simulator imports

Same replacement in `src/inandout/simulator/`.  After S3 the simulator
imports **only** from `inandout.schema.*` plus its own internal modules.

### S4 — `pyproject.toml` extras (or separate packages)

```toml
[project.optional-dependencies]
schema    = ["pydantic>=2", "pyyaml", "jsonschema"]
engine    = ["inandout[schema]", "psycopg[binary]>=3", "httpx", "structlog",
             "alembic", "orjson", "asyncpg"]
simulator = ["inandout[schema]", "fastapi", "uvicorn[standard]", "jinja2",
             "httpx", "aiofiles"]
```

The simulator `Dockerfile` installs `.[schema,simulator]` — no psycopg, no
alembic, no orjson.

### S5 — CI validation gate

New CI job: install `.[schema,simulator]` in a clean venv with no `.[engine]`
extras, then:

```bash
python -c "from inandout.simulator.app import build_app; print('OK')"
python -m pytest tests/simulators/ -q
```

The job **fails the PR** if any engine import bleeds back into the simulator.
Use `importlib.util.find_spec("inandout.ingestion")` as an assertion inside
the test suite entrypoint to confirm the boundary is clean.

---

## What stays the same

The simulator's store (SQLite/memory), `route_builder` logic, UI/HTMX
templates, SSE push, and `WebhookDispatcher` are all already fully internal.
No refactor needed there.

---

## What this enables

- The simulator can be shipped as a **standalone Docker image** with a
  sub-100 MB footprint (no DB drivers, no async HTTP clients beyond the
  outbound webhook dispatcher).
- Alternative engine implementations in any language can be tested against the
  same simulator simply by pointing at the right URL and reading the same YAML
  connector files.
- `inandout-schema` becomes the **language-neutral contract layer**.  Other
  language SDKs can generate type-safe clients from the JSON schemas rather
  than depending on the Python models.
- The existing `PLAN_MODULARIZATION.md` `inandout-core` package maps directly
  to `inandout-schema`; this plan accelerates that work.

---

## Implementation notes (shim phase — completed)

Rather than the 232-file big-bang rename that S1+S2 would require, a pragmatic
**re-export shim** approach was implemented first:

1. **`src/inandout/schema/`** created as a thin namespace that re-exports
   every public symbol from `inandout.config.*`.  Sub-module shims:
   - `schema/__init__.py` — full public surface
   - `schema/connector.py`, `schema/loader.py`, `schema/pagination.py`

2. **4 simulator files** updated (6 import lines) from
   `from inandout.config.*` → `from inandout.schema.*`:
   - `simulator/app.py`, `simulator/route_builder.py`,
     `simulator/webhooks.py`, `simulator/seed.py`

3. **CI boundary test** added at
   `tests/simulators/test_simulator_schema_boundary.py` — walks all
   simulator `.py` files with the AST parser and fails if any
   `inandout.config` import is found.

`inandout.config` is **not yet shimmed** (engine + test code stays on the
old import path).  The full S1/S2 migration (moving real code into
`inandout.schema` and making `inandout.config` the shim) remains for a
dedicated refactor PR.

