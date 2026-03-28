# Plan: Decouple the simulator from the engine package

> Status: **Implemented (shim phase — S3 complete, S1/S2 deferred)**
> Goal: make the simulator installable and runnable with **zero dependency on
> engine code**, so any alternative engine implementation (Go, Rust, Node, a
> third-party tool) can point at the same simulator by reading the same YAML
> connector files.

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

