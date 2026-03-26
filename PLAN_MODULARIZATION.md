# Plan: Monorepo Modularization

> Status: **Draft / Exploring options**
> Strategy: uv workspace monorepo, lockstep versioning, namespace packages

## Motivation

The ingestion and writeback daemons are deployed independently (separate
Kubernetes Deployments, separate docker-compose services) yet today both
container images carry the full monolith: CLI scaffolding, simulator code,
both engine runtimes, and every dependency.

Splitting into focused packages lets each image install only what it needs,
shrinking image size, reducing attack surface, and making dependency
boundaries explicit.

---

## Proposed Package Layout

| Package              | PyPI name              | Installs                                                                 |
| -------------------- | ---------------------- | ------------------------------------------------------------------------ |
| **inandout-core**    | `inandout-core`        | config, transport, postgres, observability, events, alerting, auth, secrets, engine/control, federation, deadletter, schema_registry, linter, plugins, privacy, mapping, migrations |
| **inandout-ingestion** | `inandout-ingestion` | ingestion engine, webhooks, dry-run, api (health + webhook routes)       |
| **inandout-writeback** | `inandout-writeback` | writeback engine, batch_response, validate, desired_state helpers        |
| **inandout**         | `inandout`             | CLI (typer/rich), simulators, testing helpers — "batteries included"     |

### Dependency graph

```
inandout  (meta — CLI + simulators)
├── inandout-ingestion
│   └── inandout-core
└── inandout-writeback
    └── inandout-core
```

Each runtime package depends **only** on `inandout-core`.
There is **no** dependency between `inandout-ingestion` and `inandout-writeback`.

---

## Critical Cross-Dependency to Resolve

`writeback/engine.py` line ~159 dynamically imports
`inandout.ingestion.field_mapper.apply_field_mappings`. This is the **only**
cross-import from writeback → ingestion. The resolution is to move
`field_mapper.py` (and `field_exclusion.py`) out of `ingestion/` into
`inandout-core` as a shared `mapping/` module, so both packages can import
it without depending on each other.

**Before:**

```
writeback/engine.py → from inandout.ingestion.field_mapper import apply_field_mappings
```

**After:**

```
writeback/engine.py → from inandout.mapping.field_mapper import apply_field_mappings
ingestion/engine.py → from inandout.mapping.field_mapper import apply_field_mappings
```

---

## Namespace Packages — Zero Import Changes

Each sub-package exposes its modules under the `inandout` namespace using
PEP 420 implicit namespace packages. Concretely: there is **no**
`__init__.py` at the `src/inandout/` level in any sub-package. Python
merges the namespace at runtime so `from inandout.config import …` resolves
regardless of which package provides it.

**Exception:** The root `inandout` (meta) package **does** provide
`src/inandout/__init__.py` with `__version__`, preserving backward
compatibility.

---

## Directory Structure After Split

```
in-and-out-2/                           # workspace root
├── pyproject.toml                      # uv workspace definition + shared dev tooling
├── uv.lock                             # single lockfile for all packages
├── alembic.ini
├── conftest.py
├── tests/                              # workspace-level test suite (unchanged)
│
├── packages/
│   ├── inandout-core/
│   │   ├── pyproject.toml
│   │   └── src/inandout/              # no __init__.py here (namespace pkg)
│   │       ├── config/
│   │       ├── transport/
│   │       ├── postgres/
│   │       ├── observability/
│   │       ├── events/
│   │       ├── alerting/
│   │       ├── auth/
│   │       ├── secrets/
│   │       ├── engine/
│   │       ├── federation/
│   │       ├── deadletter/
│   │       ├── schema_registry/
│   │       ├── linter/
│   │       ├── plugins/
│   │       ├── mapping/                # NEW — moved from ingestion/
│   │       │   ├── __init__.py
│   │       │   ├── field_mapper.py     # was ingestion/field_mapper.py
│   │       │   └── field_exclusion.py  # was ingestion/field_exclusion.py
│   │       └── privacy.py
│   │
│   ├── inandout-ingestion/
│   │   ├── pyproject.toml
│   │   └── src/inandout/              # no __init__.py
│   │       ├── ingestion/
│   │       │   ├── __init__.py
│   │       │   ├── engine.py
│   │       │   ├── daemon.py
│   │       │   ├── dry_run.py
│   │       │   ├── webhooks.py
│   │       │   └── quality.py
│   │       └── api/
│   │           ├── __init__.py
│   │           └── routes.py
│   │
│   ├── inandout-writeback/
│   │   ├── pyproject.toml
│   │   └── src/inandout/              # no __init__.py
│   │       └── writeback/
│   │           ├── __init__.py
│   │           ├── engine.py
│   │           ├── daemon.py
│   │           ├── validate.py
│   │           └── batch_response.py
│   │
│   └── inandout/                       # meta package
│       ├── pyproject.toml
│       └── src/inandout/
│           ├── __init__.py             # __version__ lives here
│           ├── cli/
│           ├── simulators/
│           └── testing/
│
├── migrations/                         # stays at workspace root (alembic.ini references it)
├── config/
├── connectors/
├── k8s/
├── docker-compose.yml
├── Dockerfile.ingestion
├── Dockerfile.writeback
└── Dockerfile                          # full image (CLI + both runtimes)
```

---

## Per-Package Dependencies

### inandout-core

```toml
[project]
name = "inandout-core"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = [
    "anyio>=4.7",
    "psycopg[binary]>=3.2",
    "psycopg-pool>=3.2",
    "httpx[http2]>=0.28",
    "pydantic>=2.10",
    "pydantic-settings>=2.7",
    "pyyaml>=6.0",
    "jmespath>=1.0",
    "orjson>=3.10",
    "deepdiff>=8.0",
    "croniter>=3.0",
    "aiolimiter>=1.2",
    "tenacity>=9.0",
    "structlog>=24.0",
    "prometheus-client>=0.21",
    "opentelemetry-api>=1.29",
    "opentelemetry-sdk>=1.29",
    "opentelemetry-exporter-otlp>=1.29",
    "opentelemetry-instrumentation-httpx>=0.50b0",
    "opentelemetry-instrumentation-psycopg>=0.50b0",
    "opentelemetry-instrumentation-asgi>=0.50b0",
    "cryptography>=43",
    "alembic>=1.14",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/inandout"]
```

### inandout-ingestion

```toml
[project]
name = "inandout-ingestion"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = [
    "inandout-core==0.1.0",
    "fastapi>=0.135.1",
    "uvicorn[standard]>=0.34",
    "watchfiles>=1.1.1",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/inandout"]
```

### inandout-writeback

```toml
[project]
name = "inandout-writeback"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = [
    "inandout-core==0.1.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/inandout"]
```

### inandout (meta)

```toml
[project]
name = "inandout"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = [
    "inandout-ingestion==0.1.0",
    "inandout-writeback==0.1.0",
    "typer>=0.15",
    "rich>=13.0",
]

[project.scripts]
inandout = "inandout.cli.main:app"
inandout-ingest = "inandout.cli.main:ingest_app"
inandout-writeback = "inandout.cli.main:writeback_app"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/inandout"]
```

---

## Root Workspace pyproject.toml

```toml
[project]
name = "inandout-workspace"
version = "0.1.0"
requires-python = ">=3.13"

[tool.uv.workspace]
members = ["packages/*"]

[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.24",
    "respx>=0.22",
    "testcontainers[postgres]>=4.0",
    "pytest-cov>=6.0",
    "hypothesis>=6.0",
    "factory-boy>=3.3",
    "mypy>=1.14",
    "ruff>=0.9",
]

[tool.ruff]
target-version = "py313"
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "W", "I", "N", "UP", "S", "B", "A", "C4", "PT", "RUF"]
ignore = ["S101"]

[tool.ruff.lint.per-file-ignores]
"tests/**" = ["S101", "S105", "S106"]

[tool.mypy]
python_version = "3.13"
strict = true
warn_return_any = true
warn_unused_configs = true
plugins = ["pydantic.mypy"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
addopts = "--tb=short"
norecursedirs = ["src", ".git", ".tox", "dist", "build", "*.egg-info"]
markers = [
    "acceptance: marks tests as acceptance tests requiring real external APIs",
    "load: marks tests as load/performance tests requiring Docker and extended timeout",
]

[tool.coverage.run]
source = ["packages/*/src/inandout"]
omit = ["tests/*"]
```

---

## Dockerfiles

### Dockerfile.ingestion

```dockerfile
FROM python:3.13-slim AS builder
WORKDIR /app
RUN pip install uv
COPY uv.lock pyproject.toml ./
COPY packages/ packages/
RUN uv sync --frozen --no-dev --package inandout-ingestion

FROM python:3.13-slim AS runtime
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY migrations/ migrations/
COPY alembic.ini .
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 9090
CMD ["python", "-m", "inandout.ingestion.daemon"]
```

### Dockerfile.writeback

```dockerfile
FROM python:3.13-slim AS builder
WORKDIR /app
RUN pip install uv
COPY uv.lock pyproject.toml ./
COPY packages/ packages/
RUN uv sync --frozen --no-dev --package inandout-writeback

FROM python:3.13-slim AS runtime
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY migrations/ migrations/
COPY alembic.ini .
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 9090
CMD ["python", "-m", "inandout.writeback.daemon"]
```

### Dockerfile (full — CLI + both runtimes)

```dockerfile
FROM python:3.13-slim AS builder
WORKDIR /app
RUN pip install uv
COPY uv.lock pyproject.toml ./
COPY packages/ packages/
RUN uv sync --frozen --no-dev --package inandout

FROM python:3.13-slim AS runtime
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY migrations/ migrations/
COPY alembic.ini .
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 9090
CMD ["inandout", "ingest", "run", "--config", "/config/ingestion.yaml"]
```

---

## docker-compose.yml Changes

```yaml
services:
  migrate:
    build:
      context: .
      dockerfile: Dockerfile          # full image has alembic + CLI
    command: inandout db upgrade --config /config/ingestion.yaml
    # ... (rest unchanged)

  ingest:
    build:
      context: .
      dockerfile: Dockerfile.ingestion
    command: python -m inandout.ingestion.daemon --config /config/ingestion.yaml
    # ... (rest unchanged)

  writeback:
    build:
      context: .
      dockerfile: Dockerfile.writeback
    command: python -m inandout.writeback.daemon --config /config/writeback.yaml
    # ... (rest unchanged)
```

---

## k8s Manifest Changes

Update the image references in k8s deployment manifests:

| Manifest                     | Image before        | Image after               |
| ---------------------------- | ------------------- | ------------------------- |
| `ingest-deployment.yaml`     | `inandout:latest`   | `inandout-ingestion:latest` |
| `writeback-deployment.yaml`  | `inandout:latest`   | `inandout-writeback:latest` |
| `migrate-job.yaml`           | `inandout:latest`   | `inandout:latest` (unchanged — needs CLI) |

---

## Implementation Phases

### Phase 1 — Resolve cross-import (prerequisite)

1. Create `src/inandout/mapping/` with `__init__.py`, move `field_mapper.py`
   and `field_exclusion.py` from `ingestion/` into it.
2. Update imports in `ingestion/engine.py` and `writeback/engine.py`.
3. Run full test suite to confirm no breakage.

*This can be done as a standalone PR before the rest, reducing risk.*

### Phase 2 — Scaffold workspace layout

4. Create `packages/` directory with the four sub-package dirs.
5. Move source modules into the correct sub-package `src/inandout/` trees.
6. Remove `__init__.py` from namespace-level dirs (keep it only in leaf
   modules and in the meta package root).
7. Create per-package `pyproject.toml` files.
8. Replace root `pyproject.toml` with workspace definition.

### Phase 3 — Lock and validate

9.  Run `uv lock` to regenerate the lockfile.
10. Run `uv sync --all-packages` to install everything in dev.
11. Run `uv run pytest tests/` — full suite must pass.
12. Run `uv run mypy packages/` — type checking across packages.
13. Run `uv run ruff check packages/` — lint across packages.

### Phase 4 — Dockerfiles and deploy manifests

14. Create `Dockerfile.ingestion` and `Dockerfile.writeback`.
15. Update `docker-compose.yml` build targets.
16. Update `k8s/` manifests with new image names.
17. Build and test images locally:
    ```
    docker build -f Dockerfile.ingestion -t inandout-ingestion:dev .
    docker build -f Dockerfile.writeback -t inandout-writeback:dev .
    docker compose up
    ```

### Phase 5 — Validate isolation

18. Install only `inandout-ingestion` in a clean venv:
    ```
    uv venv /tmp/test-ingest && source /tmp/test-ingest/bin/activate
    uv pip install packages/inandout-ingestion
    python -c "from inandout.ingestion.engine import IngestionEngine"  # works
    python -c "from inandout.writeback.engine import WritebackEngine"  # ImportError ✓
    ```
19. Repeat for `inandout-writeback` (writeback importable, ingestion not).
20. Compare image sizes vs. current monolith image.

---

## Lockstep Versioning Strategy

All four packages share the same version string. A single `VERSION` file (or
a shared `_version.py` in core) is the source of truth. Each sub-package
`pyproject.toml` reads from it via hatchling's `version` source or a
`dynamic = ["version"]` directive.

Pin cross-package deps with `==` (e.g., `inandout-core==0.1.0`) so a
partial upgrade is impossible.

Release process:
1. Bump `VERSION` in one place.
2. `uv lock` regenerates the lockfile.
3. Tag the commit (e.g., `v0.1.1`).
4. CI builds and publishes all four wheels to PyPI in one job.

---

## Risks and Mitigations

| Risk                                           | Mitigation                                                             |
| ---------------------------------------------- | ---------------------------------------------------------------------- |
| Namespace package resolution failures           | CI step 18-19 validates isolation; editable installs (`uv sync -e`)  |
| Alembic can't find migrations after move        | Keep `migrations/` at workspace root; `alembic.ini` path unchanged   |
| IDE (mypy/pyright) confused by namespace pkgs   | Add `mypy_path` in mypy config pointing to all `packages/*/src`      |
| Circular dependency introduced later            | CI lint rule: ban `ingestion↔writeback` imports via ruff/custom check |
| Lockstep version drift                          | Single VERSION file + `==` pins; CI verifies all versions match      |

---

## Open Questions

1. **`plugins/` placement** — Currently ingestion-only hooks. Keeping in core
   means writeback images carry the (tiny) hook framework even if unused.
   Acceptable for simplicity? (Recommended: yes, keep in core.)

2. **`api/` placement** — Routes serve health/metrics endpoints used by both
   daemons, but also webhook endpoints specific to ingestion. Options:
   (a) all in ingestion, writeback gets its own minimal health server; or
   (b) split health routes to core, webhook routes to ingestion. The plan
   above uses option (a) for simplicity.

3. **Future `inandout-testing` package** — `simulators/` and `testing/` could
   become a published package for connector authors. Deferred from this plan
   but the layout supports it (just move from `inandout/` to a new
   `packages/inandout-testing/`).
