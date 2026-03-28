# Plan: Repository Housekeeping

> Status: **Draft**
> Goal: fix several concrete bugs in the Docker/Compose configuration, slim the
> build context, and consolidate the two parallel documentation trees.

---

## On Dockerfile placement — verdict: keep as-is

Both `engine/Dockerfile` and `simulator/Dockerfile` live inside their
respective package directories.  Because the uv workspace lockfile
(`uv.lock`) and root `pyproject.toml` must be available during the build,
**the build context must be the workspace root**.  The docker-compose files
reflect this correctly with `context: .`.

This is the conventional layout for uv monorepos and gives the useful property
that each Dockerfile sits next to the code it ships.  The only time it feels
odd is if someone tries to run `docker build engine/` from inside
`engine/` — but the `justfile` and docker-compose handle invocation
correctly, so no change is needed here.

---

## H1 — Bug: host port 9090 bound twice in the combined stack

**File:** `docker-compose.yml` and `docker-compose.observability.yml`

When you run `just up-obs` (`docker compose -f docker-compose.yml -f
docker-compose.observability.yml up`), two services try to bind the same
host port:

| Service | Host port |
|---------|-----------|
| `ingest` (docker-compose.yml) | 9090 |
| `prometheus` (docker-compose.observability.yml) | 9090 |

Docker will refuse to start the second binding; the stack fails.

**Fix:** Remove the host-side exposure of Prometheus from the overlay file.
Prometheus only needs to be reachable by Grafana (inside the Docker network)
and exposed for occasional ad-hoc queries via a non-conflicting port:

```yaml
# docker-compose.observability.yml
  prometheus:
    ports:
      - "9092:9090"   # was 9090:9090; 9091 already used by writeback
```

---

## H2 — Bug: Prometheus cannot scrape engine services (network isolation)

**File:** `docker-compose.observability.yml`

The overlay file attaches `prometheus`, `grafana`, and `alertmanager` to a
named network called `inandout`:

```yaml
networks:
  - inandout
```

However:

1. The `inandout` network is never declared in either compose file (no
   top-level `networks:` block), so Compose auto-creates it as an isolated
   network.
2. The engine services (`ingest`, `writeback`, etc.) are on the implicit
   default network (`{project}_default`), not `inandout`.

Prometheus therefore cannot reach the services it needs to scrape.

**Fix:** Declare the shared network explicitly in `docker-compose.yml` and put
all services on it; then reference it as `external: false` from the overlay:

```yaml
# docker-compose.yml — add at bottom
networks:
  inandout:
    name: inandout
```

Add `networks: [inandout]` to each service in `docker-compose.yml`
(`postgres`, `simulator`, `migrate`, `ingest`, `writeback`).

The overlay services already declare `networks: [inandout]` so they will join
the same network automatically.

---

## H3 — Bug: `./config` volume mount refers to a non-existent directory

**File:** `docker-compose.yml`

All three engine services (`migrate`, `ingest`, `writeback`) mount:

```yaml
volumes:
  - ./config:/config
```

There is no `config/` directory at the workspace root.  The actual runtime
configs live at `engine/config/`.  Running `just up` after a fresh checkout
silently mounts an empty (auto-created) directory, and the services fail to
find their config files at startup.

**Fix:** Change the volume path to the real location:

```yaml
      - ./engine/config:/config
```

This makes `just up` work out of the box.  Users who need to supply their own
config (e.g. different connector URLs) can do so with a
`docker-compose.override.yml`.

---

## H4 — Cleanup: remove deprecated `version:` field from docker-compose.yml

**File:** `docker-compose.yml` line 1

```yaml
version: "3.9"
```

Docker Compose V2 (the current default on all major platforms) ignores this
field entirely and emits a deprecation warning.  The Compose Specification no
longer uses versioning.

**Fix:** Delete the line.

---

## H5 — Cleanup: expand `.dockerignore`

**File:** `.dockerignore`

The current file excludes the essentials (`.venv/`, `__pycache__/`, `.git/`,
`tests/`) but ships a large amount of irrelevant content to the Docker daemon
on every build, slowing context transfer.  None of the following directories
belong in either production image:

| Directory | Approx. impact |
|-----------|----------------|
| `plans/` | small |
| `docs/` | small |
| `book/` | medium (theme assets) |
| `k8s/` | small |
| `observability/` | small |
| `.devcontainer/` | small |
| `.github/` | small |
| `.hypothesis/` | can grow large |
| `.pytest_cache/` | small |
| `htmlcov/` | medium |
| `connectors/` | small (bind-mounted at runtime) |
| `engine/config/` | small (bind-mounted at runtime) |
| `engine/fixtures/` | small |

**Fix:** Add these patterns to `.dockerignore`:

```
plans/
docs/
book/
k8s/
observability/
.devcontainer/
.github/
.hypothesis/
.pytest_cache/
htmlcov/
.coverage
connectors/
engine/config/
engine/fixtures/
simulator/tests/
```

Note: `tests/` already catches `engine/tests/`, but `simulator/tests/` is
currently only caught as a relative path.  Being explicit is safer.

---

## H6 — Cleanup: consolidate the two documentation trees

The repo has two parallel documentation systems that are not cross-linked and
contain overlapping content:

| Location | Format | Purpose |
|---|---|---|
| `docs/` | raw Markdown | informal reference; browsable on GitHub |
| `book/src/` | mdBook source | compiled docs; deployed to GitHub Pages |

Specific overlaps:

| `docs/` file | Likely counterpart in `book/src/` |
|---|---|
| `CONNECTOR_AUTHORING.md` | `connector-authoring.md` |
| `DEPLOYMENT.md` | not yet in book — deployment chapter missing |
| `SCHEMA_CONTRACT.md` | `osi-mapping-contract.md` (partial overlap) |
| `TROUBLESHOOTING.md` | not yet in book — troubleshooting chapter missing |

**Recommended approach:**

1. Migrate `docs/DEPLOYMENT.md` and `docs/TROUBLESHOOTING.md` into
   `book/src/` as new chapters and add them to `book/src/SUMMARY.md`.
2. Migrate any unique content from `docs/CONNECTOR_AUTHORING.md` and
   `docs/SCHEMA_CONTRACT.md` into the corresponding `book/src/` pages.
3. Delete `docs/` once the content is absorbed.  Replace it with a one-line
   `docs/README.md` redirect pointing to the GitHub Pages URL if needed for
   backward compatibility.

This leaves a single authoritative documentation source and removes the
maintenance burden of keeping two trees in sync.

---

## H7 — Minor: add a CI workflow for tests and linting

**File:** `.github/workflows/` (missing)

The only GitHub Actions workflow is `deploy-docs.yml`.  There is no CI
pipeline that runs on pull requests or pushes to `main`.  The `justfile`
already defines a `ci` recipe (`lint-check typecheck test test-simulator`);
it just needs a workflow file:

```yaml
# .github/workflows/ci.yml
name: CI
on:
  push:
    branches: [main]
  pull_request:

jobs:
  ci:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
      - run: uv sync --all-packages --all-extras
      - run: just ci
```

---

## Summary table

| ID | Severity | Area | One-liner |
|----|----------|------|-----------|
| H1 | Bug | Docker Compose | Host port 9090 double-bound; combined stack won't start |
| H2 | Bug | Docker Compose | Prometheus on isolated network; can't scrape engine |
| H3 | Bug | Docker Compose | `./config` mount broken; services can't find config on first `just up` |
| H4 | Cleanup | Docker Compose | Remove obsolete `version: "3.9"` |
| H5 | Cleanup | Docker | Expand `.dockerignore` to reduce build context transfer |
| H6 | Cleanup | Docs | Merge `docs/` into `book/src/`; remove parallel tree |
| H7 | Minor | CI | Add a CI workflow that runs `just ci` on push/PR |
