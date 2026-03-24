# Scope Creep Analysis & Removal Recommendations

**Date:** 24 March 2026  
**Scope:** `src/inandout/` — everything not required by GOAL.md

This report identifies concrete candidates for removal. The reference for "in scope" is GOAL.md's
8-step implementation plan, its explicit requirement lists (T1 #1–48, T2 #1–39), and the
cross-cutting concerns section. Where a module has no traceable GOAL.md requirement, it is a
scope-creep candidate.

The goal is a **lean, focused HTTP API synchronization tool**: reliable ingestion and writeback,
declarative YAML config, PostgreSQL as the state backbone, and observable operation. Everything
else is noise.

---

## Summary

| Priority | Candidate | Source files | Test files | Effort |
|----------|-----------|:---:|:---:|--------|
| P1 | `registry/` — connector marketplace | 2 | 2 | Low |
| P1 | `generator/` — OpenAPI scaffold generator | 2 | 2 | Low |
| P1 | `ui/` — SPA web UI mount | 1 | 1 | Low |
| P1 | `diff/` — sync-run comparison engine | 1 | 1 | Low |
| P1 | `ingestion/backfill.py` — time-windowed historical load | 1 | 1 | Low |
| P1 | `migrations/connector_schema.py` + `connector_migrations/` — YAML format migration | 2 | 1 | Low |
| P1 | `observability/health_score.py` — composite numeric health score | 1 | 1 | Low |
| P1 | `api/auth.py` — Bearer-token middleware on mgmt API | 1 | 1 | Low |
| P2 | `alerting/` — Slack/PagerDuty alert dispatching | 2 | 1 | Low–Medium |
| P2 | `federation/reporter.py` — cross-instance health aggregation table | 1 | 1 | Low |
| P2 | `writeback/fan_in.py` — join enrichment before write | 1 | 2 | Low |
| P2 | `writeback/merge_hooks.py` — custom merge callback registry | 1 | 2 | Low |
| P3 | `ingestion/cdc.py` — Kafka/Kinesis/pg_logical stubs | 1 | 3 | Medium |
| P3 | `events/` — outbound event publishing (Kafka/Kinesis/pg_notify) | 2 | 1 | Medium |
| P3 | `plugins/` — runtime Python hook system | 5 | 5 | Medium–High |

**Total:** 15 removals · ~26 source files · ~25 test files · ~4 000+ lines of non-goal code.

Also noted: `fastapi` is a core dependency but the management API routes could be rewritten in
plain Starlette, which is already a hard dependency. See §16.

---

## Priority 1 — Standalone, Zero GOAL.md Justification, Remove First

These modules have no imports into core engine logic. Removal is a clean filesystem delete +
a few CLI command removals. No engine changes required.

---

### P1-1 · `src/inandout/registry/` — Connector Marketplace

**What it does:** Fetches a remote GitHub-hosted connector index, downloads connector YAML and
Python hook files, and POSTs submissions to a marketplace API.

**Why it should go:** GOAL.md Step 7 calls for *"a reference connector implementation with an
accompanying simulator to serve as the canonical example for future connector authors."* It
says nothing about a marketplace distribution system or remote index. This is infrastructure
for a public connector ecosystem that was never requested.

**What imports it:**
- `cli/main.py` — `connector list`, `connector install`, `connector search`, `connector publish`

**Test files to remove:** `tests/unit/test_registry.py`, `tests/unit/test_publish.py`

**Side effects on removal:**
- Remove the four CLI sub-commands that call it.
- No engine, daemon, or migration logic is affected.

---

### P1-2 · `src/inandout/generator/` — OpenAPI Scaffold Generator

**What it does:** Probes a remote URL for an OpenAPI/Swagger spec, extracts list endpoints, and
renders a starter connector YAML template.

**Why it should go:** GOAL.md Step 7 is about documenting the connector authoring contract (the
YAML schema, simulator interface, required test structure). Auto-generating connectors from
OpenAPI specs is a developer-experience add-on that was never in scope.

**What imports it:**
- `cli/main.py` — `generate` command (lines 706–761)

**Test files to remove:** `tests/unit/test_generator.py`, `tests/unit/test_generator_scaffold.py`

**Side effects on removal:**
- Remove the `generate` CLI command.
- `respx` (dev dependency) loses one of its two main callers (the other is the simulators,
  which should be kept). No production dependency changes.

---

### P1-3 · `src/inandout/ui/__init__.py` — SPA Web UI Mount

**What it does:** Returns a Starlette `Mount` pointing at a `ui/static/` directory, serving a
single-page application under `/ui`.

**Why it should go:** GOAL.md specifies a CLI for operator interactions (Step 6) and the
runtime control table as the operator interface. There is no mention of a web UI anywhere in
the document — not in requirements, not in cross-cutting concerns, not in the implementation
plan.

**What imports it:**
- `ingestion/daemon.py` — `build_ui_router` mounted conditionally at startup

**Test files to remove:** `tests/unit/test_ui.py`

**Side effects on removal:**
- Remove the conditional `build_ui_router` mount in `daemon.py` (~3 lines).
- No static files currently exist in `ui/static/`, so there is no actual UI to lose.

---

### P1-4 · `src/inandout/diff/` — Sync-Run Comparison Engine

**What it does:** Queries the `_history` table to compare two sync run IDs for a given
connector/datatype, returning added, removed, and changed records with field-level diffs.

**Why it should go:** GOAL.md T1 #15 and #30 require a history/audit table and selectable
history mode per datatype — not a programmatic cross-run diff tool. The diff capability adds
surface area around the history tables without a corresponding business requirement.

**What imports it:**
- `cli/main.py` — `diff` command (line 1422)
- `api/routes.py` — `GET /diff/...` endpoint (line 824)

**Test files to remove:** `tests/unit/test_diff_engine.py`

**Side effects on removal:**
- Remove the `diff` CLI command and `GET /diff` REST route (~10 lines total).

---

### P1-5 · `src/inandout/ingestion/backfill.py` — Time-Windowed Historical Load

**What it does:** Splits a user-specified date range into time windows, runs a sync per window
into a Postgres staging table, then promotes results to the live source table. Invoked via a
`backfill` CLI command.

**Why it should go:** GOAL.md covers full sync (T1 #3) and intra-sync checkpointing (#29) for
runs that fail mid-way. A separate orchestrated backfill mode with date-range windowing and
staging tables is an operational convenience feature not described anywhere in the requirements.
The GOAL.md full-sync path already handles initial loads.

**What imports it:**
- `cli/main.py` — `backfill` command (line 236)

**Test files to remove:** `tests/unit/test_backfill.py`

**Side effects on removal:**
- Remove the `backfill` CLI command.
- No engine or daemon logic is affected.

---

### P1-6 · `src/inandout/migrations/connector_schema.py` + `connector_migrations/` — YAML Format Migrations

**What it does:** Maintains an ordered registry of connector YAML *config file format*
migrations (currently v1.0→v1.1: renames `webhook.signature_header` to
`webhook.signature.header`). Provides path-finding and application of migration chains.

**Why it should go:** GOAL.md's migration requirement is exclusively about PostgreSQL database
schema migrations. The MDM Contract section specifies: *"When a new version of either tool
changes the schema of any managed table, the migration must be performed explicitly..."* There
is no concept of versioning the connector YAML format itself — breaking config changes should
be documented in the connector authoring guide and handled with a one-time manual update, not
a programmatic migration system.

**What imports it:**
- `cli/main.py` — `connector migrate` command (line 1339)

**Test files to remove:** `tests/unit/test_connector_migration.py`

**Side effects on removal:**
- Remove the `connector migrate` CLI command.
- The v1.0→v1.1 rename should be noted in CONNECTOR_AUTHORING.md as a breaking change, not
  automated.

---

### P1-7 · `src/inandout/observability/health_score.py` — Composite Numeric Health Score

**What it does:** Computes a weighted 0–1 composite health score from three components:
circuit-breaker state (40%), recent sync error rate (40%), dead-letter depth (20%). Exposed
as a `GET /health-score` REST endpoint.

**Why it should go:** GOAL.md's `GET /ready` readiness endpoint is specified to *"include a
minimal JSON body indicating which connectors are active, paused, or in circuit-breaker state."*
A single floating-point composite score opaquely combining three metrics is not what was
specified. The individual signals (circuit breaker state, error rate, DL depth) are already
exposed as dedicated Prometheus gauges — that is the correct observability mechanism per GOAL.md.

**What imports it:**
- `api/routes.py` — `GET /health-score` endpoint (line 502)

**Test files to remove:** `tests/unit/test_health_score.py`

**Side effects on removal:**
- Remove the `GET /health-score` route.
- The `inout_connector_health_score` Prometheus gauge in `observability/metrics.py` can also
  be removed.

---

### P1-8 · `src/inandout/api/auth.py` — Bearer-Token Middleware on Management API

**What it does:** Starlette middleware that enforces a Bearer token or API-key check on all
management API routes except `/health`, `/ready`, and `/metrics`.

**Why it should go:** GOAL.md defines the management interface as the runtime control table
plus health/readiness HTTP endpoints. The control table uses PostgreSQL roles as its access
control boundary. GOAL.md contains no requirement for an HTTP authentication layer on top of
the management API. Adding a custom auth mechanism without a specification creates an
undocumented security surface — any misconfiguration (wrong env var name, weak token) creates
a security gap. The correct approach per GOAL.md is network-level isolation: the management
API endpoint is kept internal (not exposed publicly), which is standard in containerized
deployments.

**What imports it:**
- `ingestion/daemon.py` — `BearerTokenMiddleware` (line 130)

**Test files to remove:** `tests/unit/test_api_auth.py`

**Side effects on removal:**
- Remove the `BearerTokenMiddleware` registration in `daemon.py` (~3 lines).

---

## Priority 2 — Wired into Core, No GOAL.md Justification

These are slightly more coupled but still have clean removal paths. Each requires targeted
edits to 1–2 core files.

---

### P2-1 · `src/inandout/alerting/` — Slack/PagerDuty Alert Dispatching

**What it does:** Dispatches alert events over HTTP to Slack incoming webhooks or PagerDuty
Events v2 API when configured alert rules fire.

**Why it should go:** GOAL.md's cross-cutting observability section specifies: *"Metrics must
be exportable in Prometheus-compatible format. Structured JSON logs must include `sync_run_id`..."*
Routing alerts to Slack or PagerDuty is the responsibility of Prometheus Alertmanager, which
is already present in the repository (`observability/alertmanager.yml`). Building a second,
parallel alert-routing mechanism inside the tool itself is redundant and creates a maintenance
surface for third-party API contracts (Slack/PagerDuty API changes).

**What imports it:**
- `config/tool.py` — `AlertingConfig` included in the tool config model

**Test files to remove:** `tests/unit/test_alerting.py`

**Side effects on removal:**
- Remove `alerting: AlertingConfig` field from `IngestionToolConfig` in `config/tool.py`.
- Any call sites in engines that call `AlertDispatcher.dispatch()` (likely 1–3 call sites).
- Alertmanager in the existing `observability/alertmanager.yml` already covers this.

---

### P2-2 · `src/inandout/federation/reporter.py` — Cross-Instance Health Aggregation Table

**What it does:** Upserts per-instance health snapshots (health score, circuit state, DL depth)
into an `inout_ops_federation` PostgreSQL table, making all running instances visible in one
query. Called periodically by the ingestion daemon.

**Why it should go:** GOAL.md's multi-instance HA requirement is: *"Cross-instance concurrency
control must use PostgreSQL advisory locks...at most one instance may hold the lock for a given
connector/datatype pair at a time."* This is fully addressed by the advisory lock mechanism in
the engine. A separate federation health table was never requested. The same visibility can be
achieved via Prometheus's multi-target scraping with an `instance` label — the correct
observability channel per GOAL.md.

**What imports it:**
- `ingestion/daemon.py` — `FederationReporter`, periodic `report_health()` calls

**Test files to remove:** `tests/unit/test_federation.py`

**Side effects on removal:**
- Remove the `FederationReporter` instantiation and periodic call from `daemon.py` (~15 lines).
- Remove the `inout_ops_federation` DDL from `postgres/schema.py`.
- Remove the `federation_routed_total` metric if it exists only for this feature.
- Remove the corresponding Alembic migration (migration 007 is named `federation`).

---

### P2-3 · `src/inandout/writeback/fan_in.py` — Join Enrichment Before Write

**What it does:** Enriches a writeback delta row by querying other PostgreSQL source tables
and merging in additional fields before the HTTP write is dispatched.

**Why it should go:** GOAL.md is unambiguous: *"There is no bridge layer. OSI-Mapping config
declares all business logic."* Any enrichment that joins across source tables before a write
is consolidation logic — exactly OSI-Mapping's domain. The writeback tool's job is *"HTTP write
mechanics, not consolidation logic"* (GOAL.md T2 Context). Performing joins here silently
duplicates logic that should be declared in `osi-mapping.yaml`.

**What imports it:**
- `writeback/engine.py` — `enrich_with_join_sources` called lazily (line 395)

**Test files to remove:** `tests/unit/test_fan_in.py`, `tests/integration/test_fan_in.py`

**Side effects on removal:**
- Remove the `enrich_with_join_sources` call and its conditional block in `writeback/engine.py`
  (~10 lines).
- Remove any `join_sources` field from the writeback connector config.

---

### P2-4 · `src/inandout/writeback/merge_hooks.py` — Custom Merge Callback Registry

**What it does:** A module-level `MergeHookRegistry` singleton that accepts user-supplied async
Python functions keyed by `(connector, datatype)` for custom conflict-merge logic during
writeback.

**Why it should go:** GOAL.md T2 #30 specifies exactly four conflict resolution strategies:
`dead-letter`, `last-writer-wins`, `skip-and-warn`, and `re-ingest-and-recompute`. All are
declaratively configured, not implemented as user code. An arbitrary Python callback registry
for custom merge functions is outside the declarative model and, more importantly, puts
consolidation logic (which GOAL.md explicitly assigns to OSI-Mapping) back inside this tool.

**What imports it:**
- `writeback/engine.py` — `merge_hook_registry` imported at top level (line 22)
- `tests/unit/test_conflict_resolution.py` — uses `merge_hook_registry` in tests

**Test files to remove:** `tests/unit/test_merge_hook_registry.py`  
**Tests to update:** `tests/unit/test_conflict_resolution.py` — remove the hook-based test cases.

**Side effects on removal:**
- Remove `merge_hook_registry` import and any call site in `writeback/engine.py` (~5 lines).

---

## Priority 3 — More Coupled, but Still Not In Scope

These require more surgical removals because references are deeper in engine logic. Still
recommended, but higher coordination cost.

---

### P3-1 · `src/inandout/ingestion/cdc.py` — Non-HTTP CDC Source Stubs

**What it does:** Defines abstract `CdcSource` plus concrete (but stub) implementations for
Kafka (`aiokafka`), Kinesis (`aioboto3`), and `pg_logical`. The `get_cdc_source()` factory is
called lazily by the ingestion engine when `ingestion.cdc` config is present.

**Why it should go:** GOAL.md's Transport Abstraction strategy is explicit: *"While the initial
implementation scope is HTTP APIs exclusively..."* The companion `REPORT_IMPACT_NON_HTTP_INGESTION.md`
was written precisely to analyse what would be needed to implement these — indicating they are
a *future* concern, not a current one. Stubs in the codebase create three concrete problems:
(1) The config model (`CdcSourceConfig`) and engine dispatch code give the impression these
work, which they don't. (2) If `aiokafka` or `aioboto3` are installed by accident, partially
broken code paths activate. (3) Tests against these stubs provide no real coverage while
adding to CI noise.

**What imports it:**
- `ingestion/engine.py` — `get_cdc_source` called in the CDC dispatch branch
- `config/ingestion.py` — `CdcSourceConfig` in `IncrementalConfig`

**Test files to remove:** `tests/unit/test_cdc.py`, `tests/unit/test_do_sync_cdc_routing.py`,
`tests/unit/test_cdc_pipeline.py`

**Side effects on removal:**
- Remove the `cdc` field from `IncrementalConfig` in `config/ingestion.py`.
- Remove the CDC dispatch branch in `ingestion/engine.py` (~20–30 lines).
- `aiokafka` and `aioboto3` are not in `pyproject.toml` (they use `try/except ImportError`),
  so no dependency file changes needed.

---

### P3-2 · `src/inandout/events/` — Outbound Event Publishing

**What it does:** After each successful ingestion upsert, publishes a structured event to one
of four backends: stdout, PostgreSQL `NOTIFY`, Kafka (stub), or Kinesis (stub). The
`PgNotifyPublisher` is fully implemented; Kafka and Kinesis require `aiokafka`/`aioboto3`.

**Why it should go:** GOAL.md never describes the ingestion tool as an event publisher for
downstream consumers. The tool writes to PostgreSQL source tables — downstream consumers (like
OSI-Mapping) read from those tables directly. The `pg_notify` mechanism that drives the
*writeback* near-real-time path is correctly implemented in `writeback/notify.py` and should
be kept. The `events/` package is a separate, additional outward event stream that was not
requested.

**What imports it:**
- `ingestion/engine.py` — `build_event` called after each upsert (lines 794, 1025)
- `ingestion/daemon.py` — `get_publisher` to initialise the publisher at startup (line 574)

**Test files to remove:** `tests/unit/test_event_publisher.py`

**Side effects on removal:**
- Remove `event_output` config field from `IngestionToolConfig` in `config/tool.py`.
- Remove the `build_event` / `publisher.publish()` call sites in `ingestion/engine.py`
  (~10 lines).
- Remove `get_publisher` startup wiring in `ingestion/daemon.py` (~5 lines).

---

### P3-3 · `src/inandout/plugins/` — Runtime Python Hook System

**What it does:** Provides a `ConnectorHooks` dataclass (transform/filter/enrich async
callbacks), a module-level `HookRegistry` singleton, `apply_hooks()` pipeline function,
auto-discovery of installed hook packages via `importlib.metadata` entry points, and a
file-system watcher for hot-reloading plugin code.

**Why it should go:** GOAL.md Step 7 describes the connector authoring contract in terms of
YAML configuration files, a simulator interface (for testing), and required test-suite
structure — not arbitrary Python hooks injected at runtime. The hook system reintroduces a
code-based extension mechanism into a tool that is explicitly designed around declarative YAML.
It also reintroduces a version of the "bridge layer" that GOAL.md eliminated: user-supplied
`transform` callbacks that reshape data before ingestion are exactly what the YAML ingestion
config's `field_mapping` and `ingestion.response_path` declarations cover.

Additionally, the `[project.entry-points."inandout.hooks"]` line in `pyproject.toml` ships
`example_hooks` as a published entry point — treating this as a first-class extension API.

**What imports it:**
- `ingestion/engine.py` — `apply_hooks()` called per record in the main ingest loop
- `ingestion/dry_run.py` — `apply_hooks()` in dry-run record processing
- `ingestion/daemon.py` — `discover_and_register_hooks()`, `watch_plugin_versions()`
- `cli/main.py` — `apply_hooks()` in test run path (line 343)

**Test files to remove:** `tests/unit/test_hooks.py`, `tests/unit/test_plugin_discovery.py`,
`tests/unit/test_plugin_version_watcher.py`  
**Tests to update:** `tests/unit/test_dry_run.py`, `tests/unit/test_dry_run_staging.py` —
remove hook-related setup.

**Side effects on removal:**
- Remove `apply_hooks()` call block in `ingestion/engine.py` (~5 lines, well-isolated).
- Remove `discover_and_register_hooks()` and `watch_plugin_versions()` from `daemon.py`
  (~10 lines).
- Remove `[project.entry-points."inandout.hooks"]` from `pyproject.toml`.
- This is the most coupled removal, but the call sites are small and isolated.

---

## Additional Note · `fastapi` as a Core Dependency

`fastapi` is listed as a core (non-optional) project dependency in `pyproject.toml` even though
the webhook server and health/readiness endpoints are built on Starlette — which is already a
hard dependency (FastAPI is a layer on top of Starlette). The management API in `api/routes.py`
uses `fastapi.APIRouter`. This is not scope creep per se (a management REST API is a reasonable
interpretation of GOAL.md's runtime control table), but GOAL.md specifies the control table as
the operator interface, not a REST layer.

**Recommendation:** If the management REST API is kept, the FastAPI dependency is acceptable.
If the REST management API is also considered scope creep (operators use the CLI and the
PostgreSQL control table directly), `api/` can be removed and FastAPI dropped. This is a lower
priority question than the 15 items above.

---

## Recommended Removal Order

```
Week 1 (pure deletes, no engine changes):
  P1-1  registry/
  P1-2  generator/
  P1-3  ui/
  P1-4  diff/
  P1-5  ingestion/backfill.py
  P1-6  migrations/connector_schema.py + connector_migrations/
  P1-7  observability/health_score.py
  P1-8  api/auth.py

Week 2 (small engine edits):
  P2-1  alerting/             — remove from config/tool.py
  P2-2  federation/           — remove from daemon.py + schema
  P2-3  writeback/fan_in.py   — remove from writeback/engine.py
  P2-4  writeback/merge_hooks.py — remove from writeback/engine.py

Week 3 (surgical engine changes, run full test suite after each):
  P3-1  ingestion/cdc.py      — remove from engine + config
  P3-2  events/               — remove from engine + daemon
  P3-3  plugins/              — remove from engine + daemon + cli
```

---

## What Stays (Confirmed In-Scope)

For clarity, these modules are explicitly in scope and should not be touched:

- `config/` — all connector/tool config models (GOAL.md Step 1)
- `postgres/` — all schema, DDL, watermark, checkpoint, housekeeping logic (Step 2)
- `simulators/` — HubSpot + Salesforce simulators (Step 3)
- `testing/` — connector test framework (Step 3)
- `transport/` — HTTP adapter, circuit breaker, rate limiter, retry budget (Step 4/5)
- `ingestion/engine.py` + `daemon.py` + `webhooks.py` + `webhook_server.py` + `webhook_lifecycle.py` (Step 4)
- `ingestion/field_mapper.py`, `primary_key.py`, `quality.py`, `timestamp_normalizer.py`, `watcher.py` (Step 4)
- `ingestion/graphql.py` — GraphQL transport variant (HTTP-based, in scope)
- `ingestion/bulk_export.py` — T1 #48 (explicitly required)
- `ingestion/debounce.py` — T1 #18 / #25 politeness
- `ingestion/dry_run.py` — T1 #43 connector validation mode (keep; remove only plugin coupling)
- `writeback/engine.py` + `daemon.py` + `ordering.py` + `batch_response.py` + `crdt.py` + `slot_monitor.py` + `notify.py` (Step 5)
- `cli/main.py` — operational commands (Step 6; trim the scope-creep sub-commands only)
- `linter/` — connector YAML static analysis (Step 7)
- `schema_registry/` — local schema file tracking for T1 #31 schema drift
- `secrets/` — credential backends (Vault, AWS, env) — T1 #11 credential management
- `observability/metrics.py`, `logging.py`, `tracing.py`, `sla.py` — all required
- `privacy.py` — GDPR right-to-erasure (cross-cutting concern)
- `migrations/` (Alembic versions) — PostgreSQL schema migrations (Step 2)
