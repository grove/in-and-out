# Documentation Plan — in-and-out

This document defines the full documentation set needed for in-and-out end users. Each entry includes its purpose, intended audience, suggested filename, and a detailed outline.

Documents are grouped by the reader's journey: first understanding the system, then getting it running, then mastering each feature.

---

## Document Index

| # | Document | Audience | Priority |
|---|---|---|---|
| 1 | Architecture Overview | All | P0 |
| 2 | Installation Guide | Operators | P0 |
| 3 | Getting Started Guide | Operators, Developers | P0 |
| 4 | Configuration Reference — Tool Config | Operators | P0 |
| 5 | Connector Authoring Guide | Integration authors | P0 |
| 6 | CLI Reference | Operators, Developers | P0 |
| 7 | Database & Migrations Guide | Operators | P0 |
| 8 | Deployment Guide (Docker & Kubernetes) | Operators | P1 |
| 9 | Writeback Guide | Integration authors | P1 |
| 10 | Webhook Configuration Guide | Integration authors | P1 |
| 11 | Observability Guide | Operators, SREs | P1 |
| 12 | Security Guide | Operators | P1 |
| 13 | Operations Runbook | Operators, SREs | P1 |
| 14 | Schema Contract Reference | Downstream developers | P1 |
| 15 | Integration Guide — OSI-Mapping & pg-trickle | MDM platform developers | P2 |
| 16 | Dead-Letter & Replay Guide | Operators | P2 |
| 17 | Testing & Simulator Guide | Integration authors | P2 |
| 18 | Connector Configuration Reference (full) | Integration authors | P2 |
| 19 | Troubleshooting Guide | Operators, Developers | P2 |
| 20 | Glossary | All | P2 |

**Priority key**: P0 = required before first production use · P1 = required before handing off to a team · P2 = completes the documentation set

---

## 1. Architecture Overview

**File**: `docs/ARCHITECTURE.md`  
**Audience**: All readers — first document anyone should read.  
**Purpose**: Explain what the system does, what it does not do, and how its pieces fit together. Establish the mental model that all other documents build on.

### Outline

1. **What is in-and-out?**
   - Declarative, bidirectional HTTP API synchronisation for MDM
   - Role in a composite MDM architecture: the I/O layer
   - What is explicitly out of scope (identity resolution, field-level conflict scoring — those belong to OSI-Mapping)

2. **System diagram**
   - External APIs → Ingestion Daemon → PostgreSQL source tables
   - OSI-Mapping + pg-trickle → desired-state tables
   - Writeback Daemon → External APIs
   - Annotation: the two daemons are decoupled and communicate only through PostgreSQL

3. **The two daemons**
   - `inandout-ingest`: what it reads, what it writes, how it runs
   - `inandout-writeback`: what it reads, what it writes, how it runs
   - Why they are separate processes (independent scaling, failure isolation)

4. **Configuration layers**
   - Layer 1: Tool config (`config/ingestion.yaml`, `config/writeback.yaml`) — daemon settings, DB connection, observability
   - Layer 2: Connector config (`connectors/*.yaml`) — HTTP mechanics per API
   - Layer 3: OSI-Mapping config — identity resolution, conflict scoring (upstream, not in this repo)

5. **PostgreSQL as the integration bus**
   - Table naming convention overview (`inout_src_*`, `inout_dst_*`, `inout_ops_*`, `inout_dl_*`)
   - Why all state lives in the DB (stateless processes, HA, auditability)

6. **Connector model overview**
   - What a connector contains (connection, auth, datatypes)
   - Generation profiles (brief definition of each)
   - Relationship: one connector file → one external system

7. **Data flow walkthrough**
   - Ingestion: poll → normalise → upsert → watermark
   - Writeback: read desired state → pre-flight read → 3-way comparison → write → identity map

8. **High availability model**
   - Stateless daemons + PostgreSQL advisory locks
   - Multiple instances safe to run concurrently

---

## 2. Installation Guide

**File**: `docs/INSTALLATION.md`  
**Audience**: Operators setting up the system for the first time.  
**Purpose**: Get the software installed and verify the environment is healthy.

### Outline

1. **Prerequisites**
   - Python 3.13+
   - PostgreSQL 15 or 16
   - `uv` (recommended package manager)
   - Docker (optional, for local Compose stack)
   - `just` (optional, for convenience recipes)

2. **Installing from source**
   ```
   git clone ...
   uv sync
   ```

3. **Installing as a package** (when published to PyPI)
   ```
   pip install inandout
   ```
   or with uv:
   ```
   uv tool install inandout
   ```

4. **Verifying the installation**
   ```
   inandout version
   inandout --help
   ```

5. **Setting up the database**
   - Creating the PostgreSQL database and user
   - Setting `INOUT_DATABASE_URL`
   - Running `inandout db upgrade` to apply all migrations
   - Verifying with `inandout db status`

6. **Quick smoke test**
   - Validating the bundled example connector
   ```
   inandout ingest validate-connector connectors/hubspot.example.yaml
   ```

7. **Environment variables reference** (summary table, full detail in Configuration Reference)

8. **Upgrading**
   - `git pull && uv sync`
   - `inandout db upgrade` must be run before restarting daemons
   - Daemon start-up schema version check: what happens if the schema is stale

---

## 3. Getting Started Guide

**File**: `docs/GETTING_STARTED.md`  
**Audience**: New operators and developers; the primary "first use" document.  
**Purpose**: Walk from zero to a running ingestion cycle using the bundled Docker Compose stack and a minimal real (or simulated) connector.

### Outline

1. **Overview**: what we will build in this guide

2. **Start the local stack**
   ```
   just up-db
   just db-upgrade
   ```
   Or manually:
   ```
   docker compose up -d postgres
   inandout db upgrade
   ```

3. **Write your first connector** (minimal polling connector)
   - Copy `connectors/hubspot.example.yaml`; adjust `connector.name`, `connection.base_url`
   - Fill in one `datatype` with a single `list` endpoint
   - Use `${MY_API_TOKEN}` for the credential — do not hard-code secrets

4. **Validate the connector**
   ```
   inandout ingest validate-connector connectors/my-connector.yaml
   ```
   - Walk through common validation errors (CFG-* codes)

5. **Dry-run (fetch without writing)**
   ```
   inandout ingest dry-run --connector connectors/my-connector.yaml --datatype contacts
   ```
   - Explains what dry-run shows: raw response, parsed records, pagination steps, no DB writes

6. **Start the ingestion daemon**
   ```
   inandout ingest run --config config/ingestion.yaml
   ```
   - What to look for in logs (first sync run, watermark saved, record counts)

7. **Verify the data in PostgreSQL**
   - Query `inout_src_{connector}_contacts`
   - Inspect `inout_ops_sync_run` for run metrics

8. **Next steps**
   - Add more datatypes
   - Enable incremental sync (watermarks)
   - Set up writeback
   - Deploy to production

---

## 4. Configuration Reference — Tool Config

**File**: `docs/TOOL_CONFIG_REFERENCE.md`  
**Audience**: Operators.  
**Purpose**: Complete reference for `config/ingestion.yaml` and `config/writeback.yaml`. Not the connector YAML — that is covered in document 18.

### Outline

1. **File locations and loading order**
   - CLI flag: `--config`
   - Environment variable override: `INOUT_CONFIG_PATH`

2. **`ingestion.yaml` full schema**
   - `database.url`, `database.pool_min`, `database.pool_max`
   - `health_server.host`, `health_server.port`
   - `webhook_server.host`, `webhook_server.port`, `webhook_server.tls.*`
   - `connectors_dir` — where connector YAML files are loaded from
   - `log.level`, `log.format` (`json` | `text`)
   - `tracing.otlp_endpoint`, `tracing.sample_rate`
   - `metrics.enabled`, `metrics.port`
   - `control_poll_interval`
   - `graceful_shutdown_timeout`

3. **`writeback.yaml` full schema**
   - Same database/observability fields as above
   - `writeback.concurrency` — parallel writes per datatype
   - `writeback.default_pre_flight_protection_level`
   - Writeback-specific health server config

4. **Environment variable reference**
   - Full table: variable name → config key → default value
   - Priority: env var overrides file value

5. **Secrets and sensitive values**
   - Never put passwords in config files; use env vars

---

## 5. Connector Authoring Guide

**File**: `docs/CONNECTOR_AUTHORING.md` *(extend existing file)*  
**Audience**: Integration authors writing connector YAML files.  
**Purpose**: Teach readers to write correct, production-ready connector files from scratch.

The existing `docs/CONNECTOR_AUTHORING.md` provides a good starting point. This guide should be expanded to cover all sections below.

### Outline

1. **What a connector file is**
   - One file per external system
   - File location (`connectors/`) and naming convention

2. **Generation profiles** — choosing the right shape
   | Profile | When to use |
   |---|---|
   | `ingestion_polling_readonly` | Read-only: no writeback |
   | `ingestion_webhook_incremental` | Receive real-time events + poll for full sync |
   | `writeback_patch` | Push desired state to an external API, no ingestion |
   | `full_duplex` | Both ingestion and writeback |
   - Describe required fields for each profile

3. **Connection configuration**
   - `base_url`, `staging_base_url`, `timeout_secs`
   - DNS / TLS notes

4. **Authentication**
   - `api_key`: header name, prefix
   - `oauth2`: grant types (client_credentials, authorization_code, refresh_token flow); token endpoint; credential_ref
   - `jwt`: signing key credential_ref, claims template, expiry
   - `basic`: username/password credential_ref
   - `custom`: header injection, variable interpolation
   - **Credential referencing**: `credential_ref` and how credentials are loaded at runtime (env vars, encrypted DB column, Vault, AWS Secrets Manager)
   - **Security rule**: never embed secrets in connector YAML

5. **Rate limiting, retry, and circuit breaker**
   - `rate_limit.rpm` / `rps`
   - `retry.max_attempts`, `retry.backoff`, `retry.jitter`, `retry.on_status_codes`
   - `circuit_breaker.*` at connector level

6. **Datatypes**
   - `kind`: `entity` vs `association`
   - `pii_fields`: what happens to listed fields (masking/pseudonymisation)
   - Naming conventions and their effect on table names

7. **Ingestion config per datatype**
   - `primary_key`: single or composite
   - `history_mode`: `overwrite` vs `append`
   - `schedule`: cron expression; how it is evaluated
   - `list` endpoint: `method`, `path`, `headers`, `query_params`, `body`, `record_selector` (JMESPath / JSONPath), `record_count_selector`
   - `get` endpoint (for individual record fetch if needed)
   - `sync_mode`: `full`, `incremental`, `delta_only`, `bulk_export`
   - `watermark`: `field`, `field_type` (timestamp/cursor/offset/sequence), `format`, `initial_value`
   - Pagination strategies: `cursor`, `offset`, `link_header`, `keyset` — all fields explained with examples
   - Pagination termination conditions
   - `transforms`: field mapping, type casting, renaming, dropping

8. **Writeback config per datatype**
   - `insert`, `update`, `delete`, `archive` endpoint configs
   - `method`, `path`, `body_template`
   - `id_location`: where to inject `external_id` in request path/body
   - `conflict_strategy`: all strategies explained with examples
   - `pre_flight_protection_level`: levels 0–3 explained
   - `optimistic_lock`: ETag / version field name
   - `identity_map`: capturing new external IDs on insert

9. **Variable interpolation reference**
   - Full table of all `${...}` variables: source, available-in context

10. **Webhook configuration** (for `ingestion_webhook_incremental` and `full_duplex`)
    - `path`, `port_override`
    - `signature_verification`: algorithm, header, secret credential_ref
    - Routing: `discriminator` field and `routes` map
    - Deduplication: `dedup_window_secs`
    - Out-of-order handling
    - Debouncing
    - References document 10 (Webhook Configuration Guide) for full detail

11. **Validation and linting**
    - Running the YAML linter: `inandout ingest validate-connector`
    - Understanding CFG-* error codes
    - Lint rule index (cross-reference to document 18)

12. **Testing a connector locally**
    - Using `dry-run` to test without a database
    - Using the built-in simulators for CI without live APIs
    - References document 17 (Testing Guide)

13. **Full annotated example**: complete `full_duplex` connector YAML

---

## 6. CLI Reference

**File**: `docs/CLI_REFERENCE.md`  
**Audience**: Operators and developers. Auto-generated section possible via `inandout api spec`.  
**Purpose**: Complete command, flag, and output reference.

### Outline

1. **Global flags**
   - `--config`, `--log-level`, `--help`, `--version`

2. **`inandout ingest` commands**
   | Command | Description |
   |---|---|
   | `run` | Start the ingestion daemon |
   | `validate` | Validate all connector files in `connectors_dir` |
   | `validate-connector <path>` | Validate a single connector file |
   | `dry-run` | Fetch records from one endpoint without writing to DB |
   - Flags, examples, exit codes for each

3. **`inandout writeback` commands**
   | Command | Description |
   |---|---|
   | `run` | Start the writeback daemon |
   - Flags, examples, exit codes

4. **`inandout db` commands**
   | Command | Description |
   |---|---|
   | `upgrade` | Apply pending migrations (Alembic `upgrade head`) |
   | `downgrade` | Roll back one migration step |
   | `status` | Show current and pending migrations |
   - Note: always run `upgrade` before starting daemons after update

5. **`inandout connector` commands**
   - `status`: show connector versions deployed to DB

6. **`inandout webhook` commands**
   - `replay <event-id>`: replay a webhook event from the audit log

7. **`inandout control` commands**
   - All control table commands: `resync`, `pause`, `resume`, `reset-watermark`, `reload-config`, `reset-circuit-breaker`, `replay-dead-letter`, `validate`, `drain`
   - How to scope commands by connector and datatype

8. **`inandout dead-letter` commands**
   - `list`, `inspect <id>`, `replay <id>`, `discard <id>`

9. **`inandout api` commands**
   - `spec`: dump OpenAPI JSON for the management API

10. **`inandout version`**

11. **Exit codes reference** (0 = success, 1 = error, 2 = config error, …)

---

## 7. Database & Migrations Guide

**File**: `docs/DATABASE.md`  
**Audience**: Operators.  
**Purpose**: Explain the database schema lifecycle, how to run and roll back migrations, and the schema version enforcement at daemon start-up.

### Outline

1. **Database requirements**
   - PostgreSQL 15 or 16
   - Required permissions: `CREATE TABLE`, `CREATE INDEX`, `SELECT`, `INSERT`, `UPDATE`, `DELETE`
   - Recommended: dedicated database and user

2. **How migrations work**
   - Alembic under the hood; migration files in `migrations/versions/`
   - Daemons **do not auto-migrate**; they check the schema version at boot and refuse to start if stale
   - Migration files are numbered and dated for clarity

3. **Running migrations**
   ```
   inandout db upgrade          # apply all pending
   inandout db status           # show current / head revision
   inandout db history          # list all revisions
   ```

4. **Rolling back**
   ```
   inandout db downgrade        # one step back
   inandout db downgrade <rev>  # to a specific revision
   ```
   - Warning: some rollbacks are destructive (data loss). Always back up first.

5. **Schema version enforcement**
   - What happens at daemon start-up if schema is behind
   - Resolving "schema mismatch" errors

6. **Production migration workflow**
   - Recommended order: migrate DB → roll out new daemon version
   - Zero-downtime migration notes (additive migrations are safe; destructive ones require coordination)

7. **Database table reference**
   - Complete table: all `inout_*` tables, purpose, owning subsystem
   - Column descriptions for key operational tables (`inout_ops_sync_run`, `inout_ops_watermark`, `inout_ops_control`, `inout_ops_identity_map`)

8. **Multi-tenant / multi-database setups**
   - Running separate daemons for separate databases

---

## 8. Deployment Guide

**File**: `docs/DEPLOYMENT.md`  
**Audience**: Operators and DevOps/SREs.  
**Purpose**: Production deployment for both Docker Compose and Kubernetes.

### Outline

1. **Deployment architecture overview**
   - Stateless daemons; all state in PostgreSQL
   - Two deployable units: `inandout-ingest` and `inandout-writeback`
   - Ingestion daemon also hosts: webhook receiver + Prometheus metrics endpoint

2. **Docker Compose deployment**
   - Using the bundled `docker-compose.yml`
   - Service dependencies and health checks
   - Injecting config and connector files via volumes
   - Environment variables for secrets
   - `just up` / `just down` reference

3. **Kubernetes deployment**
   - Overview of manifests in `k8s/`
   - Walk through: namespace → configmap → secret → migrate job → ingest deployment → writeback deployment
   - `migrate-job.yaml`: why migrations run as a Job before daemon Deployments
   - `hpa.yaml`: horizontal scaling rules
   - `servicemonitor.yaml`: wiring Prometheus scraping
   - Kustomize overlays: how to use `kustomization.yaml` for environment-specific config
   - Secrets management: prefer Kubernetes Secrets or external-secrets operator over baking secrets into images

4. **Configuration injection patterns**
   - Environment variables
   - Mounted ConfigMaps for tool config
   - Mounted Secret for connector YAML containing `credential_ref` names
   - Never bake secrets into the container image

5. **High availability**
   - Running multiple replicas: safe by design (PostgreSQL advisory locks prevent duplicate sync runs)
   - Which daemon to scale: usually ingestion is I/O-bound; writeback bound by target API rate limits
   - `inout_ops_sync_lock` table: how distributed locking works

6. **Health and readiness probes**
   - Health endpoint URL and what it checks
   - Readiness endpoint: starts serving after first successful DB connection + schema check
   - Recommended liveness/readiness probe config for Kubernetes

7. **Resource sizing guidelines**
   - Ingestion: CPU-light, I/O-bound; memory scales with page size × concurrency
   - Writeback: memory scales with batch size; CPU-light
   - PostgreSQL: primary sizing driver; index strategy matters

8. **Graceful shutdown**
   - SIGTERM handling: completes current page/batch before exit
   - `graceful_shutdown_timeout` config
   - Pod preStop hooks in Kubernetes

9. **Upgrading in production**
   - Run `migrate-job` → update `ingest-deployment` → update `writeback-deployment`
   - Rollback procedure

---

## 9. Writeback Guide

**File**: `docs/WRITEBACK.md`  
**Audience**: Integration authors and MDM platform developers.  
**Purpose**: Explain the full writeback data flow, conflict detection, write-anomaly protection, and how to configure correct writeback behaviour per connector.

### Outline

1. **Overview**
   - What the writeback daemon does
   - Relationship to OSI-Mapping (desired-state tables are populated by OSI-Mapping)
   - The desired-state table schema (`inout_dst_*`)

2. **The write cycle**
   - Reading from `inout_dst_{connector}_{datatype}`
   - Supported `action` values: `insert`, `update`, `delete`, `archive`, `noop`, `upsert`
   - Pre-flight read: why it is mandatory
   - Three-way comparison: `current` vs `base` vs `last_written_state`

3. **Conflict detection and resolution strategies**
   - When a conflict is detected
   - Strategy: `dead_letter` (default) — routed to dead-letter queue with full context
   - Strategy: `last_writer_wins` — overwrites regardless
   - Strategy: `skip_and_warn` — discards write, logs warning
   - Strategy: `re_ingest_and_recompute` — signals ingestion; MDM recomputes
   - Strategy: `server_wins` — accepts target system state as authoritative
   - Strategy: `merge_fields` — field-level merge of non-conflicting fields
   - How to choose the right strategy per datatype

4. **Write-anomaly protection levels**
   | Level | Name | Mechanism | Trade-off |
   |---|---|---|---|
   | 0 | None | No protection | Fastest; no safety |
   | 1 | Conditional writes | ETag / `If-Match` header | Closes TOCTOU fully; requires API support |
   | 2 | Optimistic | Pre-flight read + 3-way compare | Millisecond residual TOCTOU window |
   | 3 | Post-write verify | Read after write to confirm | Safest; doubles API calls |
   - How to configure `pre_flight_protection_level` per datatype
   - How to configure `optimistic_lock.field_name` for ETag/version fields

5. **Identity map**
   - Why it exists: capturing external IDs generated by the target system on `insert`
   - `inout_ops_identity_map` schema: `cluster_id ↔ external_id ↔ internal_id`
   - Preventing duplicate inserts under concurrent retries
   - Configuring `identity_map.capture_field`

6. **Last-written-state table (`inout_dst_*_lwstate`)**
   - Purpose: provides the `last_written_state` for 3-way comparison on next cycle
   - Atomically updated after every successful write

7. **Circuit breaker**
   - When it trips on the writeback side
   - How to inspect: `inandout connector status`
   - How to reset: `inandout control reset-circuit-breaker`

8. **Writeback concurrency and ordering**
   - `writeback.concurrency` config
   - Ordering: respecting `cluster_id`-level ordering and dependency ordering for write batches

9. **Batch response handling**
   - APIs that accept bulk write requests
   - Partial success handling

10. **Example walkthrough**: full update cycle from desired-state row to successful write

---

## 10. Webhook Configuration Guide

**File**: `docs/WEBHOOKS.md`  
**Audience**: Integration authors.  
**Purpose**: Configure inbound webhook reception, signature verification, event routing, deduplication, and lifecycle management.

### Outline

1. **Overview**
   - The ingestion daemon runs a persistent HTTPS webhook server
   - Webhooks complement polling: reduce latency for real-time data changes
   - Webhooks are available for `ingestion_webhook_incremental` and `full_duplex` profiles

2. **Network setup**
   - Webhook server port (default): separate from the Prometheus metrics port
   - TLS configuration (mandatory for production)
   - Reverse proxy / load balancer considerations
   - Public URL that external systems must be able to reach

3. **Signature verification**
   - Required for all production connections
   - `signature_verification.algorithm`: `hmac-sha256` etc.
   - `signature_verification.header`: name of the header carrying the signature
   - `signature_verification.secret_credential_ref`: references a stored credential
   - What happens on invalid signatures: rejected with 401, logged in `inout_ops_webhook_log`

4. **Event routing**
   - `discriminator`: JMESPath/JSONPath expression evaluated on the request body to extract the event type
   - `routes` map: event type string → datatype name
   - Fan-out: one event can route to multiple datatypes (list value)
   - Catch-all route: `"*"` key

5. **Deduplication**
   - `dedup_id_selector`: expression to extract a stable event ID from the request
   - `dedup_window_secs`: how long seen event IDs are retained
   - Storage: `inout_ops_webhook_seen` table

6. **Out-of-order handling**
   - `out_of_order_handling`: `accept_latest_timestamp`, `accept_highest_sequence`, `ignore`
   - When to use each strategy
   - Required fields on the event payload for sequence/timestamp strategies

7. **Debouncing**
   - `debounce_window_ms`: coalesce rapid successive events for the same record
   - Use case: burst of field-level update events for a single record

8. **Webhook subscription lifecycle**
   - Automatic registration at daemon start-up
   - Renewal of expiring subscriptions
   - Cleanup of stale subscriptions
   - Configuration: `subscription_url_template`, `subscription_expiry_secs`

9. **Webhook event replay**
   - All events logged to `inout_ops_webhook_log`
   - Replaying from the audit log: `inandout webhook replay <event-id>`

10. **Testing webhooks locally**
    - Using `ngrok` or similar to expose local port during development
    - Replaying events with `curl` against the local server

---

## 11. Observability Guide

**File**: `docs/OBSERVABILITY.md`  
**Audience**: Operators and SREs.  
**Purpose**: Explain the full observability stack: structured logs, Prometheus metrics, OpenTelemetry traces, and Grafana dashboards.

### Outline

1. **Overview**
   - Three pillars: logs, metrics, traces

2. **Structured logging**
   - `log.format`: `json` (production) vs `text` (development)
   - `log.level`: `debug`, `info`, `warning`, `error`
   - Key log fields: `connector`, `datatype`, `sync_run_id`, `action`, `duration_ms`, `record_count`
   - Log sampling for high-volume ingestion

3. **Prometheus metrics**
   - Metrics endpoint: `:9090/metrics` (configurable)
   - Key metrics reference table:
     - `inandout_sync_run_records_total` (labels: connector, datatype, status)
     - `inandout_sync_run_duration_seconds`
     - `inandout_writeback_writes_total` (labels: connector, datatype, action, status)
     - `inandout_webhook_events_total` (labels: connector, status)
     - `inandout_circuit_breaker_state` (labels: connector, datatype, direction)
     - `inandout_dead_letter_queue_depth` (labels: connector, datatype)
   - Kubernetes `ServiceMonitor` configuration

4. **OpenTelemetry tracing**
   - `tracing.otlp_endpoint`: where to send spans
   - `tracing.sample_rate`: 0.0–1.0
   - Instrumented operations: HTTP calls (via HTTPX), DB queries (via psycopg), sync runs
   - Trace context propagation

5. **Grafana dashboards**
   - Bundled dashboards in `observability/grafana/dashboards/`
   - Auto-provisioning via `observability/grafana/provisioning/`
   - Starting the observability stack: `just up-obs`
   - Walk-through of each dashboard panel

6. **AlertManager rules**
   - Bundled rules in `observability/alertmanager.yml`
   - Key alerts: circuit breaker tripped, dead-letter queue growing, sync run failure rate, writeback error spike
   - Customising alert thresholds

7. **Operational database table: `inout_ops_sync_run`**
   - The primary operational record of what ran, when, and with what results
   - Key columns: `connector`, `datatype`, `status`, `records_fetched`, `records_inserted`, `records_updated`, `records_deleted`, `records_errored`, `watermark_before`, `watermark_after`, `started_at`, `finished_at`
   - Useful queries for investigations

---

## 12. Security Guide

**File**: `docs/SECURITY.md`  
**Audience**: Operators, security engineers.  
**Purpose**: Credential management, webhook signature verification, TLS requirements, and secure deployment practices.

### Outline

1. **Credential management**
   - Rule: never embed secrets in connector YAML files
   - `credential_ref`: how it works — name resolves at runtime
   - Resolution hierarchy: environment variable → encrypted PostgreSQL column → Vault → AWS Secrets Manager
   - Configuring the secrets backend in tool config

2. **Connector YAML security**
   - Using `${ENV_VAR}` for any value that varies between environments
   - Git-safe connector files: no secrets, only `credential_ref` names

3. **Webhook signature verification**
   - Mandatory for production webhooks
   - HMAC-SHA256 verification flow
   - Credential storage for HMAC secrets

4. **TLS configuration**
   - Webhook server TLS: required for inbound webhooks from external systems
   - Certificate provisioning (cert-manager in Kubernetes)
   - Outbound TLS: `httpx` enforces TLS verification by default; how to configure custom CA bundles

5. **Database security**
   - Minimum required permissions
   - Encrypted columns for stored credentials
   - `INOUT_DATABASE_URL` handling: env var, never in config files committed to source control

6. **Network security**
   - Ingestion daemon outbound: only to configured `base_url` endpoints
   - Webhook server inbound: HMAC signature required; invalid requests rejected with no side effects
   - Prometheus metrics endpoint: should be scrape-only, not publicly exposed

7. **PII handling**
   - `pii_fields` per datatype: listed fields are masked/pseudonymised in logs and dead-letter output
   - Data stored in `inout_src_*` tables is not masked (raw data store); downstream access controls are the operator's responsibility

8. **Audit trail**
   - `inout_ops_sync_run`: complete record of every sync operation
   - `inout_ops_webhook_log`: every received webhook event (before signature check passes)
   - `inout_ops_identity_map`: mapping of cluster IDs to external IDs

---

## 13. Operations Runbook

**File**: `docs/RUNBOOK.md`  
**Audience**: Operators and SREs. Intended for use during incidents.  
**Purpose**: Step-by-step procedures for common operational tasks.

### Outline

1. **Starting and stopping daemons**
   - Start, graceful stop, force kill
   - Health check verification

2. **Pausing and resuming a connector**
   ```sql
   -- via control table
   INSERT INTO inout_ops_control (connector, datatype, command) VALUES ('hubspot', '*', 'pause');
   ```
   Or via CLI: `inandout control pause --connector hubspot`

3. **Forcing a full resync (reset watermark)**
   - When to use: after data loss, after a long pause, after a major API schema change
   - `inandout control reset-watermark --connector hubspot --datatype contacts`
   - Expected behaviour: full re-fetch, full upsert; no duplicate rows created

4. **Hot-reloading connector config**
   - Connector file changes that do not require a migration are picked up automatically at next sync cycle
   - Connector changes that alter database tables (e.g., adding a new datatype) require: migration → `reload-config` control command
   - `inandout control reload-config --connector hubspot`

5. **Resetting a tripped circuit breaker**
   - Diagnose: `inandout connector status`
   - Fix the underlying condition (API issue, empty result set)
   - Reset: `inandout control reset-circuit-breaker --connector hubspot --datatype contacts`

6. **Investigating a failed sync run**
   - Query `inout_ops_sync_run` for status and error
   - Check logs for the `sync_run_id`
   - Check dead-letter queue: `inandout dead-letter list --connector hubspot --datatype contacts`

7. **Replaying dead-letter records**
   - `inandout dead-letter replay <id>` — replay a single record
   - `inandout control replay-dead-letter --connector hubspot` — replay the full queue
   - When to replay vs when to discard

8. **Draining before maintenance**
   - `inandout control drain --connector hubspot` — wait for in-flight work to finish before shutdown

9. **Scaling up/down**
   - Adding a replica: safe (advisory locks prevent duplicate runs)
   - Scaling to zero for maintenance

10. **Database maintenance**
    - Periodic `VACUUM` / `ANALYZE` recommendations for high-write tables
    - Index health monitoring for `inout_src_*` tables

11. **Upgrading in production**
    - Pre-upgrade checklist
    - Run migration → update image → verify

---

## 14. Schema Contract Reference

**File**: `docs/SCHEMA_CONTRACT.md` *(extend existing file)*  
**Audience**: Downstream developers consuming `inout_src_*` tables or writing to `inout_dst_*` tables.  
**Purpose**: Define the guaranteed column contract on every table produced by in-and-out.

The existing `docs/SCHEMA_CONTRACT.md` should be reviewed and extended to cover:

### Outline

1. **Source table contract (`inout_src_*`)**
   - Every source table has these guaranteed columns:
     | Column | Type | Description |
     |---|---|---|
     | `external_id` | text | Unique identifier in the source system |
     | `data` | jsonb | Parsed record payload |
     | `raw` | jsonb | Original API response (before transforms) |
     | `_ingested_at` | timestamptz | When the daemon ingested the record |
     | `_sync_run_id` | uuid | FK to `inout_ops_sync_run` |
     | `_raw_hash` | text | xxhash of `raw` for change detection |
     | `_deleted` | boolean | Soft-delete flag |
     | `_deleted_at` | timestamptz | When soft-deletion was recorded |
     | `_schema_version` | integer | Tracks structural API schema changes |
     | `_source_version` | text | ETag / version token from the source API |
     | `_last_written` | timestamptz | Last time writeback wrote to this record |
     | `_lineage` | jsonb | Provenance metadata |

2. **History table contract (`inout_src_*_history`)**
   - Append-only; all source table columns plus `_history_id` (serial PK) and `_valid_from` / `_valid_to`

3. **Desired-state table contract (`inout_dst_*`)**
   - Columns: `cluster_id`, `action`, `data`, `base`, `base_version`, `_requested_at`, `_status`, `_processed_at`
   - Who writes these rows (OSI-Mapping) and who reads them (writeback daemon)

4. **Schema versioning and drift detection**
   - `_schema_version` increment policy
   - How downstream consumers should handle schema version changes
   - Schema registry query patterns

---

## 15. Integration Guide — OSI-Mapping & pg-trickle

**File**: `docs/INTEGRATION.md`  
**Audience**: MDM platform architects and developers.  
**Purpose**: Explain how in-and-out fits into the broader composite MDM pipeline with OSI-Mapping and pg-trickle IVM.

### Outline

1. **The composite MDM architecture**
   - in-and-out (I/O layer) + OSI-Mapping (identity resolution) + pg-trickle (IVM/streaming) = full MDM pipeline
   - Data flow diagram with table names at each stage

2. **Publishing data to OSI-Mapping**
   - `inout_src_*` tables are the upstream inputs to OSI-Mapping consolidation rules
   - How OSI-Mapping reads from these tables via pg-trickle IVM
   - Schema contract compatibility requirements (see document 14)

3. **Consuming desired-state from OSI-Mapping**
   - OSI-Mapping writes to `inout_dst_*` tables
   - Required schema and column conventions in desired-state tables
   - `cluster_id` field: OSI-Mapping's merge-group identifier

4. **pg-trickle IVM integration**
   - `_delta_{connector}_{datatype}` stream tables — owned by pg-trickle
   - How incrementally maintained views feed OSI-Mapping without polling

5. **Connector versioning and schema drift**
   - `inout_ops_connector_version`: tracking deployed connector versions
   - How schema version changes in `inout_src_*` signal OSI-Mapping to adapt

6. **Federation scenarios**
   - Multiple in-and-out instances writing to different connector sets
   - How OSI-Mapping deduplicates across sources

---

## 16. Dead-Letter & Replay Guide

**File**: `docs/DEAD_LETTER.md`  
**Audience**: Operators, integration authors.  
**Purpose**: Explain the dead-letter queues for both ingestion and writeback, how to inspect failures, and how to replay or discard entries.

### Outline

1. **What is a dead-letter entry?**
   - Ingestion: records that failed to be parsed, transformed, or written to the DB after all retries
   - Writeback: records that failed to be written to the target API after all retries, or conflicted under `dead_letter` strategy

2. **Dead-letter table schema**
   - `inout_dl_ingestion_{connector}_{datatype}`: columns, error context, raw payload
   - `inout_dl_writeback_{connector}_{datatype}`: columns, conflict details, API response

3. **Inspecting dead-letter entries**
   ```
   inandout dead-letter list --connector hubspot --datatype contacts
   inandout dead-letter inspect <id>
   ```

4. **Root-cause analysis patterns**
   - Schema mismatch (external API changed)
   - Auth failure (credential expired)
   - Conflict with external change (writeback)
   - Rate limit exhaustion
   - Transient network error (should not reach dead-letter; retry should handle)

5. **Replaying entries**
   - Single record: `inandout dead-letter replay <id>`
   - Full queue: `inandout control replay-dead-letter --connector hubspot`
   - Bulk replay: idempotency guarantees (upsert semantics for ingestion)
   - When replay fails again: entry stays in dead-letter with updated error context

6. **Discarding entries**
   ```
   inandout dead-letter discard <id>
   ```
   - When to discard vs replay
   - Discard is irreversible — confirm impact before discarding writeback entries

---

## 17. Testing & Simulator Guide

**File**: `docs/TESTING.md`  
**Audience**: Integration authors and developers.  
**Purpose**: How to test connector configurations and code changes without live external APIs.

### Outline

1. **Test suite structure**
   - `tests/unit/` — pure logic, no external deps
   - `tests/integration/` — requires a running PostgreSQL (via Testcontainers)
   - `tests/contract/` — schema contract compliance tests
   - `tests/acceptance/` — requires real external APIs (CI-excluded by default)
   - `tests/load/` — performance tests
   - `tests/simulators/` — HTTP simulator configurations

2. **Running the tests**
   ```
   just test              # unit only
   just test-integration  # needs Postgres
   just test-all          # unit + integration + contract
   ```

3. **Built-in HTTP simulators**
   - What they are: lightweight HTTP stubs that mimic external API behaviour
   - Located in `tests/simulators/` and `src/inandout/simulators/`
   - Available simulators: how to list them
   - Using a simulator in a test

4. **Writing a connector fixture for CI**
   - `fixtures/connectors/valid/` — minimal fixtures per profile
   - How to add a new fixture: naming, required fields per profile
   - `just validate-connectors` to verify all fixtures

5. **Testing connector dry-run**
   ```
   inandout ingest dry-run --connector connectors/my-connector.yaml --datatype contacts
   ```
   - What dry-run output means
   - Using `--env staging` for staging API endpoints

6. **Unit testing transform expressions**
   - Testing JMESPath record selectors and field transforms in isolation
   - Hypothesis-based property testing for transform logic

7. **Integration test patterns**
   - Using `testcontainers[postgres]` to spin up a real DB in CI
   - Fixture data patterns (factory-boy)

---

## 18. Connector Configuration Reference (Full)

**File**: `docs/CONNECTOR_CONFIG_REFERENCE.md`  
**Audience**: Integration authors; intended as an exhaustive reference, not a tutorial.  
**Purpose**: Document every possible field in a connector YAML file, with type, default, required flag, and description.

### Outline

1. **Top-level fields**
   - `schema_version`, `connector.name`, `connector.system`, `connector.description`, `connector.api_version`, `connector.generation_profile`, `connector.connector_version`, `connector.tags`

2. **`connector.connection` section** — all fields

3. **`connector.auth` section** — all fields for each auth type

4. **`connector.rate_limit` section** — all fields

5. **`connector.retry` section** — all fields

6. **`connector.circuit_breaker` section** — all fields

7. **`connector.webhooks` section** — all fields (see document 10 for conceptual detail)

8. **`connector.datatypes.{name}` section**
   - Top-level datatype fields
   - `ingestion` sub-section: all fields
   - `ingestion.list` sub-section: all fields
   - `ingestion.get` sub-section (optional): all fields
   - Pagination sub-sections: cursor, offset, link_header, keyset — all fields
   - `writeback` sub-section: all fields
   - Action endpoint sub-sections (insert/update/delete/archive): all fields

9. **Validation rules (CFG-* codes)**
   - Complete index of linter rule codes, description, and fix guidance

10. **Annotated JSON Schema** — reference to `schemas/connector.schema.json`

---

## 19. Troubleshooting Guide

**File**: `docs/TROUBLESHOOTING.md`  
**Audience**: Operators and developers facing problems.  
**Purpose**: Indexed catalogue of known failure modes and their resolutions.

### Outline

1. **Daemon fails to start**
   - "schema mismatch" — run `inandout db upgrade`
   - "cannot connect to database" — check `INOUT_DATABASE_URL`, network, credentials
   - "no connectors found" — check `connectors_dir` path and YAML filenames

2. **Connector validation errors (CFG-*)**
   - Top 10 most common lint errors with fix recipes

3. **Ingestion produces no records**
   - Dry-run to isolate: API responding? `record_selector` finding records?
   - Check auth: expired token?
   - Check pagination: `termination_condition` prematurely terminating?
   - Check circuit breaker state

4. **Ingestion deletes all records unexpectedly**
   - Empty-result circuit breaker: what it detects and how to investigate
   - Result-shrinkage circuit breaker: same
   - How to recover: `reset-circuit-breaker` after fixing root cause

5. **Watermark not advancing**
   - `sync_mode` set to `full` instead of `incremental`
   - `watermark.field` not found in response
   - Transaction rollback preventing watermark commit

6. **Writeback records stuck in `pending` state**
   - Writeback daemon not running
   - Connection or auth issue to target API
   - Rate limit: check wait_until in logs

7. **Writeback conflicts accumulating in dead-letter**
   - Review conflict strategy setting
   - Investigate source of conflict: parallel writes from another system?
   - Replay after resolving the underlying cause

8. **Webhook events not arriving**
   - TLS / HMAC verification: check daemon logs for rejected events
   - Subscription registration: `inandout connector status` to verify subscription health
   - Network connectivity: can the external system reach the webhook server?

9. **High memory / CPU usage**
   - Large page sizes: reduce `pagination.page_size`
   - High concurrency: reduce `writeback.concurrency`
   - Profiling tips

10. **Performance: sync runs are slow**
    - Check `inout_ops_sync_run` for page timings
    - API rate limit: tool is throttling to respect configured limits
    - Database write bottleneck: check index health, connection pool size

---

## 20. Glossary

**File**: `docs/GLOSSARY.md`  
**Audience**: All readers.  
**Purpose**: Define terms used throughout the documentation.

### Terms to define (partial list)

| Term | Definition |
|---|---|
| Connector | A YAML file describing how to communicate with one external HTTP API |
| Datatype | A logical object type within a connector (e.g., `contacts`, `deals`) |
| Generation profile | A named shape for a connector: which capabilities it has |
| Ingestion | Pulling data from an external API into PostgreSQL |
| Writeback | Pushing desired-state changes from PostgreSQL to an external API |
| Watermark | A persisted cursor/timestamp marking the furthest record ingested |
| History mode | Whether source table rows are overwritten (overwrite) or accumulated (append) |
| Pre-flight read | Reading a record from the target system immediately before writing, for conflict detection |
| Three-way comparison | Comparing current API state against the `base` snapshot and `last_written_state` |
| Conflict | When the current API state differs from what OSI-Mapping expected when computing the desired change |
| Dead-letter | A queue of records that failed permanently and require operator intervention |
| Circuit breaker | A safety mechanism that pauses operations when anomalous conditions are detected |
| Identity map | The mapping between OSI-Mapping `cluster_id`s and external system IDs |
| Credential ref | A symbolic name for a secret, resolved at runtime from a secrets backend |
| OSI-Mapping | The upstream identity resolution engine; computes desired state from consolidated records |
| pg-trickle | The IVM (incrementally maintained view) engine that streams changes from `inout_src_*` tables to OSI-Mapping |
| Sync run | One execution of the poll/fetch/write cycle for a single connector+datatype pair |
| Control table | `inout_ops_control`: a PostgreSQL table used to send real-time commands to running daemons |
| CFG-* | Numbered validation rule codes emitted by the connector YAML linter |

---

## Suggested Documentation Site Structure

If the documentation is served as a static site (e.g., MkDocs, Docusaurus), the suggested navigation tree is:

```
Getting Started
  ├── Architecture Overview
  ├── Installation
  └── Getting Started Tutorial

Configuration
  ├── Tool Config Reference
  ├── Connector Authoring Guide
  └── Connector Config Reference (Full)

Operations
  ├── Database & Migrations
  ├── Deployment (Docker & Kubernetes)
  ├── Observability
  ├── Security
  └── Runbook

Features
  ├── Writeback
  ├── Webhooks
  └── Dead-Letter & Replay

Reference
  ├── CLI Reference
  ├── Schema Contract
  └── Glossary

Advanced
  ├── Integration (OSI-Mapping & pg-trickle)
  └── Testing & Simulators
```
