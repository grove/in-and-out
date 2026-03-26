# Frequently Asked Questions

## General

### What is in-and-out?

A declarative, bidirectional HTTP API synchronization tool for Master Data Management (MDM). It pulls data from external APIs (HubSpot, Salesforce, Stripe, etc.) into PostgreSQL and pushes desired-state changes back out — all driven by YAML configuration, no custom code required.

### How does it relate to OSI-Mapping?

in-and-out handles the **I/O layer** only — fetching and writing data via HTTP APIs. Identity resolution, multi-source consolidation, and field-level conflict scoring are handled by **OSI-Mapping**, a separate Rust-based system. The two communicate through PostgreSQL tables.

```text
External APIs → [in-and-out Ingestion] → PostgreSQL source tables
                                               ↓
                      [OSI-Mapping: identity resolution & consolidation]
                                               ↓
External APIs ← [in-and-out Writeback] ← desired-state tables
```

### Why two separate daemons?

Ingestion and writeback run as independent processes for three reasons:

1. **Independent scaling** — ingestion is I/O-bound (waiting on APIs); writeback is rate-limit-bound.
2. **Failure isolation** — a writeback error doesn't block ingestion.
3. **Loose coupling** — both daemons communicate only through PostgreSQL, never through direct calls.

### Why Python?

Python was chosen after evaluating Go, Rust, Java, TypeScript, and C#. The deciding factors were maturity of the config-interpretation ecosystem (Pydantic, PyYAML, JMESPath), developer velocity, and strong AI-assisted code generation quality. See the Programming Languages report for the full evaluation.

---

## Configuration

### Where do I put my config files?

There are two layers of configuration:

- **Tool config** — `config/ingestion.yaml` and `config/writeback.yaml` control database connections, health server, observability, and housekeeping.
- **Connector config** — one YAML file per external system in `connectors/`, describing base URL, auth, rate limits, datatypes, pagination, and webhook setup.

### Can I use environment variables in YAML?

Yes. Use `${ENV_VAR}` syntax anywhere in your YAML files. **Never put credentials directly in config files** — always reference environment variables or an external secrets manager.

### What sync modes are available?

| Mode | Description |
|------|-------------|
| **Full sync** | Fetches the entire dataset on each run. |
| **Incremental** | Uses a high-water mark (timestamp, offset, cursor, or sequence) to fetch only new/changed records. |
| **Webhook** | Receives real-time push events from the source system. |

You can combine modes — for example, webhooks for real-time updates with a periodic full sync as a safety net.

### What pagination styles are supported?

Cursor-based, offset-based, and link-header pagination — all configured declaratively in the connector YAML. No code required.

### What authentication methods are supported?

- OAuth2 with automatic token refresh
- API key (header or query parameter)
- JWT
- HTTP Basic
- Custom auth flows

All declared in the connector YAML under the `auth` section.

### Do I need to restart the daemon after changing a connector file?

No. Connector YAML files are watched for changes and hot-reloaded at the start of the next sync cycle.

---

## Database

### What database does in-and-out use?

PostgreSQL 15 or later. It uses JSONB columns for flexible payload storage, advisory locks for distributed concurrency, and logical replication for change detection.

### What tables does in-and-out create?

Per connector and datatype, you get:

| Table pattern | Purpose |
|---------------|---------|
| `inout_src_{connector}_{datatype}` | Ingested source data |
| `inout_src_{connector}_{datatype}_history` | Historical versions (if history tracking is enabled) |
| `inout_dst_{connector}_{datatype}` | Desired-state records for writeback |
| `inout_dst_{connector}_{datatype}_lwstate` | Last-written-state (audit trail for conflict detection) |

Plus shared operational tables: `inout_ops_sync_run`, `inout_ops_watermark`, `inout_ops_control`, `inout_ops_identity_map`, and dead-letter tables.

### How do I run database migrations?

```bash
inandout db upgrade          # apply all pending migrations
inandout db upgrade head     # same as above
inandout db downgrade -1     # roll back the most recent migration
inandout db status           # show current schema version
```

Migrations are Alembic-managed and designed to be additive — safe to apply while daemons are running.

### What is REPLICA IDENTITY FULL and why do I need it?

Writeback change detection uses PostgreSQL logical replication. For the replication stream to include old row values (needed for conflict detection), tables must have `REPLICA IDENTITY FULL` set. The migrations handle this for you, but if you create custom tables, you'll need to set it manually.

---

## Ingestion

### How does change detection work?

Each incoming record is SHA-256 hashed. The hash is compared to the stored hash — the database is only updated when the payload has actually changed. This reduces write amplification and makes sync runs idempotent.

### What happens when a record is deleted in the source system?

Records are **soft-deleted** — they're marked with `_deleted = true` and `_deleted_at` timestamp rather than being removed from the database. This preserves audit history and prevents data loss.

### What is the circuit breaker?

If a sync run returns zero results when it previously returned many, the circuit breaker halts the sync to prevent mass false deletions. This protects against API outages that return empty responses instead of errors.

### Can I resume a failed full sync?

Yes. Full syncs use intra-sync checkpointing — if a large sync fails partway through, it resumes from the last checkpoint rather than starting over.

### How does webhook deduplication work?

Each webhook event ID is tracked. If the same event arrives twice (common with at-least-once delivery), the duplicate is silently discarded.

---

## Writeback

### What is three-way conflict detection?

Before writing to an external API, the writeback daemon compares three states:

1. **Current state** — what the external system has right now (fetched via pre-flight read)
2. **Base state** — what the external system had when the MDM system made its decision
3. **Last-written state** — what in-and-out last successfully wrote

If someone changed the record externally after the MDM decision was made, that's a conflict.

### What conflict resolution strategies are available?

| Strategy | Behaviour |
|----------|-----------|
| `dead-letter` | Park the record for manual review (safest). |
| `last-writer-wins` | Overwrite regardless of conflict. |
| `skip-and-warn` | Skip the write, log a warning. |
| `re-ingest-and-recompute` | Re-fetch the record, send back through MDM for a new decision. |

### Why are pre-flight reads mandatory?

There's always a time gap between when the MDM system decides what to write and when the write actually happens. The pre-flight read bridges that gap by fetching the current state immediately before writing, enabling conflict detection.

### What are write-anomaly protection levels?

| Level | Mechanism | TOCTOU window |
|-------|-----------|---------------|
| Level 1 | Conditional writes (ETags, If-Match) | Zero |
| Level 2 | Pre-flight read + 3-way merge | Milliseconds |
| Level 3 | Levels 1 + 2 + verification read | Catches post-write anomalies |

Higher levels are safer but require more HTTP requests per write.

### What is a dead-letter queue?

Records that fail to write (due to conflicts, API errors, or validation failures) are saved to a dead-letter table for later review and replay. This prevents data loss and lets operators handle failures at their own pace.

### How does dependency ordering work?

When writing related records (e.g., creating a company before its contacts), the writeback daemon performs a topological sort to ensure parent records are written before children.

---

## Deployment

### How do I run in-and-out locally?

```bash
docker compose up -d               # start PostgreSQL
inandout db upgrade                 # run migrations
inandout ingest run --config config/ingestion.yaml    # start ingestion
inandout writeback run --config config/writeback.yaml # start writeback (separate terminal)
```

Or use the justfile shortcuts:

```bash
just up         # start infrastructure
just migrate    # run migrations
just ingest     # start ingestion daemon
just writeback  # start writeback daemon
```

### Can I run multiple instances?

Yes. The daemons are stateless — all state lives in PostgreSQL. Advisory locks prevent concurrent syncs from conflicting. You can safely run multiple instances behind a load balancer.

### How do I deploy to Kubernetes?

Kubernetes manifests are provided in the `k8s/` directory, including:

- Separate Deployments for ingestion and writeback
- ConfigMaps and Secrets for configuration
- HorizontalPodAutoscaler for scaling
- ServiceMonitor for Prometheus discovery
- Health probes (liveness and readiness)

Apply with: `kubectl apply -k k8s/`

### Are zero-downtime upgrades possible?

Yes. Migrations are designed to be additive (new columns, new tables) so they can be applied while daemons are running. Roll out new daemon versions after applying migrations.

---

## Observability

### What health endpoints are available?

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | Liveness probe — is the process alive? |
| `GET /ready` | Readiness probe — is it ready to accept work? |
| `GET /metrics` | Prometheus-format metrics. |

### What metrics are exposed?

Key Prometheus metrics include:

- `sync_run_duration_seconds` — how long each sync takes
- `records_ingested`, `records_errored`, `records_skipped` — record-level counters
- `high_water_mark_lag_seconds` — how far behind incremental sync is
- `circuit_breaker_state` — open/closed/half-open
- `writeback_conflict_count` — conflict occurrences
- `replication_slot_lag_bytes` — logical replication backlog

### How do I set up Grafana dashboards?

A `docker-compose.observability.yml` is included with pre-configured Prometheus, Grafana, and alerting. Dashboards are in `observability/grafana/dashboards/`.

```bash
docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d
```

### What logging format is used?

JSON structured logging via structlog. Every log line includes context fields: connector name, datatype, sync run ID, and correlation ID. Configure the log level in the tool config YAML.

---

## Connectors

### How do I add a new connector?

Create a YAML file in `connectors/` following the schema. No code is required. The file declares:

1. Connection details (base URL, timeouts)
2. Authentication scheme
3. Rate limits and retry policy
4. One or more datatypes with their endpoints, pagination, and field mappings

Validate with: `inandout ingest validate-connector --connector connectors/my-system.yaml`

Test with a dry run: `inandout ingest dry-run --connector connectors/my-system.yaml --datatype contacts`

### What generation profiles are available?

Profiles provide starting templates for common patterns:

| Profile | Use case |
|---------|----------|
| `ingestion_polling_readonly` | Read-only polling, no writeback. |
| `ingestion_webhook_incremental` | Webhooks with periodic full sync backup. |
| `writeback_patch` | Push changes only, no ingestion. |
| `full_duplex` | Bidirectional sync. |

### How do I handle API rate limits?

Rate limits are declared per connector in YAML. The daemon respects them automatically with exponential backoff and configurable retry counts. If the API returns a `Retry-After` header, that's honoured too.

### How are webhook signatures verified?

Webhook signature verification is configured declaratively per connector using HMAC schemes. The daemon validates incoming webhook payloads against the configured secret before processing.

---

## Troubleshooting

### My sync run returns no records but there should be data

Check the circuit breaker state in metrics (`circuit_breaker_state`). If it's open, a previous run detected an anomalous empty result. Investigate the source API, then reset the circuit breaker via the control table.

### Writeback keeps detecting conflicts

This usually means the source system is being modified outside of the MDM pipeline. Options:

1. Switch to `last-writer-wins` if external changes are acceptable.
2. Use `re-ingest-and-recompute` to let the MDM system re-decide.
3. Check the dead-letter queue for details on conflicting records.

### The replication slot is growing / disk is filling up

The PostgreSQL replication slot will retain WAL segments if the writeback daemon falls behind. Monitor `replication_slot_lag_bytes`. If the daemon is down, the slot keeps growing. Restart the daemon or, if recovery is impossible, drop and recreate the slot (you'll need to do a full table scan to catch up).

### How do I pause a connector without stopping the daemon?

Insert a control command into the `inout_ops_control` table:

```sql
INSERT INTO inout_ops_control (connector, command) VALUES ('hubspot', 'pause');
```

Resume with:

```sql
INSERT INTO inout_ops_control (connector, command) VALUES ('hubspot', 'resume');
```

### How do I reset a watermark to re-sync from scratch?

```sql
INSERT INTO inout_ops_control (connector, datatype, command) 
VALUES ('hubspot', 'contacts', 'reset_watermark');
```

The next sync cycle will perform a full sync for that datatype.

### How do I replay failed dead-letter records?

Use the CLI: `inandout webhook replay --connector hubspot --time-window 1h`

Or review and re-queue individual records from the dead-letter tables (`inout_dl_ingestion_*` and `inout_dl_writeback_*`).

---

## Testing

### How do I run the tests?

```bash
just test              # unit tests only
just test-all          # everything except acceptance & load
just test-integration  # integration tests (needs Docker for testcontainers)
just test-cov          # unit tests with coverage report
```

### Can I test without hitting real APIs?

Yes. The project includes HTTP simulators — configurable stubs that mimic API behaviour. These are used in integration tests and can be used for manual testing too. Find them under `tests/simulators/`.

### What kinds of tests exist?

| Type | Location | What it covers |
|------|----------|----------------|
| Unit | `tests/unit/` | Pure logic — pagination, conflict detection, hashing |
| Integration | `tests/integration/` | PostgreSQL interactions via testcontainers |
| Contract | `tests/contract/` | Schema validation and API contracts |
| Acceptance | `tests/acceptance/` | End-to-end with real APIs (requires credentials) |
| Load | `tests/load/` | Performance and throughput benchmarks |
