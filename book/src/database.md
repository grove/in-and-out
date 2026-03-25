# Database & Migrations Guide

This guide covers the database schema, migration lifecycle, and operational tables used by in-and-out.

## Database requirements

| Requirement | Details |
|---|---|
| PostgreSQL | Version 15 or 16 |
| User permissions | `CREATE TABLE`, `CREATE INDEX`, `SELECT`, `INSERT`, `UPDATE`, `DELETE` |
| Recommendation | Use a dedicated database and user for in-and-out |

## How migrations work

in-and-out uses [Alembic](https://alembic.sqlalchemy.org/) for schema migrations. Migration files are stored in `migrations/versions/` and are numbered and dated for clarity.

**Key rules:**

1. **Daemons never auto-migrate.** They check the schema version at startup and refuse to start if the database schema is behind.
2. **The operator must run migrations explicitly** before starting or restarting daemons.
3. Migration files are sequential — each depends on the previous one.

## Running migrations

### Apply all pending migrations

```bash
inandout db upgrade
```

Or targeting a specific revision:

```bash
inandout db upgrade abc123
```

### Check current status

```bash
inandout db status
```

Shows the current revision and whether any migrations are pending.

### View migration history

All migration revisions are stored as numbered files in `migrations/versions/`. You can inspect them directly, or check the Alembic version table in PostgreSQL:

```sql
SELECT * FROM alembic_version;
```

## Rolling back

### Roll back one step

```bash
inandout db downgrade -1
```

### Roll back to a specific revision

```bash
inandout db downgrade <revision-id>
```

> **Warning**: Some rollbacks are destructive — they may drop columns or tables. Always back up the database before downgrading.

## Schema version enforcement

At startup, each daemon validates that the database schema matches the expected version. If the schema is behind, the daemon logs an error and **refuses to start**.

The error message will tell you to run:

```bash
inandout db upgrade
```

This design prevents data corruption from running code against an incompatible schema.

## Production migration workflow

The recommended order for production upgrades:

1. **Back up the database**
2. **Run migrations**: `inandout db upgrade`
3. **Roll out the new daemon version** (ingestion first, then writeback)
4. **Verify** with `inandout db status` and health endpoint checks

### Zero-downtime notes

- **Additive migrations** (new tables, new columns with defaults) are safe to apply while daemons are running on the previous version.
- **Destructive migrations** (dropping columns, renaming tables) require: stop daemons → migrate → restart daemons.

## Database table reference

### Operational tables (`inout_ops_*`)

| Table | Purpose |
|---|---|
| `inout_ops_sync_run` | Log of every ingestion sync cycle |
| `inout_ops_watermark` | Per-connector/datatype incremental sync cursor |
| `inout_ops_control` | Runtime control commands from operators |
| `inout_ops_sync_lock` | Distributed lock (one sync per connector/datatype) |
| `inout_ops_sync_checkpoint` | Intra-sync page checkpoint for crash recovery |
| `inout_ops_identity_map` | Maps MDM cluster IDs to external system IDs |
| `inout_ops_writeback_result` | Audit log of every writeback operation |
| `inout_ops_connector_version` | Deployed connector version tracking |
| `inout_ops_connector_health` | Connector health status |
| `inout_ops_webhook_log` | Audit log of every received webhook event |
| `inout_ops_webhook_seen` | Webhook event deduplication |
| `inout_ops_webhook_subscriptions` | Webhook subscription lifecycle |
| `inout_ops_webhook_route_seq` | Per-route sequence tracking for fan-out |
| `inout_ops_federation` | Multi-instance coordination |
| `inout_ops_meta` | Metadata store (schema version, etc.) |

### Source tables (`inout_src_*`) — created at runtime

| Table pattern | Purpose |
|---|---|
| `inout_src_{connector}_{datatype}` | Current-state source records |
| `inout_src_{connector}_{datatype}_history` | Record version history (when `history_mode: append`) |
| `inout_src_{shared_table}` | Fan-in shared table (when multiple connectors write the same datatype) |

### Desired-state tables (`inout_dst_*`) — created at runtime

| Table pattern | Purpose |
|---|---|
| `inout_dst_{connector}_{datatype}` | Desired-state for writeback (populated by OSI-Mapping) |
| `inout_dst_{connector}_{datatype}_lwstate` | Last-written-state for three-way conflict detection |

### Dead-letter tables (`inout_dl_*`) — created at runtime

| Table pattern | Purpose |
|---|---|
| `inout_dl_ingestion_{connector}_{datatype}` | Records that failed ingestion after all retries |
| `inout_dl_writeback_{connector}_{datatype}` | Records that failed writeback or conflicted under `dead_letter` strategy |

## Key table schemas

### `inout_ops_sync_run`

The primary operational record of every sync cycle.

| Column | Type | Description |
|---|---|---|
| `id` | UUID (PK) | Unique run identifier |
| `connector` | TEXT | Connector name |
| `datatype` | TEXT | Datatype name |
| `mode` | TEXT | `full` or `incremental` |
| `status` | TEXT | `running`, `completed`, `failed`, `skipped`, `aborted` |
| `started_at` | TIMESTAMPTZ | When the sync started |
| `finished_at` | TIMESTAMPTZ | When the sync finished (NULL if running) |
| `records_fetched` | INT | Total records fetched from the API |
| `records_inserted` | INT | New records inserted |
| `records_updated` | INT | Existing records updated (hash changed) |
| `records_deleted` | INT | Records soft-deleted |
| `records_errored` | INT | Records sent to dead-letter |
| `records_written` | INT | Records written to source table |
| `records_skipped` | INT | Records skipped (unchanged hash) |
| `error_message` | TEXT | Error message if status=failed |
| `error_detail` | JSONB | Structured error context |
| `high_water_mark_before` | TEXT | Watermark at start of run |
| `high_water_mark_after` | TEXT | Watermark at end of run |

### `inout_ops_watermark`

Tracks the incremental sync cursor per connector/datatype.

| Column | Type | Description |
|---|---|---|
| `connector` | TEXT (PK) | Connector name |
| `datatype` | TEXT (PK) | Datatype name |
| `watermark_type` | TEXT | `timestamp`, `cursor`, `offset`, or `sequence` |
| `watermark_value` | TEXT | Current watermark value |
| `updated_at` | TIMESTAMPTZ | Last update time |
| `updated_by_run_id` | UUID (FK) | Sync run that set this watermark |

### `inout_ops_control`

Runtime operator commands picked up by running daemons.

| Column | Type | Description |
|---|---|---|
| `id` | UUID (PK) | Command identifier |
| `target_tool` | TEXT | `ingestion` or `writeback` |
| `connector` | TEXT | Target connector (NULL = all) |
| `datatype` | TEXT | Target datatype (NULL = all) |
| `command` | TEXT | Command name |
| `status` | TEXT | `pending`, `acknowledged`, `completed`, `failed` |
| `payload` | JSONB | Command parameters |
| `issued_at` | TIMESTAMPTZ | When the command was issued |
| `issued_by` | TEXT | Operator identifier (for audit) |
| `acknowledged_at` | TIMESTAMPTZ | When the daemon picked up the command |
| `completed_at` | TIMESTAMPTZ | When the command completed |
| `result` | JSONB | Command result |

### `inout_ops_identity_map`

Maps MDM cluster IDs to external system IDs after writeback inserts.

| Column | Type | Description |
|---|---|---|
| `connector` | TEXT (PK) | Connector name |
| `datatype` | TEXT (PK) | Datatype name |
| `external_id` | TEXT (PK) | Source system record ID |
| `internal_id` | TEXT | Target system ID (from insert response) |
| `cluster_id` | TEXT | OSI-Mapping cluster identifier |
| `target_external_id` | TEXT | Reverse lookup key |
| `created_at` | TIMESTAMPTZ | When the mapping was created |
| `updated_at` | TIMESTAMPTZ | Last update time |

### `inout_ops_sync_lock`

Distributed locking — ensures only one daemon instance processes a given connector/datatype at a time.

| Column | Type | Description |
|---|---|---|
| `connector` | TEXT (PK) | Connector name |
| `datatype` | TEXT (PK) | Datatype name |
| `locked_until` | TIMESTAMPTZ | Lock expiry time |
| `locked_by` | TEXT | Instance identifier holding the lock |

### `inout_ops_sync_checkpoint`

Enables crash recovery by recording progress within a sync run.

| Column | Type | Description |
|---|---|---|
| `run_id` | UUID (PK, FK) | References `inout_ops_sync_run.id` (CASCADE on delete) |
| `connector` | TEXT | Connector name |
| `datatype` | TEXT | Datatype name |
| `page_number` | INT | Last committed page number |
| `cursor_value` | TEXT | Cursor/offset at checkpoint |
| `records_committed` | INT | Records committed up to this point |
| `checkpointed_at` | TIMESTAMPTZ | When the checkpoint was saved |

### Source table columns (`inout_src_*`)

Every dynamically-created source table includes these columns:

| Column | Type | Description |
|---|---|---|
| `external_id` | TEXT (PK) | Source system primary key |
| `data` | JSONB | Processed payload (after field mappings) |
| `raw` | JSONB | Unmodified API response |
| `_ingested_at` | TIMESTAMPTZ | Ingestion timestamp |
| `_sync_run_id` | UUID | FK to `inout_ops_sync_run.id` |
| `_raw_hash` | TEXT | Hash for change detection (skip updates when unchanged) |
| `_deleted` | BOOLEAN | Soft-delete flag |
| `_deleted_at` | TIMESTAMPTZ | When soft-deletion was recorded |
| `_schema_version` | INT | Detected API schema version |
| `_source_version` | TEXT | ETag/version from the source API |
| `_last_written` | JSONB | Last writeback payload (for conflict detection) |
| `_lineage` | JSONB | Provenance metadata (run_id, api_path, page_number) |
| `_connector` | TEXT | **Fan-in only** — discriminator for shared tables |

### Dead-letter table columns (`inout_dl_ingestion_*`)

| Column | Type | Description |
|---|---|---|
| `id` | BIGSERIAL (PK) | Row identifier |
| `external_id` | TEXT | Source record ID (if available) |
| `raw` | JSONB | Raw payload that failed |
| `error_message` | TEXT | Error description |
| `error_class` | TEXT | Error classification |
| `failed_at` | TIMESTAMPTZ | When the failure occurred |
| `sync_run_id` | UUID | Originating sync run |
| `requeued_at` | TIMESTAMPTZ | When the entry was replayed (if replayed) |
| `requeue_count` | INT | Number of replay attempts |

## Useful operational queries

### Recent sync runs

```sql
SELECT connector, datatype, status, records_fetched,
       records_inserted, records_updated,
       started_at, finished_at,
       finished_at - started_at AS duration
FROM inout_ops_sync_run
ORDER BY started_at DESC
LIMIT 20;
```

### Failed sync runs

```sql
SELECT connector, datatype, error_message, error_detail, started_at
FROM inout_ops_sync_run
WHERE status = 'failed'
ORDER BY started_at DESC
LIMIT 10;
```

### Current watermarks

```sql
SELECT connector, datatype, watermark_type, watermark_value, updated_at
FROM inout_ops_watermark
ORDER BY connector, datatype;
```

### Pending control commands

```sql
SELECT id, connector, datatype, command, issued_by, issued_at
FROM inout_ops_control
WHERE status = 'pending'
ORDER BY issued_at DESC;
```

### Dead-letter queue depth

```sql
-- Check ingestion dead-letter tables
SELECT schemaname, tablename,
       (SELECT count(*) FROM pg_class c
        WHERE c.relname = tablename) AS row_count
FROM pg_tables
WHERE tablename LIKE 'inout_dl_%'
ORDER BY tablename;
```
