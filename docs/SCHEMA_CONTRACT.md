# PostgreSQL Schema Contract

This document defines the PostgreSQL schema contract between the three components
of the in-and-out / OSI-Mapping architecture:

1. **In-and-Out Ingestion** — writes to `inout_src_*` tables
2. **OSI-Mapping + pg-trickle** — reads `inout_src_*`, writes to `_delta_*` stream tables
3. **In-and-Out Writeback** — reads `_delta_*`, writes to `inout_dst_*` and `inout_ops_*`

---

## 1. Ingestion Output Tables (`inout_src_*`)

### Naming

```
inout_src_{connector}_{datatype}
```

### Columns

| Column           | Type         | Nullable | Default     | Description                                      |
|------------------|--------------|----------|-------------|--------------------------------------------------|
| `external_id`    | TEXT         | NOT NULL | —           | Source system primary key                        |
| `data`           | JSONB        | NOT NULL | —           | Processed payload (after field mappings)         |
| `raw`            | JSONB        | NOT NULL | —           | Unmodified copy of source response               |
| `_ingested_at`   | TIMESTAMPTZ  | NOT NULL | `NOW()`     | Timestamp when record was ingested               |
| `_sync_run_id`   | UUID         | NULL     | —           | FK to `inout_ops_sync_run.id`                    |
| `_raw_hash`      | TEXT         | NOT NULL | —           | SHA-256 of `raw` (for change detection)          |
| `_deleted`       | BOOLEAN      | NOT NULL | `FALSE`     | Soft-delete tombstone flag                       |
| `_deleted_at`    | TIMESTAMPTZ  | NULL     | —           | Timestamp when record was soft-deleted           |
| `_schema_version`| INTEGER      | NOT NULL | `1`         | Detected schema version (increments on schema change) |
| `_source_version`| TEXT         | NULL     | —           | Source system version/etag when available        |
| `_last_written`  | JSONB        | NULL     | —           | Last payload successfully written back to source |
| `_lineage`       | JSONB        | NULL     | —           | Provenance: run_id, api_path, page_number        |

### Primary Key

`PRIMARY KEY (external_id)`

### Indexes

- `_ingested_at` index for housekeeping/time-range queries

### Constraints

- `external_id` is NOT NULL
- Upsert semantics: `ON CONFLICT (external_id) DO UPDATE` — hash-gated

---

## 2. Writeback Desired-State Tables (`inout_dst_*`)

OSI-Mapping populates these via `_delta_*` stream tables. Writeback reads from them
when `use_desired_state_table = true`.

### Naming

```
inout_dst_{connector}_{datatype}
```

### Columns

| Column           | Type         | Nullable | Default     | Description                              |
|------------------|--------------|----------|-------------|------------------------------------------|
| `id`             | UUID         | NOT NULL | `gen_random_uuid()` | Row PK                         |
| `action`         | TEXT         | NOT NULL | —           | `insert` / `update` / `delete` / `noop` |
| `cluster_id`     | TEXT         | NULL     | —           | OSI-Mapping cluster identifier           |
| `external_id`    | TEXT         | NULL     | —           | Source system external ID                |
| `data`           | JSONB        | NOT NULL | —           | Full desired-state payload               |
| `base`           | JSONB        | NULL     | —           | Base state for three-way merge           |
| `base_version`   | TEXT         | NULL     | —           | Version/etag at time of base snapshot    |
| `_status`        | TEXT         | NOT NULL | `pending`   | `pending` / `processing` / `done`        |
| `_processed_at`  | TIMESTAMPTZ  | NULL     | —           | Timestamp when writeback processed row   |
| `created_at`     | TIMESTAMPTZ  | NOT NULL | `NOW()`     | Row creation timestamp                   |

### Replication Identity

```sql
ALTER TABLE inout_dst_{connector}_{datatype} REPLICA IDENTITY FULL;
```

Required for logical replication-based change detection (writeback daemon).

---

## 3. Last-Written-State Tables (`inout_dst_*_lwstate`)

The writeback daemon maintains these to support three-way conflict detection.

### Naming

```
inout_dst_{connector}_{datatype}_lwstate
```

### Columns

| Column             | Type         | Nullable | Description                                        |
|--------------------|--------------|----------|----------------------------------------------------|
| `external_id`      | TEXT         | NOT NULL | Source system external ID                          |
| `connector`        | TEXT         | NOT NULL | Connector name                                     |
| `datatype`         | TEXT         | NOT NULL | Datatype name                                      |
| `written_state`    | JSONB        | NOT NULL | Last payload successfully written                  |
| `written_etag`     | TEXT         | NULL     | ETag returned by server after last write           |
| `written_at`       | TIMESTAMPTZ  | NOT NULL | Timestamp of last successful write                 |
| `written_by_run_id`| UUID         | NULL     | FK to `inout_ops_sync_run.id`                      |

### Primary Key

`PRIMARY KEY (external_id, connector, datatype)`

### Replication Identity

```sql
ALTER TABLE inout_dst_{connector}_{datatype}_lwstate REPLICA IDENTITY FULL;
```

---

## 4. Identity Map Table (`inout_ops_identity_map`)

Tracks the mapping between in-and-out `external_id` values and the internal IDs
assigned by target systems after successful writeback inserts.

### Columns

| Column        | Type         | Nullable | Description                               |
|---------------|--------------|----------|-------------------------------------------|
| `id`          | BIGSERIAL    | NOT NULL | Row PK                                    |
| `cluster_id`  | TEXT         | NOT NULL | OSI-Mapping cluster ID                    |
| `connector`   | TEXT         | NOT NULL | Source connector name                     |
| `datatype`    | TEXT         | NOT NULL | Datatype name                             |
| `external_id` | TEXT         | NOT NULL | Source system ID                          |
| `internal_id` | TEXT         | NULL     | Target system ID (from insert response)   |
| `created_at`  | TIMESTAMPTZ  | NOT NULL | `NOW()`                                   |
| `updated_at`  | TIMESTAMPTZ  | NOT NULL | `NOW()`                                   |

### Unique Constraint

```sql
UNIQUE (cluster_id, connector, datatype)
```

### Index

```sql
INDEX ON inout_ops_identity_map (connector, datatype, external_id)
```

---

## 5. Operational Tables (`inout_ops_*`)

### `inout_ops_sync_run` — Sync run log

| Column                 | Type         | Description                              |
|------------------------|--------------|------------------------------------------|
| `id`                   | UUID PK      | Run identifier                           |
| `connector`            | TEXT         | Connector name                           |
| `datatype`             | TEXT         | Datatype name                            |
| `mode`                 | TEXT         | `full` or `incremental`                  |
| `status`               | TEXT         | `running` / `completed` / `failed` / `skipped` |
| `started_at`           | TIMESTAMPTZ  | When run started                         |
| `finished_at`          | TIMESTAMPTZ  | When run finished (NULL if running)      |
| `records_fetched`      | INT          | Total records fetched                    |
| `records_inserted`     | INT          | Records inserted (new)                   |
| `records_updated`      | INT          | Records updated (changed hash)           |
| `records_deleted`      | INT          | Records soft-deleted                     |
| `records_errored`      | INT          | Records sent to dead-letter              |
| `error_message`        | TEXT         | Error message if status=failed           |
| `high_water_mark_before`| TEXT        | Watermark value at start of run          |
| `high_water_mark_after` | TEXT        | Watermark value at end of run            |

### `inout_ops_watermark` — Incremental sync watermarks

| Column              | Type         | Description                              |
|---------------------|--------------|------------------------------------------|
| `connector`         | TEXT PK      | Connector name                           |
| `datatype`          | TEXT PK      | Datatype name                            |
| `watermark_type`    | TEXT         | `timestamp` / `cursor` / `offset` / `sequence` |
| `watermark_value`   | TEXT         | Watermark value (string form)            |
| `updated_at`        | TIMESTAMPTZ  | When watermark was last updated          |
| `updated_by_run_id` | UUID         | FK to sync_run.id                        |

### `inout_ops_control` — Runtime control commands

| Column           | Type         | Description                              |
|------------------|--------------|------------------------------------------|
| `id`             | UUID PK      | Command identifier                       |
| `connector`      | TEXT         | Target connector (NULL = all)            |
| `datatype`       | TEXT         | Target datatype (NULL = all)             |
| `command`        | TEXT         | Command name                             |
| `status`         | TEXT         | `pending` / `acknowledged` / `completed` / `failed` |
| `payload`        | JSONB        | Command parameters                       |
| `issued_at`      | TIMESTAMPTZ  | When command was issued                  |
| `issued_by`      | TEXT         | Who issued the command (operator ID)     |
| `acknowledged_at`| TIMESTAMPTZ  | When daemon picked up command            |
| `completed_at`   | TIMESTAMPTZ  | When command completed                   |
| `result`         | JSONB        | Command result                           |

### `inout_ops_sync_checkpoint` — Intra-sync checkpoints

| Column              | Type         | Description                              |
|---------------------|--------------|------------------------------------------|
| `run_id`            | UUID PK      | FK to sync_run.id                        |
| `connector`         | TEXT         | Connector name                           |
| `datatype`          | TEXT         | Datatype name                            |
| `page_number`       | INT          | Last committed page number               |
| `cursor_value`      | TEXT         | Cursor/watermark at checkpoint           |
| `records_committed` | INT          | Records committed up to this checkpoint  |
| `checkpointed_at`   | TIMESTAMPTZ  | When checkpoint was saved                |

---

## 6. Table Naming Conventions

| Pattern                              | Used for                            |
|--------------------------------------|-------------------------------------|
| `inout_src_{connector}_{datatype}`   | Ingestion source tables             |
| `inout_dst_{connector}_{datatype}`   | Writeback desired-state tables      |
| `inout_dst_{connector}_{datatype}_lwstate` | Writeback last-written-state  |
| `inout_dl_ingestion_{connector}_{datatype}` | Ingestion dead-letter queue  |
| `inout_dl_writeback_{connector}_{datatype}` | Writeback dead-letter queue  |
| `inout_ops_sync_run`                 | Sync run log (all connectors)       |
| `inout_ops_watermark`                | Watermarks (all connectors)         |
| `inout_ops_control`                  | Runtime control commands            |
| `inout_ops_sync_checkpoint`          | Intra-sync page checkpoints         |
| `inout_ops_identity_map`             | External-to-internal ID mapping     |
| `inout_ops_connector_version`        | Connector version tracking          |
| `inout_ops_sync_lock`                | Distributed sync lock (row-level)   |
| `_delta_{connector}_{datatype}`      | OSI-Mapping output stream tables    |

All operational tables use the `inout_ops_` prefix. Source/destination tables use
`inout_src_` / `inout_dst_` prefixes. Dead-letter tables use `inout_dl_`.
OSI-Mapping stream tables use `_delta_` prefix (OSI convention).

---

## 7. Migration Coordination

**in-and-out never auto-migrates.**

Schema migrations are managed via Alembic. The engine checks `SCHEMA_VERSION`
at startup:

```python
# src/inandout/postgres/schema.py
SCHEMA_VERSION: int = 18  # increment when adding migrations
```

On startup, the daemon validates that the database schema is at the expected version.
If not, it **refuses to start** and logs an error directing the operator to run:

```bash
alembic upgrade head
```

Hot-reloading connector configs that change table schemas
also requires a migration. The daemon will log a warning and skip the config reload
for schema-affecting changes until migrations are applied.

### Running migrations

```bash
# Apply all pending migrations
uv run alembic upgrade head

# Check current migration state
uv run alembic current

# Generate a new migration
uv run alembic revision --autogenerate -m "add_my_new_column"
```
