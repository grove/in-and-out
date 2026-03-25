# Integration Contract: in-and-out & OSI-Mapping

This document defines the contract between **in-and-out** and **OSI-Mapping** — the two systems that together form the bidirectional MDM data pipeline. The contract is expressed entirely through PostgreSQL table schemas. There are no APIs, no message queues, and no shared code between the two systems.

## Architectural context

in-and-out handles the I/O layer: pulling data from external HTTP APIs into PostgreSQL and pushing desired-state changes back out. OSI-Mapping handles the consolidation layer: identity resolution, conflict resolution, and delta detection.

The two systems communicate exclusively through PostgreSQL tables:

```
External APIs
    │
    ▼
┌─────────────────────┐
│  in-and-out          │
│  Ingestion Daemon    │
└────────┬────────────┘
         │ writes
         ▼
┌─────────────────────┐
│  inout_src_* tables  │ ◄── CONTRACT BOUNDARY (in-and-out → OSI-Mapping)
└────────┬────────────┘
         │ reads
         ▼
┌─────────────────────┐
│  OSI-Mapping Engine  │
│  (Rust + PostgreSQL  │
│   view pipeline)     │
└────────┬────────────┘
         │ writes
         ▼
┌─────────────────────┐
│  _delta_* views      │ ◄── CONTRACT BOUNDARY (OSI-Mapping → in-and-out)
│  inout_dst_* tables  │
└────────┬────────────┘
         │ reads
         ▼
┌─────────────────────┐
│  in-and-out          │
│  Writeback Daemon    │
└─────────────────────┘
         │
         ▼
    External APIs
```

**Key property**: neither system imports code or calls functions from the other. They are deployed, versioned, and scaled independently. PostgreSQL is the integration bus.

---

## Contract boundary 1: Ingestion output → OSI-Mapping input

in-and-out's ingestion daemon writes source tables that OSI-Mapping reads. This is the **inbound contract**.

### Table naming

```
inout_src_{connector}_{datatype}
```

### Required columns

Every source table **must** contain the following columns. OSI-Mapping's forward views depend on them.

| Column | Type | Nullable | Default | Purpose |
|---|---|---|---|---|
| `external_id` | TEXT | NOT NULL | — | Source system primary key. The unique identifier for this record in the external system. |
| `data` | JSONB | NOT NULL | — | Processed payload after field mappings. This is what OSI-Mapping reads for consolidation. |
| `raw` | JSONB | NOT NULL | — | Unmodified copy of the API response. Preserved for auditing and reprocessing. |
| `_ingested_at` | TIMESTAMPTZ | NOT NULL | `NOW()` | When the record was ingested. Used for housekeeping queries and incremental view refresh. |
| `_sync_run_id` | UUID | NULL | — | FK to `inout_ops_sync_run.id`. Links the record to the sync run that produced it. |
| `_raw_hash` | TEXT | NOT NULL | — | SHA-256 hash of `raw`. Used for change detection — ingestion skips upserts when the hash is unchanged. |
| `_deleted` | BOOLEAN | NOT NULL | `FALSE` | Soft-delete tombstone. When `TRUE`, OSI-Mapping treats the record as absent during resolution. |
| `_deleted_at` | TIMESTAMPTZ | NULL | — | Timestamp of soft-deletion. |
| `_schema_version` | INTEGER | NOT NULL | `1` | Detected schema version. Increments when in-and-out detects a structural change in the API response. |
| `_source_version` | TEXT | NULL | — | ETag or version identifier from the source API, when available. Used for conditional writes during writeback. |
| `_last_written` | JSONB | NULL | — | Last payload successfully written back to the source system. OSI-Mapping uses this to suppress noop echoes. |
| `_lineage` | JSONB | NULL | — | Provenance metadata: `run_id`, `api_path`, `page_number`. For debugging and audit trails. |

### Primary key

`PRIMARY KEY (external_id)`

### Upsert semantics

Ingestion uses hash-gated upserts:

```sql
INSERT INTO inout_src_{connector}_{datatype} (external_id, data, raw, _raw_hash, ...)
VALUES ($1, $2, $3, $4, ...)
ON CONFLICT (external_id) DO UPDATE
SET data = EXCLUDED.data,
    raw = EXCLUDED.raw,
    _raw_hash = EXCLUDED._raw_hash,
    _ingested_at = NOW(),
    _sync_run_id = EXCLUDED._sync_run_id
WHERE inout_src_{connector}_{datatype}._raw_hash != EXCLUDED._raw_hash;
```

This means OSI-Mapping can trust that:
- A record with a given `external_id` always reflects the latest known state.
- `_ingested_at` only advances when the record actually changed.
- Unchanged records are not touched (no spurious timestamp updates).

### What OSI-Mapping reads from these tables

OSI-Mapping's YAML configuration declares source tables that must match this schema:

```yaml
sources:
  crm:
    table: inout_src_hubspot_contacts
    primary_key: external_id
  erp:
    table: inout_src_sap_customers
    primary_key: external_id
```

The engine's **forward views** (`_fwd_{mapping}`) project these columns into a normalised target shape, carrying `_last_written` as `_base` for delta comparison downstream.

---

## Contract boundary 2: OSI-Mapping output → Writeback input

OSI-Mapping produces PostgreSQL views (or materialised via pg-trickle) that in-and-out's writeback daemon consumes. This is the **outbound contract**.

### The OSI-Mapping view pipeline

OSI-Mapping generates six views per mapping. in-and-out interacts with the last two:

| Stage | View | Owner | in-and-out reads? |
|---|---|---|---|
| 1. Forward | `_fwd_{mapping}` | OSI-Mapping | No |
| 2. Identity | `_id_{target}` | OSI-Mapping | No |
| 3. Resolution | `_resolved_{target}` | OSI-Mapping | No (but useful for debugging) |
| 4. Analytics | `{target}` | OSI-Mapping | No (consumed by BI/apps) |
| 5. Reverse | `_rev_{mapping}` | OSI-Mapping | No |
| 6. Delta | `_delta_{mapping}` | OSI-Mapping | **Yes — this is the writeback input** |

### Delta view columns

The `_delta_{mapping}` view (or its materialised equivalent) must expose the following columns for in-and-out's writeback daemon to consume:

| Column | Type | Nullable | Description |
|---|---|---|---|
| `action` | TEXT | NOT NULL | One of `insert`, `update`, `delete`, or `noop`. Determines the HTTP method the writeback daemon issues. |
| `cluster_id` | TEXT | NULL | OSI-Mapping's transitive-closure cluster identifier. Links records that represent the same real-world entity across systems. |
| `external_id` | TEXT | NULL | The source system's primary key. Used to construct the API URL for updates and deletes. NULL for inserts into a system that has no prior record. |
| `data` | JSONB | NOT NULL | The full desired-state payload. For updates, this is the resolved value after conflict resolution. For inserts, this is the new record. |
| `base` | JSONB | NULL | The original source-system snapshot at the time of the last ingestion. Used as the "base" in three-way merge conflict detection during writeback. |

### Desired-state tables (`inout_dst_*`)

In production, delta views are typically materialised into tables (via pg-trickle or a bridge layer) so the writeback daemon can track processing state. These tables add operational columns:

```
inout_dst_{connector}_{datatype}
```

| Column | Type | Nullable | Default | Description |
|---|---|---|---|---|
| `id` | UUID | NOT NULL | `gen_random_uuid()` | Row primary key. |
| `action` | TEXT | NOT NULL | — | `insert` / `update` / `delete` / `noop` |
| `cluster_id` | TEXT | NULL | — | OSI-Mapping cluster identifier. |
| `external_id` | TEXT | NULL | — | Source system external ID. |
| `data` | JSONB | NOT NULL | — | Full desired-state payload. |
| `base` | JSONB | NULL | — | Base state for three-way merge. |
| `_status` | TEXT | NOT NULL | `pending` | Processing state: `pending` → `processing` → `done`. |
| `_processed_at` | TIMESTAMPTZ | NULL | — | When writeback processed this row. |
| `created_at` | TIMESTAMPTZ | NOT NULL | `NOW()` | Row creation time. |

These tables require `REPLICA IDENTITY FULL` for logical-replication-based change detection:

```sql
ALTER TABLE inout_dst_{connector}_{datatype} REPLICA IDENTITY FULL;
```

### How writeback consumes these tables

The writeback daemon polls `inout_dst_*` for rows where `_status = 'pending'`:

1. Claims the row by setting `_status = 'processing'`.
2. Issues a pre-flight `GET` to the external API to read the current state.
3. Runs a **three-way comparison**: `base` (what OSI-Mapping saw) vs. `current` (what the API returns now) vs. `data` (the desired state).
4. If no conflict, issues the write (`POST`, `PATCH`, `PUT`, or `DELETE` depending on `action`).
5. Updates `_status = 'done'` and records the result in `inout_ops_writeback_result`.
6. On insert, captures the generated ID in `inout_ops_identity_map`.

---

## Contract boundary 3: Writeback feedback → Ingestion

After a successful writeback, in-and-out records what was written so that the *next* ingestion cycle can detect noop echoes. This closes the bidirectional loop.

### Last-written-state tables

```
inout_dst_{connector}_{datatype}_lwstate
```

| Column | Type | Nullable | Description |
|---|---|---|---|
| `external_id` | TEXT | NOT NULL | Source system external ID. |
| `connector` | TEXT | NOT NULL | Connector name. |
| `datatype` | TEXT | NOT NULL | Datatype name. |
| `written_state` | JSONB | NOT NULL | Last payload successfully written to the API. |
| `written_etag` | TEXT | NULL | ETag returned by the API after the write. |
| `written_at` | TIMESTAMPTZ | NOT NULL | Timestamp of the last successful write. |
| `written_by_run_id` | UUID | NULL | FK to `inout_ops_sync_run.id`. |

Primary key: `(external_id, connector, datatype)`.

### `_last_written` column on source tables

During ingestion, the `_last_written` column on `inout_src_*` is populated from the lwstate table. OSI-Mapping reads this as `_base` in its forward views. This enables **noop suppression**: if the resolved value equals `_base`, the delta view emits `noop` instead of `update`, preventing round-trip echoes.

### Identity map table

```
inout_ops_identity_map
```

| Column | Type | Description |
|---|---|---|
| `cluster_id` | TEXT | OSI-Mapping cluster ID |
| `connector` | TEXT | Connector name |
| `datatype` | TEXT | Datatype name |
| `external_id` | TEXT | Source system ID |
| `internal_id` | TEXT | Target system ID (from insert API response) |

Unique constraint: `(cluster_id, connector, datatype)`.

This table is written by in-and-out's writeback daemon and can be read by OSI-Mapping or the bridge layer to resolve cross-system foreign keys.

---

## Ownership summary

| Table prefix | Owner (creates & writes) | Consumer (reads) |
|---|---|---|
| `inout_src_*` | in-and-out ingestion | OSI-Mapping forward views |
| `_fwd_*`, `_id_*`, `_resolved_*`, `_rev_*` | OSI-Mapping | Internal to OSI-Mapping |
| `_delta_*` | OSI-Mapping | Bridge layer / in-and-out writeback |
| `inout_dst_*` | Bridge layer (from OSI-Mapping output) | in-and-out writeback |
| `inout_dst_*_lwstate` | in-and-out writeback | in-and-out ingestion (for `_last_written`) |
| `inout_ops_identity_map` | in-and-out writeback | OSI-Mapping / bridge layer |
| `inout_ops_*` (other) | in-and-out | in-and-out |
| `inout_dl_*` | in-and-out | Operators |

---

## Invariants both systems must uphold

### in-and-out guarantees

1. **Exactly-once upsert per external_id.** Source tables never contain duplicate rows for the same `external_id`.
2. **Hash-gated updates.** Records are only updated when `_raw_hash` changes. OSI-Mapping can rely on `_ingested_at` as a meaningful change indicator.
3. **Soft-delete semantics.** Deleted records are never physically removed during normal operation. They are marked with `_deleted = TRUE` so OSI-Mapping can detect disappearances.
4. **Schema version tracking.** When the source API's response structure changes, `_schema_version` increments. OSI-Mapping can use this to handle schema evolution.
5. **Writeback feedback.** After a successful write, `_last_written` is populated on the source table. This enables noop suppression in OSI-Mapping's delta views.

### OSI-Mapping guarantees

1. **Deterministic resolution.** Given the same source data, the view pipeline always produces the same output. No hidden state, no randomness.
2. **Action classification.** Every row in the delta view has a well-defined `action` value (`insert`, `update`, `delete`, or `noop`). The writeback daemon does not need to compute the action itself.
3. **Base preservation.** The `base` column faithfully reflects the source system's original state at the time of ingestion. This is critical for three-way merge conflict detection.
4. **Noop suppression.** When the resolved value matches `_base`, the delta view emits `noop`. This prevents round-trip echoes that would cause infinite update loops.
5. **Cluster stability.** A `cluster_id` identifies the same real-world entity across runs. It only changes when the underlying identity rules change or new matching evidence appears.

---

## Versioning and schema evolution

### Adding columns to source tables

in-and-out may add new columns to `inout_src_*` tables via Alembic migrations. OSI-Mapping's forward views `SELECT` specific columns, so additive changes are backwards-compatible. Coordinate by:

1. in-and-out releases a migration adding the column.
2. Operator runs `inandout db upgrade`.
3. OSI-Mapping YAML is updated to reference the new column (if desired).
4. OSI-Mapping's `render` command regenerates the SQL views.

### Adding columns to delta views

OSI-Mapping may add new columns to `_delta_*` output. in-and-out's writeback daemon reads specific columns, so additive changes are backwards-compatible. No coordination needed.

### Breaking changes

Any change that removes or renames a column on either side of the contract boundary is a **breaking change** and requires coordinated deployment:

1. Deploy the new version of the producer (in-and-out or OSI-Mapping) with the column present but deprecated.
2. Update the consumer to stop reading the old column.
3. Remove the deprecated column in a subsequent release.

---

## Example: end-to-end data flow

To make the contract concrete, here is a complete example with HubSpot (CRM) and SAP (ERP) contacts being consolidated into a unified contact entity.

### 1. Ingestion writes source tables

```sql
-- in-and-out ingests from HubSpot
INSERT INTO inout_src_hubspot_contacts (external_id, data, raw, _raw_hash, ...)
VALUES ('100',
        '{"email": "alice@example.com", "name": "Alice"}'::jsonb,
        '{"id": 100, "properties": {"email": "alice@example.com", ...}}'::jsonb,
        'sha256:abc123...',
        ...);

-- in-and-out ingests from SAP
INSERT INTO inout_src_sap_customers (external_id, data, raw, _raw_hash, ...)
VALUES ('CUST-001',
        '{"email": "alice@example.com", "name": "Alice Smith"}'::jsonb,
        '{"customer_id": "CUST-001", "contact_email": "alice@example.com", ...}'::jsonb,
        'sha256:def456...',
        ...);
```

### 2. OSI-Mapping resolves identity and conflicts

OSI-Mapping's YAML declares both sources map to a `contact` target, with `email` as an identity field and `name` using `coalesce` strategy (CRM wins by priority):

```yaml
sources:
  crm:
    table: inout_src_hubspot_contacts
    primary_key: external_id
  erp:
    table: inout_src_sap_customers
    primary_key: external_id

targets:
  contact:
    fields:
      email: identity
      name: coalesce

mappings:
  - name: hubspot_contacts
    source: crm
    target: contact
    priority: 1
    fields:
      - { source: email, target: email }
      - { source: name, target: name }

  - name: sap_customers
    source: erp
    target: contact
    priority: 2
    fields:
      - { source: email, target: email }
      - { source: name, target: name }
```

The generated views produce:
- `_id_contact`: both rows get the same `_cluster_id` (matched on email).
- `_resolved_contact`: `name = "Alice"` (CRM wins by priority), `email = "alice@example.com"`.
- `_delta_hubspot_contacts`: `action = "noop"` (CRM already has the resolved value).
- `_delta_sap_customers`: `action = "update"`, `data = {"name": "Alice", "email": "alice@example.com"}`, `base = {"name": "Alice Smith", ...}`.

### 3. Writeback reads the delta and writes to SAP

The writeback daemon picks up the `update` row from `inout_dst_sap_customers`:

1. Pre-flight: `GET /api/customer/CUST-001` returns `{"name": "Alice Smith"}`.
2. Three-way merge: `base.name = "Alice Smith"` matches current — no external conflict.
3. Issues: `PATCH /api/customer/CUST-001` with `{"name": "Alice"}`.
4. Records in `inout_dst_sap_customers_lwstate`: `written_state = {"name": "Alice", ...}`.

### 4. Next ingestion cycle closes the loop

SAP now returns `{"name": "Alice"}`. Ingestion updates `inout_src_sap_customers`. OSI-Mapping's delta view now sees `_base.name = "Alice"` matches resolved `name = "Alice"` → `action = "noop"`. No further writeback.

---

## Migration coordination

in-and-out **never auto-migrates**. Schema changes are managed via Alembic. On startup, each daemon checks the schema version and refuses to start if migrations are pending.

OSI-Mapping **never modifies in-and-out tables**. It only reads `inout_src_*` and creates its own views. View regeneration (via `osi-mapping render`) is idempotent and can be run at any time.

The recommended deployment order for schema changes:

1. Back up the database.
2. Run `inandout db upgrade` to apply in-and-out migrations.
3. Run `osi-mapping render` to regenerate views against the updated schema.
4. Restart daemons.
