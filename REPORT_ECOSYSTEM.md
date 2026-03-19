# Ecosystem Research Report: Existing Technology Landscape

This report surveys the existing tools and platforms that address similar problems to those described in [GOAL.md](GOAL.md) — the "In-and-Out" declarative MDM synchronization tools. Each section covers a tool's architecture and capabilities, and concludes with lessons we can apply to strengthen our own ingestion and writeback engines.

---

## Table of Contents

1. [Airbyte](#1-airbyte)
2. [Singer / Meltano](#2-singer--meltano)
3. [dlt (data load tool)](#3-dlt-data-load-tool)
4. [Fivetran](#4-fivetran)
5. [Hightouch](#5-hightouch)
6. [Polytomic](#6-polytomic)
7. [RudderStack](#7-rudderstack)
8. [Debezium](#8-debezium)
9. [Summary Matrix](#9-summary-matrix)
10. [Cross-Cutting Lessons for In-and-Out](#10-cross-cutting-lessons-for-in-and-out)

---

## 1. Airbyte

### Overview

Airbyte is an open-source ELT platform with 300+ connectors that moves data from sources to destinations. Its architecture is built around a well-defined **Airbyte Protocol** with strongly typed message envelopes.

### Architecture & Key Concepts

- **Actor Model:** Every connector is either a *Source* (extracts data) or a *Destination* (loads data). They communicate through a stream of `AirbyteMessage` envelopes on stdout/stdin.
- **Message Types:** `RECORD`, `STATE`, `SCHEMA`, `LOG`, `TRACE`, and `CONTROL`. Each message is self-contained with its schema.
- **Streams & Catalog:** A Source declares its available streams (datatypes) with JSON Schema via `AirbyteCatalog`. Users select which streams to sync and how via `ConfiguredAirbyteCatalog`.
- **Sync Modes:**
  - Source side: `full_refresh` or `incremental` (cursor-based).
  - Destination side: `overwrite`, `append`, or `append_dedup` (using a primary key for upsert semantics).
- **State & Checkpointing:** Three state types — `Stream` (per-stream cursors), `Global` (single checkpoint across all streams), and `Legacy`. State messages are emitted periodically to enable resumable syncs after failures.
- **ConnectorSpecification:** Each connector declares its configuration schema as JSONSchema, enabling dynamic UI rendering and validation.
- **AirbyteControlMessage:** Allows mid-sync control signals, e.g., notifying the platform that an OAuth token was refreshed so it can persist the new credentials.

### Low-Code CDK (YAML-Based Declarative Connectors)

Airbyte's **Low-Code CDK** is the closest existing analog to our planned declarative connector format:

- **DeclarativeStream:** Defines a stream with a `retriever` containing:
  - `requester`: HTTP method, URL, headers, authenticators, error handlers.
  - `paginator`: Offset, cursor, or page-number based with configurable stop conditions.
  - `record_selector`: JSONPath-based extraction of records from response envelopes.
  - `partition_router`: Parameterized sub-queries (e.g., iterate over parent IDs to resolve child records — comparable to our "linked/nested object resolution").
- **DatetimeBasedCursor:** Declarative incremental sync using a timestamp field, with configurable lookback windows.
- **Schema Loader:** Inline JSON Schema or external `.json` file, per stream.
- **Transformations:** `AddFields`, `RemoveFields`, and custom Python transformations within the YAML spec.

### What We Can Learn

| Area | Lesson |
|------|--------|
| **Message Protocol** | The strongly-typed envelope (RECORD/STATE/SCHEMA) with separate schema declarations per stream is excellent. Our ingestion tool should emit/store similar structured metadata alongside raw JSONB payloads — especially per-stream state and schema information. |
| **Declarative YAML** | The Low-Code CDK's YAML structure with `requester`, `paginator`, `record_selector`, and `partition_router` is a proven pattern for declarative HTTP connector definition. We should adopt a similar hierarchical structure. |
| **Partition Router** | Airbyte's partition router elegantly handles our requirement #9 (parameterized sources) and #16 (linked/nested object resolution). A parent stream emits IDs that sub-streams iterate over. |
| **State Checkpointing** | Per-stream vs. global state types give flexibility. Our ingestion tool needs both per-datatype cursors and cross-datatype transaction boundaries. |
| **Mid-Sync Control Messages** | The `AirbyteControlMessage` for credential refresh is directly relevant to our OAuth2 token management (requirement #11). We should support mid-sync credential updates. |
| **Sync Mode Combinations** | Source mode × destination mode (e.g., `incremental` + `append_dedup`) is a clean separation. We can adopt the same approach, where the ingestion "source mode" is independent of the "destination write strategy." |

---

## 2. Singer / Meltano

### Singer Specification (v0.3.0)

Singer is the original open-source specification for composable data pipelines, defining a **Tap** (extractor) and **Target** (loader) that communicate through structured messages on stdout.

- **Message Types:** `RECORD`, `SCHEMA`, `STATE` — simpler than Airbyte but foundational.
- **Composability:** Taps and targets are pipe-composable: `tap | target`. Config, catalog, and state are passed as files.
- **Catalog:** Defines available streams with `key_properties` (primary keys) and `bookmark_properties` (cursor fields). Supports `replication_method` of `FULL_TABLE`, `INCREMENTAL`, or `LOG_BASED`.
- **State:** A JSON blob persisted between runs, carrying high-water marks per stream.

### Meltano

Meltano is the open-source ELT platform that operationalizes Singer with project-level configuration and orchestration:

- **YAML Configuration (`meltano.yml`):** Defines plugins (extractors, loaders, mappers, utilities), schedules, and environments.
- **600+ Connectors** from the MeltanoHub registry.
- **Plugin Types:** Extractors (Singer taps), Loaders (Singer targets), Mappers (inline stream-level transforms), Transforms (dbt), Utilities (e.g., Airflow, Superset).
- **Stream/Property Selection:** Wildcards and glob patterns for selecting which streams and which properties within streams to sync (e.g., `!*.email_address` to exclude a field globally).
- **Metadata Extras:** Per-stream configuration including `replication-method`, `replication-key`, `schema`, and `key-properties`.
- **Plugin Inheritance & Variants:** Multiple implementations of the same connector can coexist, with inheritance for shared config.
- **State Management:** Meltano manages Singer state files automatically, persisting per-job state.
- **Mappers:** Inline stream-level transformations between tap and target — renaming fields, filtering records, hashing PII.
- **dbt Integration:** First-class support for transformation after loading.

### What We Can Learn

| Area | Lesson |
|------|--------|
| **Simplicity of Spec** | Singer's minimalism (three message types, stdin/stdout pipes) achieved massive ecosystem adoption. Our internal protocol should be simple enough to reason about, even if our implementation is richer. |
| **Stream/Property Selection** | Meltano's wildcard-based property selection (requirement #21) is well-designed. Our declarative config should support similar glob patterns for field inclusion/exclusion. |
| **Replication Methods** | The `FULL_TABLE / INCREMENTAL / LOG_BASED` taxonomy maps directly to our sync mode requirements. Adding `LOG_BASED` as a formal mode acknowledges CDC/webhook patterns. |
| **Mappers** | Meltano's mapper concept (transform in-flight between extraction and loading) is useful for our raw-to-canonical data path. |
| **Plugin Registry** | A centralized connector registry with variants and inheritance helps manage multiple API versions or regional endpoints for the same system. |
| **Project-Level YAML** | Meltano's approach of one `meltano.yml` per project with environment overrides is a good model for multi-deployment configuration management (requirement #28 — runtime parameters). |

---

## 3. dlt (data load tool)

### Overview

dlt is a Python library for building data pipelines as code. It is notable for its approach to **declarative REST API sources** and **automatic schema evolution**.

### Key Concepts

- **Python-First:** Pipelines are defined in Python, not YAML. However, dlt provides a `rest_api` verified source that accepts a declarative Python dictionary describing endpoints, pagination, authentication, and response parsing.
- **REST API Source Configuration:** Define a `client` (base URL, auth, paginator) and a list of `resources` (endpoints), each with:
  - Path and HTTP method.
  - Pagination strategy (offset, cursor, JSON response, header links).
  - Authentication (API key, OAuth2, bearer token, HTTP basic).
  - Incremental loading via cursor fields.
  - **Resolved resources:** Child endpoints that depend on data from parent endpoints (e.g., `/orders/{order_id}/items`). dlt resolves these automatically by iterating parent records.
- **Schema Inference & Evolution:** dlt automatically infers schema from JSON data and evolves it as data changes — adding columns, widening types. Schemas are versioned.
- **Normalization:** Nested JSON is automatically flattened into related tables. Lists within records become child tables linked by auto-generated foreign keys.
- **Write Dispositions:** `replace`, `append`, `merge` (with dedup key). The `merge` disposition supports upsert behavior using a primary key.
- **State Management:** dlt manages pipeline state (cursors, incremental markers) via a persistent state store, enabling resumable incremental loads.

### What We Can Learn

| Area | Lesson |
|------|--------|
| **Resolved Resources (Parent→Child)** | dlt's pattern for declaring child resources that reference parent fields is a clean implementation of our requirement #16 (linked/nested object resolution). The `resolve` parameter explicitly links child endpoints to parent data fields. |
| **Auto-Normalization** | Automatic flattening of nested JSON into related tables with generated foreign keys is directly relevant. However, our JSONB-first approach (requirement #2) means we should normalize *optionally* and preserve the raw nested structure. |
| **Schema Evolution** | dlt's schema versioning (tracking when columns were added, types widened) aligns with our requirement #15 (change history / audit trail). We should version our JSONB schemas similarly. |
| **Merge Write Disposition** | dlt's `merge` mode with primary key dedup is a simple model for what our ingestion tool needs when writing to PostgreSQL — upsert by external ID in the JSONB table. |
| **REST API DSL** | The dictionary-based REST API source (base URL, per-resource auth overrides, paginator config) is almost exactly the kind of declarative config we need, though we prefer YAML over Python dicts. |

---

## 4. Fivetran

### Overview

Fivetran is the dominant managed ELT platform. It is fully managed (no user code), handling connector maintenance, schema migrations, and incremental syncs automatically. Fivetran recently acquired **Census**, the leading reverse ETL platform, signaling convergence of forward and reverse data flows.

### Architecture & Key Concepts

- **Managed Connectors:** Fivetran builds, operates, and maintains connectors. Users don't write code — they configure connections and select schemas/tables to sync.
- **Canonical Schema:** Fivetran's philosophy is to deliver a "faithful replication of source data with as few transformations as necessary." It creates normalized destination schemas that mirror the source system's data model.
- **Sync Modes:**
  - **Soft Delete:** Marks deleted records with a `_fivetran_deleted` flag and timestamp rather than physically removing them.
  - **History Mode:** Retains all historical versions of every record, creating new rows for each change.
- **Data Type Handling:** Sophisticated type inference for untyped sources (CSV, JSON). Uses a type hierarchy (BOOLEAN → SHORT → INT → LONG → DOUBLE → STRING → JSON) to find the smallest lossless type. Automatically promotes column types when source types widen.
- **JSON Handling:** First-level JSON fields are promoted to columns. Nested objects and arrays remain as JSON/STRING columns, not unpacked.
- **Data Checkpoints:** Fivetran checkpoints data during sync. If a sync fails, the next run resumes from the last checkpoint, not from the last successful sync.
- **Re-sync:** Invalidates incremental cursors and re-fetches all records from source. Overwrites existing rows but preserves tables.
- **Connector SDK:** Allows building custom connectors with a structured API when Fivetran's 300+ native connectors don't cover a system.

### What We Can Learn

| Area | Lesson |
|------|--------|
| **Soft Delete Mode** | Fivetran's `_fivetran_deleted` flag is a proven approach for our requirement #4 (deletion tracking). Adding `_deleted_at` and `_deleted` columns to our per-datatype tables avoids destructive deletes and enables audit trails. |
| **History Mode** | Fivetran's history mode (one row per version) is a direct solution to requirement #15 (change history / versioned audit trail). We should offer both "latest-only" and "full-history" table modes. |
| **Type Hierarchy for Schema Evolution** | The type promotion hierarchy (always widen, never narrow) is a sound principle for JSONB-stored data when we need derived typed columns. |
| **Checkpointing** | Resumable syncs from the last checkpoint (not last successful run) is critical for large tables. Our ingestion tool should checkpoint within a sync run, not only between runs. |
| **Canonical Schema Philosophy** | Fivetran's "faithful replication" philosophy aligns with our requirement #17 (raw data preservation). Transform post-load, not pre-load. |
| **Census Acquisition (Reverse ETL)** | The acquisition of Census validates the strategic importance of bidirectional data flow. Census's model (warehouse → operational tools) via SQL models and sync schedules is exactly our writeback pattern. |

---

## 5. Hightouch

### Overview

Hightouch is a leading **Reverse ETL** platform that moves data from data warehouses/lakes back to operational tools (CRMs, ad platforms, support tools). It is directly relevant to our Tool 2 (Writeback).

### Architecture & Key Concepts

- **Sources → Models → Syncs → Destinations:** Users define a Source (data warehouse), create Models (SQL queries or tables that define the data to sync), configure Syncs (how and when to push data), and choose Destinations (external tools).
- **Sync Types:** `insert`, `update`, `upsert`, `archive` (soft delete). Each sync maps a model to a destination object type.
- **Field Mapping:** Declarative mapping between model columns and destination fields. Supports expressions and transformations.
- **CDC with Lightning Sync Engine:** Uses warehouse-computed change detection to identify which rows changed since the last sync, avoiding full table scans.
- **Scheduling:** Cron schedules, event-triggered, or manual syncs.
- **Row-Level Sync Logs:** Tracks the outcome (success/failure/skipped) of every row in every sync, providing per-record audit trails.

### What We Can Learn

| Area | Lesson |
|------|--------|
| **Models Abstraction** | Hightouch's "Model" (a SQL query defining what to sync) is analogous to our "desired-state input table" (requirement #7). Decoupling the data selection from the sync mechanism is a clean pattern. |
| **Row-Level Sync Logs** | Per-record success/failure tracking is essential for our requirement #13 (response capture & audit logging) and #14 (duplicate insert prevention). We should maintain a detailed sync log per writeback operation. |
| **Sync Types** | The `insert / update / upsert / archive` taxonomy matches our `action` column in the desired-state table. Adding `archive` (soft delete) as a distinct operation type is worth considering. |
| **Field Mapping** | Declarative field-level mapping between source and destination schemas directly supports our requirement #12 (API asymmetry handling) and #17 (pre-write data transformation). |
| **CDC for Reverse ETL** | Hightouch's warehouse-computed CDC (comparing current vs. previous state) is a practical approach for our writeback tool to detect changed records in the desired-state table. |

---

## 6. Polytomic

### Overview

Polytomic positions itself as a **bidirectional data sync** platform, unifying ETL, reverse ETL, and CDC in a single product. It supports both forward (API→warehouse) and reverse (warehouse→API) data flows.

### Architecture & Key Concepts

- **Bidirectional Sync:** A single platform handles both directions — inbound data extraction and outbound data writes — which is the exact paradigm of our In-and-Out project.
- **Models & Syncs:** Define data models (SQL or tables), configure field mapping and filters, and set up syncs to destinations.
- **CDC Streaming:** Supports real-time CDC from databases, capturing insert/update/delete events.
- **HTTP API Connections:** Builds connectors to arbitrary HTTP APIs, not just databases.
- **Data Enrichment:** Can enrich records by joining data from external sources (e.g., ZoomInfo, Apollo.io) before syncing.
- **Infrastructure-as-Code:** Supports Terraform for managing sync configurations as code — important for enterprise deployment patterns.
- **Self-Hosted:** Available as a self-hosted option, with SOC2 compliance and RBAC.
- **Incremental Syncing:** Tracks changes using record-level comparison and only syncs modified records.
- **200+ Integrations:** Pre-built connectors for common SaaS tools.

### What We Can Learn

| Area | Lesson |
|------|--------|
| **Bidirectional Architecture** | Polytomic validates the core thesis of In-and-Out: a single platform handling both ingestion and writeback. Their success proves market demand for unified bidirectional sync. |
| **Enrichment as a First-Class Concept** | Data enrichment during sync (joining external data) could be relevant for our writeback tool — e.g., enriching the desired-state record with additional context before writing. |
| **Terraform/IaC Support** | Infrastructure-as-code for sync configuration aligns with our declarative YAML approach. We should ensure our configs are IaC-friendly (version-controlled, diff-able, templatable). |
| **Self-Hosted Deployment Model** | Enterprise MDM systems often require on-premise deployment. Polytomic's self-hosted option validates that our tool should be designed for self-hosted deployment from the start. |
| **Restricting Operations to Changed Records** | Polytomic's incremental sync optimizations (only processing changed records on enrichment runs) reinforce requirement #2 in our writeback tool (smart writes targeting only changed records). |

---

## 7. RudderStack

### Overview

RudderStack is a **Customer Data Platform (CDP)** focused on real-time event streaming, identity resolution, and reverse ETL. It is warehouse-native, meaning the data warehouse is the central store.

### Architecture & Key Concepts

- **Event Stream:** SDK-based collection of events from web, mobile, and server-side sources. Events flow in real time to warehouse and 200+ downstream destinations.
- **Reverse ETL:** Reads data from warehouses (PostgreSQL, Snowflake, BigQuery, Redshift, Databricks) and syncs to downstream tools. Supports PostgreSQL as a Reverse ETL source.
- **Profiles (Identity Resolution):** Builds unified customer profiles ("Customer 360") by stitching together identities from multiple sources — using probabilistic and deterministic matching.
- **Data Quality Toolkit:** Schema management, event validation, and consent flow automation at the pipeline level.
- **Transformations:** JavaScript-based in-flight transformations for events before they reach destinations.
- **Warehouse-Native:** RudderStack doesn't store your data long-term — the warehouse is the single source of truth.
- **200+ Pre-Built Integrations:** Destinations for analytics, ad platforms, CRMs, marketing automation, etc.

### What We Can Learn

| Area | Lesson |
|------|--------|
| **Identity Resolution** | RudderStack's Profiles feature for stitching identities across sources is conceptually related to our `cluster_id` concept (requirement #7-8). Their approach to probabilistic + deterministic matching could inform our identity mapping design. |
| **Warehouse-Native Philosophy** | The principle that the warehouse is the source of truth (not the pipeline tool) aligns perfectly with our PostgreSQL-centric MDM architecture. Our tools are just the transport layer. |
| **Reverse ETL from PostgreSQL** | RudderStack explicitly supports PostgreSQL as a reverse ETL source, validating our architecture where PostgreSQL tables drive writeback. |
| **Event Validation & Schema Enforcement** | Pre-delivery schema validation prevents bad data from reaching destinations. We should validate outbound writeback payloads against the target API's expected schema before sending. |
| **Real-Time Event Pipelines** | RudderStack processes billions of events daily (e.g., Bol.com: 1B events/day). Their architecture demonstrates that real-time event processing at scale is achievable — important for our webhook/event-driven ingestion (requirement #6). |
| **Consent Automation** | Data compliance tooling (GDPR, HIPAA) integrated into the pipeline is a forward-looking feature we should design for, especially in MDM contexts where PII is central. |

---

## 8. Debezium

### Overview

Debezium is the leading open-source **Change Data Capture (CDC)** platform. While primarily focused on database-to-database CDC (not HTTP API sync), its patterns for capturing and streaming changes are deeply relevant to our tools — particularly for the writeback side where we react to PostgreSQL changes.

### Architecture & Key Concepts

- **PostgreSQL Connector:** Uses PostgreSQL's `pgoutput` logical decoding plugin to capture row-level changes (INSERT, UPDATE, DELETE, TRUNCATE) from the WAL (Write-Ahead Log).
- **Change Event Structure:** Every event includes:
  - `before`: The state of the row before the change (depends on REPLICA IDENTITY setting).
  - `after`: The state after the change.
  - `source`: Metadata including LSN (Log Sequence Number), transaction ID, timestamp, schema/table name.
  - `op`: Operation type — `c` (create), `u` (update), `d` (delete), `r` (read/snapshot), `t` (truncate), `m` (message).
- **Snapshot Modes:** `initial` (snapshot then stream), `always`, `never`, `when_needed`, `initial_only`. Also supports ad hoc incremental and blocking snapshots triggered by signals.
- **Incremental Snapshots:** Reads tables in configurable chunks (default 1024 rows) by primary key, interleaved with streaming — no downtime. Uses watermarks and a snapshot window with de-duplication to handle concurrent writes during the snapshot.
- **Signaling:** External signals (via a signaling table or Kafka topic) can trigger ad hoc snapshots, stop snapshots, or send control commands to the connector at runtime.
- **Transaction Metadata:** Emits `BEGIN`/`END` events for transactions, with event counts per data collection, enabling consumers to know transaction boundaries.
- **Tombstone Events:** After a delete event, emits a tombstone (null-value record) for the same key, enabling Kafka log compaction to fully remove the key.
- **Replica Identity:** Controls what data is available in the `before` field of UPDATE/DELETE events. `FULL` provides all columns; `DEFAULT` provides only primary key columns.
- **Heartbeat Mechanism:** Periodic heartbeat messages prevent replication slot growth when monitored tables have low write activity but other tables in the database are active.
- **Fault Tolerance:** Detailed handling of failures — connector crash, Kafka unavailability, PostgreSQL failover, cluster topology changes (PG 15, 16, 17 behaviors differ).

### What We Can Learn

| Area | Lesson |
|------|--------|
| **Logical Replication for Writeback Triggers** | Debezium's use of `pgoutput` to capture changes from PostgreSQL is directly applicable for our requirement #10 (near real-time writeback via PG triggers/logical replication). We could use the same mechanism to detect changes in the desired-state tables and trigger writeback. |
| **Before/After Event Structure** | The `before`/`after` structure in change events is exactly what our writeback tool needs for **base-aware updates** (requirement #4) and **client-side patching** (requirement #5). The `before` state is the base, the `after` state is the desired state. |
| **Replica Identity Configuration** | To get the `before` values needed for 3-way merge, we'd need `REPLICA IDENTITY FULL` on our desired-state tables. This is a critical PostgreSQL configuration detail. |
| **Incremental Snapshots with Watermarks** | Debezium's chunk-based incremental snapshot with de-duplication windows is relevant if our ingestion tool needs to backfill or re-sync large tables without stopping real-time processing. |
| **Transaction Boundaries** | Transaction metadata (BEGIN/END with event counts) helps group related changes. Our writeback tool should process all changes from a single MDM transaction atomically when possible. |
| **Heartbeat for Slot Management** | WAL management (replication slot growth, heartbeat to keep cursors advancing) is a critical operational concern. Our writeback tool must manage this if it uses logical replication. |
| **Signaling for Runtime Control** | The ability to trigger snapshots or control behavior via a signaling table is elegant. Our tools could use a similar pattern — a PostgreSQL control table where ops teams insert commands to trigger re-syncs, pause processing, or adjust behavior at runtime. |
| **Tombstone Events for Deletion** | Debezium's tombstone pattern (explicit null-value record after delete) ensures downstream consumers can fully clean up. Our ingestion tool should apply a similar pattern: after detecting a deletion, write a tombstone record to the per-datatype table. |

---

## 9. Summary Matrix

| Tool | Direction | Declarative Config | Incremental Sync | CDC/Real-Time | Deletion Handling | Conflict Resolution | Identity Mapping | Open Source |
|------|-----------|-------------------|-------------------|---------------|-------------------|--------------------|-----------------|----|
| **Airbyte** | Inbound | YAML (Low-Code CDK) | ✅ Cursor-based | Partial (polling) | Via full refresh diff | N/A (read only) | N/A | ✅ |
| **Singer/Meltano** | Inbound | YAML (meltano.yml) | ✅ Bookmark-based | Via LOG_BASED mode | Full table diff | N/A (read only) | N/A | ✅ |
| **dlt** | Inbound | Python dict/code | ✅ Cursor-based | No | Via merge mode | N/A (read only) | N/A | ✅ |
| **Fivetran** | Inbound | Managed UI | ✅ Managed | ✅ (some connectors) | Soft delete / History | N/A (read only) | N/A | ❌ |
| **Hightouch** | Outbound | UI + field mapping | ✅ CDC-based | ✅ Lightning engine | Archive (soft delete) | Partial (row logs) | Partial | ❌ |
| **Polytomic** | Both | UI + Terraform | ✅ Record comparison | ✅ CDC streaming | Yes | Partial | Partial | ❌ |
| **RudderStack** | Both | Code + UI | ✅ Warehouse-native | ✅ Event streaming | N/A | N/A | ✅ Profiles | Partial |
| **Debezium** | DB→DB | JSON config | ✅ WAL-based | ✅ Native CDC | Tombstones | N/A | N/A | ✅ |
| **In-and-Out** | Both | YAML/JSON | ✅ HWM + Full | ✅ Webhooks + PG listen | Verification + diff | ✅ OCC/3-way merge | ✅ cluster_id mapping | ✅ (planned) |

---

## 10. Cross-Cutting Lessons for In-and-Out

### 10.1 Declarative Configuration Design

The strongest pattern across the ecosystem is a **hierarchical YAML/JSON configuration** that separates concerns:

```
connector:
  auth: { type: oauth2, ... }         # Authentication (shared)
  base_url: https://api.example.com
  rate_limit: { requests_per_second: 10 }
  datatypes:
    - name: contacts
      ingestion:
        list_endpoint: /contacts
        detail_endpoint: /contacts/{id}
        pagination: { type: cursor, field: after }
        incremental: { cursor_field: updatedAt }
        primary_key: [id]
      writeback:
        insert: { method: POST, path: /contacts }
        update: { method: PATCH, path: /contacts/{id} }
        delete: { method: DELETE, path: /contacts/{id} }
        field_mapping: { ... }
```

**Key takeaways from the ecosystem:**
- **Airbyte's CDK** proves YAML-defined connectors work at scale (hundreds of connectors).
- **Meltano** shows project-level YAML with environment overrides handles deployment variation.
- **Polytomic's Terraform** support shows IaC-compatible configs are expected by enterprises.
- **dlt's `resolve` pattern** shows child-resource dependencies can be declared inline.

### 10.2 Ingestion Tool Strengthening

From the ecosystem research, key capabilities to ensure our ingestion tool is best-in-class:

1. **Checkpoint within syncs, not just between them** (Fivetran). A large full-sync that fails at 90% should resume at 90%, not 0%.
2. **History mode as a first-class feature** (Fivetran). Offer both "latest state" and "all versions" table modes, selectable per datatype.
3. **Soft delete with verification** (Fivetran + our requirement #5). Our deletion verification step goes beyond what any tool in the ecosystem offers — this is a genuine differentiator.
4. **Stream/property selection with wildcards** (Meltano). Flexible glob patterns for selecting which fields to ingest.
5. **Schema versioning** (dlt). Track when the external API's schema changes and version our internal schema accordingly.
6. **Parent→child resource resolution as a declarative primitive** (Airbyte partition_router, dlt resolved resources). This pattern is well-proven and should be central to our config format.
7. **Circuit breakers for empty responses** (our requirement #13). No tool in the ecosystem explicitly addresses this. Our approach of treating suspiciously empty responses as errors is unique and valuable.

### 10.3 Writeback Tool Strengthening

The writeback/reverse ETL space is less mature than ingestion. Key insights:

1. **Row-level sync logs** (Hightouch). Every write attempt should produce a detailed log: input record, HTTP request/response, outcome, and generated IDs.
2. **Before/after state for merge** (Debezium). Use PostgreSQL logical replication to get both old and new values from the desired-state table, enabling true 3-way merge.
3. **Replica Identity FULL** (Debezium). Configure our desired-state tables with `REPLICA IDENTITY FULL` to get `before` values in change events.
4. **Identity mapping as a first-class table** (our requirement #8). No existing tool handles this well. The `cluster_id → external_id` mapping table is a unique MDM capability.
5. **Separate processing paths** (our requirement #15). Hightouch's distinct sync types (insert/update/archive) reinforce that operations should flow through separate code paths.
6. **External reference writeback** (our requirement #16). Writing the MDM's cluster_id back into the target system's "external reference" field creates a bidirectional link — no tool in the ecosystem explicitly does this.
7. **Upsert as a first-class action** (our requirement #19). Polytomic and Hightouch support upsert patterns; we should too.

### 10.4 Gaps in Existing Tools (Our Differentiators)

Several of our planned features have **no direct equivalent** in the ecosystem:

| Feature | Status in Ecosystem |
|---------|-------------------|
| **Deletion verification** (requirement #5) | No tool verifies deletes with a targeted lookup before marking a record as deleted. |
| **Circuit breakers for empty responses** (#13) | Not implemented by any surveyed tool. |
| **Webhook lifecycle management** (#7) | Managed tools handle this internally but don't expose it as a declarative feature. |
| **Shared event receivers with fan-out** (#19) | No tool declares event routing rules for shared webhook endpoints. |
| **Base-aware 3-way merge writeback** (#4) | Debezium provides the before/after data, but no writeback tool uses it for 3-way merge. |
| **CRDT-aware writes** (#6) | No tool in the ecosystem supports CRDT structures for conflict-free writes. |
| **Identity mapping tables** (#8) | No tool maintains explicit `cluster_id → external_id` mapping for MDM use cases. |
| **Duplicate insert prevention via write log** (#14) | Hightouch tracks row-level outcomes but doesn't use this for pre-write deduplication. |
| **Read-only datatype flagging** (#23) | Not a concept in any surveyed tool — they implicitly handle one direction only. |

### 10.5 Technology & Protocol Recommendations

Based on the ecosystem survey:

1. **PostgreSQL logical replication** (via `pgoutput`) should be our primary mechanism for real-time writeback triggers, borrowing Debezium's proven approach.
2. **JSONB with operational metadata columns** for the per-datatype table design — combining Fivetran's canonical schema philosophy with our flexible schema requirement.
3. **State management** should support per-datatype cursors (like Airbyte Stream state) with periodic checkpointing within a sync run (like Fivetran).
4. **The connector config format** should follow Airbyte's CDK pattern: hierarchical YAML with `requester`, `paginator`, `record_selector`, and `partition_router` concepts, extended with our writeback operations.
5. **Audit logging** should cover both directions — ingestion provenance (raw data preservation) and writeback outcomes (response capture), creating a complete bidirectional data lineage.

---

*Report generated from ecosystem research covering: Airbyte (protocol, Low-Code CDK), Singer specification v0.3.0, Meltano (plugins, configuration), dlt (REST API source, normalization, schema evolution), Fivetran (core concepts, sync modes, data types, schema handling, Census acquisition), Hightouch (reverse ETL, sync types, Lightning engine), Polytomic (bidirectional sync, enrichment, Terraform/IaC), RudderStack (CDP, reverse ETL, identity resolution, event streaming), and Debezium (PostgreSQL CDC connector, change events, snapshots, streaming, fault tolerance).*
