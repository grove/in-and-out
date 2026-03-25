# Architecture Overview

This document explains what in-and-out does, how its pieces fit together, and the mental model that all other documentation builds on.

## What is in-and-out?

in-and-out is a **declarative, bidirectional HTTP API synchronisation tool** built for Master Data Management (MDM). It occupies the I/O layer of a composite MDM architecture:

- **Inbound**: pull data from external HTTP APIs into PostgreSQL
- **Outbound**: push desired-state changes from PostgreSQL back to external APIs

Everything is driven by YAML configuration files — one per external system. No code is needed to add a new integration. The engine handles scheduling, pagination, authentication, retry, circuit breaking, conflict detection, dead-letter queuing, and observability.

### What is explicitly out of scope

| Concern | Handled by |
|---|---|
| Identity resolution & deduplication | OSI-Mapping |
| Field-level conflict scoring | OSI-Mapping |
| Incremental view maintenance | pg-trickle |
| Business rules between ingest and writeback | Your bridge layer |

## System diagram

```
┌──────────────┐          ┌──────────────────┐          ┌──────────────┐
│  External    │  HTTP    │   Ingestion      │  SQL     │              │
│  APIs        │ ──────►  │   Daemon         │ ──────►  │  PostgreSQL  │
│  (HubSpot,   │  polling │  (inandout       │  upsert  │              │
│   Salesforce, │  + webhk │   ingest run)    │          │ inout_src_*  │
│   ...)       │          └──────────────────┘          │ inout_ops_*  │
└──────────────┘                                        │ inout_dl_*   │
       ▲                                                │              │
       │                  ┌──────────────────┐          │ inout_dst_*  │
       │         HTTP     │   Writeback      │  SQL     │              │
       └──────────────────│   Daemon         │ ◄──────  │              │
                 write    │  (inandout       │  read    │              │
                          │   writeback run) │          │              │
                          └──────────────────┘          └──────┬───────┘
                                                               │
                                                    ┌──────────┴──────────┐
                                                    │                     │
                                                    │  OSI-Mapping +      │
                                                    │  pg-trickle         │
                                                    │  (identity          │
                                                    │   resolution)       │
                                                    └─────────────────────┘
```

**Key insight**: the two daemons never communicate directly. They are decoupled through PostgreSQL tables. This means they can be deployed, scaled, and restarted independently.

## The two daemons

### Ingestion daemon (`inandout ingest run`)

| What it reads | What it writes |
|---|---|
| External HTTP APIs (polling + webhooks) | `inout_src_{connector}_{datatype}` — source tables |
| Connector YAML files | `inout_ops_sync_run` — sync run log |
| `inout_ops_watermark` — resume cursors | `inout_ops_watermark` — updated cursors |
| `inout_ops_control` — operator commands | `inout_dl_ingestion_*` — dead-letter queue |

The ingestion daemon runs a continuous loop per connector/datatype pair. Each cycle:

1. Acquires a distributed lock (PostgreSQL advisory lock)
2. Reads the current watermark
3. Calls the external API with pagination
4. Normalises and upserts records into the source table
5. Updates the watermark atomically
6. Records the sync run in the operations log

It also hosts:
- A **webhook HTTP server** for real-time event ingestion
- A **health/readiness endpoint** for orchestration
- A **Prometheus metrics endpoint** for monitoring

### Writeback daemon (`inandout writeback run`)

| What it reads | What it writes |
|---|---|
| `inout_dst_{connector}_{datatype}` — desired-state tables | External HTTP APIs (insert/update/delete) |
| `inout_dst_*_lwstate` — last-written state | `inout_dst_*_lwstate` — updated after writes |
| `inout_ops_control` — operator commands | `inout_ops_writeback_result` — audit log |
| | `inout_ops_identity_map` — external ID capture |
| | `inout_dl_writeback_*` — dead-letter queue |

The writeback daemon reads desired-state rows, performs a pre-flight read of the current state from the external API, runs a three-way comparison (current vs. base vs. last-written), detects conflicts, and issues the write.

### Why separate processes?

- **Independent scaling**: ingestion is I/O-bound (waiting on APIs); writeback is bound by target API rate limits. Scale each independently.
- **Failure isolation**: a writeback failure doesn't block ingestion, and vice versa.
- **Simpler reasoning**: each daemon has a single responsibility.

## Configuration layers

in-and-out uses three layers of configuration:

### Layer 1: Tool config

Files: `config/ingestion.yaml`, `config/writeback.yaml`

Controls daemon-level settings: database connection, health server, logging, tracing, housekeeping retention, and scheduling defaults. See the [Configuration Reference](./configuration.md) for all fields.

### Layer 2: Connector config

Files: `connectors/*.yaml`

One file per external system. Declares HTTP mechanics: base URL, authentication, rate limits, datatypes, pagination, incremental sync, and writeback operations. See the [Connector Authoring Guide](./connector-authoring.md) for details.

### Layer 3: OSI-Mapping config (upstream)

Not part of this repository. OSI-Mapping reads from `inout_src_*` tables and writes to `inout_dst_*` tables. The [Schema Contract](https://github.com/grove/in-and-out) defines the interface between the two systems.

## PostgreSQL as the integration bus

All state lives in PostgreSQL. The daemons are stateless — kill one and restart it, and it picks up where it left off.

### Table naming conventions

| Prefix | Purpose | Example |
|---|---|---|
| `inout_src_` | Ingestion source tables | `inout_src_hubspot_contacts` |
| `inout_dst_` | Writeback desired-state tables | `inout_dst_hubspot_contacts` |
| `inout_dst_*_lwstate` | Last-written-state for conflict detection | `inout_dst_hubspot_contacts_lwstate` |
| `inout_ops_` | Operational tables (logs, locks, watermarks) | `inout_ops_sync_run` |
| `inout_dl_` | Dead-letter queues | `inout_dl_ingestion_hubspot_contacts` |
| `_delta_` | OSI-Mapping stream tables (not owned by in-and-out) | `_delta_hubspot_contacts` |

### Why PostgreSQL?

- **Stateless processes**: daemons can crash and restart without data loss.
- **High availability**: standard PostgreSQL HA (streaming replication, Patroni) applies.
- **Auditability**: every sync run, every writeback result, every command is persisted.
- **Familiar tooling**: query operational state with SQL.

## Connector model

A connector file describes one external system. It contains:

| Section | Purpose |
|---|---|
| `connector.connection` | Base URL, timeouts |
| `connector.auth` | Authentication (OAuth2, API key, JWT, custom) |
| `connector.rate_limit` | Requests per second, burst |
| `connector.retry` | Max retries, backoff, jitter |
| `connector.circuit_breaker` | Error threshold, pause duration |
| `connector.webhooks` | Inbound webhook path, signature verification, fan-out routing |
| `connector.datatypes` | One or more datatypes, each with ingestion and/or writeback config |

### Generation profiles

Each connector declares a **generation profile** that determines which features are required:

| Profile | Description |
|---|---|
| `ingestion_polling_readonly` | Poll-only ingestion, no writeback |
| `ingestion_webhook_incremental` | Webhook events + polling for full sync |
| `writeback_patch` | Writeback only, no ingestion |
| `full_duplex` | Both ingestion and writeback |

## Data flow walkthrough

### Ingestion flow

```
External API
    │
    ▼
  Poll (GET with pagination)
    │
    ▼
  Parse response (record_selector extracts records)
    │
    ▼
  Apply field mappings and transforms
    │
    ▼
  Compute raw hash for change detection
    │
    ▼
  Upsert into inout_src_{connector}_{datatype}
  (ON CONFLICT: update only if hash changed)
    │
    ▼
  Update watermark in inout_ops_watermark
    │
    ▼
  Record sync run in inout_ops_sync_run
```

### Writeback flow

```
inout_dst_{connector}_{datatype}  (desired-state row)
    │
    ▼
  Pre-flight read: GET current state from external API
    │
    ▼
  Three-way comparison:
    current (from API) vs. base (from desired-state) vs. last_written (from lwstate)
    │
    ├─ No conflict → issue write (POST/PATCH/DELETE)
    │
    └─ Conflict detected → apply conflict_resolution strategy
       ├─ dead_letter: route to dead-letter queue
       ├─ last_writer_wins: write anyway
       ├─ skip_and_warn: skip, log warning
       ├─ server_wins: accept external state
       └─ re_ingest_and_recompute: signal re-ingestion
    │
    ▼
  Record result in inout_ops_writeback_result
    │
    ▼
  Update inout_dst_*_lwstate with written payload
    │
    ▼
  Capture external ID in inout_ops_identity_map (on insert)
```

## High availability model

Both daemons are designed for safe concurrent execution:

- **Distributed locking**: each connector/datatype sync cycle acquires a PostgreSQL advisory lock via `inout_ops_sync_lock`. If another instance holds the lock, the current instance skips that cycle.
- **Idempotent writes**: ingestion uses upsert semantics keyed on `external_id`. Running the same sync twice produces no duplicates.
- **Crash recovery**: `inout_ops_sync_checkpoint` records intra-sync progress. If a daemon crashes mid-page, the next run resumes from the last checkpoint.

You can safely run multiple replicas of each daemon. The advisory lock mechanism ensures that at most one instance processes a given connector/datatype at any time.
