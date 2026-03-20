# pg-trickle: Incremental View Maintenance for the OSI-Mapping Pipeline

**Research date:** 2026-03-20  
**pg-trickle release:** v0.9.0 (released today)  
**Repository:** https://github.com/grove/pg-trickle/  
**Authors:** Geir O. Grønmo (grove) and Baard H. Rehn Johansen (BaardBouvet) — the same team as OSI-Mapping

---

## Executive Summary

**pg-trickle** is a PostgreSQL extension (Rust + pgrx) that implements **Incremental View Maintenance (IVM)** via differential dataflow. It converts plain SQL views into **stream tables** — materialized query results that automatically refresh themselves when their source data changes, processing only the changed rows rather than recomputing the full result set from scratch.

**Key relevance to this project:** pg-trickle is built by the same team as OSI-Mapping and is explicitly designed to serve as the execution engine for OSI-Mapping's 6-stage view pipeline. Without pg-trickle, every writeback cycle must re-execute all six OSI stages from scratch — an O(total rows) operation. With pg-trickle, the same pipeline becomes O(changed rows): incremental, fast, and self-orchestrating.

**Concerns it addresses in our architecture:**

| Concern | Without pg-trickle | With pg-trickle |
|---|---|---|
| View query performance | O(all source rows) full recompute each cycle | O(changed rows) incremental delta per cycle |
| ETL feedback loop | Requires external orchestration to re-trigger OSI views | CDC auto-detects writes to `_written_` / `_cluster_members_` → OSI views self-update |
| Refresh ordering | Manual coordination of 6-stage pipeline | DAG-aware scheduler maintains topological order automatically |
| Ingestion-writeback pipelining | Writeback may read partially-ingested data | Watermark gating holds OSI refresh until ingestion cycle is complete |
| Transitive closure scaling | Full recursive CTE recomputation every cycle | WITH RECURSIVE in DIFFERENTIAL mode — only affected clusters recomputed |

**Status caveat:** pg-trickle v0.9.0 is an early release targeting PostgreSQL 18 exclusively. It is not yet production-hardened. This must be factored into any adoption timeline.

---

## 1. What Is pg-trickle?

pg-trickle is a PostgreSQL 18 extension written in Rust using the pgrx framework. The core concept:

> Define a SQL query and a refresh schedule. pg-trickle materializes the result, installs CDC (change data capture) triggers on all source tables, and on each refresh cycle executes only a "delta query" (ΔQ) derived algebraically from the original query — merging only the changes into the materialized result.

### How Stream Tables Differ from Materialized Views

| | PostgreSQL `MATERIALIZED VIEW` | pg-trickle `stream table` |
|---|---|---|
| `REFRESH` cost | O(all source rows) — full recompute | O(changed rows) — incremental delta |
| Auto-refresh | No — must explicitly call `REFRESH` | Yes — background scheduler or immediate |
| Scheduling | None | Cron / interval / calculated / watermark-gated |
| CDC | None | Automatic (trigger-based or WAL-based) |
| DAG dependencies | None | Topological refresh ordering |
| `WITH RECURSIVE` | Works | Works in DIFFERENTIAL mode |

### Four Refresh Modes

| Mode | Description | Use case |
|---|---|---|
| `AUTO` | Try differential, fall back to full | Default; smart selection per query complexity |
| `DIFFERENTIAL` | Incremental delta only | Standard for most views |
| `FULL` | Complete recomputation | Queries that can't be differentiated |
| `IMMEDIATE` | Within the same transaction as source DML | Read-your-writes (zero-lag, higher write cost) |

### Technical Foundation

pg-trickle is grounded in the [DBSP differential dataflow framework](https://arxiv.org/abs/2203.16684) (Budiu et al., 2022). The DVM (Differential View Maintenance) engine:
- Parses each stream table's defining query into an operator tree
- Algebraically differentiates the tree to produce a delta query ΔQ
- ΔQ processes only the CDC change buffers, not the full source tables
- Results are merged into the stream table via a single MERGE statement

### Hybrid CDC Layer

Change capture does not require WAL/logical replication. By default:
1. Lightweight `AFTER INSERT/UPDATE/DELETE` row-level triggers write to `pgtrickle_changes` schema tables
2. Optionally upgrades to WAL-based (logical replication) capture automatically when `wal_level = logical` is available

This means pg-trickle works out-of-the-box with no PostgreSQL configuration changes.

---

## 2. Relationship to OSI-Mapping

### Same Team

The two projects share the same authors: Geir O. Grønmo and Baard H. Rehn Johansen. pg-trickle is not an incidental fit — it is the planned execution substrate for OSI-Mapping's view pipeline. The projects are designed to compose.

### OSI-Mapping Without pg-trickle

OSI-Mapping generates a 6-stage PostgreSQL view DAG:

```
Stage 1: _fwd_{mapping}       — project source → target shape
Stage 2: _id_{target}         — assign _cluster_id (WITH RECURSIVE transitive closure)
Stage 3: _resolved_{target}   — apply conflict strategies, produce golden record  
Stage 4: {target}             — clean analytics view
Stage 5: _rev_{mapping}       — reverse-project resolved values to source shape
Stage 6: _delta_{mapping}     — classify changes: _action = insert/update/delete/noop
```

As plain PostgreSQL views, every query against `_delta_crm_contacts` re-executes all six stages from scratch, including the `WITH RECURSIVE` transitive closure at Stage 2. For large datasets (millions of records, hundreds of source tables), this is prohibitively expensive.

### OSI-Mapping With pg-trickle

When each OSI stage becomes a pg-trickle stream table:
- Stage 2 (`_id_{target}`) runs only the recursive CTE delta for the affected rows — not all clusters
- Stages 3–6 cascade incrementally, processing only what changed in Stage 2
- `_delta_{mapping}` is a pre-computed, materialized stream table — writeback reads from it instantly
- When ETL feedback writes land in `_written_` and `_cluster_members_`, pg-trickle's CDC triggers detect those writes and schedule Stage 5–6 re-evaluation automatically

The OSI 6-stage pipeline becomes a self-maintaining DAG, not a set of on-demand recomputed views.

---

## 3. Concerns pg-trickle Addresses

### 3.1 View Performance and Scalability

**Problem (without pg-trickle):** Every writeback cycle queries `_delta_{mapping}`, which re-executes the full 6-stage OSI pipeline from scratch. At Stage 2, the transitive closure `WITH RECURSIVE` visits every source row to recompute cluster assignments. This is O(source rows) regardless of how much actually changed.

**Solution (with pg-trickle):** pg-trickle supports `WITH RECURSIVE` in DIFFERENTIAL mode using semi-naive evaluation + Delete-and-Rederive (DRed) for deletes. Per-cycle cost becomes O(changed rows). Adding one new record to a source table updates only the clusters that actually changed — not all clusters.

For a production-scale MDM system (thousands of source records per connector), this is the difference between a 10-second write cycle and a 100-millisecond write cycle.

### 3.2 ETL Feedback Loop Automation

**Problem (without pg-trickle):** After In-and-Out Writeback writes to `_written_{mapping}` and `_cluster_members_{mapping}`, the OSI view pipeline does not automatically re-evaluate. The feedback — "this external system row now maps to cluster ID X" — must be communicated back to OSI to update noop detection and avoid re-writing values next cycle. Without automatic re-evaluation, this requires external orchestration: writing the ETL feedback, then explicitly triggering OSI view recomputation, then signaling writeback that it can proceed.

**Solution (with pg-trickle):** pg-trickle's CDC triggers are installed on **all tables that stream tables depend on**. When writeback writes to `_written_{mapping}`, the CDC trigger fires. The scheduler detects that Stage 5 (`_rev_{mapping}`) and Stage 6 (`_delta_{mapping}`) have pending changes and schedules an incremental refresh. No external orchestration is needed — the feedback loop is closed automatically within PostgreSQL.

```
Writeback writes to _written_{mapping}
  → pg-trickle CDC trigger fires on _written_{mapping}
  → _delta_{mapping} stream table scheduled for incremental refresh
  → Only rows affected by the feedback are recomputed
  → Next read of _delta_{mapping} reflects the feedback
```

### 3.3 DAG-Aware Refresh Ordering

**Problem (without pg-trickle):** The 6 OSI stages have strict dependencies. Stage 6 depends on Stage 5, which depends on Stage 3, which depends on Stage 2. External orchestration (cron jobs, Airflow DAGs, dbt runs) must enforce this ordering. Any scheduling error causes stale data to propagate forward.

**Solution (with pg-trickle):** pg-trickle's scheduler builds a dependency graph from the stream table definitions and maintains them in topological order. Stage N is never refreshed before Stage N-1. With diamond dependency consistency (v0.2.0), parallel paths through the DAG are refreshed atomically — no "new Stage 3, old Stage 5" race conditions.

### 3.4 Ingestion-to-Writeback Pipelining (Watermark Gating)

**Problem (without pg-trickle):** In-and-Out Ingestion runs as a polling loop. Between pages of a multi-page fetch, some source tables are partially updated. If OSI views are evaluated mid-ingestion, writeback sees an inconsistent snapshot — some source records are from the new cycle, some from the prior cycle.

**Solution (with pg-trickle, v0.7.0+):** Ingestion posts a **watermark** after each complete sync cycle:

```sql
SELECT pgtrickle.advance_watermark('inout_src_crm_contact', now());
```

The OSI stream tables are declared in a watermark group for all ingestion source tables. The scheduler gates OSI refreshes until all watermarks are aligned within a configurable tolerance. Writeback will always read from a complete, consistent ingestion snapshot.

This replaces any need for a separate "ingestion complete" signal or external coordination layer.

### 3.5 Bootstrap Source Gating for Bulk Loads

**Problem (without pg-trickle):** Initial loads and periodic full re-syncs write millions of rows. Without gating, the CDC layer processes each row as it arrives, triggering repeated intermediate refreshes against partially-loaded data — wasteful and potentially incorrect.

**Solution (with pg-trickle, v0.5.0+):** Ingestion can gate a source before bulk loads:

```sql
SELECT pgtrickle.gate_source('inout_src_crm_contact');
-- ... bulk insert millions of rows ...
SELECT pgtrickle.ungate_source('inout_src_crm_contact');
-- → single clean refresh after full load
```

A single clean refresh runs after the gate is lifted, processing all changes in one efficient differential pass.

### 3.6 Cross-Source Snapshot Consistency

**Problem (without pg-trickle):** A single ingestion cycle updates multiple source tables. If OSI evaluates Stage 3 after seeing new data from `inout_src_crm` but old data from `inout_src_sap`, the produced golden record is internally inconsistent.

**Solution (with pg-trickle, v0.4.0+):** The LSN tick watermark captures `pg_current_wal_lsn()` at the start of each scheduler tick and caps CDC consumption to that LSN. All source tables are evaluated at the same logical point-in-time within each refresh cycle.

---

## 4. Integration Architecture

### Revised Architecture with pg-trickle

```
External APIs → [In-and-Out Ingestion] → PostgreSQL source tables
                                          │
                       advance_watermark() after each cycle
                                          │
                                          ▼
               ┌──────────────────────────────────────────┐
               │  pg-trickle: Watermark Gating            │
               │  (holds OSI refresh until cycle complete) │
               └──────────────────┬───────────────────────┘
                                  │ CDC triggers on source tables
                                  ▼
               ┌──────────────────────────────────────────┐
               │  OSI-MAPPING + pg-trickle (Stream Tables)│
               │  ────────────────────────────────────────│
               │  Stage 2: _id_{target}                   │
               │    WITH RECURSIVE — DIFFERENTIAL mode    │
               │    Only affected clusters recomputed     │
               │  Stage 3: _resolved_{target}             │
               │    Incremental conflict resolution       │
               │  Stage 5: _rev_{mapping}                 │
               │    Incremental reverse projection        │
               │  Stage 6: _delta_{mapping}               │
               │    Incremental delta classification      │
               │    ↑ feeds from _written_ stream table   │
               └──────────────────┬───────────────────────┘
                                  │
                       pre-computed, instantly readable
                                  │
                                  ▼
               ┌──────────────────────────────────────────┐
               │  _delta_{mapping} stream table           │
               │  _action, _cluster_id, fields, _base     │
               │  (materialized, not re-evaluated on read) │
               └──────────────────┬───────────────────────┘
                                  │
                                  ▼
           [In-and-Out Writeback] → External APIs
             (HTTP execution, 3-way merge, payload wrapping)
                                  │
                    writes ETL feedback rows to:
                    - _written_{mapping}
                    - _cluster_members_{mapping}
                                  │
                                  ▼ (pg-trickle CDC triggers fire)
               ┌──────────────────────────────────────────┐
               │  pg-trickle incremental refresh          │
               │  Stages 5–6 re-evaluate for affected     │
               │  rows only. Next read of _delta reflects │
               │  written-state noop suppression.         │
               └──────────────────────────────────────────┘
```

### What pg-trickle Does NOT Replace

pg-trickle is a database extension — it does not replace:

- **HTTP mechanics** (in-and-out ingestion: pagination, HWM, webhooks, auth)
- **3-way pre-flight GET** (in-and-out writeback conflict detection)
- **Performing ETL feedback writes** (writeback writes to `_written_` / `_cluster_members_` — pg-trickle only *detects* those writes)
- **Payload template wrapping** (`transform.template` in connector config — still in-and-out's responsibility)
- **API-level error handling, retries, dead-letter** (in-and-out writeback)

### Configuration Model (Three YAML Files)

With pg-trickle integrated, the configuration surface is:

| Config File | Owner | Declares |
|---|---|---|
| `osi-mapping.yaml` | OSI-Mapping | Business logic: identity rules, conflict strategies, filters, transforms, noop detection |
| `pgtrickle.sql` or dbt macros | pg-trickle | Stream table definitions wrapping OSI views; schedules; watermark groups; source gates |
| `connectors/*.yaml` | In-and-Out | HTTP mechanics: endpoints, auth, pagination, rate limits, payload templates |

The pg-trickle configuration is minimal — mostly `create_stream_table(name, '<OSI view query>', schedule => 'calculated')` calls, one per OSI view stage.

---

## 5. SQL Support Relevant to OSI-Mapping

### WITH RECURSIVE — Transitive Closure

OSI-Mapping's Stage 2 (`_id_{target}`) uses a `WITH RECURSIVE` CTE to compute entity clusters via transitive closure. This is non-trivial to differentiate.

pg-trickle supports `WITH RECURSIVE` in all three modes:
- **FULL**: Re-run the entire recursive CTE from scratch (always correct, expensive)
- **DIFFERENTIAL**: Semi-naive evaluation (INSERT-only path) and Delete-and-Rederive (DRed) for DELETE/UPDATE paths
- **IMMEDIATE**: Semi-naive evaluation bounded by `ivm_recursive_max_depth`

For the transitive closure use case in OSI, DIFFERENTIAL mode should work for most operations:
- A new `alice@example.com` email added to source A → semi-naive evaluation propagates the transitive links → only Alice's cluster is recomputed
- An email deleted from source A → DRed removes the affected links and recomputes only the disturbed cluster

As of v0.9.0, `WITH RECURSIVE` in DIFFERENTIAL mode covering DELETE/UPDATE (P2-1, the DRed path for `ChangeBuffer`) is partially implemented but deferred to complete in v0.10.0. In the meantime:
- INSERT-heavy workloads (new records): DIFFERENTIAL works
- DELETE/UPDATE-heavy workloads: AUTO mode falls back to FULL for the recursive CTE stages, then DIFFERENTIAL for non-recursive stages downstream

This needs to be tracked as a maturity concern for production adoption.

### Other Relevant SQL Features

All of the following required by OSI's views are supported in DIFFERENTIAL mode:

| OSI View Feature | pg-trickle Support |
|---|---|
| `WITH RECURSIVE` CTE (Stage 2) | ✅ DIFFERENTIAL (INSERT-path), FULL fallback for deletes until v0.10.0 |
| Multi-table JOINs (Stage 3–5) | ✅ INNER, LEFT, RIGHT, FULL OUTER |
| `COALESCE` / `CASE WHEN` (conflict resolution) | ✅ Full |
| `DISTINCT ON` / deduplication | ✅ Full |
| Window functions (analytics) | ✅ Full (partition-based recomputation) |
| `UNION ALL` (multi-source fan-in) | ✅ Full |
| Scalar subqueries + `EXISTS` | ✅ Full |
| Aggregate functions (GROUP BY) | ✅ Full (algebraic for COUNT/SUM/AVG/MIN/MAX since v0.9.0) |
| `LATERAL` joins | ✅ Full |
| Expression functions | ✅ Stable functions allowed; volatile rejected in DIFFERENTIAL |
| Views as sources | ✅ Auto-inlined as subqueries |

---

## 6. Operational Features

### Scheduling

```sql
-- Default: calculated schedule (derived from consumer refresh cycles)
SELECT pgtrickle.create_stream_table(
    '_delta_crm_contacts',
    'SELECT * FROM _osi_delta_crm_contacts'
    -- schedule defaults to 'calculated'
);

-- Explicit schedule
SELECT pgtrickle.create_stream_table(
    '_resolved_contact',
    '<OSI stage 3 query>',
    schedule => '30s'
);
```

With `schedule => 'calculated'`, leaf stream tables (those read by writeback) set the cadence, and upstream stages inherit the tightest downstream schedule. No manual coordination of OSI stages.

### Watermark-Gated ETL Coordination

```sql
-- Create watermark group for all source tables
SELECT pgtrickle.create_watermark_group(
    'ingestion_cycle',
    sources => ARRAY['inout_src_crm_contact', 'inout_src_sap_customer'],
    tolerance => '5 seconds'
);

-- In-and-Out Ingestion: after each complete sync cycle
SELECT pgtrickle.advance_watermark('inout_src_crm_contact', now());
SELECT pgtrickle.advance_watermark('inout_src_sap_customer', now());
-- OSI stream tables now refresh, knowing data is complete
```

### Bootstrap Source Gating

```sql
-- Before initial load or full re-sync
SELECT pgtrickle.gate_source('inout_src_crm_contact');

-- ... In-and-Out Ingestion runs full load ...

-- After load completes
SELECT pgtrickle.ungate_source('inout_src_crm_contact');
-- → single clean differential refresh of all downstream OSI stages
```

### Monitoring

```sql
-- Health check (returns OK/WARN/ERROR rows)
SELECT * FROM pgtrickle.health_check();

-- Refresh history and latency
SELECT * FROM pgtrickle.pgt_refresh_history ORDER BY completed_at DESC LIMIT 20;

-- Dependency tree (ASCII DAG view)
SELECT * FROM pgtrickle.dependency_tree();

-- Stream table status
SELECT name, status, last_refresh_at, rows_inserted, rows_deleted
FROM pgtrickle.pg_stat_stream_tables;
```

### dbt Integration

pg-trickle ships a first-class `dbt-pgtrickle` integration (available via `dbt-pgtrickle` Python package from v0.14.0+). Stream tables can be declared as dbt model materializations:

```yaml
# In dbt model config
materialized: stream_table
schedule: '30s'
refresh_mode: 'AUTO'
```

This is relevant because OSI-Mapping also has a dbt integration path. Both tools can be driven via a single `dbt run`.

---

## 7. Limitations and Caveats

### 7.1 PostgreSQL 18 Only

pg-trickle requires PostgreSQL 18. As of March 2026, PostgreSQL 18 is pre-release (or very recent GA). This is the most significant adoption constraint — most production PostgreSQL deployments are on 15, 16, or 17. PG 16/17 compatibility is planned for v0.12.0.

### 7.2 Early Release Status

v0.9.0 ships today (2026-03-20). The API is not stable-declared. Breaking changes between minor versions are possible until v1.0.0 (currently planned after v0.14.0). The team explicitly flags: *"not yet production-hardened."*

Mitigation: Start with non-critical integrations. Monitor the roadmap for v1.0.0.

### 7.3 WITH RECURSIVE DIFFERENTIAL Mode Partial Implementation

As described in §5, DELETE/UPDATE paths through `WITH RECURSIVE` CTEs fall back to FULL refresh until v0.10.0 completes P2-1 (DRed for ChangeBuffer). For OSI's transitive closure:
- **Insert-heavy** (adding new source records): DIFFERENTIAL works correctly
- **Delete/update-heavy** (removing or modifying existing sources): Falls back to FULL for Stage 2

In FULL fallback mode, Stage 2 recomputes from scratch — the same cost as today without pg-trickle. Performance gain only materializes for INSERT paths until v0.10.0.

### 7.4 No Direct DML on Stream Tables

Stream tables are managed exclusively by the pg-trickle refresh engine. Direct `INSERT/UPDATE/DELETE` on stream tables is rejected. This means:
- **In-and-Out Writeback must NOT write directly to `_delta_{mapping}`** or any other stream table
- All ETL feedback writes go to the underlying source tables (`_written_{mapping}`, `_cluster_members_{mapping}`) — these are regular tables, not stream tables
- pg-trickle detects those writes and propagates them forward

### 7.5 Foreign Keys on Stream Tables Not Supported

FK constraints referencing stream tables are not supported (bulk MERGE during refresh does not respect FK ordering). This is unlikely to be a practical issue for our architecture but is worth noting.

### 7.6 VOLATILE Functions in DIFFERENTIAL Mode

Functions marked `VOLATILE` (e.g., `random()`, `clock_timestamp()`) are rejected in DIFFERENTIAL mode. OSI expressions that use volatile functions must use FULL mode. Most OSI transforms (`expression` / `reverse_expression`) use deterministic SQL — this should not be an issue in practice.

---

## 8. Dependency on pg-trickle vs. Pure-OSI Mode

pg-trickle is an **optimization and automation layer**, not a logical requirement. The architecture works without it:

| Scenario | Without pg-trickle | With pg-trickle |
|---|---|---|
| Correctness | ✅ | ✅ |
| Small datasets (<100K rows) | ✅ Acceptable performance | ✅ Faster |
| Large datasets (>1M rows) | ⚠️ Slow (full recompute each cycle) | ✅ Fast (incremental) |
| ETL feedback automation | ⚠️ Requires external orchestration | ✅ Automatic |
| Refresh ordering | ⚠️ Manual scheduling | ✅ Automatic, topological |
| Watermark-gated coordination | ⚠️ Custom code required | ✅ Built-in |
| Production readiness | ✅ | ⚠️ Early release, PG18 only |

**Recommended phasing:**
1. **Phase 1 (development/early production):** Use plain OSI views, no pg-trickle. Validate the architecture, build connectors, integration-test the full pipeline. This is faster to start with.
2. **Phase 2 (scale):** Introduce pg-trickle when datasets grow beyond ~100K rows per source or external orchestration of the ETL feedback loop becomes burdensome. By then, pg-trickle will likely be closer to v1.0.0.

---

## 9. Impact on Existing Architecture Documents

### GOAL.md

Add pg-trickle as an optional infrastructure component to the architecture diagram. Key change: the OSI layer box becomes "OSI-Mapping YAML + pg-trickle (IVM engine)".

### CONFIG_DESIGN.md

Architecture Context section should note pg-trickle as the execution substrate for OSI views. No changes to the in-and-out connector config format.

### REPORT_OSI_MAPPING.md

- Section 6 (Architecture): Add pg-trickle layer to the diagram
- Section 14 (Recommendations): Add Rec #7 — "Adopt pg-trickle as the OSI execution substrate for incremental refresh"
- Section 16 (Risk Mitigation): Update Risk 4 (Performance) — pg-trickle is the explicit answer

---

## 10. Strategic Recommendations

### Rec 1: Use pg-trickle as the OSI Execution Substrate

When datasets scale beyond trivial size, replace plain OSI views with pg-trickle stream tables wrapping the same queries. The SQL interface is identical — writeback reads from `_delta_{mapping}` whether it's a view or a stream table. No connector config changes required.

### Rec 2: Implement Watermark-Gated Ingestion from Day One

Even without pg-trickle managing the OSI views, watermarks provide a clean ingestion→OSI→writeback pipeline signal. Design ingestion to call `advance_watermark()` after each cycle, even if the initial implementation doesn't enforce gating on the OSI side. This makes the transition to pg-trickle watermark groups trivial later.

### Rec 3: Treat Phase 1 (v0.1.x–v0.9.0) as Pre-Production

pg-trickle is moving fast (v0.9.0 released today, v0.1.0 was February 26, 2026 — less than a month of release history). Track the roadmap. Target pg-trickle v0.12.0+ for PG 16/17 compatibility and v1.0.0 for production adoption.

### Rec 4: Gate OSI Adoption on pg-trickle Availability for Large-Scale Scenarios

For single-source (non-MDM) use cases, plain OSI views with writeback are viable indefinitely. For multi-source MDM (5+ sources, >1M records), the transitive closure at Stage 2 will be the bottleneck — pg-trickle is the designed answer.

---

## 11. Summary

**pg-trickle is the execution engine that makes OSI-Mapping viable at production scale.** OSI-Mapping defines the logic (all of the bridge layer). pg-trickle makes that logic run efficiently (incremental, self-orchestrating, automatically triggered on change). In-and-Out handles the HTTP mechanics.

**Without pg-trickle:** OSI views are correct but O(all rows) per cycle. Requires external orchestration.  
**With pg-trickle:** OSI stream tables are correct and O(changed rows) per cycle. Self-orchestrating.

The three-project architecture is:

| Component | Role |
|---|---|
| **OSI-Mapping** | Declarative consolidation logic — identity, conflict, filters, transforms, noop detection |
| **pg-trickle** | Incremental view maintenance — makes OSI's pipeline efficient and self-orchestrating |
| **In-and-Out** | HTTP API connector — reliable ingestion and conflict-aware writeback |

All three are by the same team, designed to compose, and all live inside PostgreSQL.

---

## References

- pg-trickle repository: https://github.com/grove/pg-trickle/
- pg-trickle README: https://github.com/grove/pg-trickle/blob/main/README.md
- pg-trickle ESSENCE.md: https://github.com/grove/pg-trickle/blob/main/ESSENCE.md
- pg-trickle ROADMAP.md: https://github.com/grove/pg-trickle/blob/main/ROADMAP.md
- DBSP differential dataflow paper: https://arxiv.org/abs/2203.16684
- OSI-Mapping repository: https://github.com/BaardBouvet/OSI-mapping
- [REPORT_OSI_MAPPING.md](REPORT_OSI_MAPPING.md) — OSI-Mapping integration analysis
- [GOAL.md](GOAL.md) — Project goals and requirements
- [CONFIG_DESIGN.md](CONFIG_DESIGN.md) — Configuration design
