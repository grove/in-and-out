# Plan: pg-trickle Mechanisms for MDM System Onboarding

> **Date:** March 26, 2026
> **Status:** Draft
> **Scope:** Upstream feature requests and integration patterns for pg-trickle to support safe onboarding of new source systems into an in-and-out + OSI-mapping MDM pipeline

---

## 1. Problem Statement

### The Scenario

An MDM pipeline built from three components:

```
External APIs → [in-and-out Ingestion] → PostgreSQL source tables
                                              ↓
                              [OSI-Mapping Engine via pg-trickle stream tables]
                                              ↓
                     [in-and-out Writeback] ← delta stream tables
```

OSI-Mapping's six-stage view pipeline (forward → identity → resolution → analytics → reverse → delta) is materialised as pg-trickle stream tables. The `_id_{target}` stream table uses `WITH RECURSIVE` for transitive-closure identity resolution. The `_delta_{mapping}` stream tables drive writeback to external systems.

The pipeline works correctly with N systems in steady state. The problem arises when system N+1 is added.

### What Goes Wrong

Adding a new source system to a running pipeline creates three compounding hazards:

#### Hazard 1: Mass writeback from empty baseline

When `inout_src_system3_contacts` is added to the OSI-Mapping YAML (via `ALTER QUERY` on the relevant stream tables), the `_delta_system3_contacts` stream table classifies **every row in the golden record** as a delta against system 3, because system 3 has no prior baseline (`_base` is NULL for all system 3 rows). If writeback is active, this triggers a mass write to system 3 — potentially creating thousands of duplicate records or overwriting existing data.

#### Hazard 2: Transitive identity cluster re-merges

Adding system 3 may reveal that two records from systems 1 and 2 — currently in separate identity clusters — are actually the same entity. System 3 has an email address or tax ID that bridges them. The `_id_{target}` stream table's connected-components algorithm merges these clusters, which produces new delta rows for systems 1 and 2, not just system 3. The blast radius extends beyond the system being onboarded.

#### Hazard 3: Partial-data identity resolution

If the `_id_{target}` stream table refreshes while system 3's initial ingestion is still in progress (e.g., halfway through pagination), identity resolution runs against incomplete data. Clusters that _will_ merge once all data arrives haven't merged yet, so the interim golden record is wrong. Worse, the delta stream tables may emit writeback actions based on this transient state.

### The Invariant

**No delta row for system 3 should be emitted until system 3's initial load is complete.** Whether delta emission for systems 1 and 2 is also paused during this window is an operator policy choice — the two modes have different tradeoffs, described in Section 4.

### The Ordering Requirement

The safe onboarding sequence is:

1. **Ingest** — populate `inout_src_system3_*` tables completely (in-and-out handles this via `ingestion_polling_readonly` profile)
2. **Gate** — prevent pg-trickle from refreshing downstream stream tables until ingestion is done
3. **Resolve** — allow identity resolution to run across all systems (refresh `_id_{target}`)
4. **Inspect** — operator reviews cluster diffs and dry-run writeback
5. **Enable** — ungating allows delta propagation and writeback

Steps 2–5 require mechanisms in pg-trickle. Some exist today; others need to be added.

---

## 2. What pg-trickle Already Provides

### 2.1 Bootstrap Source Gating (v0.5.0)

**Directly applicable.** The `gate_source()` / `ungate_source()` API was designed for exactly this pattern: pause scheduler-driven refreshes for all stream tables that depend on a given source table while it is being bulk-loaded.

```sql
-- Before in-and-out starts ingesting system 3:
SELECT pgtrickle.gate_source('inout_src_system3_contacts');
SELECT pgtrickle.gate_source('inout_src_system3_companies');

-- in-and-out runs full initial ingestion...

-- After ingestion completes:
SELECT pgtrickle.ungate_source('inout_src_system3_contacts');
SELECT pgtrickle.ungate_source('inout_src_system3_companies');
```

**What it solves:** Hazard 3 (partial-data identity resolution). The `_id_{target}` stream table will not refresh until all system 3 source tables are ungated.

**What it does NOT solve:** Once ungated, _all_ downstream stream tables refresh at once — including the `_delta_system3_*` tables. There is no mechanism to allow identity resolution to complete while still suppressing delta emission. This means the operator cannot inspect cluster diffs before writeback deltas are produced.

### 2.2 Watermark Gating (v0.7.0)

**Complementary to source gating.** Watermark groups enforce temporal alignment across sources. in-and-out could advance watermarks per source table after each sync run, and a watermark group ensures that downstream stream tables only refresh when all sources are caught up.

```sql
-- Create a group requiring all three systems to be aligned:
SELECT pgtrickle.create_watermark_group(
    'mdm_contact_pipeline',
    ARRAY['inout_src_system1_contacts', 'inout_src_system2_contacts', 'inout_src_system3_contacts'],
    60  -- 60-second tolerance
);

-- in-and-out advances watermarks after each sync run:
SELECT pgtrickle.advance_watermark('inout_src_system3_contacts', '2026-03-26 12:00:00+00');
```

**What it solves:** Ongoing temporal alignment after onboarding completes. Prevents the pipeline from processing system 3 changes from 12:05 PM when systems 1 and 2 are still at 12:00 PM.

**What it does NOT solve:** Same limitation as source gating — no selective refresh suppression. Also, watermark groups require all sources to _exist_ and have watermarks before gating can work, which creates a chicken-and-egg problem during initial setup.

### 2.3 Tiered Scheduling

**Marginally useful.** Setting the delta stream tables to `frozen` tier while the identity stream table is `hot` could approximate selective suppression, but it is coarse-grained and requires manual tier toggling.

### 2.4 ALTER QUERY with Deferred Initialization

**Applicable for the initial hookup.** When adding system 3's source tables to the stream table DAG:

```sql
SELECT pgtrickle.alter_stream_table(
    '_id_contact',
    query => '... WITH RECURSIVE ... including inout_src_system3_contacts ...',
    initialize => false  -- don't populate yet
);
```

This prevents the altered stream table from immediately running a full refresh.

### 2.5 Diamond Dependency Consistency

**Naturally applicable.** The OSI-Mapping pipeline has diamond-shaped dependencies (multiple forward views feed into a single identity view). Atomic diamond refresh ensures the identity view sees a consistent snapshot of all forward views.

---

## 3. Gaps and Proposed Mechanisms

### Gap 1: Selective Refresh Suppression (Delta Gating)

**Problem:** Source gating and watermark gating are all-or-nothing. Once the gate is lifted, every stream table in the DAG refreshes — including the `_delta_*` tables that drive writeback. The operator needs identity resolution to complete _before_ deltas are emitted, so they can inspect cluster diffs.

**Proposed mechanism: `gate_stream_table()` / `ungate_stream_table()`**

A per-stream-table gate that prevents the scheduler from refreshing a specific stream table, independent of its sources. Unlike tiered scheduling (`frozen`), this is an explicit operational control with audit trail, not a long-term scheduling policy.

```sql
-- Gate delta stream tables but NOT the identity/resolution tables:
SELECT pgtrickle.gate_stream_table('_delta_system1_contacts');
SELECT pgtrickle.gate_stream_table('_delta_system2_contacts');
SELECT pgtrickle.gate_stream_table('_delta_system3_contacts');

-- Ungate sources — identity resolution runs through to _resolved_*
SELECT pgtrickle.ungate_source('inout_src_system3_contacts');

-- Operator inspects clusters, runs dry-run writeback...

-- Then enables delta flow:
SELECT pgtrickle.ungate_stream_table('_delta_system1_contacts');
SELECT pgtrickle.ungate_stream_table('_delta_system2_contacts');
SELECT pgtrickle.ungate_stream_table('_delta_system3_contacts');
```

**Implementation notes:**
- Leverages existing `pgt_source_gates` pattern — add a `pgt_stream_table_gates` catalog table
- Gated stream tables are skipped by the scheduler but can still be refreshed manually via `refresh_stream_table()` (matching source gate semantics)
- `bootstrap_gate_status()` extended (or new `stream_table_gate_status()`) to show affected downstream tables
- This is distinct from `SUSPENDED` status because SUSPENDED implies an error condition and resets consecutive errors on resume; gating is an intentional operational hold

**Alternative considered:** Using `SUSPENDED` status. Rejected because SUSPENDED is designed for error recovery (resets consecutive_errors), and because it conflates operational holds with error states in monitoring.

### Gap 2: Cluster Change Metrics

**Problem:** After system 3 is ingested and identity resolution re-runs, the operator needs to understand what changed in the identity graph. Today, the only way to inspect this is to write bespoke SQL queries against the `_id_{target}` stream table before and after. `pg_stat_stream_tables` reports `rows_inserted` and `rows_deleted` but not cluster-level metrics.

**Proposed mechanism: Per-refresh cluster change counters in `pgt_refresh_history`**

For stream tables whose defining query contains a `WITH RECURSIVE` that produces a `_cluster_id` column (the OSI-Mapping identity pattern), track additional metrics per refresh:

| Column | Type | Description |
|---|---|---|
| `clusters_created` | `bigint` | New clusters that didn't exist before this refresh |
| `clusters_merged` | `bigint` | Previously distinct clusters that were unified (two or more clusters collapsed into one) |
| `clusters_split` | `bigint` | Previously unified clusters that separated |
| `rows_reparented` | `bigint` | Rows that moved from one cluster to another |

**Why this matters for onboarding:** When system 3 is added, the operator expects to see `clusters_merged > 0` (system 3 bridges previously separate entities) and needs to know the magnitude. If `clusters_merged` is 5, that's normal. If it's 5,000, something is wrong with the identity rules.

**Implementation approach:**
- The DVM engine already knows which rows were inserted and deleted during a differential refresh
- Cluster merge detection: a row that was deleted from cluster A and inserted into cluster B in the same refresh represents a cluster merge
- These metrics are computed from the delta itself — no additional queries needed
- Store in `pgt_refresh_history` alongside existing `rows_inserted` / `rows_deleted`
- Expose via `pgtrickle.refresh_timeline()` for monitoring dashboards

**Scope note:** This is an opt-in feature. Stream tables must declare a `cluster_column` hint (or pg-trickle infers it from `WITH RECURSIVE` output columns named `_cluster_id` or `cluster_id`). Stream tables without cluster semantics report NULL for these columns.

### Gap 3: Staged ALTER QUERY Activation

**Problem:** When adding system 3's source tables to the identity stream table, `ALTER QUERY` immediately makes the new query effective. Combined with source gating, this works — but requires the operator to manually coordinate gating, ALTER QUERY, and ungating in the correct sequence. A mistake (e.g., forgetting to gate before ALTER QUERY) allows the new query to run against empty source tables.

**Proposed mechanism: `pending_query` on `alter_stream_table()`**

Allow `ALTER QUERY` to accept a new query that is stored but not activated until an explicit condition is met:

```sql
-- Store the new query but don't activate it yet:
SELECT pgtrickle.alter_stream_table(
    '_id_contact',
    pending_query => '... WITH RECURSIVE ... including inout_src_system3_contacts ...'
);

-- The old query (without system 3) continues running.
-- in-and-out completes initial ingestion of system 3...

-- Activate the pending query (triggers a FULL refresh with the new query):
SELECT pgtrickle.activate_pending_query('_id_contact');

-- Or cancel it:
SELECT pgtrickle.cancel_pending_query('_id_contact');
```

**Implementation notes:**
- Add `pending_query` TEXT column to `pgt_stream_tables`
- `activate_pending_query()` is equivalent to `alter_stream_table(query => pending_query)` followed by clearing the pending_query column
- No automatic activation condition (keeping it simple) — the explicit function call is the trigger
- `pgt_status()` extended to show `has_pending_query` boolean
- This prevents the "forgot to gate before ALTER QUERY" mistake by separating the two operations

**Alternative considered:** `pending_until` condition-based activation (e.g., `pending_until => 'inout_src_system3=ready'`). Rejected as over-engineered for v1 — the explicit activation function is simpler, more auditable, and avoids adding a condition language to pg-trickle.

### Gap 4: Delta Suppression by Source Attribution

**Problem:** After identity resolution completes, the `_delta_*` stream tables emit changes for _all_ systems, not just system 3. Delta rows for systems 1 and 2 are emitted because the cluster re-merges changed the golden record. The operator may want to:
- Allow deltas for systems 1 and 2 (they reflect real changes from the re-merge)
- Suppress deltas for system 3 (because system 3 has no baseline yet — everything looks like an insert)
- Or the reverse: suppress systems 1 and 2 deltas (don't touch existing systems while reviewing) and only allow system 3

**Proposed mechanism: Per-source delta suppression via `_delta_*` stream table configuration**

This is more of an OSI-Mapping concern than a pg-trickle concern. OSI-Mapping's `_delta_{mapping}` view already classifies changes per source. The simplest implementation is to allow the bridge layer (or in-and-out's writeback daemon) to filter delta rows by a `_suppressed` flag.

However, if delta suppression should happen _inside_ pg-trickle (i.e., the stream table itself doesn't emit suppressed rows), the mechanism would be:

```sql
-- Suppress delta emission for a specific source mapping:
SELECT pgtrickle.alter_stream_table(
    '_delta_system3_contacts',
    tier => 'frozen'    -- existing mechanism, coarse but effective
);
```

**Recommendation:** Use the existing `frozen` tier for coarse per-stream-table suppression (since each `_delta_{mapping}` is its own stream table). Reserve the per-source delta suppression for the OSI-Mapping or bridge layer, not pg-trickle.

---

## 4. Integration Pattern: Full Onboarding Workflow

There are two modes, chosen based on how much confidence the operator has in the identity rules and how much downtime existing systems can tolerate.

### Mode A: Zero-downtime (recommended when identity rules are trusted)

Systems 1 and 2 keep flowing throughout. Only system 3's delta stream table is gated.

The key insight: source gating on system 3's tables means the `_id_{target}` stream table never sees system 3's data during the initial load window, so identity resolution continues to run on exactly the same graph as before. No cluster re-merges fire while the gate is active. Systems 1 and 2 experience no interruption.

Once the gate is lifted and identity resolution re-runs with system 3's data, any cluster re-merges that affect systems 1 and 2 flow through immediately as legitimate writeback changes — because they _are_ real changes: system 3 revealed that two records already in the pipeline represent the same entity.

```
Phase 1: Prepare
├── Gate system 3 source tables only (existing: gate_source)
├── Gate _delta_system3_* stream tables only (NEW: gate_stream_table)
├── Store pending query for _id_* and downstream STs (NEW: pending_query)
└── Deploy system 3 connector in ingestion_polling_readonly mode (in-and-out)

Phase 2: Initial Load
├── in-and-out runs full ingestion for system 3
├── Source tables populate: inout_src_system3_*
├── pg-trickle scheduler is idle for system 3 sources only (gated)
└── Systems 1 and 2 continue operating with ZERO interruption

Phase 3: Activate Identity Resolution
├── Ungate system 3 source tables (existing: ungate_source)
├── Activate pending queries (NEW: activate_pending_query)
├── pg-trickle refreshes _fwd_*, _id_*, _resolved_*, {target}
├── Systems 1 and 2 delta tables refresh — re-merge deltas flow to writeback
├── _delta_system3_* is still gated — system 3 writeback suppressed
└── Monitor cluster change metrics (NEW: clusters_merged in refresh_timeline)

Phase 4: Review system 3 only
├── Operator queries _id_{target} for cluster diffs involving system 3
├── Operator uses in-and-out dry-run writeback to preview system 3 changes
├── If clusters_merged is unexpected, investigate identity rules
└── Decision: proceed or roll back

Phase 5: Enable System 3 Writeback
├── Ungate _delta_system3_* stream tables
└── System 3 is now fully online
```

### Mode B: Conservative (use when identity rules are new or untrusted)

All delta stream tables are gated until the operator has reviewed the full impact of the re-merge. Systems 1 and 2 writeback is paused for the duration of the review window.

Use this mode when: the cluster re-merges from system 3 might be large or unexpected, when identity rules have not been validated against production data, or when changes to systems 1 and 2 require explicit sign-off before execution.

```
Phase 1: Prepare
├── Gate system 3 source tables (existing: gate_source)
├── Gate ALL _delta_* stream tables (NEW: gate_stream_table)
├── Store pending query for _id_* and downstream STs (NEW: pending_query)
└── Deploy system 3 connector in ingestion_polling_readonly mode (in-and-out)

Phase 2: Initial Load
├── in-and-out runs full ingestion for system 3
├── Source tables populate: inout_src_system3_*
├── pg-trickle scheduler is idle for system 3 sources (gated)
└── Systems 1 and 2 delta tables are gated — writeback is paused

Phase 3: Activate Identity Resolution
├── Ungate system 3 source tables (existing: ungate_source)
├── Activate pending queries (NEW: activate_pending_query)
├── pg-trickle refreshes _fwd_*, _id_*, _resolved_*, {target}
├── All _delta_* tables are still gated — no writeback occurs for any system
└── Monitor cluster change metrics (NEW: clusters_merged in refresh_timeline)

Phase 4: Review all systems
├── Operator queries _id_{target} for all cluster diffs
├── Operator uses in-and-out dry-run writeback for all three systems
├── If clusters_merged is unexpected, investigate identity rules
└── Decision: proceed or roll back (cancel pending queries, re-gate)

Phase 5: Enable Writeback
├── Ungate _delta_system1_* and _delta_system2_* first
├── Monitor writeback results for existing systems
├── Ungate _delta_system3_* stream tables
└── System 3 is now fully online
```

### Rollback Path (both modes)

If review reveals problems:

```sql
-- Roll back: revert to the original identity query
SELECT pgtrickle.alter_stream_table(
    '_id_contact',
    query => '... original query without system 3 ...'
);
-- Or if using pending_query: SELECT pgtrickle.cancel_pending_query('_id_contact');

-- Re-gate system 3 sources
SELECT pgtrickle.gate_source('inout_src_system3_contacts');

-- Restore normal operation for systems 1 and 2 (Mode B only)
SELECT pgtrickle.ungate_stream_table('_delta_system1_contacts');
SELECT pgtrickle.ungate_stream_table('_delta_system2_contacts');

-- Investigate identity rules, fix osi-mapping.yaml, retry from Phase 1
```

---

## 5. Priority and Sequencing

| Priority | Mechanism | New/Existing | Scope | Rationale |
|---|---|---|---|---|
| **P0** | Source gating for initial load protection | Existing (v0.5.0) | — | Already available. Only needs documentation for MDM onboarding pattern. |
| **P0** | Watermark gating for steady-state alignment | Existing (v0.7.0) | — | Already available. in-and-out should publish watermarks after each sync run. |
| **P1** | Stream table gating (`gate_stream_table`) | **New** | pg-trickle | Critical for separating identity resolution from delta emission. Without this, operators cannot review cluster diffs before writeback fires. |
| **P2** | Cluster change metrics | **New** | pg-trickle | Important for operational visibility during onboarding. Without it, operators are flying blind on the impact of adding a new system. |
| **P3** | Pending query activation | **New** | pg-trickle | Nice-to-have safety net. The same outcome can be achieved by manually coordinating gate_source + alter_stream_table, but this makes the sequencing less error-prone. |
| **—** | Delta suppression by source | Not pg-trickle | OSI-Mapping / bridge | Better handled at the application layer. Use frozen tier for coarse stream table suppression. |

---

## 6. What in-and-out Must Do (Regardless of pg-trickle Changes)

Even with all proposed pg-trickle mechanisms, in-and-out has its own responsibilities:

1. **Publish watermarks** — after each sync run completes, call `pgtrickle.advance_watermark()` with the sync run's high-water mark timestamp. This is the integration point between in-and-out's ingestion daemon and pg-trickle's watermark gating.

2. **Gate sources on connector deployment** — when a new connector is deployed in `ingestion_polling_readonly` mode, in-and-out should call `gate_source()` for all source tables that connector will populate. The ungate happens after the first full sync completes.

3. **Control table integration** — expose the onboarding workflow as control table commands:
   ```sql
   -- Zero-downtime mode: only suppress system 3 deltas
   INSERT INTO inout_ops_control (connector, command, params)
   VALUES ('system3', 'begin_onboarding', '{"mode": "zero_downtime"}');

   -- Conservative mode: suppress all delta tables during review
   INSERT INTO inout_ops_control (connector, command, params)
   VALUES ('system3', 'begin_onboarding', '{"mode": "conservative"}');
   ```
   This orchestrates the multi-step sequence (gate sources, gate deltas, start ingestion) as a single operator action. The `mode` parameter controls which delta tables are gated.

4. **Dry-run writeback includes cluster context** — when the operator runs dry-run writeback during Phase 4, the output should include `cluster_id` and a flag indicating whether the cluster was affected by the re-merge (new members from system 3).

---

## 7. Open Questions

1. **Should `gate_stream_table()` propagate downstream?** If `_delta_system3_contacts` is gated, should stream tables that depend on it also be implicitly gated? Source gating propagates downstream (any ST that reads the gated source is paused). Stream table gating could follow the same convention, or it could be non-propagating (only the named ST is gated). Non-propagating is simpler and sufficient for the MDM use case.

2. **Cluster change metrics: opt-in or automatic?** Tracking cluster merges requires knowing which column is the cluster ID. pg-trickle could infer this from `WITH RECURSIVE` output, or it could require an explicit hint. The hint approach is simpler and avoids false positives.

3. **Should pending_query support cascading activation?** When the `_id_contact` query changes, downstream stream tables (`_resolved_contact`, `_delta_*`) may also need query changes (they reference the identity view). Should `activate_pending_query()` activate all pending queries in topological order? Or should each be activated individually?

4. **Watermark integration with in-and-out's sync_run table** — in-and-out already records sync runs with timestamps in `inout_ops_sync_run`. Should watermark advancement be automatic (tied to sync run completion) or explicit (in-and-out calls `advance_watermark()`)? Explicit is safer but requires in-and-out to be aware of pg-trickle.
