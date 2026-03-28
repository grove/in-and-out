# Evaluation: Shadow Mode Onboarding

> **Date:** March 27, 2026
> **Status:** Evaluation
> **Scope:** Evaluate a shadow mode (dual-write detection) approach for onboarding new source systems into an in-and-out + OSI-mapping MDM pipeline backed by pg-trickle stream tables
> **Related:** [PLAN_ONBOARDING_PROPOSAL.md](PLAN_ONBOARDING_PROPOSAL.md) (gating-based approach), [PLAN_ONBOARDING_BLUE_GREEN.md](PLAN_ONBOARDING_BLUE_GREEN.md) (blue/green approach)

---

## 1. Core Idea

Instead of a discrete cutover moment where system 3 goes from "not in the pipeline" to "fully live," shadow mode introduces a continuous observation phase. System 3 is integrated into the production identity resolution pipeline but its writeback output is intercepted and compared against reality rather than executed.

The pipeline computes what _would_ be written to system 3. An observer process fetches what system 3 _actually_ contains. The two are compared. Discrepancies reveal genuine sync gaps. Noops confirm alignment. Only when the shadow delta converges to a stable, understood set of changes does the operator (or an automated threshold) switch system 3 to live writeback.

```
                    ┌─────────────────────────────────┐
  System 1 API ──→ │  inout_src_system1_contacts       │
  System 2 API ──→ │  inout_src_system2_contacts       │
  System 3 API ──→ │  inout_src_system3_contacts       │  ← ingestion active
                    │              ↓                    │
                    │  _id_contact (all 3 systems)      │
                    │  _resolved_contact                │
                    │              ↓                    │
                    │  _delta_system1 → live writeback  │
                    │  _delta_system2 → live writeback  │
                    │  _delta_system3 → SHADOW TABLE    │  ← intercepted
                    └───────────────┬──────────────────┘
                                    │
                    ┌───────────────▼──────────────────┐
                    │       Shadow Observer             │
                    │                                   │
                    │  For each shadow delta row:        │
                    │  1. Fetch current state from       │
                    │     system 3 API (GET)             │
                    │  2. Compare shadow desired-state   │
                    │     vs actual state                │
                    │  3. Classify: aligned / gap /      │
                    │     conflict / noop                │
                    │  4. Record result                  │
                    └───────────────────────────────────┘
```

---

## 2. Detailed Design

### 2.1 Pipeline Integration

System 3 is added to the live OSI-Mapping pipeline normally. Source tables, forward views, identity resolution, and conflict resolution all include system 3 from the start. This means:

- Identity resolution runs across all three systems.
- Cluster re-merges happen immediately.
- The golden record reflects all three systems.
- Delta views for systems 1 and 2 may emit re-merge deltas — these flow to live writeback (they are real changes).
- Delta views for system 3 emit deltas — but these are **intercepted** instead of executed.

This is the key architectural distinction from the gating approach: the production pipeline _does_ include system 3 from the start. Only the writeback execution for system 3 is suppressed.

### 2.2 The Shadow Table

Instead of materialising `_delta_system3_contacts` into the standard `inout_dst_system3_contacts` desired-state table, the delta is materialised into a **shadow table** with additional comparison columns:

```sql
CREATE TABLE inout_shadow_system3_contacts (
    -- Standard delta columns (from _delta_system3_contacts)
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    action          TEXT NOT NULL,           -- insert / update / delete / noop
    cluster_id      TEXT,
    external_id     TEXT,
    data            JSONB NOT NULL,          -- desired state from golden record
    base            JSONB,                   -- source snapshot at last ingestion

    -- Shadow comparison columns
    actual_data     JSONB,                   -- fetched from system 3 API
    actual_fetched_at TIMESTAMPTZ,           -- when the API fetch occurred
    comparison      TEXT,                    -- classification (see below)
    diff            JSONB,                   -- field-level diff (desired vs actual)
    shadow_run_id   UUID,                    -- groups comparison runs

    -- Operational
    delta_created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    compared_at     TIMESTAMPTZ,
    _status         TEXT NOT NULL DEFAULT 'pending'  -- pending → compared → resolved
);
```

### 2.3 Shadow Observer Process

A new in-and-out daemon mode (or a periodic control table command) that processes shadow table rows:

```
For each row in inout_shadow_system3_contacts WHERE _status = 'pending':
  1. If action = 'insert':
     - Search system 3 API by identity fields (email, tax_id, etc.)
     - If found: comparison = 'already_exists' (may need update, not insert)
     - If not found: comparison = 'genuine_insert'

  2. If action = 'update':
     - GET system 3 API by external_id
     - Compare data (desired) vs actual_data (from API)
     - If identical: comparison = 'aligned' (noop — system 3 already has correct data)
     - If different: comparison = 'gap' (genuine sync needed)
     - If API returns different from both base and data: comparison = 'conflict'
       (system 3 was modified outside the pipeline)

  3. If action = 'delete':
     - GET system 3 API by external_id
     - If 404: comparison = 'already_deleted'
     - If exists: comparison = 'genuine_delete'

  4. If action = 'noop':
     - comparison = 'noop' (skip — no work needed)

  5. Store actual_data, diff, comparison. Set _status = 'compared'.
```

### 2.4 Comparison Classifications

| Classification | Meaning | Expected During Onboarding? | Action When Going Live |
|---|---|---|---|
| `aligned` | System 3 already has the correct data | Yes — common if system 3 was manually synced before | No writeback needed |
| `genuine_insert` | Record exists in golden record but not in system 3 | Yes — if system 3 is new and has less data | Will be inserted on go-live |
| `genuine_update` | Record exists in both but system 3 has stale data | Yes — if system 3 wasn't previously synced | Will be updated on go-live |
| `genuine_delete` | Record should be removed from system 3 | Rare during onboarding | Will be deleted on go-live |
| `already_exists` | An insert was planned but the record already exists | Yes — if system 3 has overlapping data | Reclassify as update or noop |
| `already_deleted` | A delete was planned but the record is already gone | Rare | No action needed |
| `conflict` | System 3 has data that differs from both base and desired | Maybe — indicates external modifications | Requires conflict resolution policy |
| `noop` | No change needed | Yes — most rows should be noops in steady state | Skip |

### 2.5 Convergence Metrics

The shadow observer computes aggregate metrics per shadow run:

```sql
SELECT
    comparison,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM inout_shadow_system3_contacts
WHERE shadow_run_id = $1
GROUP BY comparison
ORDER BY count DESC;
```

Example output during onboarding:

| comparison | count | pct |
|---|---|---|
| aligned | 8,423 | 84.23% |
| genuine_update | 1,200 | 12.00% |
| genuine_insert | 350 | 3.50% |
| conflict | 25 | 0.25% |
| noop | 2 | 0.02% |

Over successive shadow runs, the operator watches for convergence:

- `aligned` should grow toward 100% as the pipeline stabilises
- `genuine_update` should shrink as ingestion catches up
- `conflict` should be investigated individually
- When the shadow reaches an acceptable threshold (e.g., >95% aligned + noop), the system is ready

### 2.6 Go-Live Transition

When the operator approves (or an automated threshold triggers):

```sql
-- 1. Switch system 3 from shadow to live writeback
INSERT INTO inout_ops_control (connector, command, params)
VALUES ('system3', 'enable_writeback', '{"from_shadow": true}');
```

The transition:
1. Freezes the shadow table (no new shadow rows).
2. Materialises `_delta_system3_contacts` into the standard `inout_dst_system3_contacts` table.
3. Processes only `genuine_insert`, `genuine_update`, and `genuine_delete` rows — skips `aligned` and `noop`.
4. Enables the normal writeback daemon for system 3.
5. Archives the shadow table for audit.

**The first live writeback run is smaller than expected.** Because the shadow comparison identified which records are already `aligned`, they are filtered out. The go-live only processes actual gaps — typically a much smaller set than the full golden record delta.

---

## 3. Handling Hazards

### Hazard 1: Mass writeback from empty baseline

**Eliminated.** The shadow mode explicitly compares each delta row against system 3's actual state before executing anything. Rows classified as `aligned` are never written. The mass-writeback scenario cannot occur because the observer verifies every row before it reaches the writeback daemon.

### Hazard 2: Transitive identity cluster re-merges

**Controlled.** Cluster re-merges affect systems 1 and 2's delta tables immediately — those deltas flow to live writeback because they are legitimate business changes. System 3's deltas go to the shadow table. The operator sees re-merge impact in the shadow comparison metrics (`genuine_update` count) and can assess whether the identity rules are correct before enabling writeback.

### Hazard 3: Partial-data identity resolution

**Mitigated by source gating.** During system 3's initial ingestion, source gating (existing pg-trickle feature) prevents `_id_contact` from refreshing with partial data. Once ingestion completes and the gate lifts, identity resolution runs once against the full dataset. This is the same mitigation as the gating approach — shadow mode inherits it.

---

## 4. Advantages

| Advantage | Detail |
|---|---|
| **Continuous, not discrete** | Unlike gating (point-in-time review) or blue/green (binary cutover), shadow mode provides continuous convergence visibility. The operator sees the system approaching readiness over time. |
| **No storage duplication** | Unlike blue/green, shadow mode uses the production pipeline. Only the shadow table is additional — and it's per-system, not per-pipeline. |
| **Self-calibrating go-live** | The shadow comparison itself determines readiness. Instead of the operator guessing "is it safe?", the metrics show alignment percentage objectively. |
| **Handles pre-existing data gracefully** | If system 3 already has records (from a prior manual sync or another integration), the shadow classification identifies them as `aligned` rather than blindly overwriting. This is the biggest practical advantage over the other approaches. |
| **The first writeback is minimal** | Go-live only processes genuine gaps, not the full golden record. This dramatically reduces the blast radius of enabling system 3. |
| **Natural pilot mode** | Shadow mode can run indefinitely. Some organisations may want to shadow for weeks before enabling writeback, especially for regulated industries. |
| **Audit trail** | The shadow table is a complete record of what the pipeline _would_ have done. This is useful for compliance and debugging. |

---

## 5. Disadvantages

| Disadvantage | Detail | Severity |
|---|---|---|
| **API cost** | The shadow observer makes read-only API calls to system 3 for every shadow delta row. For 10K rows, that's 10K GET requests. | Medium — rate-limit-aware, but adds API load. |
| **Latency to go-live** | Shadow mode requires at least one full comparison run before go-live. For large datasets, this takes time. | Low — the comparison can be parallelised and rate-limited. |
| **Complexity** | Shadow mode adds a new daemon mode, a new table schema, and a new comparison classification system. This is more complex than gating (which uses existing pg-trickle primitives). | High — non-trivial implementation. |
| **Identity matching for inserts** | When the shadow classifies an `insert`, it must search system 3's API by identity fields (not external_id, which is unknown for new records). This requires configurable search logic per connector. | Medium — requires connector YAML to declare search endpoints. |
| **Re-merge deltas hit systems 1 and 2 immediately** | Because system 3 is in the production pipeline from the start, cluster re-merges affect systems 1 and 2's writeback immediately. If the re-merge is wrong, it's already been executed. | High — **solved by extended shadow mode** (§10), which shadows all three systems with in-database comparison for systems 1 and 2, requiring no additional API calls. Can also be mitigated by combining with delta gating (§9). |
| **Stale comparisons** | If the shadow run takes hours and system 3's API data changes during that time, the `actual_data` snapshot is stale by the time the operator reviews it. | Low — shadow runs can be re-executed. |

---

## 6. Implementation Requirements

### 6.1 New in-and-out Components

| Component | Description | Scope |
|---|---|---|
| Shadow table schema | Per-connector shadow table with comparison columns | Migration |
| Shadow observer daemon | New run mode that fetches actual state from API and classifies deltas | Engine |
| Shadow comparison logic | Per-action classification (aligned, gap, conflict, etc.) | Engine |
| Shadow convergence metrics | Aggregate comparison stats per run | Observability |
| Shadow → live transition | Control table command to promote from shadow to live | Engine |
| Shadow table archival | Move completed shadow tables out of the hot path | Migration |

### 6.2 Connector YAML Extensions

```yaml
# In the connector YAML for system 3:
onboarding:
  shadow:
    enabled: true
    comparison_endpoint:
      # How to look up a record by external_id for comparison
      path: "/contacts/{external_id}"
      method: GET
    search_endpoint:
      # How to search for a record by identity fields (for insert classification)
      path: "/contacts/search"
      method: POST
      identity_fields: [email]
    convergence:
      # Auto-promote threshold
      aligned_pct: 95
      max_conflicts: 10
      auto_promote: false  # require manual approval
```

### 6.3 pg-trickle Requirements

| pg-trickle Feature | Required? | Existing? |
|---|---|---|
| Source gating (for initial load) | Yes | Yes (v0.5.0) |
| Stream table gating (for delta interception) | Helpful but not required | No (proposed in PLAN_ONBOARDING_PROPOSAL.md) |
| Stream table for shadow materialisation | Yes — the shadow table is populated by a stream table that reads from `_delta_system3_*` | Yes — standard stream table |
| Tiered scheduling for shadow table | Helpful — run shadow comparison at lower priority | Yes (v0.7.0) |

**New pg-trickle features needed:** None strictly required. The stream table gating from the proposal plan would simplify the shadow interception, but it can be achieved by routing `_delta_system3_*` to the shadow table via the bridge layer instead of `inout_dst_system3_*`.

### 6.4 Shadow Table as a pg-trickle Stream Table

The shadow table itself can be a pg-trickle stream table over the delta view:

```sql
SELECT pgtrickle.create_stream_table(
    'inout_shadow_system3_contacts',
    $$SELECT
        action, cluster_id, external_id, data, base,
        NULL::jsonb AS actual_data,
        NULL::timestamptz AS actual_fetched_at,
        'pending'::text AS comparison,
        NULL::jsonb AS diff,
        NULL::uuid AS shadow_run_id,
        NOW() AS delta_created_at,
        NULL::timestamptz AS compared_at,
        'pending'::text AS _status
      FROM _delta_system3_contacts
      WHERE action != 'noop'$$,
    schedule => '5m',
    refresh_mode => 'DIFFERENTIAL'
);
```

However, stream tables do not support direct DML (the shadow observer needs to UPDATE comparison results). A better design is:

1. **Stream table** materialises the delta (read-only, refreshed by pg-trickle).
2. **Shadow comparison table** is a regular table that the observer writes to.
3. The observer joins the stream table (what should change) with its own API fetch results (what system 3 actually has) and writes the classification to the comparison table.

```sql
-- Stream table: what the pipeline wants to write to system 3
SELECT pgtrickle.create_stream_table(
    'shadow_delta_system3_contacts',
    'SELECT action, cluster_id, external_id, data, base
     FROM _delta_system3_contacts
     WHERE action != ''noop''',
    schedule => '5m'
);

-- Regular table: comparison results (writable by shadow observer)
CREATE TABLE inout_shadow_comparison_system3_contacts (
    external_id     TEXT PRIMARY KEY,
    desired_action  TEXT NOT NULL,
    desired_data    JSONB NOT NULL,
    actual_data     JSONB,
    comparison      TEXT,           -- aligned / gap / conflict / etc.
    diff            JSONB,
    shadow_run_id   UUID,
    compared_at     TIMESTAMPTZ
);
```

---

## 7. Operational Workflow

### 7.1 Onboarding Timeline

```
Day 0: Deploy
├── Gate system 3 source tables
├── Deploy system 3 connector in ingestion_polling_readonly mode
├── Create shadow stream table (delta → shadow)
└── Configure shadow observer in connector YAML

Day 0–1: Initial Load
├── in-and-out ingests system 3 fully
├── pg-trickle source gate holds identity resolution
└── Systems 1 and 2 operate normally

Day 1: Enable Identity Resolution
├── Ungate system 3 sources
├── Identity resolution runs with all 3 systems
├── Systems 1 and 2 delta tables may emit re-merge deltas (live writeback)
├── System 3 delta materialises into shadow stream table
└── Shadow observer begins first comparison run

Day 1–3: Shadow Convergence
├── Shadow observer fetches actual state from system 3 API
├── Comparison metrics converge toward alignment
├── Operator reviews conflict rows
├── Re-runs shadow comparison as system 3 data changes
└── Convergence metrics stabilise

Day 3+: Go-Live Decision
├── Metrics: 96% aligned, 3% genuine_update, 1% genuine_insert, 0 conflicts
├── Operator approves (or auto-threshold triggers)
├── Shadow → live transition executes
├── First writeback processes only genuine gaps (~4% of total)
└── System 3 is fully online
```

### 7.2 Dashboard

The shadow comparison produces a natural dashboard:

```
System 3 Onboarding — Shadow Status
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Phase: SHADOW (run #4, 2026-03-28 14:00)

Comparison Summary:
  aligned          8,423  (84.2%)  ████████████████░░░░
  genuine_update   1,200  (12.0%)  ██░░░░░░░░░░░░░░░░░░
  genuine_insert     350  ( 3.5%)  █░░░░░░░░░░░░░░░░░░░
  conflict            25  ( 0.3%)  ░░░░░░░░░░░░░░░░░░░░
  noop                 2  ( 0.0%)  ░░░░░░░░░░░░░░░░░░░░

Convergence Trend:
  Run #1: 72.1% aligned
  Run #2: 79.4% aligned (+7.3)
  Run #3: 82.8% aligned (+3.4)
  Run #4: 84.2% aligned (+1.4)  ← convergence slowing

Threshold: 95% aligned, 0 conflicts
Status: NOT READY (25 conflicts, 84.2% aligned)

Top Conflicts:
  external_id=C-1042: name differs (desired: "Alice Smith", actual: "A. Smith")
  external_id=C-2891: email differs (desired: "bob@co.com", actual: "bob@company.com")
  ...
```

---

## 8. Comparison With Other Approaches

| Criterion | Gating (Proposal) | Blue/Green | Shadow Mode | Extended Shadow (§10) |
|---|---|---|---|---|
| **Production pipeline modified** | Yes (ALTER QUERY) | No (parallel pipeline) | Yes (system 3 added to live) | Yes (system 3 added to live) |
| **Storage overhead** | Minimal | 2× full pipeline | Shadow table (system 3 only) | Shadow tables (all 3 systems) |
| **Review window** | Fixed (gate → review → ungate) | Unlimited (parallel) | Continuous (system 3 only) | Continuous (all 3 systems) |
| **Systems 1 & 2 protected** | Mode B only (paused) | Yes (parallel pipeline) | **No** (re-merge deltas flow immediately) | **Yes** (in-database shadow) |
| **First writeback blast radius** | Full golden record delta | Full golden record delta (after cutover) | Only genuine gaps (system 3) | Only genuine gaps (all 3 systems) |
| **Handles pre-existing data** | No (all non-noop deltas are written) | No | Yes (classifies as `aligned`) | Yes (classifies as `aligned`) |
| **New pg-trickle features** | `gate_stream_table`, cluster metrics | None | None strictly required | None strictly required |
| **New in-and-out features** | Control table commands | Blue pipeline generator | Shadow observer daemon | Shadow observer + in-DB comparison |
| **Automation potential** | High (threshold-gated) | High (state machine) | Highest (continuous convergence) | Highest (continuous, all systems) |
| **Implementation complexity** | Low | Medium | High | High (but no extra API calls for 1 & 2) |
| **API cost during onboarding** | None (read-only ingestion) | Doubled (dual ingestion) or none (DB replication) | Medium (system 3 comparison fetches) | Medium (system 3 only — systems 1 & 2 are free) |
| **Per-row attribution** | No | No | No | Yes (`origin` field identifies cause) |
| **Golden record unchanged during review** | Yes (gated) | Yes (parallel pipeline) | No (reflects all 3 systems) | No (reflects all 3 systems) |

---

## 9. Combining Shadow Mode With Gating

Shadow mode and gating are not mutually exclusive. The strongest approach combines them:

1. **Source gating** during initial load (prevents partial-data identity resolution) — existing pg-trickle.
2. **Delta gating** on `_delta_system3_*` — proposed in PLAN_ONBOARDING_PROPOSAL.md.
3. **Shadow comparison** runs while system 3's delta is gated — the observer fetches API state and classifies deltas without writing anything.
4. **Go-live** ungates the delta table and processes only genuine gaps.

This gives:
- Source gating's safety during initial load
- Shadow mode's convergence visibility and blast radius reduction
- The gating approach's simplicity for systems 1 and 2

---

## 10. Extended Shadow Mode: Protecting Systems 1 and 2

Basic shadow mode (§1–§9) has a fundamental gap: **it only shadows system 3's writeback**. When system 3 joins the identity resolution pipeline, cluster re-merges can cause deltas for systems 1 and 2 — and those deltas flow to live writeback immediately. If a re-merge is wrong (e.g., two previously separate customers are falsely merged because system 3 has a duplicate email), systems 1 and 2 execute those incorrect changes before anyone can review them.

Extended shadow mode closes this gap by shadowing **all three systems'** writeback during the onboarding window.

### 10.1 The Problem: Re-merge Deltas

When system 3's data enters identity resolution, the `_id_contact` view discovers new identity links. Two customers who were separate in the two-system world may now be connected (transitively) through a system 3 record. This cluster re-merge changes the golden record, which changes what `_delta_system1_contacts` and `_delta_system2_contacts` emit.

Concretely, four kinds of changes can appear in systems 1 and 2's delta tables after system 3 joins:

| Change Type | Example | What Happens |
|---|---|---|
| **System 3 contributes fields** | System 3 has a phone number that systems 1 and 2 lack. Golden record now includes it. | `_delta_system1` and `_delta_system2` emit updates adding the phone number. |
| **System 3 has exclusive records** | System 3 has contacts that don't exist in systems 1 or 2. Golden record creates new rows. | `_delta_system1` and `_delta_system2` emit **inserts** — new records pushed into existing systems. |
| **Cluster re-merge** | System 3 bridges two previously separate clusters (e.g., same email, different names). | All three systems receive deltas reflecting the merged cluster. Field values change based on conflict resolution priority. |
| **No-ops** | System 3 has data identical to what's already in the golden record. | No change — `_delta_*` rows are `noop`. |

The third type — cluster re-merge — is the most dangerous. It can change names, addresses, and other fields in systems 1 and 2 without any direct edit from the user. In basic shadow mode, these changes execute immediately.

### 10.2 Key Insight: The `base` Column Eliminates API Calls

Shadowing system 3 requires API calls because in-and-out doesn't know what system 3 _actually_ contains until it fetches it. But for systems 1 and 2, in-and-out already has this information.

Every `inout_src_{connector}_{datatype}` table has a `base` column — a JSONB snapshot of the record's state at the time of the last ingestion. The `base` column **is** the last known actual state of that record in the source system.

This means:

- For **system 3** → must call the API to compare (shadow observer, as described in §2.3)
- For **systems 1 and 2** → compare `_delta_*.data` (desired) against `_delta_*.base` (known actual) **entirely in the database** — zero API calls

The `base` column was designed for three-way merge in writeback. Extended shadow mode repurposes it for in-database shadow comparison.

### 10.3 In-Database Shadow Comparison

For systems 1 and 2, the shadow comparison is a single SQL query per system. No daemon, no API calls, no rate limiting:

```sql
-- Shadow comparison for system 1 — purely in-database
INSERT INTO inout_shadow_comparison_system1_contacts
    (external_id, desired_action, desired_data, known_current,
     diff, classification, origin, shadow_run_id)
SELECT
    d.external_id,
    d.action,
    d.data     AS desired_data,
    d.base     AS known_current,
    jsonb_diff(d.base, d.data)  AS diff,

    -- Classification: what kind of change is this?
    CASE
        WHEN d.action = 'noop'
            THEN 'noop'
        WHEN d.base IS NULL AND d.action = 'insert'
            THEN 'new_record'         -- golden record wants to push a record
                                      -- that doesn't exist in system 1
        WHEN d.data = d.base
            THEN 'aligned'            -- system 1 already has the correct data
        WHEN d.base IS NOT NULL AND d.data != d.base
            THEN 'field_update'       -- golden record wants to change fields
                                      -- that system 1 currently has differently
        ELSE  'unclassified'
    END AS classification,

    -- Attribution: WHY did this change happen?
    CASE
        WHEN EXISTS (
            SELECT 1 FROM _id_contact ic
            WHERE  ic.cluster_id = d.cluster_id
              AND  ic.source     = 'system3'
        )
            THEN 'system3_caused'     -- this cluster includes a system 3 record,
                                      -- so the change is (at least partially)
                                      -- caused by system 3 joining the pipeline
        ELSE
            'independent'             -- this change would have happened regardless
                                      -- of system 3 (e.g., system 2 updated a
                                      -- record that system 1 should know about)
    END AS origin,

    $1 AS shadow_run_id               -- group results by comparison run
FROM _delta_system1_contacts d
WHERE d.action != 'noop';
```

The same query runs for system 2, substituting `system2` for `system1`.

### 10.4 The Shadow Comparison Table for Systems 1 and 2

```sql
CREATE TABLE inout_shadow_comparison_system1_contacts (
    external_id     TEXT NOT NULL,
    desired_action  TEXT NOT NULL,          -- insert / update / delete
    desired_data    JSONB NOT NULL,         -- what the golden record says
    known_current   JSONB,                  -- base column (last known state)
    diff            JSONB,                  -- field-level difference
    classification  TEXT NOT NULL,          -- noop / aligned / new_record /
                                            -- field_update / unclassified
    origin          TEXT NOT NULL,          -- system3_caused / independent
    shadow_run_id   UUID NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (external_id, shadow_run_id)
);

CREATE INDEX ON inout_shadow_comparison_system1_contacts (origin);
CREATE INDEX ON inout_shadow_comparison_system1_contacts (classification);
```

### 10.5 Attribution: Why Did This Change Happen?

The `origin` column is the key. It answers: **"Was this change caused by system 3 joining the pipeline, or would it have happened anyway?"**

| Origin | Meaning | Operator Action |
|---|---|---|
| `system3_caused` | The cluster that produced this delta includes at least one system 3 record. The change is a consequence of system 3 joining — either because system 3 contributed new data, bridged two clusters, or introduced a new record. | **Hold for review.** This is the change that extended shadow mode exists to catch. |
| `independent` | The cluster does not include any system 3 record. This change was produced by the normal two-system pipeline: system 1 or 2 updated a record, and the delta reflects that update. | **Allow immediately.** This change would have happened regardless of onboarding. Holding it would degrade normal pipeline operation. |

This distinction is critical. Without attribution, shadowing systems 1 and 2 would hold back _all_ changes — including routine syncs between systems 1 and 2. That would degrade the existing pipeline. With attribution, only system-3-caused changes are held; everything else flows normally.

### 10.6 Classification Reference (Systems 1 and 2)

| Classification | Meaning | Expected? | What Happens on Go-Live |
|---|---|---|---|
| `aligned` | System 1/2 already has the data the golden record wants. | Yes if pre-existing data overlaps. | No writeback needed. |
| `new_record` | Golden record wants to insert a record that doesn't exist in system 1/2. This typically means system 3 has a record exclusive to it, and the golden record is pushing it to other systems. | Yes — this is the "hidden hazard" most operators miss. | Inserted into system 1/2 on go-live. |
| `field_update` | Golden record wants to update fields that system 1/2 has differently. May be caused by system 3 contributing better data (higher priority) or by cluster re-merge changing conflict resolution outcomes. | Yes during re-merge window. | Updated in system 1/2 on go-live. |
| `noop` | No change needed. | Yes — most rows in steady state. | Skipped. |
| `unclassified` | Edge case not covered above. | Rare. | Requires manual review. |

### 10.7 Go-Live With Attribution Filtering

Extended shadow mode splits the go-live decision into two independent dimensions:

**Dimension 1: System 3's own writeback** (same as basic shadow mode)
- Uses the shadow observer with API comparison (§2.3)
- Convergence metrics drive the decision
- When ready: enable live writeback for system 3, processing only genuine gaps

**Dimension 2: System-3-caused changes to systems 1 and 2** (new in extended mode)
- Uses in-database comparison (§10.3)
- The operator reviews `system3_caused` rows in the shadow comparison tables
- When ready: release held changes to systems 1 and 2's writeback

These two dimensions can be approved **independently**. An operator might approve system 3's own writeback first (system 3 starts receiving data) while still holding system-3-caused changes to systems 1 and 2 for review. Or vice versa.

```sql
-- Go-live query: what will be written to system 1 when we approve?
SELECT classification, origin, COUNT(*) AS count
FROM inout_shadow_comparison_system1_contacts
WHERE shadow_run_id = (SELECT MAX(shadow_run_id)
                       FROM inout_shadow_comparison_system1_contacts)
  AND origin = 'system3_caused'
GROUP BY classification, origin
ORDER BY count DESC;
```

Example output:

| classification | origin | count |
|---|---|---|
| `field_update` | system3_caused | 842 |
| `new_record` | system3_caused | 156 |
| `aligned` | system3_caused | 3,201 |

The operator sees: 842 field updates and 156 new records will be written to system 1 because of system 3. The 3,201 aligned rows need no action. The operator can drill into the 156 `new_record` rows to verify they make business sense before approving.

### 10.8 Handling Independent Changes During Shadow

A critical design decision: **independent changes must not be held.** If systems 1 and 2 are syncing records between themselves (normal pipeline operation), those deltas should flow immediately regardless of the onboarding shadow.

The bridge layer implements this filter:

```sql
-- Bridge layer logic during extended shadow mode:
-- Only route system3_caused changes to the shadow table.
-- Independent changes flow to live writeback as normal.

INSERT INTO inout_dst_system1_contacts  -- live writeback (normal)
SELECT d.*
FROM _delta_system1_contacts d
WHERE d.action != 'noop'
  AND NOT EXISTS (
    SELECT 1 FROM _id_contact ic
    WHERE  ic.cluster_id = d.cluster_id
      AND  ic.source = 'system3'
  );

INSERT INTO inout_shadow_comparison_system1_contacts  -- shadow (held)
SELECT ...  -- classification and attribution query from §10.3
FROM _delta_system1_contacts d
WHERE d.action != 'noop'
  AND EXISTS (
    SELECT 1 FROM _id_contact ic
    WHERE  ic.cluster_id = d.cluster_id
      AND  ic.source = 'system3'
  );
```

### 10.9 Updated Architecture Diagram

```
                    ┌──────────────────────────────────────────────────────┐
  System 1 API ──→ │  inout_src_system1_contacts                          │
  System 2 API ──→ │  inout_src_system2_contacts                          │
  System 3 API ──→ │  inout_src_system3_contacts                          │
                    │                 ↓                                    │
                    │  _id_contact (all 3 systems)                         │
                    │  _resolved_contact                                   │
                    │                 ↓                                    │
                    │  _delta_system1 ─┬─ independent ──→ LIVE WRITEBACK   │
                    │                  └─ system3_caused → SHADOW TABLE 1  │
                    │                                     (in-database)    │
                    │  _delta_system2 ─┬─ independent ──→ LIVE WRITEBACK   │
                    │                  └─ system3_caused → SHADOW TABLE 2  │
                    │                                     (in-database)    │
                    │  _delta_system3 ──────────────────→ SHADOW TABLE 3   │
                    │                                     (API comparison) │
                    └──────────────────────────────────────────────────────┘

  Shadow Table 1 & 2: in-database comparison using base column — no API calls
  Shadow Table 3:     shadow observer fetches from system 3 API — same as §2.3
```

### 10.10 What Extended Shadow Mode Achieves

| Property | Basic Shadow (§1–§9) | Extended Shadow (§10) |
|---|---|---|
| System 3 writeback protected | Yes (API comparison) | Yes (API comparison) |
| Systems 1 & 2 writeback protected | **No** | **Yes** (in-database) |
| API calls for systems 1 & 2 | N/A | **Zero** |
| Per-row attribution | No | Yes (`origin` field) |
| Independent changes degraded | N/A | **No** — independent changes flow normally |
| Go-live granularity | Single decision (system 3) | Per-system, per-dimension |
| Handles system 3 pre-existing data | Yes | Yes |
| Golden record reflects all 3 systems during review | Yes | Yes |

**Comparison with blue/green:** Extended shadow mode now matches blue/green's core safety guarantee — nothing is written to any system without review. The key differences:

- **Extended shadow mode is better when** system 3 has pre-existing data (classifies as `aligned` instead of blindly overwriting), you want per-row attribution of _why_ each change happened, and you want independent changes between systems 1 and 2 to continue flowing during onboarding.

- **Blue/green is better when** BI consumers depend on the golden record not changing shape during review (the analytics view `{target}` in the production pipeline reflects all 3 systems in shadow mode, but is frozen in blue/green), or when the identity resolution rules have never been tested against system 3's data and you want zero impact until cutover.

### 10.11 Convergence Dashboard (Extended)

```
Extended Shadow — Onboarding Status
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

System 3 (API shadow, run #4):
  aligned          8,423  (84.2%)  ████████████████░░░░
  genuine_update   1,200  (12.0%)  ██░░░░░░░░░░░░░░░░░░
  genuine_insert     350  ( 3.5%)  █░░░░░░░░░░░░░░░░░░░
  conflict            25  ( 0.3%)  ░░░░░░░░░░░░░░░░░░░░
  Status: NOT READY (84.2% aligned, 25 conflicts)

System 1 — system3_caused changes (in-DB, run #4):
  aligned          3,201  (76.2%)  ███████████████░░░░░
  field_update       842  (20.0%)  ████░░░░░░░░░░░░░░░░
  new_record         156  ( 3.7%)  █░░░░░░░░░░░░░░░░░░░
  Status: REVIEW NEEDED (998 changes held)

System 2 — system3_caused changes (in-DB, run #4):
  aligned          2,890  (81.4%)  ████████████████░░░░
  field_update       580  (16.3%)  ███░░░░░░░░░░░░░░░░░
  new_record          78  ( 2.2%)  ░░░░░░░░░░░░░░░░░░░░
  Status: REVIEW NEEDED (658 changes held)

Systems 1 & 2 — independent changes:
  Flowing to live writeback normally ✓
  (234 updates written since onboarding started)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

---

## 11. Open Questions

1. **Rate limiting the shadow observer:** The observer makes GET requests to system 3's API. Should it respect the same rate limits as regular ingestion? Or should it have its own budget? If the shadow has 10K rows and the API allows 100 req/s, a full comparison takes ~100 seconds. Reasonable, but needs to be configurable.

2. **Incremental shadow comparison:** After the first full comparison, subsequent runs should only re-compare rows where the delta changed (pg-trickle's stream table tracks this). But should the observer also re-check previously `aligned` rows periodically? If system 3's data drifts, aligned rows may become gaps.

3. **Insert identity matching:** For `insert` action rows, the observer must search system 3 by identity fields (email, tax_id) rather than `external_id` (which doesn't exist yet in system 3). This requires the connector YAML to declare a search endpoint. Not all APIs have one. For APIs without search, the observer can only classify inserts as `genuine_insert` and skip the `already_exists` detection.

4. **Shadow comparison freshness for systems 1 and 2:** The in-database comparison uses the `base` column (last ingestion snapshot). If system 1 or 2's data changes between ingestion cycles, the `base` column is slightly stale. In practice this is a small window (ingestion runs every few minutes), but for high-frequency systems, should the shadow comparison trigger a just-in-time ingestion cycle before classifying?

5. **Shadow table lifecycle:** How long should shadow tables be retained after go-live? They have audit value but consume storage. Recommend: archive to cold storage (or compress) after 30 days, delete after 90 days.

6. **Interaction with in-and-out's three-way merge:** The writeback daemon already does a pre-flight GET and three-way comparison before writing. Shadow mode's observer does a similar fetch. Could the shadow observer _be_ the writeback daemon in dry-run mode? This would avoid implementing a separate comparison engine — just run writeback with `--dry-run` forever until promotion. The dry-run output already contains the classification information needed for convergence metrics.

7. **Extended shadow mode and the golden record:** Extended shadow mode does not freeze the golden record — the analytics view `{target}` reflects all three systems from the start. If BI dashboards or downstream consumers depend on the golden record not changing shape during onboarding, extended shadow mode is not sufficient and blue/green should be considered instead (see §10.10).

8. **Bridge layer implementation:** The attribution filter (§10.8) runs an EXISTS subquery against `_id_contact` for every delta row. For large datasets, should this be materialised as a flag on the delta view itself (via a pg-trickle stream table) rather than computed at routing time? This would trade storage for query performance.

9. **Partial go-live ordering:** Extended shadow mode allows per-system go-live (§10.7). What is the recommended order? Suggested: system 3 first (its own writeback), then systems 1 and 2 together (since their `system3_caused` changes are correlated — a cluster re-merge typically affects all systems in the cluster).
