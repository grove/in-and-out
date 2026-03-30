# Evaluation: Shadow Mode Onboarding

> **Date:** March 27, 2026
> **Status:** Evaluation
> **Scope:** Evaluate a shadow mode (dual-write detection) approach for onboarding new source systems into an in-and-out + OSI-mapping MDM pipeline backed by pg-trickle stream tables
> **Related:** [PLAN_ONBOARDING_PROPOSAL.md](PLAN_ONBOARDING_PROPOSAL.md) (gating-based approach), [PLAN_ONBOARDING_BLUE_GREEN.md](PLAN_ONBOARDING_BLUE_GREEN.md) (blue/green approach), [PLAN_ONBOARDING_SHADOW_MODE_EXTENDED.md](PLAN_ONBOARDING_SHADOW_MODE_EXTENDED.md) (extended shadow mode)

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
| **Re-merge deltas hit systems 1 and 2 immediately** | Because system 3 is in the production pipeline from the start, cluster re-merges affect systems 1 and 2's writeback immediately. If the re-merge is wrong, it's already been executed. | High — **solved by [extended shadow mode](PLAN_ONBOARDING_SHADOW_MODE_EXTENDED.md)**, which shadows all three systems with in-database comparison for systems 1 and 2, requiring no additional API calls. Can also be mitigated by combining with delta gating (§9). |
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

| Criterion | Gating (Proposal) | Blue/Green | Shadow Mode |
|---|---|---|---|
| **Production pipeline modified** | Yes (ALTER QUERY) | No (parallel pipeline) | Yes (system 3 added to live) |
| **Storage overhead** | Minimal | 2× full pipeline | Shadow table (system 3 only) |
| **Review window** | Fixed (gate → review → ungate) | Unlimited (parallel) | Continuous (system 3 only) |
| **Systems 1 & 2 protected** | Mode B only (paused) | Yes (parallel pipeline) | **No** (re-merge deltas flow immediately) |
| **First writeback blast radius** | Full golden record delta | Full golden record delta (after cutover) | Only genuine gaps (system 3) |
| **Handles pre-existing data** | No (all non-noop deltas are written) | No | Yes (classifies as `aligned`) |
| **New pg-trickle features** | `gate_stream_table`, cluster metrics | None | None strictly required |
| **New in-and-out features** | Control table commands | Blue pipeline generator | Shadow observer daemon |
| **Automation potential** | High (threshold-gated) | High (state machine) | Highest (continuous convergence) |
| **Implementation complexity** | Low | Medium | High |
| **API cost during onboarding** | None (read-only ingestion) | Doubled (dual ingestion) or none (DB replication) | Medium (system 3 comparison fetches) |
| **Golden record unchanged during review** | Yes (gated) | Yes (parallel pipeline) | No (reflects all 3 systems) |

For a comparison that includes the extended shadow mode variant, see [PLAN_ONBOARDING_SHADOW_MODE_EXTENDED.md](PLAN_ONBOARDING_SHADOW_MODE_EXTENDED.md) §11.

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

## 10. Open Questions

1. **Rate limiting the shadow observer:** The observer makes GET requests to system 3's API. Should it respect the same rate limits as regular ingestion? Or should it have its own budget? If the shadow has 10K rows and the API allows 100 req/s, a full comparison takes ~100 seconds. Reasonable, but needs to be configurable.

2. **Incremental shadow comparison:** After the first full comparison, subsequent runs should only re-compare rows where the delta changed (pg-trickle's stream table tracks this). But should the observer also re-check previously `aligned` rows periodically? If system 3's data drifts, aligned rows may become gaps.

3. **Insert identity matching:** For `insert` action rows, the observer must search system 3 by identity fields (email, tax_id) rather than `external_id` (which doesn't exist yet in system 3). This requires the connector YAML to declare a search endpoint. Not all APIs have one. For APIs without search, the observer can only classify inserts as `genuine_insert` and skip the `already_exists` detection.

4. **Shadow table lifecycle:** How long should shadow tables be retained after go-live? They have audit value but consume storage. Recommend: archive to cold storage (or compress) after 30 days, delete after 90 days.

5. **Interaction with in-and-out's three-way merge:** The writeback daemon already does a pre-flight GET and three-way comparison before writing. Shadow mode's observer does a similar fetch. Could the shadow observer _be_ the writeback daemon in dry-run mode? This would avoid implementing a separate comparison engine — just run writeback with `--dry-run` forever until promotion. The dry-run output already contains the classification information needed for convergence metrics.
