# Evaluation: Extended Shadow Mode Onboarding

> **Date:** March 27, 2026
> **Status:** Evaluation
> **Scope:** Evaluate an extended shadow mode that protects all three systems' writeback during onboarding, using in-database comparison for existing systems and API comparison for the new system
> **Related:** [PLAN_ONBOARDING_SHADOW_MODE.md](PLAN_ONBOARDING_SHADOW_MODE.md) (basic shadow mode), [PLAN_ONBOARDING_PROPOSAL.md](PLAN_ONBOARDING_PROPOSAL.md) (gating-based approach), [PLAN_ONBOARDING_BLUE_GREEN.md](PLAN_ONBOARDING_BLUE_GREEN.md) (blue/green approach)

---

## 1. Motivation

Basic shadow mode ([PLAN_ONBOARDING_SHADOW_MODE.md](PLAN_ONBOARDING_SHADOW_MODE.md) §1–§9) has a fundamental gap: **it only shadows system 3's writeback**. When system 3 joins the identity resolution pipeline, cluster re-merges can cause deltas for systems 1 and 2 — and those deltas flow to live writeback immediately. If a re-merge is wrong (e.g., two previously separate customers are falsely merged because system 3 has a duplicate email), systems 1 and 2 execute those incorrect changes before anyone can review them.

Extended shadow mode closes this gap by shadowing **all three systems'** writeback during the onboarding window.

### 1.1 The Problem: Re-merge Deltas

When system 3's data enters identity resolution, the `_id_contact` view discovers new identity links. Two customers who were separate in the two-system world may now be connected (transitively) through a system 3 record. This cluster re-merge changes the golden record, which changes what `_delta_system1_contacts` and `_delta_system2_contacts` emit.

Concretely, four kinds of changes can appear in systems 1 and 2's delta tables after system 3 joins:

| Change Type | Example | What Happens |
|---|---|---|
| **System 3 contributes fields** | System 3 has a phone number that systems 1 and 2 lack. Golden record now includes it. | `_delta_system1` and `_delta_system2` emit updates adding the phone number. |
| **System 3 has exclusive records** | System 3 has contacts that don't exist in systems 1 or 2. Golden record creates new rows. | `_delta_system1` and `_delta_system2` emit **inserts** — new records pushed into existing systems. |
| **Cluster re-merge** | System 3 bridges two previously separate clusters (e.g., same email, different names). | All three systems receive deltas reflecting the merged cluster. Field values change based on conflict resolution priority. |
| **No-ops** | System 3 has data identical to what's already in the golden record. | No change — `_delta_*` rows are `noop`. |

The third type — cluster re-merge — is the most dangerous. It can change names, addresses, and other fields in systems 1 and 2 without any direct edit from the user. In basic shadow mode, these changes execute immediately.

### 1.2 Key Insight: The `base` Column Eliminates API Calls

Shadowing system 3 requires API calls because in-and-out doesn't know what system 3 _actually_ contains until it fetches it. But for systems 1 and 2, in-and-out already has this information.

Every `inout_src_{connector}_{datatype}` table has a `base` column — a JSONB snapshot of the record's state at the time of the last ingestion. The `base` column **is** the last known actual state of that record in the source system.

This means:

- For **system 3** → must call the API to compare (shadow observer, as described in [PLAN_ONBOARDING_SHADOW_MODE.md](PLAN_ONBOARDING_SHADOW_MODE.md) §2.3)
- For **systems 1 and 2** → compare `_delta_*.data` (desired) against `_delta_*.base` (known actual) **entirely in the database** — zero API calls

The `base` column was designed for three-way merge in writeback. Extended shadow mode repurposes it for in-database shadow comparison.

---

## 2. In-Database Shadow Comparison

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

---

## 3. The Shadow Comparison Table for Systems 1 and 2

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

---

## 4. Attribution: Why Did This Change Happen?

The `origin` column is the key. It answers: **"Was this change caused by system 3 joining the pipeline, or would it have happened anyway?"**

| Origin | Meaning | Operator Action |
|---|---|---|
| `system3_caused` | The cluster that produced this delta includes at least one system 3 record. The change is a consequence of system 3 joining — either because system 3 contributed new data, bridged two clusters, or introduced a new record. | **Hold for review.** This is the change that extended shadow mode exists to catch. |
| `independent` | The cluster does not include any system 3 record. This change was produced by the normal two-system pipeline: system 1 or 2 updated a record, and the delta reflects that update. | **Allow immediately.** This change would have happened regardless of onboarding. Holding it would degrade normal pipeline operation. |

This distinction is critical. Without attribution, shadowing systems 1 and 2 would hold back _all_ changes — including routine syncs between systems 1 and 2. That would degrade the existing pipeline. With attribution, only system-3-caused changes are held; everything else flows normally.

---

## 5. Classification Reference (Systems 1 and 2)

| Classification | Meaning | Expected? | What Happens on Go-Live |
|---|---|---|---|
| `aligned` | System 1/2 already has the data the golden record wants. | Yes if pre-existing data overlaps. | No writeback needed. |
| `new_record` | Golden record wants to insert a record that doesn't exist in system 1/2. This typically means system 3 has a record exclusive to it, and the golden record is pushing it to other systems. | Yes — this is the "hidden hazard" most operators miss. | Inserted into system 1/2 on go-live. |
| `field_update` | Golden record wants to update fields that system 1/2 has differently. May be caused by system 3 contributing better data (higher priority) or by cluster re-merge changing conflict resolution outcomes. | Yes during re-merge window. | Updated in system 1/2 on go-live. |
| `noop` | No change needed. | Yes — most rows in steady state. | Skipped. |
| `unclassified` | Edge case not covered above. | Rare. | Requires manual review. |

---

## 6. Go-Live With Attribution Filtering

Extended shadow mode splits the go-live decision into two independent dimensions:

**Dimension 1: System 3's own writeback** (same as basic shadow mode)
- Uses the shadow observer with API comparison ([PLAN_ONBOARDING_SHADOW_MODE.md](PLAN_ONBOARDING_SHADOW_MODE.md) §2.3)
- Convergence metrics drive the decision
- When ready: enable live writeback for system 3, processing only genuine gaps

**Dimension 2: System-3-caused changes to systems 1 and 2** (new in extended mode)
- Uses in-database comparison (§2)
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

---

## 7. Handling Independent Changes During Shadow

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
SELECT ...  -- classification and attribution query from §2
FROM _delta_system1_contacts d
WHERE d.action != 'noop'
  AND EXISTS (
    SELECT 1 FROM _id_contact ic
    WHERE  ic.cluster_id = d.cluster_id
      AND  ic.source = 'system3'
  );
```

---

## 8. Architecture Diagram

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
  Shadow Table 3:     shadow observer fetches from system 3 API — same as
                      PLAN_ONBOARDING_SHADOW_MODE.md §2.3
```

---

## 9. What Extended Shadow Mode Achieves

| Property | Basic Shadow | Extended Shadow |
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

---

## 10. Convergence Dashboard (Extended)

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

## 11. Comparison With All Approaches

| Criterion | Gating (Proposal) | Blue/Green | Shadow Mode | Extended Shadow |
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

## 12. Open Questions

1. **Shadow comparison freshness for systems 1 and 2:** The in-database comparison uses the `base` column (last ingestion snapshot). If system 1 or 2's data changes between ingestion cycles, the `base` column is slightly stale. In practice this is a small window (ingestion runs every few minutes), but for high-frequency systems, should the shadow comparison trigger a just-in-time ingestion cycle before classifying?

2. **Extended shadow mode and the golden record:** Extended shadow mode does not freeze the golden record — the analytics view `{target}` reflects all three systems from the start. If BI dashboards or downstream consumers depend on the golden record not changing shape during onboarding, extended shadow mode is not sufficient and blue/green should be considered instead (see §9).

3. **Bridge layer implementation:** The attribution filter (§7) runs an EXISTS subquery against `_id_contact` for every delta row. For large datasets, should this be materialised as a flag on the delta view itself (via a pg-trickle stream table) rather than computed at routing time? This would trade storage for query performance.

4. **Partial go-live ordering:** Extended shadow mode allows per-system go-live (§6). What is the recommended order? Suggested: system 3 first (its own writeback), then systems 1 and 2 together (since their `system3_caused` changes are correlated — a cluster re-merge typically affects all systems in the cluster).
