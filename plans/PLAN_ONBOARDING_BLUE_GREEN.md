# Evaluation: Blue/Green Source Table Onboarding

> **Date:** March 27, 2026
> **Status:** Evaluation
> **Scope:** Evaluate a blue/green deployment approach for onboarding new source systems into an in-and-out + OSI-mapping MDM pipeline backed by pg-trickle stream tables
> **Related:** [PLAN_ONBOARDING_PROPOSAL.md](PLAN_ONBOARDING_PROPOSAL.md) (gating-based approach)

---

## 1. Core Idea

Instead of adding system 3's source tables directly to the live OSI-Mapping pipeline and gating to control timing, maintain two parallel pipeline instances:

- **Green (live):** The current production pipeline. Systems 1 and 2 flow through identity resolution, conflict resolution, and writeback as normal. Untouched throughout the onboarding process.
- **Blue (staging):** A parallel pipeline that includes all three systems. System 3's data is ingested here. Identity resolution runs against the combined dataset. Deltas are computed but not executed — they are reviewed.

Once the operator approves the blue pipeline's output, traffic is cut over from green to blue. Blue becomes the new green.

```
                    ┌─────────────────────────────────────┐
                    │         GREEN (live)                 │
                    │                                     │
  System 1 API ──→ │  inout_src_system1_contacts          │
  System 2 API ──→ │  inout_src_system2_contacts          │
                    │         ↓                           │
                    │  _id_contact (WITH RECURSIVE)        │
                    │  _resolved_contact                   │
                    │  _delta_system1_contacts → writeback │
                    │  _delta_system2_contacts → writeback │
                    └─────────────────────────────────────┘

                    ┌─────────────────────────────────────┐
                    │         BLUE (staging)               │
                    │                                     │
  System 1 API ──→ │  inout_src_system1_contacts_blue     │
  System 2 API ──→ │  inout_src_system2_contacts_blue     │
  System 3 API ──→ │  inout_src_system3_contacts_blue     │
                    │         ↓                           │
                    │  _id_contact_blue (WITH RECURSIVE)   │
                    │  _resolved_contact_blue              │
                    │  _delta_system1_contacts_blue (shadow)│
                    │  _delta_system2_contacts_blue (shadow)│
                    │  _delta_system3_contacts_blue (shadow)│
                    └─────────────────────────────────────┘
```

---

## 2. Detailed Design

### 2.1 What Gets Duplicated

| Component | Green (live) | Blue (staging) | Shared? |
|---|---|---|---|
| Source tables (`inout_src_*`) | `inout_src_system1_contacts` | `inout_src_system1_contacts_blue` | No — full copy |
| Forward views (`_fwd_*`) | `_fwd_crm_contacts` | `_fwd_crm_contacts_blue` | No |
| Identity view (`_id_*`) | `_id_contact` | `_id_contact_blue` | No |
| Resolution view (`_resolved_*`) | `_resolved_contact` | `_resolved_contact_blue` | No |
| Analytics view (`{target}`) | `contact` | `contact_blue` | No |
| Reverse views (`_rev_*`) | `_rev_crm_contacts` | `_rev_crm_contacts_blue` | No |
| Delta views (`_delta_*`) | `_delta_crm_contacts` | `_delta_crm_contacts_blue` | No |
| Desired-state tables (`inout_dst_*`) | `inout_dst_crm_contacts` | `inout_dst_crm_contacts_blue` | No |
| in-and-out connector YAML | `hubspot.yaml` | `hubspot-blue.yaml` | No |
| OSI-Mapping YAML | `osi-mapping.yaml` | `osi-mapping-blue.yaml` | No |

**Everything is duplicated.** The blue pipeline is a complete, independent copy of the production pipeline with system 3 added. The green pipeline is not modified at all.

### 2.2 How Source Tables Are Populated

The blue source tables for systems 1 and 2 need data. Three options:

**Option A: Dual ingestion (live API pulls to both green and blue)**

in-and-out runs two connector instances per existing system — one writing to green, one to blue. This keeps blue tables fully up to date.

- Doubles API calls to systems 1 and 2.
- May violate rate limits.
- Simple but expensive.

**Option B: Database-level replication (green → blue)**

Use pg-trickle or PostgreSQL logical replication to replicate `inout_src_system1_contacts` → `inout_src_system1_contacts_blue` within the same database.

```sql
-- Using pg-trickle stream table as a replicator:
SELECT pgtrickle.create_stream_table(
    'inout_src_system1_contacts_blue',
    'SELECT * FROM inout_src_system1_contacts',
    schedule => '30s',
    refresh_mode => 'DIFFERENTIAL'
);
```

- No extra API calls.
- Source tables stay in sync with a configurable lag.
- Uses pg-trickle's existing DVM engine for incremental sync — efficient.
- The stream table IS the blue source table; OSI-Mapping reads from it directly.

**Option C: Snapshot + catch-up**

Take a one-time `INSERT INTO ... SELECT` snapshot of systems 1 and 2's source tables at onboarding start, then replay any new ingestion events from the WAL or change buffers.

- Most complex to implement.
- Only useful if the onboarding window is short (minutes, not days).

**Recommendation: Option B.** It is elegant, uses existing pg-trickle mechanics, and keeps the blue pipeline within seconds of the green. The stream table doubles as the blue source table.

### 2.3 System 3 in the Blue Pipeline

System 3's source table exists only in blue. Normal in-and-out ingestion populates `inout_src_system3_contacts_blue` (or simply `inout_src_system3_contacts` — it doesn't exist in green, so there's no naming conflict unless we want consistency).

One ingestion daemon instance pulls from system 3's API and writes to the blue source table.

### 2.4 The Blue OSI-Mapping Pipeline

A separate `osi-mapping-blue.yaml` declares:

```yaml
sources:
  crm:
    table: inout_src_system1_contacts_blue
    primary_key: external_id
  erp:
    table: inout_src_system2_contacts_blue
    primary_key: external_id
  system3:
    table: inout_src_system3_contacts_blue
    primary_key: external_id

targets:
  contact:
    fields:
      email: identity
      name: coalesce
      # ... same as production plus system 3's fields
```

The engine generates `_blue` suffixed views and stream tables.

### 2.5 Review Phase

The blue pipeline's delta stream tables (`_delta_*_blue`) materialise deltas but are not connected to any writeback daemon. The operator reviews them:

```sql
-- What would change for system 1?
SELECT action, COUNT(*)
FROM _delta_crm_contacts_blue
WHERE action != 'noop'
GROUP BY action;

-- Cluster diff: which clusters merged vs green?
SELECT b._cluster_id, COUNT(*) AS blue_members, g.green_members
FROM _id_contact_blue b
LEFT JOIN (
    SELECT _cluster_id, COUNT(*) AS green_members
    FROM _id_contact
    GROUP BY _cluster_id
) g ON b._cluster_id = g._cluster_id
WHERE b._cluster_id != g._cluster_id OR g._cluster_id IS NULL
GROUP BY b._cluster_id, g.green_members;
```

The review can take hours or days. Green is completely unaffected.

### 2.6 Cutover

Once approved, the cutover sequence:

```sql
BEGIN;

-- 1. Pause green writeback (control table)
INSERT INTO inout_ops_control (connector, command) VALUES ('hubspot', 'pause');
INSERT INTO inout_ops_control (connector, command) VALUES ('sap', 'pause');

-- 2. Wait for in-flight writeback to complete (drain)

-- 3. Swap source table references in OSI-Mapping
-- Option A: Rename tables (fast, requires exclusive lock)
ALTER TABLE inout_src_system1_contacts RENAME TO inout_src_system1_contacts_old;
ALTER TABLE inout_src_system1_contacts_blue RENAME TO inout_src_system1_contacts;
ALTER TABLE inout_src_system2_contacts RENAME TO inout_src_system2_contacts_old;
ALTER TABLE inout_src_system2_contacts_blue RENAME TO inout_src_system2_contacts;

-- Option B: ALTER QUERY on all stream tables to point to blue sources
-- (more complex, but avoids table renames)

-- 4. Replace the production OSI-Mapping YAML with the blue version
-- (includes system 3)

-- 5. Resume writeback
INSERT INTO inout_ops_control (connector, command) VALUES ('hubspot', 'resume');
INSERT INTO inout_ops_control (connector, command) VALUES ('sap', 'resume');
INSERT INTO inout_ops_control (connector, command) VALUES ('system3', 'resume');

COMMIT;

-- 6. Drop old tables and blue pipeline artifacts
-- (after confirming stable operation)
```

**Cutover downtime:** The pause window for writeback is limited to the drain time of in-flight requests — typically seconds. Ingestion can continue to the blue tables throughout (they become the production tables after rename). Identity resolution was already running on the blue pipeline; it just becomes the production pipeline.

---

## 3. Advantages

| Advantage | Detail |
|---|---|
| **Zero-risk to production** | The green pipeline is never modified. If blue fails, green continues unchanged. |
| **Unlimited review window** | The operator can take days to review blue's output. No gating pressure. |
| **Accurate cluster diff** | Both green and blue are running simultaneously, so a direct cluster-level comparison is possible (not just "before" vs "after"). |
| **Testable end-to-end** | The blue pipeline runs the full stack — identity resolution, conflict resolution, delta computation — on real data. It's not a simulation; it's a parallel production run. |
| **Rollback is trivial** | Drop the blue tables. Green never changed. |
| **No new pg-trickle features required** | Source gating, stream tables, ALTER QUERY — all existing mechanisms. No new API surface needed. |

---

## 4. Disadvantages

| Disadvantage | Detail | Severity |
|---|---|---|
| **Storage cost** | Full duplication of all source tables, stream tables, and view pipeline. For 3 systems with 10 datatypes each and 1M records per datatype, this doubles storage. | Medium — temporary (clean up after cutover). |
| **Compute cost** | The blue pipeline runs its own pg-trickle refresh cycles. DVM engine processes deltas for both pipelines simultaneously. | Medium — can be mitigated by scheduling blue at lower frequency (warm/cold tier). |
| **Configuration complexity** | Two OSI-Mapping YAML files, two connector configs per system (or stream table replication), naming conventions for blue artifacts. | High — significant operational overhead, error-prone in manual workflows. |
| **Cutover atomicity** | The rename/swap during cutover requires exclusive locks on source tables. Any concurrent ingestion writes will block briefly. | Low — the drain window is short. |
| **Schema entanglement** | If OSI-Mapping's view pipeline references source tables by name (which it does), every view and stream table in the blue pipeline must use `_blue` suffixed names. The engine must generate these consistently. | Medium — requires either OSI-Mapping engine support for name prefixes or manual YAML authoring. |
| **Watermark divergence** | The blue pipeline's watermarks diverge from green's during the review window. After cutover, watermarks must be realigned. | Low — one-time reconciliation. |

---

## 5. When To Use Blue/Green

**Use blue/green when:**
- The onboarding carries high risk (system 3 has millions of records, identity rules are untested)
- The review window is expected to be long (days, not hours)
- Operator confidence in identity rules is low
- Production must not be touched under any circumstances until approval
- The organisation has the infrastructure budget for temporary double storage

**Use gating (PLAN_ONBOARDING_PROPOSAL.md) when:**
- Identity rules are well-understood and have been tested against synthetic data
- The review window is short (minutes to hours)
- Storage cost is a concern
- The team is comfortable with pg-trickle gating semantics
- Simpler operational workflow is preferred

---

## 6. Automation Potential

Blue/green onboarding can be fully automated with the following components:

### 6.1 Blue Pipeline Generator

An in-and-out CLI command:

```bash
inandout onboard create-blue \
    --connector connectors/system3.yaml \
    --osi-mapping config/osi-mapping.yaml \
    --output-dir config/blue/
```

This generates:
- `osi-mapping-blue.yaml` with `_blue` suffixed source declarations
- Stream table creation SQL for replicating systems 1 and 2's source tables
- Stream table creation SQL for the full blue view pipeline
- A rollback script that drops all blue artifacts

### 6.2 Automated Comparison

```bash
inandout onboard compare-blue \
    --green-identity _id_contact \
    --blue-identity _id_contact_blue \
    --threshold-merge-pct 5
```

Outputs:
- Cluster merge count and percentage
- New identity keys introduced by system 3
- Delta summary per system (insert/update/delete/noop counts)
- PASS/FAIL against threshold

### 6.3 Automated Cutover

```bash
inandout onboard cutover-blue \
    --osi-mapping config/blue/osi-mapping-blue.yaml \
    --drain-timeout 30s \
    --confirm
```

Executes the transactional rename sequence. Requires `--confirm` flag (no silent cutover).

### 6.4 State Machine

| State | Trigger | Next State |
|---|---|---|
| `created` | `create-blue` command | `ingesting` |
| `ingesting` | System 3 first full sync completes | `resolving` |
| `resolving` | Blue `_id_*` first refresh completes | `reviewing` |
| `reviewing` | Operator approval OR auto-threshold PASS | `cutting_over` |
| `cutting_over` | Cutover transaction commits | `live` |
| `live` | Cleanup old green artifacts | `done` |
| (any) | Operator cancellation | `rolled_back` |

---

## 7. Interaction With pg-trickle

| pg-trickle Feature | Role in Blue/Green |
|---|---|
| Stream tables as source replicators | Blue source tables for systems 1 and 2 are stream tables over green sources — built-in incremental sync |
| DIFFERENTIAL mode | Keeps blue sources nearly real-time with green at minimal cost |
| Source gating | Gate system 3's blue source table during initial load (same as the gating approach) |
| Tiered scheduling | Blue pipeline stream tables can run at `warm` or `cold` tier to reduce compute |
| `create_or_replace_stream_table` | Idempotent creation of blue pipeline — safe to re-run |
| Watermark gating | Not directly used during blue/green (the blue pipeline has its own watermark namespace) |
| Bootstrap gate status | Monitor blue source load progress |

**No new pg-trickle features are required.** The blue/green approach uses only existing v0.5.0+ capabilities.

---

## 8. Storage and Performance Estimate

For a pipeline with 3 systems, 5 datatypes each, 500K records per datatype:

| Component | Green (existing) | Blue (additional) |
|---|---|---|
| Source tables | 10 tables × 500K rows | 15 tables × 500K rows (includes system 3) |
| Stream tables (6 per mapping) | ~60 stream tables | ~90 stream tables |
| Estimated additional storage | — | ~2× source table storage + ~1.5× stream table storage |
| Estimated additional CPU | — | ~40–60% increase in pg-trickle scheduler load (can be mitigated by tiered scheduling) |
| Duration of additional cost | — | Temporary: days to weeks until cutover and cleanup |

---

## 9. Open Questions

1. **OSI-Mapping engine support for name prefixes:** Does the engine support generating views with a user-specified suffix? If not, the blue YAML must be hand-edited to rename all targets and mappings. This is the biggest operational friction point.

2. **Foreign key resolution across green/blue:** If system 3's contacts reference companies, and the company identity view is also duplicated in blue, the FK resolution must use the blue company identity — not green's. This happens naturally if the entire pipeline is duplicated, but requires discipline in YAML authoring.

3. **Cutover for IMMEDIATE-mode stream tables:** If any stream table in the pipeline uses `refresh_mode => 'IMMEDIATE'`, the cutover rename may fail because IVM triggers reference the old table name. Verify that pg-trickle handles table renames for IMMEDIATE-mode STs.

4. **Partial blue/green:** Can we avoid duplicating the entire pipeline by only creating a blue variant of the identity and delta stages? The forward views could reference the live source tables. This reduces storage but reintroduces the problem of modifying the live pipeline — defeating the purpose.
