# Integration Strategy: In-and-Out + OSI-Mapping

**Date:** March 20, 2026  
**Status:** Strategic Analysis  
**Recommendation:** Adopt OSI-mapping as upstream consolidation layer; refocus in-and-out as specialized HTTP API bidirectional connector

---

## Executive Summary

OSI-mapping is a **declarative specification and reference engine for multi-source data consolidation**—essentially a purpose-built MDM (Master Data Management) system that solves nine cascading integration problems that compound when using custom code.

**Critical Discovery:** The current in-and-out design attempted to include identity resolution, conflict resolution, and MDM semantics within the bidirectional sync tool. This is **fundamentally the wrong place** for these responsibilities. OSI-mapping handles these concerns better, with clearer semantics and testable guarantees.

**Strategic Implication:** By adopting OSI-mapping as the upstream consolidation layer, in-and-out can **dramatically simplify its scope** to focus exclusively on **reliable, conflict-aware HTTP API synchronization**—what it does better than anything else.

---

## 1. What Is OSI-Mapping?

### Project Definition

OSI-mapping is a **declarative integration schema specification** (JSON Schema Draft 2020-12) paired with a **reference Rust implementation** that compiles multi-source consolidation rules into a PostgreSQL view pipeline.

**It is NOT:**
- An ETL orchestrator (no scheduling—that's external)
- A CDC system (no change detection—external systems detect changes)
- A database migration tool
- A data quality platform
- A replacement for in-and-out

**It IS:**
- A specification language for declaring how fields from multiple source systems map to a shared target model
- A deterministic rule engine for conflict resolution and identity matching
- A bidirectional sync engine (computes what changes need to flow back to each source)
- A database-agnostic specification (currently PostgreSQL views, extensible to other targets)

### The Nine Problems It Solves

Integration teams working with 2+ external systems face cascading problems that compound:

1. **Identity Matching:** Systems don't share primary keys. CRM account 2000 and ERP customer CUST-001 represent the same real company. How do you match them?
2. **Conflict Resolution:** Same contact exists in CRM (name: "Alice"), ERP (name: "Alice Smith"), and HRIS (name: "A. Anderson"). Which name is correct?
3. **Field Merging:** Different systems have different fields. CRM has first_name/last_name, ERP has full_name. How do you consolidate?
4. **Transitive Identity:** If A matches B (same email) and B matches C (same tax ID), then A = B = C. But this requires connected-component algorithms.
5. **FK Resolution Across Namespaces:** Contact C1 in CRM references company 100. Contact C2 in ERP references company CUST-001. When these companies merge, which company ID should the contact point to?
6. **Bidirectional Sync:** Start with one-way (CRM → ERP). Then stakeholders ask: "Can we update CRM when ERP changes?" Entire system breaks if not designed for bidirectionality from the start.
7. **Noop Suppression:** Prevent round-trip echoes. If CRM says name="Alice", resolved value is "Alice", don't generate an update just because other sources differ.
8. **Nested & Denormalized Data:** APIs return embedded objects (order with nested line items). Extracting, consolidating, and reassembling requires careful handling.
9. **Testability:** Integration logic is critical and fragile. How do you test without building bespoke test infrastructure?

OSI-mapping solves all nine **together**, via a single declarative YAML file that serves as the contract.

---

## 2. OSI-Mapping Architecture

### Three-Tier System

```
Specification Layer    → JSON Schema (Draft 2020-12)
                         Defines valid mapping documents

Declaration Layer      → Single YAML file per integration
                         One file describes full consolidation contract
                         (targets, mappings, tests, source metadata)

Engine Layer          → Rust compiler (engine-rs)
                       Validates YAML
                       Generates PostgreSQL SQL DDL
                       Creates view DAG for execution
```

### View Pipeline (6 Stages)

The engine generates a directed acyclic graph of PostgreSQL views. Each mapping produces 6 views:

| Stage | View Name | Responsibility | Details |
|-------|-----------|---|---|
| **1. Forward** | `_fwd_{mapping}` | Project source → target shape | Applies forward transforms, filters, captures original values in `_base` |
| **2. Identity** | `_id_{target}` | Assign cluster IDs via transitive closure | Runs connected-components algorithm on identity fields |
| **3. Resolution** | `_resolved_{target}` | Apply conflict strategies, merge rows | Groups by `_cluster_id`, applies per-field resolution strategy |
| **4. Analytics** | `{target}` | Clean golden record for BI/apps | Produced always; authoritative output |
| **5. Reverse** | `_rev_{mapping}` | Project resolved target back to source shape | Includes FK resolution, reverse expressions |
| **6. Delta** | `_delta_{mapping}` | Compute change classification | Inserts, updates, deletes vs original |

### Engine Capabilities

- **Validation:** 11-pass semantic validator
- **Code Generation:** Pure SQL (ANSI) with PostgreSQL extensions (JSONB, windows, arrays)
- **CLI:** `validate`, `render` (generate SQL), `dot` (visualize DAG)
- **Testing:** Inline test execution with full pipeline

---

## 3. OSI-Mapping: Key Concepts & Features

### A. Identity Resolution

**Problem:** Systems don't share PKs. Match records via domain-meaningful attributes.

**Solution:** Declare identity fields using business keys:

```yaml
targets:
  contact:
    fields:
      email: identity                 # Match key 1
      tax_id: identity                # Match key 2
      name: coalesce
```

**Algorithm:**
1. All rows with matching identity field values form edges in a graph
2. Transitive closure: if A matches B and B matches C, all get same `_cluster_id`
3. Groups rows for downstream resolution

**Composite Matching (Link Groups):**
```yaml
fields:
  first_name:
    strategy: identity
    link_group: person_key
  last_name:
    strategy: identity
    link_group: person_key
  date_of_birth:
    strategy: identity
    link_group: person_key
```
Requires ALL three fields to match together (prevents false positives).

### B. Conflict Resolution Strategies

When multiple sources contribute values for the same field, the **strategy** determines the winner:

```yaml
targets:
  contact:
    fields:
      email: identity                      # Match key
      name: coalesce                       # First non-null by priority
      title: last_modified                 # Most recent wins
      score: { strategy: expression, expression: "max(score)" }
      tags: collect                        # Gather all values
      is_deleted: bool_or                  # True if any source is true
```

**Strategy Details:**

| Strategy | Mechanism | Config |
|----------|-----------|--------|
| `identity` | Match key (required ≥1 per target) | — |
| `coalesce` | First non-null by priority | `priority: int` on field mappings |
| `last_modified` | Newest timestamp wins | `last_modified: {field \| expression}` |
| `expression` | Custom SQL aggregation | `expression: "max(...)"` |
| `collect` | Gather all values | — |
| `bool_or` | True if any source true | — |

**Atomic Resolution (Groups):**
Related fields that must come from the same source:

```yaml
fields:
  street:
    strategy: last_modified
    group: address_block
  city:
    strategy: last_modified
    group: address_block
  zip:
    strategy: last_modified
    group: address_block
```

The winning source (by timestamp across ANY field in the group) provides ALL fields in the group. Prevents mixing street from CRM with city from ERP.

### C. Foreign Key Resolution

**Problem:** When entities merge (CRM company 100 = ERP company CUST-001), contacts pointing to either must preserve correct local IDs during reverse sync.

**Solution:** Declare `references` on target fields:

```yaml
targets:
  contact:
    fields:
      company_id:
        strategy: coalesce
        references: company

mappings:
  - name: crm_contacts
    target: contact
    fields:
      - source: company_id
        target: company_id
        references: crm_companies   # Resolves CRM company mapping
```

**Mechanism:**
1. Resolved contact references merged company entity
2. Engine traces back to which source row belongs to the merged entity
3. Extracts that source's local company ID
4. Emits correct local ID in reverse view

**Result:** Contacts preserve their original company references locally, while globally unified.

### D. Nested & Denormalized Structures

**Problem:** APIs return embedded data (order with line items, nested arrays).

**Solution:** Declare child mappings with `parent:` and `array:`:

```yaml
mappings:
  - name: orders
    source: order_source
    target: order

  - name: order_lines
    parent: orders              # Inherits source
    array: lines                # Column name (JSONB array)
    parent_fields: { order_id: id }  # Bridge parent context
    target: order_line
    fields:
      - source: line_num
        target: line_number
```

**Engine's nested handling:**
- **Forward:** Extract JSONB arrays via `jsonb_array_elements()`
- **Resolution:** Resolve children independently, keep parent FK
- **Reverse:** Collect resolved children, reassemble into arrays via `jsonb_agg()`

### E. Bidirectional Sync & Delta Detection

**Default:** Every mapping implies both forward and reverse directions.

**Delta View** compares resolved values against original source (`_base`):
```sql
SELECT
  CASE
    WHEN _base IS NULL AND resolved IS NOT NULL THEN 'insert'
    WHEN _base IS NOT NULL AND resolved IS NULL THEN 'delete'
    WHEN _base IS NOT NULL AND resolved IS NOT NULL AND _base != resolved THEN 'update'
    ELSE 'noop'
  END AS change_type
```

**Noop Suppression:**
- Compare resolved values against `_base` (captured at forward time)
- Prevents round-trip echoes (CRM says name="Alice", resolved name="Alice" → noop, don't update ERP)
- Optional `normalize` expression for lossy comparisons (rounding, case-folding)

### F. Lineage & Provenance

Every row emitted includes metadata:
- `_cluster_id` — Entity identity (for linking across systems)
- `_src_id` — Source row PK (for tracing origin)
- `_entity_id` — Deterministic entity hash
- `_base` — Original source values (for delta comparison)
- `_ts_{field}` — Per-field timestamp (for lineage)

---

## 4. Input & Output Contracts

### Input Contract (to OSI Engine)

**Source:** Single YAML file declaring:

```yaml
version: "1.0"
description: "Contact consolidation: CRM + ERP → unified contact"

sources:
  crm:
    table: inout_src_crm_contact
    primary_key: id
  erp:
    table: inout_src_erp_customer
    primary_key: customer_id

targets:
  contact:
    fields:
      email: identity
      name: coalesce
      company_id: { references: company }

mappings:
  - name: crm_contacts
    source: crm
    target: contact
    fields:
      - source: email
        target: email
      - source: name
        target: name

tests:
  - description: "Alice exists in both; CRM name wins"
    input: {...}
    expected: {...}
```

**Data Contract:**
- PostgreSQL database with source tables populated
- Table names and primary_key must match declarations
- Assumes `_base` is populated by ingestion tool (original source snapshot)
- Optional: `cluster_members`, `written_state` feedback tables from ETL

### Output Contract (from OSI Engine)

**SQL DDL:** Set of PostgreSQL views:

```sql
CREATE VIEW _fwd_crm_contacts AS ...
CREATE VIEW _id_contact AS ...
CREATE VIEW _resolved_contact AS ...
CREATE VIEW contact AS ...
CREATE VIEW _rev_crm_contacts AS ...
CREATE VIEW _delta_crm_contacts AS ...
```

**Each view exposes:**
- Core business fields (email, name, etc.)
- Metadata columns:
  - `_cluster_id` — Entity identity
  - `_src_id` — Source PK for tracing
  - `_base` — Original source values
  - `_ts_{field}` — Timestamp per field

**Consumer Responsibility:**
- Read from delta views to identify changes
- Propagate changes back to external systems
- Write feedback (generated IDs) to `cluster_members` table

---

## 5. Current In-and-Out Design vs. OSI-Mapping

### The Conceptual Mismatch

**Current In-and-Out Assumption:**
```
External
 APIs
   ↓
[Ingestion Tool]  ← pulls data
   ↓
PostgreSQL tables
   ↓
[External MDM]  ← computes identity, conflict resolution, desired-state
   ↓
Desired-State Table  ← cluster_id, base, action pre-populated
   ↓
[Writeback Tool]  ← executes writes
   ↓
External APIs
```

**Reality:** There's no external MDM. That's exactly the 9 cascading problems OSI solves!

**With OSI-Mapping:**
```
External APIs
   ↓
[Ingestion Tool]  ← pulls data, stores raw snapshot
   ↓
PostgreSQL tables (inout_src_*)
   ↓
[OSI-Mapping Engine]  ← consolidates, resolves identity, computes delta
   ↓
PostgreSQL views (_delta_{mapping})  ← action-classified, per-source,
                                       with _cluster_id and _base
   ↓ (optional: thin business-filter query on top)
[Writeback Tool]  ← executes writes, unchanged
   ↓
External APIs
```

### Key Differences

| Aspect | Current In-and-Out | With OSI-Mapping |
|--------|---|---|
| **MDM Responsibility** | Assumed external | OSI-Mapping is the MDM |
| **Identity Computation** | Writeback tool | OSI via `_cluster_id` |
| **Conflict Resolution** | Assumed external | OSI via field strategies |
| **Desired-State Source** | Assumed external | OSI `_delta_{mapping}` views (directly) |
| **Base State Origin** | Assumed external | OSI's `_base` (source snapshot) |
| **Action Classification** | Assumed external | OSI's `_action` in delta views |
| **Per-Source Routing** | Assumed external | OSI produces one `_delta` per mapping |
| **Reverse Mapping** | Part of writeback config | OSI computes in `_rev` views |
| **Payload Reshaping** | Part of writeback config | In-and-out `transform.template` (unchanged) |
| **Business Filtering** | N/A | Optional thin query on OSI delta views |
| **Testing** | Per-connector integration tests | OSI's YAML tests + in-and-out e2e tests |

---

## 6. Proposed Integrated Architecture

### High-Level Flow

```
┌──────────────────────────────────────┐
│      External HTTP APIs              │
│  (CRM, ERP, HRIS, Warehouse, etc.)   │
└──────────┬───────────────┬───────────┘
           │               │
        pulls            reads from
           │               │
           ▼               │
┌──────────────────────────┐
│  IN-AND-OUT INGESTION    │
│  ──────────────────────  │ Responsibility: Reliable HTTP connector
│  - Pagination            │  - Pull data via HWM, full-table, webhooks
│  - Pagination, HWM modes │  - Deduplicate by external_id
│  - Schema tracking       │  - Normalize timestamps
│  - Rate limiting         │  - Track deletions
│  - Error handling        │  - Handle pagination, field selection
└──┬───────────────────────┘
   │ writes snapshots (with _base)
   ▼
┌──────────────────────────────────────┐
│  PostgreSQL Source Tables            │
│  ──────────────────────────────────  │
│  inout_src_crm_contact               │
│  inout_src_sap_customer              │
│  inout_src_hris_employee             │
│  (raw data exactly as pulled)        │
└────────────┬─────────────────────────┘
             │
          reads
             │
             ▼
┌──────────────────────────────────────┐
│  OSI-MAPPING CONSOLIDATION ENGINE    │
│  (Rust + PostgreSQL)                 │
│  ──────────────────────────────────  │
│  Responsibility: MDM layer           │
│  - Identity resolution via transitive│
│    closure (email ↔ tax_id)          │
│  - Conflict resolution by strategy   │
│    (coalesce, last_modified, etc.)   │
│  - FK resolution across ID spaces    │
│  - Reverse projection to source shape│
│  - Delta classification per source   │
│    (insert/update/delete/noop)       │
│  - Lineage & provenance              │
│                                      │
│  Generates views:                    │
│  - _resolved_contact (golden record) │
│  - _rev_crm_contacts (source shape)  │
│  - _delta_crm_contacts (classified)  │
│  - _cluster_id (entity identity)     │
│  - _base (original source values)    │
│                                      │
│  _delta views are the desired-state  │
│  source: action, cluster_id, data,   │
│  and _base are all present.          │
│                                      │
│  Business logic declared IN the YAML:│
│  - filter / reverse_filter (routing) │
│  - expression/reverse_expression     │
│  - written_state + derive_noop       │
│  - cluster_members (insert feedback) │
│  - reverse_required (delete prop.)   │
└────────────┬─────────────────────────┘
             │
          consumes
             ▼
┌──────────────────────────────────────┐
│  DESIRED-STATE TABLES                │
│  ──────────────────────────────────  │
│  inout_dst_crm_contact               │
│  inout_dst_sap_customer              │
│  inout_dst_hris_employee             │
│                                      │
│  Columns:                            │
│  - action (insert/update/delete)     │
│  - cluster_id                        │
│  - data (JSONB payload)              │
│  - base (original source snapshot)   │
│  - base_version (ETag/version)       │
└────────────┬─────────────────────────┘
             │
          consumes
             │
             ▼
┌──────────────────────────────────────┐
│  IN-AND-OUT WRITEBACK                │
│  ──────────────────────────────────  │
│  Responsibility: HTTP conflict       │
│  detection & write execution         │
│  - 3-way merge (base vs current)     │
│  - Conditional writes (ETags)        │
│  - Field mapping & transforms        │
│  - Identity mapping feedback         │
│  - Last-written-state tracking       │
│  - Rate limiting & retries           │
│  - Dead-letter handling              │
│  - Circuit breaker                   │
└────────────┬─────────────────────────┘
             │ writes changes
             ↓
┌──────────────────────────────────────┐
│      External HTTP APIs              │
│  (modifications written back)        │
└──────────────────────────────────────┘
```

### Architectural Responsibilities

**OSI-Mapping (Consolidation + Business Logic Layer)**
- Declares target entity schemas (what contact should look like)
- Declares field mappings with forward/reverse transforms (`expression` / `reverse_expression`)
- Declares identity rules (when two rows represent same entity)
- Declares conflict strategies (which source wins for each field)
- Declares business filters (`filter` for forward, `reverse_filter` for writeback)
- Computes transitive closure for identity matching
- Produces golden records and per-source change deltas
- Reverse-projects resolved values back to per-source shapes
- Classifies changes as insert/update/delete/noop per source (`_delta` views)
- Handles target-centric noop suppression (`written_state` + `derive_noop: true`)
- Tracks insert feedback via `cluster_members` table (ETL writes, OSI reads)
- Handles delete propagation via `reverse_required: true` on fields
- Maintains lineage & provenance
- Fully testable via embedded inline YAML tests
- **This is 100% of what was previously called the "bridge layer"**

**Optional Business-Filter Query** ~~(formerly "Bridge Layer")~~

*Not needed.* Business filtering, routing, and transforms are all declared in the OSI mapping YAML via `reverse_filter`, `expression`/`reverse_expression`, `written_state`, and `reverse_required`. The only remaining gap is cross-entity transaction atomicity (rare edge case, orchestration concern).

**In-and-Out Ingestion (HTTP Source Connector)**
- Pulls data from external HTTP APIs reliably
- Handles pagination, HWM, full-table, webhook modes
- Deduplicates and upserts by external_id
- Tracks schema drift
- Captures original source snapshot (`_base`)
- Normalizes timestamps
- Rate limited, error handling, dead-letter fallback
- **NOT responsible for:** Identity resolution, conflict resolution, MDM logic

**In-and-Out Writeback (HTTP Target Connector)**
- Reads desired-state directly from OSI delta views (`_delta_{mapping}`)
- Issues pre-flight GET for conflict detection
- Compares base vs current vs resolved
- Issues conditional writes (ETags) when available
- Constructs payloads via `transform.template` (wraps source-shaped delta fields into target API JSON structure)
- Writes ETL feedback: updates `_written_{mapping}` and `_cluster_members_{mapping}` tables after each write
- Handles retries, rate limiting, dead-letter
- **Unchanged core logic:** 3-way merge conflict detection

---

## 7. Data Flow Example: CRM + ERP → Unified Contact

### Step 1: Ingestion
```
HubSpot API GET /contacts/1
→ {id: 100, email: alice@x.com, name: "Alice"}
→ inout_src_crm_contact with _base={...original...}

SAP API GET /customer?id=CUST-001
→ {customer_id: "CUST-001", contact_email: "alice@x.com", full_name: "Alice Smith"}
→ inout_src_sap_customer with _base={...original...}
```

### Step 2: OSI Forward View (`_fwd_crm`, `_fwd_sap`)
```
_fwd_crm:
  email: "alice@x.com"
  name: "Alice"
  _base: {email: "alice@x.com", name: "Alice"}
  _src_id: crm:100

_fwd_sap:
  email: "alice@x.com"
  name: "Alice Smith"
  _base: {email: "alice@x.com", full_name: "Alice Smith"}
  _src_id: sap:CUST-001
```

### Step 3: OSI Identity View (`_id_contact`)
```
Both rows match on email="alice@x.com"
→ Both assigned _cluster_id = <UUID>
```

### Step 4: OSI Resolution View (`_resolved_contact`)
```
Groups by _cluster_id:
  email: "alice@x.com"    (identity field, only value)
  name: "Alice"           (CRM has priority 1, SAP has priority 2)
  _cluster_id: <UUID>
  _src_id: [crm:100, sap:CUST-001]
  _base: {email: "alice@x.com", name: "Alice"}
```

### Step 5: OSI Delta View (`_delta_crm_contacts`)

OSI's delta view already classifies the change and includes all fields needed for writeback:

```sql
-- OSI produces this automatically from _rev vs _base comparison:
_action: 'noop'            -- resolved name "Alice" = _base name "Alice" → no change needed
_cluster_id: <UUID>
id: 100
email: alice@x.com
name: Alice
_base: {id: 100, email: "alice@x.com", name: "Alice"}
```

For SAP (`_delta_sap_contacts`):
```sql
_action: 'update'          -- resolved name "Alice" ≠ _base full_name "Alice Smith" → update
_cluster_id: <UUID>
customer_id: CUST-001
contact_email: alice@x.com
full_name: Alice           -- Resolved: CRM priority wins
_base: {customer_id: "CUST-001", contact_email: "alice@x.com", full_name: "Alice Smith"}
```

**No separate bridge layer needed** — this is the desired-state, produced directly by OSI.
Optional: a thin `WHERE` clause can filter (e.g., `WHERE email IS NOT NULL`).

### Step 6: Writeback Conflict Detection
```
1. Read from _delta_sap_contacts: _action=update, _cluster_id=<UUID>,
   full_name="Alice", _base={full_name: "Alice Smith"}
2. Issue GET /customer/CUST-001 → {full_name: "Alice Anderson"}
3. Three-way comparison:
   - base (from OSI _base): {full_name: "Alice Smith"}
   - current (pre-flight): {full_name: "Alice Anderson"}
   - desired (from OSI delta): {full_name: "Alice"}
4. Conflict detected: base≠current ("Anderson" added externally)
5. Apply conflict_resolution strategy:
   - dead_letter: Route to DLQ for operator review
   - last_writer_wins: Overwrite (use caution)
   - skip_and_warn: Leave alone, log warning
   - re_ingest_and_recompute: Trigger next ingestion cycle
```

### Step 7: OSI Delta View (Reverse Mapping)
```
_delta_crm:
  _action: noop             (resolved matches _base → no write needed)
  {id: 100, email: "alice@x.com", name: "Alice"}

_delta_sap:
  _action: update           (resolved name differs from _base's full_name)
  {customer_id: "CUST-001", contact_email: "alice@x.com", full_name: "Alice"}
```

### Step 8: ETL Writes Back
```
If no conflict:
  PATCH /contacts/100 {name: "Alice"}
  PATCH /customer/CUST-001/contact {full_name: "Alice"}

OSI _delta views determined what needed updating.
In-and-out WriteBACK executed HTTP calls and tracked last-written-state.
```

### Step 9: Feedback Loop
```
After successful writes:
  INSERT INTO cluster_members (_cluster_id, _src_id)
  VALUES (<UUID>, 'crm:100'), (<UUID>, 'sap:CUST-001')
  
  This tells OSI: "entity <UUID> contains these source rows"
  On next run, matching is pre-seeded.
```

---

## 8. Configuration Simplification

### Before (Current In-and-Out Design)

In-and-out connector config had to declare MDM semantics:

```yaml
# in-and-out CONFIG_DESIGN.md style
connector:
  name: hubspot-crm
  datatypes:
    contact:
      ingestion:
        list:
          method: GET
          path: /contacts
      
      writeback:
        operations:
          lookup:
            method: GET
            path: /contacts/${external_id}
          update:
            method: PATCH
            path: /contacts/${external_id}
        
        # These shouldn't be here—they're MDM concerns!
        read_write_mapping:
          properties.email: email
          properties.firstname: first_name
        
        # These shouldn't be here either!
        managed_fields:
          - email
          - first_name
          - last_name
        
        # Conflict resolution should be in OSI, not here!
        conflict_resolution: dead_letter
```

### After (OSI-Integrated Design)

In-and-out connector config focuses on HTTP mechanics:

```yaml
# in-and-out SIMPLIFIED
connector:
  name: hubspot-crm
  api_version: v3
  connection:
    base_url: https://api.hubapi.com
  auth:
    type: api_key
    api_key:
      location: header
      name: X-API-Key
  
  datatypes:
    contact:
      ingestion:
        primary_key: id
        list:
          method: GET
          path: /crm/v3/objects/contacts
          record_selector: results
      
      writeback:
        operations:
          lookup:
            method: GET
            path: /crm/v3/objects/contacts/${external_id}
          update:
            method: PATCH
            path: /crm/v3/objects/contacts/${external_id}
            transform:
              template:
                properties:
                  email: "${data.email}"
                  firstname: "${data.first_name}"
                  lastname: "${data.last_name}"
```

**Consolidation logic lives ONCE in OSI YAML** (the single source of truth):

```yaml
# osi/consolidation.yaml (NEW authoritative file)
version: "1.0"
description: "Multi-system contact consolidation"

sources:
  crm:
    table: inout_src_crm_contact
    primary_key: id
  erp:
    table: inout_src_sap_customer
    primary_key: customer_id

targets:
  contact:
    fields:
      email: identity
      first_name: coalesce
      last_name: coalesce
      name: coalesce
      company_id: { references: company }

mappings:
  - name: crm_contacts
    source: crm
    target: contact
    fields:
      - source: email
        target: email
      - source: firstname
        target: first_name
        priority: 1
      - source: lastname
        target: last_name
        priority: 1
      - source: name
        target: name
        priority: 1
  
  - name: sap_contacts
    source: erp
    target: contact
    fields:
      - source: contact_email
        target: email
      - source: full_name
        target: name
        priority: 2

tests:
  - description: "CRM email matches SAP email → single contact"
    input:
      crm: [{id: 100, email: alice@x.com, firstname: Alice, lastname: Smith}]
      sap: [{customer_id: CUST-001, contact_email: alice@x.com, full_name: Alice S.}]
    expected:
      resolved:
        - email: alice@x.com
          first_name: Alice
          last_name: Smith
```

**Result:**
- In-and-out config: **simpler, more focused**
- OSI config: **Single source of truth for consolidation**
- Bridge layer: **Eliminated — OSI covers 100% via filter/reverse_filter, expression/reverse_expression, written_state + derive_noop**
- Requirements: **More precise, non-overlapping**

---

## 9. Impact on In-and-Out Requirements (GOAL.md)

### Requirements That Can Be Removed (OSI-Mapping Now Owns These)

| Requirement | Current Scope | With OSI | Reason |
|---|---|---|---|
| **#1: Per-Datatype Mapping** | Writeback must declare API endpoint per datatype | OSI declares this in central config | Consolidation mapping, not HTTP sync concern |
| **#7: Desired-State Table** | Writeback tool must produce these | OSI delta views produce these directly | MDM + delta classification is OSI's concern |
| **#8: Identity Mapping** | Writeback tool computes cluster_id | OSI's `_cluster_id` via transitive closure | Identity resolution, not HTTP protocol concern |
| **#12: API Asymmetry Handling** | Writeback handles read≠write schemas | Split: OSI handles consolidation asymmetry, in-and-out handles target asymmetry | Consolidated schema asymmetry is MDM concern |
| **#34: Cluster Merge & Split Propagation** | Writeback tool handles merge/split actions | OSI delta views emit merge/split action classifications | Identity resolution logic, not HTTP sync concern |

### Requirements Simplified (Clearer Responsibility)

| Requirement | Change | Impact |
|---|---|---|
| **#8: Identity Mapping** | Change from "compute + enforce unique constraint" to "accept pre-computed from bridge" | Writeback now ~50 lines simpler |
| **#16: External Reference Field** | Move from writeback config to OSI reverse view | OSI reverse view can include cluster_id as a mapped field |
| **#23: Pre-Write Validation** | Simplified: validate against target API schema (not consolidation schema) | Less redundant validation |
| **#37: Connector Validation Mode** | Move to bridge layer + OSI validation | OSI tests full consolidation pipeline |

### Requirements Unchanged (Core Sync Logic Stays)

| Requirement | Status | Why |
|---|---|---|
| **#3: Conflict Prevention (3-way merge)** | ✅ Unchanged | Pre-flight read + base/current/resolved comparison still valid |
| **#5: Client-Side Patching** | ✅ Unchanged | Diff logic against resolved values still applies |
| **#9: Last-Written-State** | ✅ Unchanged | Still needed for tracking + future re-ingestion |
| **#11: Politeness & Rate Limiting** | ✅ Unchanged | HTTP protocol concern, stays in in-and-out |
| **#13-25: Error Handling, Retry, DLQ, Circuit Breaker** | ✅ Unchanged | Operational concerns stay in writeback tool |

### New Requirements (Due to OSI Integration)

1. **Ingestion must populate `_base` column** — Captured original source values, needed by OSI for delta detection
2. **Writeback must read `cluster_id` from desired-state** — No longer computed, now provided
3. **Writeback must write cluster feedback to PostgreSQL** — `cluster_members` table for OSI's next cycle
4. **Configuration must reference OSI mapping schema** — In-and-out knows what consolidated entity it syncs
5. **Lineage preservation** — Must not drop `_cluster_id`, `_src_id`, `_base` in desired-state table

---

## 10. Implementation Timeline

### Phase 1: Ingestion Refactoring (Weeks 1-2)
**Focus: Understand that ingestion is now a source connector, not an MDM tool**

- Remove any MDM semantics from ingestion config schema
- Add `_base` column to source tables (original source snapshots)
- Document: "These tables are staging; OSI-mapping reads them"
- Update tests: confirm `_base` is correctly populated

**Deliverables:**
- Simplified ingestion config (no clustering, no identity mapping)
- Source tables with `_base` column
- Documentation of in-and-out ↔ OSI data contract

### Phase 2: Writeback Adaptation (Weeks 3-4)
**Focus: Writeback becomes a config consumer, not a config producer**

- Accept `cluster_id` as provided column (no longer compute)
- Accept `base` from desired-state (aligned with OSI's `_base`)
- Simplify identity mapping (just track cluster_id → external_id reads/writes)
- Add feedback: write `cluster_members` after successful inserts

**Deliverables:**
- Writeback tool reads pre-populated desired-state tables
- Feedback loop writes to PostgreSQL for OSI
- Simplified identity mapping logic
- Updated tests for new data contracts

### Phase 3: Business-Filter Patterns & Direct Delta Consumption (Weeks 5-6)
**Focus: Document how writeback reads OSI delta views**

- Create dbt example: `_resolved_contact` → `desired_state_contact`
- Create Python example: same transformation
- Document business rule patterns (filters, transformations)
- Show how to handle atomicity across entities

**Deliverables:**
- dbt template project
- Python SDK for bridge building
- Example patterns: simple mapping, filtering, merging
- Integration test: OSI → bridge → writeback

### Phase 4: Integration Testing (Weeks 7-8)
**Focus: End-to-end validation**

- Test: CRM API → ingestion → OSI consolidation → bridge → writeback → ERP API
- Test: External changes detected, re-ingested, resolved, written back
- Test: Noop suppression working
- Test: Conflict detection scenarios

**Deliverables:**
- Integration test suite
- Example scenarios (2-system, 3-system, nested entities)
- Observability dashboard (lineage, change tracking)

---

## 11. What Gets Discontinued vs. Repurposed

### Discontinued Concepts

1. **MDM-like features in in-and-out config**
   - Per-datatype mapping (OSI declares)
   - Cluster ID computation (OSI computes)
   - API asymmetry handling for consolidation (OSI handles)
   - Pre-conflict-resolution logic (OSI does it)

2. **Writeback tool responsibility for desired-state production**
   - No longer produce desired-state from raw ingestion
   - No longer compute cluster_id from external_id
   - No longer assume external MDM exists

### Repurposed Concepts

| Concept | Current Role | New Role | Handler |
|---------|---|---|---|
| **Conflict Detection** | Writeback tool unique responsibility | Still in writeback, same core algorithm | In-and-out |
| **3-way Merge** | Custom logic in writeback | Still in writeback, base now from OSI | In-and-out |
| **Identity Mapping Table** | Compute + validate uniqueness in writeback | Accept pre-computed from bridge | In-and-out |
| **Noop Suppression** | Custom bindseparate checks | OSI's `_base` comparison + writeback validation | OSI + in-and-out |
| **Reverse Mapping** | Writeback tool computes reverse payload | OSI computes in delta view; writeback consumes | OSI |
| **Last-Written-State** | Writeback tracks write outcomes | Still tracked in writeback | In-and-out |

---

## 12. OSI-Mapping Strengths & Limitations

### Strengths

✅ **Deterministic Identity Resolution** — Transitive closure computable, testable, repeatable  
✅ **Conflict Resolution Strategies** — Field-level control, composable, explicit  
✅ **Multi-System Consolidation** — Designed for N sources simultaneously, not pairwise  
✅ **FK Resolution** — Automatic translation across ID namespaces (rare feature in industry)  
✅ **Bidirectional by Default** — Every mapping implies reverse direction  
✅ **Lineage & Provenance** — Data lineage captured automatically  
✅ **Testable** — Full pipeline executable in test containers  
✅ **Declarative** — Single YAML file is the contract, not custom code  
✅ **Portable** — Rust engine is self-contained, no external dependencies  
✅ **Composable** — Works with any upstream source (not HTTP-specific)  

### Limitations & Gaps

❌ **No Orchestration** — OSI doesn't schedule syncs; external system must trigger  
❌ **No CDC** — OSI doesn't detect changes; external CDC tool must populate tables  
❌ **PostgreSQL Views Only** — Currently generates views; other targets would need engine extensions  
❌ **No Streaming (native IVM)** — Batch processing model; views re-execute from scratch  
❌ **No Transaction Semantics** — Cannot guarantee all-or-nothing ACID guarantee across sources  
❌ **Expression Safety** — SQL expression safety checked at validation time; assumes trusted authors  

**These gaps are not weaknesses—they're intentional scope boundaries. OSI is the consolidation layer, not the orchestration or execution layer.**

> **Note:** The "No CDC" and "No Streaming/IVM" gaps are directly addressed by **pg-trickle** (https://github.com/grove/pg-trickle/), a companion PostgreSQL extension by the same team. pg-trickle converts OSI's view pipeline into automatically-refreshing stream tables maintained via differential dataflow. See [REPORT_PG_TRICKLE.md](REPORT_PG_TRICKLE.md) for details.

---

## 13. OSI-Mapping Covers 100% of the Bridge Layer

### Confirmed After Schema Reference Analysis

A thorough reading of the OSI-Mapping schema reference (https://github.com/BaardBouvet/OSI-mapping/blob/main/docs/reference/schema-reference.md) confirms that the author's claim is correct: **OSI-Mapping's schema handles every concern we previously attributed to a "bridge layer".**

| Concern | OSI Feature | Example |
|---|---|---|
| Business filtering (forward) | `filter: "status = 'active'"` | Only active rows contribute to golden record |
| Business filtering (reverse) | `reverse_filter: "type LIKE '%customer%'"` | Only customer-type entities written back to this source |
| Multi-target routing | Multiple mappings, each with its own `reverse_filter` | Enterprise → ERP, SMB → CRM, all declared per mapping |
| Field-level transforms | `expression: "split_part(full_name, ' ', 1)"` | Forward: split full_name into first_name |
| Reverse transforms | `reverse_expression: "first_name \|\| ' ' \|\| last_name"` | Reverse: reconstruct full_name from parts |
| Constants / injections | `direction: forward_only` with an `expression` | Inject `type: 'customer'` for this source in reverse |
| Target-centric noop | `written_state: true` + `derive_noop: true` | Skip write if resolved value matches what was last actually sent |
| Insert feedback | `cluster_members: true` | ETL writes back generated IDs; OSI reads them on next cycle |
| Delete propagation | `reverse_required: true` on a field | If `is_active` is null in resolved record, treat as delete |
| Precision-loss noop | `normalize: "trunc(%s::numeric, 0)"` | Don't emit update if only rounding difference changes |
| Nested array elements | `derive_tombstones: true` | Propagate element-level deletions across sources |
| Composite transforms | `default_expression: "first_name \|\| ' ' \|\| last_name"` | Computed fallback when no source provides value |

### What Remains (Outside OSI's Scope)

These are NOT bridge layer concerns — they are separate component responsibilities:

1. **HTTP payload wrapping** — In-and-out's `transform.template` constructs the final JSON envelope (`{"properties": {"firstname": "..."}}`) for target APIs. This is HTTP config, not business logic.
2. **ETL feedback writes** — In-and-out writeback maintains `_written_{mapping}` and `_cluster_members_{mapping}` tables after each sync cycle. This is in-and-out's operational responsibility.
3. **Cross-entity transaction atomicity** — Ensuring contact + company update atomically. Rare edge case; orchestration concern, not a bridge layer.

### Architecture Is Two YAML Files

```
osi-mapping.yaml          (business logic: identity, conflict, filtering, routing, transforms)
connectors/hubspot.yaml   (HTTP mechanics: auth, pagination, endpoints, rate limits, payload templates)
```

No bridge layer. No intermediate code. No separate SQL views needed.

---

## 14. Strategic Recommendations

### 1. Adopt OSI-Mapping as Upstream Consolidation Layer ✅

**Action:** Directly integrate OSI-mapping into the in-and-out architecture. Make it a required dependency for any multi-source scenario.

**Rationale:**
- Solves identity resolution & conflict resolution deterministically
- Single source of truth for consolidation rules
- Fully testable via embedded YAML tests
- Declarative (not custom code)
- No reinvention of algorithms (transitive closure is hard)

### 2. Refocus In-and-Out on HTTP API Sync ✅

**Action:** Remove all MDM semantics from in-and-out. Focus exclusively on:
- Reliable HTTP ingestion (pagination, HWM, webhooks)
- HTTP writeback with conflict detection (3-way merge)
- Error handling, retries, dead-letter
- Lineage preservation

**Rationale:**
- Each tool does one thing well
- Configuration becomes simpler
- Less feature creep
- Clear boundaries

### 3. Document OSI YAML Patterns for Common Cases ✅

**Action:** Create reference OSI YAML examples showing how to express common integration patterns:
- Filtering (`reverse_filter`)
- Multi-target routing (multiple mappings with different `reverse_filter`)
- Field transforms (`expression` / `reverse_expression`)
- Target-centric noop (`written_state` + `derive_noop`)
- Insert propagation (`cluster_members`)
- Delete propagation (`reverse_required`)

**Rationale:**
- OSI covers 100% of business logic but teams need worked examples
- The OSI YAML _is_ the integration contract — document it as such
- Each pattern is testable inline via OSI's embedded test harness

### 4. Create Integration Tests ✅

**Action:** Build end-to-end tests: API pull → OSI consolidation → bridge → writeback → API push

**Rationale:**
- Validates the full architecture works
- Catches impedance mismatches early
- Serves as documentation
- Enables confidence in production use

### 5. Update GOAL.md & CONFIG_DESIGN.md ✅

**Action:** Rewrite to reflect OSI-integrated architecture. Clearly delineate:
- What in-and-out does (HTTP sync)
- What OSI does (consolidation)
- What bridge layer does (business logic)
- Requirements per layer

**Rationale:**
- Prevents future confusion
- Guides implementation priorities
- Communicates architecture to stakeholders

### 6. Plan Phase-Out of Overlapping Logic ✅

**Action:** Identify and remove from in-and-out:
- Identity resolution (OSI)
- Conflict resolution strategies (OSI)
- MDM-like cluster_id computation (OSI)
- Desired-state production (bridge)

**Rationale:**
- Reduces code complexity
- Prevents bugs from duplicate logic
- Clarifies responsibilities

### 7. Adopt pg-trickle as the OSI Execution Substrate

**Action:** Use [pg-trickle](https://github.com/grove/pg-trickle/) to convert OSI's 6-stage view pipeline into automatically-refreshing stream tables. See [REPORT_PG_TRICKLE.md](REPORT_PG_TRICKLE.md) for the full analysis.

**What pg-trickle provides:**
- **Incremental refresh (O(changed rows), not O(all rows))** — Critical for production-scale datasets
- **ETL feedback loop automation** — CDC auto-detects writes to `_written_` / `_cluster_members_`; OSI views re-evaluate without external orchestration
- **DAG-aware scheduling** — Maintains OSI's 6-stage pipeline in topological order automatically
- **Watermark gating** — Holds OSI refresh until ingestion cycle is confirmed complete
- **WITH RECURSIVE in DIFFERENTIAL mode** — Incremental transitive closure updates (only affected clusters recomputed)

**Rationale:**
- Same authors as OSI-Mapping — explicitly designed to compose with it
- Replaces need for external orchestration of OSI pipeline
- Makes large-scale MDM viable (thousands of source records, hundreds of connectors)
- All within PostgreSQL — no additional infrastructure

**Phasing:**
1. Phase 1 (development/early production): Use plain OSI views — simpler, validate the architecture
2. Phase 2 (scale): Wrap OSI views with pg-trickle stream tables when datasets grow beyond ~100K rows per source

**Caveats:** pg-trickle v0.9.0 targets PostgreSQL 18 and is pre-1.0. Plan adoption for v1.0.0 and PG 16/17 compatibility (v0.12.0).

---

## 15. Competitive / Strategic Analysis

### What makes this different from point solutions?

| Tool | What It Does | What In-and-Out + OSI Does |
|---|---|---|
| **Airbyte** | Ingest from 300+ sources to warehouse | Ingest + consolidate + sync back |
| **Fivetran** | Managed data integration (inbound only) | Open-source, bidirectional, conflict-aware |
| **Hightouch** | Reverse-ETL (warehouse → APIs) | Same, but with MDM consolidation |
| **dbt Cloud** | Transform warehouse data | Bridge layer + orchestration |
| **OSI-Mapping alone** | Declarative consolidation spec | No HTTP sync, no orchestration |
| **In-and-Out alone** | Bidirectional HTTP sync | No MDM, no consolidation |
| **pg-trickle alone** | Incremental view maintenance for PostgreSQL | No HTTP sync, no consolidation logic |
| **In-and-Out + OSI** | Bidirectional HTTP sync with MDM | ✅ Complete, open, modular |
| **In-and-Out + OSI + pg-trickle** | Full stack with incremental IVM | ✅ Production-scale, self-orchestrating |

### Unique Capabilities

1. **Open-source MDM** (OSI-Mapping is public)
2. **Bidirectional by default** (not forward-only)
3. **Multi-language support** (YAML spec, pluggable engines)
4. **No vendor lock-in** (can run locally, migrate easily)
5. **Composable** (swap ingestion connectors, consolidation engines, orchestrators)

---

## 16. Risk Mitigation

### Risk 1: OSI-Mapping Immaturity

**Concern:** OSI-Mapping is relatively new (created recently).

**Mitigation:**
- Start with simple 2-source scenarios
- Port in-and-out tests to OSI (validate compatibility)
- Build integration tests gradually
- Keep escape hatch: can skip OSI for single-source scenarios

### Risk 2: Coupling In-and-Out to OSI-Mapping

**Concern:** What if OSI-Mapping changes its spec or becomes unmaintained?

**Mitigation:**
- In-and-out reads PostgreSQL views, not OSI directly
- Could swap OSI with alternative consolidation engine
- Versioning: in-and-out declares required OSI version
- Keep bridge layer as abstraction (can switch consolidation layer underneath)

### Risk 3: Underestimating OSI's Scope

**Concern:** Teams may still build separate bridge layers not knowing OSI already handles filtering, routing, and transforms natively.

**Mitigation:**
- Document OSI's `filter`/`reverse_filter`, `expression`/`reverse_expression`, `written_state`, and `cluster_members` features prominently
- Provide worked examples in OSI YAML for common patterns (routing, transforms, noop detection)
- Default contract: writeback reads `_delta_{mapping}` directly; no intermediate layer

### Risk 4: Performance (Views + Triggers)

**Concern:** PostgreSQL views + 6-stage pipeline might be slow at scale. The `WITH RECURSIVE` transitive closure at Stage 2 is especially expensive — O(all source rows) per full recompute.

**Mitigation:**
- OSI generates standard SQL (no magic); simple scenarios are fast
- For moderate scale (up to ~100K rows per source): plain OSI views are acceptable, especially for development
- **For production scale (>100K rows): use pg-trickle** — converts OSI's view pipeline into differential stream tables, reducing per-cycle cost from O(all rows) to O(changed rows). Same SQL interface, no connector change required
- pg-trickle supports `WITH RECURSIVE` in DIFFERENTIAL mode — only affected clusters recomputed, not all
- See [REPORT_PG_TRICKLE.md](REPORT_PG_TRICKLE.md) for the complete performance analysis and integration guide

---

## 17. Conclusion & Next Steps

### Summary

OSI-Mapping represents a **paradigm shift** from custom integration code to **declarative, testable consolidation rules**. By adopting it as the upstream layer, in-and-out can:

1. **Dramatically simplify its scope** — Focus on HTTP sync mechanics, not MDM concerns
2. **Eliminate redundant logic** — Identity resolution happens once (in OSI), not in filters throughout the codebase
3. **Improve testability** — OSI's YAML tests are executable; integration tests can be deterministic
4. **Reduce configuration burden** — Single YAML file declares all consolidation rules, not scattered across multiple connector configs
5. **Enable multi-system scenarios** — Naturally handles 2+ sources simultaneously, not just pairwise
6. **Provide clear architecture** — Two components: OSI-Mapping (consolidation + business logic) and in-and-out (HTTP mechanics). No bridge layer.

### Immediate Actions

**Week 1:**
- [ ] Review this report with stakeholders
- [ ] Validate alignment with business goals
- [ ] Confirm OSI-Mapping is acceptable upstream dependency

**Week 2-3:**
- [ ] Update GOAL.md to reflect OSI-integrated architecture
- [ ] Update CONFIG_DESIGN.md to remove MDM semantics
- [ ] Create ARCHITECTURE.md explaining three-layer pattern

**Week 4-6:**
- [ ] Implement Phase 1: Ingestion refactoring
- [ ] Implement Phase 2: Writeback adaptation
- [ ] Create bridge layer examples (dbt, Python)

**Week 7-8:**
- [ ] Build integration tests
- [ ] End-to-end validation scenarios
- [ ] Documentation and examples

---

## Appendix A: OSI-Mapping File Structure

```
spec/
  mapping-schema.json       # JSON Schema (formal spec)

docs/
  motivation.md             # Why multi-source consolidation matters
  design/
    design-rationale.md     # Architecture and tradeoffs
    ai-guidelines.md        # Best practices for AI authoring
  reference/
    schema-reference.md     # Complete schema documentation
    annotated-example.md    # Walkthrough of real mapping

examples/
  hello-world/              # Simplest mapping
  nested-arrays/            # JSONB arrays
  vocabulary-normalization/ # Reconciling different values for same concept
  references-and-fk/        # Foreign key resolution
  ...35+ more...

engine-rs/
  src/
    main.rs                 # CLI entry
    model.rs                # AST definitions
    parser.rs               # YAML → model
    validate.rs             # 11-pass validator
    dag.rs                  # View dependency graph
    render/                 # SQL generation
      forward.rs
      identity.rs           # Transitive closure
      resolution.rs         # Conflict resolution
      reverse.rs            # FK resolution
      delta.rs              # Change detection
  Cargo.toml
```

---

## Appendix B: Integration Checklist

### Configuration Alignment
- [ ] In-and-out ingestion config matches OSI source declarations
- [ ] OSI mapping file references in-and-out table names
- [ ] Bridge layer reads from OSI views (names, schemas)
- [ ] Writeback config reads from desired-state tables

### Data Schema
- [ ] Source tables include `_base` column (original snapshot)
- [ ] `_base` populated correctly by ingestion tool
- [ ] Desired-state table has required columns: `cluster_id`, `action`, `data`, `base`
- [ ] Feedback table schema for `cluster_members` defined

### Operational
- [ ] OSI view generation triggered after ingestion completes
- [ ] Bridge layer triggered after OSI consolidation
- [ ] Writeback triggered after bridge produces desired-state
- [ ] Feedback loop writes after writeback succeeds

### Testing
- [ ] Unit tests: Each layer independently
- [ ] Integration tests: Ingestion → OSI → Bridge → Writeback
- [ ] Scenario tests: 2-source, 3-source, nested entities
- [ ] Conflict detection scenarios

### Observability
- [ ] Lineage tracking: `_cluster_id`, `_src_id` preserved through all tables
- [ ] Change tracking: Which entity changed, which source won for each field
- [ ] Error logging: Clear error messages from each layer

---

## References

- OSI-Mapping Repository: https://github.com/BaardBouvet/OSI-mapping
- engine-rs (Rust implementation): https://github.com/BaardBouvet/OSI-mapping/tree/main/engine-rs
- Examples: https://github.com/BaardBouvet/OSI-mapping/tree/main/examples

---

**Document Version:** 1.0  
**Last Updated:** March 20, 2026  
**Status:** Ready for stakeholder review
