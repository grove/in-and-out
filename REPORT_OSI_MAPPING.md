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
PostgreSQL views (_resolved_*, _delta_*)
   ↓
[Bridge Layer]  ← app logic, produces desired-state table
   ↓
Desired-State Table  ← cluster_id, base, action populated FROM OSI
   ↓
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
| **Desired-State Source** | Assumed external | Bridge layer (app-specific) |
| **Base State Origin** | Assumed external | OSI's `_base` (source snapshot) |
| **Writeback Cluster_id** | Pre-populated by external | Provided by bridge layer from OSI |
| **Reverse Mapping** | Part of writeback config | OSI computes in delta view |
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
│  - Bidirectional delta detection     │
│  - Lineage & provenance              │
│                                      │
│  Generates views:                    │
│  - _resolved_contact (golden record) │
│  - _delta_crm_contacts (changes)     │
│  - _rev_crm_contacts (reverse shape) │
│  - _cluster_id (entity identity)     │
│  - _base (original source values)    │
└────────────┬─────────────────────────┘
             │
          reads
             │
             ▼
┌──────────────────────────────────────┐
│  BRIDGE LAYER (dbt/Python/SQL)       │
│  ──────────────────────────────────  │
│  Responsibility: Business logic      │
│  - Read unified entities from OSI    │
│  - Apply app-specific rules          │
│  - Decide which changes to writeback │
│  - Route to target systems           │
│  - Produce desired-state table       │
│                                      │
│  Example dbt model:                  │
│  SELECT                              │
│    r._cluster_id,                    │
│    'update' as action,               │
│    jsonb_build_object(...) as data,  │
│    r._base                           │
│  FROM _resolved_contact r            │
│  WHERE <business_filters>            │
└────────────┬─────────────────────────┘
             │ produces
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

**OSI-Mapping (Consolidation Layer)**
- Declares target entity schemas (what contact should look like)
- Declares field mappings (how to construct targets from sources)
- Declares identity rules (when two rows represent same entity)
- Declares conflict strategies (which source wins for each field)
- Computes transitive closure for identity matching
- Produces golden records and change deltas
- Maintains lineage & provenance
- Provides base snapshots for 3-way merge
- Fully testable via embedded YAML tests

**Bridge Layer (Business Logic)**
- Decides when to route changes (filters, conditions)
- Applies app-specific conflict resolution (if OSI's isn't sufficient)
- Produces desired-state table format
- Handles atomicity across related entities
- Can be dbt YAML, Python, stored procedures, or custom code

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
- Reads desired-state tables (provided by bridge)
- Issues pre-flight GET for conflict detection
- Compares base vs current vs resolved
- Issues conditional writes (ETags) when available
- Constructs payloads via field mapping templates
- Tracks last-written-state and identity links
- Handles retries, rate limiting, dead-letter
- Writes generated IDs back to PostgreSQL feedback table
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

### Step 5: Bridge Layer
```sql
INSERT INTO inout_dst_crm_contact
SELECT
  CASE
    WHEN r.name != prev.name THEN 'update'
    ELSE 'noop'
  END as action,
  r._cluster_id,
  jsonb_build_object('email', r.email, 'name', r.name) as data,
  r._base,
  NULL as base_version
FROM _resolved_contact r
LEFT JOIN previous_resolved prev USING (cluster_id)
WHERE r._cluster_id IS NOT NULL
```

Produces:
```
action: 'update'
cluster_id: <UUID>
data: {email: "alice@x.com", name: "Alice"}
base: {email: "alice@x.com", name: "Alice"}
base_version: NULL
```

### Step 6: Writeback Conflict Detection
```
1. Read desired-state: action=update, cluster_id=<UUID>, data={...}, base={...}
2. Issue GET /contacts/100 → {id: 100, email: "alice@x.com", name: "Alice Anderson"}
3. Three-way comparison:
   - base (from OSI): {email: "alice@x.com", name: "Alice"}
   - current (pre-flight): {email: "alice@x.com", name: "Alice Anderson"}
   - resolved (from bridge): {email: "alice@x.com", name: "Alice"}
4. Conflict detected: "Anderson" added externally
5. Apply conflict_resolution strategy:
   - dead_letter: Route to DLQ for operator review
   - last_writer_wins: Overwrite (use caution)
   - skip_and_warn: Leave alone, log warning
   - re_ingest_and_recompute: Trigger next ingestion cycle
```

### Step 7: OSI Delta View (Reverse Mapping)
```
_delta_crm:
  {id: 100, email: "alice@x.com", name: "Alice"}
  (change type: noop if no conflict)

_delta_sap:
  {customer_id: "CUST-001", contact_email: "alice@x.com", full_name: "Alice Smith"}
  (compare resolved against source's _base)
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
- Bridge layer: **Clearly separated business logic**
- Requirements: **More precise, non-overlapping**

---

## 9. Impact on In-and-Out Requirements (GOAL.md)

### Requirements That Can Be Removed (OSI-Mapping Now Owns These)

| Requirement | Current Scope | With OSI | Reason |
|---|---|---|---|
| **#1: Per-Datatype Mapping** | Writeback must declare API endpoint per datatype | OSI declares this in central config | Consolidation mapping, not HTTP sync concern |
| **#7: Desired-State Table** | Writeback tool must produce these | Bridge layer produces these | MDM responsibility, not sync tool concern |
| **#8: Identity Mapping** | Writeback tool computes cluster_id | OSI's `_cluster_id` via transitive closure | Identity resolution, not HTTP protocol concern |
| **#12: API Asymmetry Handling** | Writeback handles read≠write schemas | Split: OSI handles consolidation asymmetry, in-and-out handles target asymmetry | Consolidated schema asymmetry is MDM concern |
| **#34: Cluster Merge & Split Propagation** | Writeback tool handles merge/split actions | Bridge layer produces merge/split actions in desired-state | Business logic decision, not HTTP sync concern |

### Requirements Simplified (Clearer Responsibility)

| Requirement | Change | Impact |
|---|---|---|
| **#8: Identity Mapping** | Change from "compute + enforce unique constraint" to "accept pre-computed from bridge" | Writeback now ~50 lines simpler |
| **#16: External Reference Field** | Move from writeback config to bridge layer | Bridge decides what field to populate |
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

### Phase 3: Bridge Layer Patterns (Weeks 5-6)
**Focus: Document how to build bridge layer**

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

❌ **No Orchestration** — OSI doesn't schedule syncs; external system (cron, Airflow) must trigger  
❌ **No CDC** — OSI doesn't detect changes; external CDC tool must populate tables  
❌ **PostgreSQL Views Only** — Currently generates views; other targets would need engine extensions  
❌ **No Streaming** — Batch processing model; incremental IVM discussed but deferred  
❌ **No Transaction Semantics** — Cannot guarantee all-or-nothing ACID guarantee across sources  
❌ **Expression Safety** — SQL expression safety checked at validation time; assumes trusted authors  

**These gaps are not weaknesses—they're intentional scope boundaries. OSI is the consolidation layer, not the orchestration or execution layer.**

---

## 13. Bridge Layer: The Missing Piece

### What Is It?

The **bridge layer** is the application logic between OSI's golden records and in-and-out's writeback tool. It decides:
- When is a change worth writing back? (filtering)
- Should this entity go to all targets or just some? (routing)
- Are there app-specific conflict overrides? (business rules)
- How should related entities stay atomic? (transaction boundaries)

### Implementations

**Option A: dbt YAML** (SQL-centric)
```yaml
# models/desired_state/contact_sync.sql
select
  r._cluster_id as cluster_id,
  case
    when r._ts_updated_at > cast(null as timestamp) -- check if changed
    then 'update'
    else 'noop'
  end as action,
  jsonb_build_object(
    'email', r.email,
    'first_name', r.first_name,
    'last_name', r.last_name
  ) as data,
  r._base,
  null as base_version
from {{ ref('resolved_contact') }} r
where r._cluster_id is not null
  and r.email is not null -- business rule: email required
```

**Option B: Python** (logic-centric)
```python
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

def build_desired_state(session: Session):
    resolved = session.execute("""
        SELECT _cluster_id, email, first_name, ...
        FROM _resolved_contact
    """).fetchall()
    
    for row in resolved:
        desired_action = decide_action(row)  # Custom logic
        desired_data = construct_payload(row)
        
        insert_desired_state(
            cluster_id=row['_cluster_id'],
            action=desired_action,
            data=desired_data,
            base=row['_base']
        )
```

**Option C: PostgreSQL Stored Procedure**
```sql
CREATE OR REPLACE FUNCTION build_desired_state_contact()
RETURNS void AS $$
BEGIN
  INSERT INTO inout_dst_crm_contact
    (cluster_id, action, data, base, base_version)
  SELECT
    r._cluster_id,
    (CASE WHEN r.email != prev.email THEN 'update' ELSE 'noop' END),
    to_jsonb(r),
    r._base,
    NULL
  FROM _resolved_contact r
  LEFT JOIN previous_contact prev USING (cluster_id);
END
$$ LANGUAGE plpgsql;
```

### Responsibility

- Reads from `_resolved_{target}` views (golden records)
- Applies business filters and transformations
- Produces desired-state tables in in-and-out format
- Can be idempotent or incremental
- Can handle complex atomicity requirements

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

### 3. Document Bridge Layer Pattern ✅

**Action:** Create reference implementations (dbt, Python, SQL) showing how to build a bridge from OSI output to in-and-out input.

**Rationale:**
- OSI is a library, not an application
- Bridge layer is where app-specific logic lives
- Makes integration pattern explicit
- Customers can customize without modifying in-and-out

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
| **In-and-Out + OSI** | Bidirectional HTTP sync with MDM | ✅ Complete, open, modular |

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

### Risk 3: Bridge Layer Complexity

**Concern:** Bridge layer adds another moving part.

**Mitigation:**
- Provide templates (dbt, Python)
- Start simple (direct pass-through: OSI output → desired-state)
- Incrementally add business logic
- Monitor adoption patterns

### Risk 4: Performance (Views + Triggers)

**Concern:** PostgreSQL views + 6-stage pipeline might be slow.

**Mitigation:**
- OSI generates standard SQL (no magic)
- Materialized views option (IVM) discussed in OSI
- Start with modest datasets (10K–100K records)
- Profile and optimize as needed
- Can parallelize ingestion ↔ consolidation

---

## 17. Conclusion & Next Steps

### Summary

OSI-Mapping represents a **paradigm shift** from custom integration code to **declarative, testable consolidation rules**. By adopting it as the upstream layer, in-and-out can:

1. **Dramatically simplify its scope** — Focus on HTTP sync mechanics, not MDM concerns
2. **Eliminate redundant logic** — Identity resolution happens once (in OSI), not in filters throughout the codebase
3. **Improve testability** — OSI's YAML tests are executable; integration tests can be deterministic
4. **Reduce configuration burden** — Single YAML file declares all consolidation rules, not scattered across multiple connector configs
5. **Enable multi-system scenarios** — Naturally handles 2+ sources simultaneously, not just pairwise
6. **Provide clear architecture** — Three clear layers: ingestion (HTTP), consolidation (OSI), sync (in-and-out)

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
