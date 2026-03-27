# Report: Record Linkage in in-and-out + OSI-Mapping

> **Date:** March 27, 2026
> **Status:** Analysis
> **Scope:** Record linkage considerations for a multi-system MDM pipeline, with particular attention to onboarding scenarios
> **Related:** [PLAN_ONBOARDING_PROPOSAL.md](PLAN_ONBOARDING_PROPOSAL.md), [PLAN_ONBOARDING_BLUE_GREEN.md](PLAN_ONBOARDING_BLUE_GREEN.md), [PLAN_ONBOARDING_SHADOW_MODE.md](PLAN_ONBOARDING_SHADOW_MODE.md), [REPORT_OSI_MAPPING.md](REPORT_OSI_MAPPING.md)

---

## 1. What Is Record Linkage?

Record linkage is the problem of determining whether two records from different source systems refer to the same real-world entity. In the MDM literature, this is also called entity resolution, identity matching, deduplication, or merge/purge.

In an OSI-mapping pipeline, record linkage is the responsibility of the `_id_{target}` view — the identity resolution stage. It takes forward-mapped rows from all source systems and assigns a `_cluster_id` to each row, grouping records that represent the same entity. Everything downstream — conflict resolution, the golden record, reverse mapping, delta computation — depends on these cluster assignments being correct.

If record linkage is wrong, every subsequent stage amplifies the error:

```
Wrong link           → wrong cluster      → wrong golden record
                                          → wrong deltas for ALL systems
                                          → wrong writeback (data corruption)

Wrong non-link       → separate clusters  → duplicate golden records
(missed match)                            → no sync between the systems
                                          → data divergence
```

Record linkage is the highest-leverage component in the pipeline. Getting it right matters more than anything else.

---

## 2. How OSI-Mapping Performs Record Linkage Today

### 2.1 Deterministic Matching

OSI-mapping uses **deterministic record linkage**: two records are linked if and only if they share an identical value on a declared identity field. There is no scoring, no probabilistic threshold, no machine learning.

```yaml
targets:
  contact:
    fields:
      email: identity          # exact match on email
      tax_id: identity         # exact match on tax_id
      name: coalesce
```

This generates SQL that creates edges in an identity graph: for every pair of records that share the same `email` or the same `tax_id`, an edge is created.

### 2.2 Transitive Closure (Connected Components)

Identity edges are resolved via `WITH RECURSIVE` connected-components in the `_id_{target}` view. If record A matches record B on email, and record B matches record C on tax_id, then A, B, and C all receive the same `_cluster_id` — even though A and C share no field in common.

```
System 1:  Alice  <alice@co.com>          Tax: 12345
System 2:  A. Smith  <alice@co.com>       Tax: —
System 3:  Alice Smith  <—>               Tax: 12345

Edges:
  System 1 ←→ System 2  (email match)
  System 1 ←→ System 3  (tax_id match)

Cluster: {System 1, System 2, System 3}  → all one entity
```

Transitive closure is correct mathematically — it computes connected components — but it trusts every edge equally. A single wrong edge can merge two large clusters that have nothing to do with each other.

### 2.3 Link Groups (Composite Keys)

To reduce false positives, OSI-mapping supports **link groups**: requiring all fields in a group to match simultaneously.

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

This is equivalent to matching on the composite key `(first_name, last_name, date_of_birth)`. It prevents matching "Alice Smith" in system 1 with "Alice Johnson" in system 2 just because first names match.

### 2.4 What This Produces

The `_id_{target}` view outputs one row per source record, each annotated with `_cluster_id`. Downstream stages group by cluster. The mapping is deterministic and reproducible: given the same source data and the same identity declarations, the output is always the same.

---

## 3. Record Linkage Correctness

### 3.1 The Two Error Types

Record linkage has two fundamental error modes:

| Error | Name | Effect | Severity |
|---|---|---|---|
| **False positive** | Over-linking | Two unrelated entities merged into one cluster | **High** — writeback pushes wrong data into both systems; hard to undo |
| **False negative** | Under-linking | Same entity in two separate clusters; treated as different | **High** — causes duplicate inserts, data divergence, and contradictory writeback |

**Both error types cause active harm.** The harm is different in character but comparable in severity.

**False positives (over-linking)** cause **data corruption by merging**: incorrect field values from an unrelated entity are pushed into systems that were previously correct. The golden record is wrong, and every system receives wrong updates. Reversal requires splitting the cluster and undoing the writeback — difficult because the overwritten values may be lost.

**False negatives (under-linking)** cause **data corruption by duplicating**: two clusters that should be one each produce their own golden record. Each golden record generates reverse deltas for every system. If system 1 has record A and system 2 has record B, and A and B are the same entity but in separate clusters:

- Cluster 1 (containing A) computes a delta that **inserts** A's data into system 2 — because system 2 doesn't have a record in this cluster.
- Cluster 2 (containing B) computes a delta that **inserts** B's data into system 1 — because system 1 doesn't have a record in that cluster.
- Both systems now have **two records for the same real-world entity**: the original plus the duplicate insert from the other cluster.

```
Before (under-linked):
  Cluster 1: {System 1: Alice <alice@co.com>}
  Cluster 2: {System 2: Alice Smith <alice@co.com>}

Golden record 1: Alice <alice@co.com>
Golden record 2: Alice Smith <alice@co.com>

Delta for system 2 from cluster 1: INSERT Alice <alice@co.com>     ← duplicate
Delta for system 1 from cluster 2: INSERT Alice Smith <alice@co.com> ← duplicate

After writeback:
  System 1: Alice (original) + Alice Smith (duplicate)
  System 2: Alice Smith (original) + Alice (duplicate)
```

This is not merely "the status quo is preserved." Under-linking actively creates duplicates across every system touched by the affected clusters. In a pipeline with writeback enabled, **every missed link is a potential duplicate insert.**

The consequence compounds: once duplicates exist in source systems, subsequent ingestion pulls them back into the pipeline, creating _additional_ clusters and _additional_ cross-system inserts. Without intervention, the duplicate count grows with every sync cycle.

**The asymmetry between the two errors is situational:**

| Situation | Worse Error | Reason |
|---|---|---|
| **Writeback enabled for all systems** | Roughly equal | Over-linking corrupts fields; under-linking creates duplicates. Both require manual cleanup. |
| **Writeback enabled for some systems** | Under-linking may be worse | Duplicate inserts flow to every writable system. Over-linking only corrupts fields within the merged cluster. |
| **Writeback disabled (read-only golden record)** | Over-linking is worse | Under-linking just produces duplicate golden records (annoying, not harmful). Over-linking produces a wrong golden record that downstream consumers (BI dashboards, reports) use for decisions. |
| **During onboarding (system 3 joining)** | Over-linking is worse for _existing_ systems; under-linking is worse for _new_ system | Over-linking changes data in systems 1 and 2 that were previously stable. Under-linking inserts duplicates into system 3 (and potentially into systems 1 and 2 if system 3 has exclusive records). |

**Implication for identity rule design:** The conventional wisdom "be conservative, avoid false positives" is correct for read-only MDM. For a bidirectional pipeline with writeback, **both directions are dangerous**, and the identity rules must be tuned to minimise both — not just one.

### 3.2 High-Confidence vs Low-Confidence Links

Not all identity matches carry the same risk:

| Link Type | Confidence | Example | False Positive Risk |
|---|---|---|---|
| **Strong unique identifier** | Very high | Tax ID, SSN, DUNS number | Very low — these are designed to be unique |
| **Email address** | High | `alice@company.com` | Low for personal email; **medium for shared/generic email** |
| **Full name + DOB** | Medium | "Alice Smith, 1985-03-15" | Low for uncommon names; medium for common names |
| **Phone number** | Medium | `+1-555-1234` | Medium — phones are reassigned, shared by families |
| **First name only** | Very low | "Alice" | **Very high** — millions of matches |
| **Generic/default values** | None | `noemail@company.com`, `000-000-0000` | **Certain** — these are not identity signals at all |

A robust record linkage configuration accounts for this gradient. OSI-mapping's current model (identity fields + link groups) works well for high-confidence links but provides no mechanism for expressing confidence levels or handling uncertain matches differently.

### 3.3 The Junk Value Problem

The single most dangerous record linkage failure mode in production MDM systems is the **junk value cluster**: a single non-unique value that links thousands of unrelated records.

```
Email: noemail@company.com
  → System 1: 2,340 records
  → System 2: 1,890 records
  → System 3: 3,100 records

All 7,330 records merge into ONE cluster.
Golden record: random winner from 7,330 candidates.
Writeback: pushes that random winner's data to all 7,330 records across all systems.
```

This is catastrophic and can happen with any identity field:
- **Email:** `noemail@X.com`, `test@test.com`, `admin@company.com`
- **Phone:** `000-000-0000`, `+1-555-0000`, `999-999-9999`
- **Tax ID:** `000000000`, `123456789` (default/placeholder values)
- **Name:** `Unknown`, `Test User`, `DO NOT USE`

OSI-mapping has no built-in protection against junk values. The identity field declaration treats every value equally — `noemail@company.com` creates edges just like `alice@company.com`.

---

## 4. Record Linkage Considerations for This System

### 4.1 Deterministic vs Probabilistic Linkage

**How OSI-mapping works:** Deterministic — exact field match creates a link of full strength.

**Alternative:** Probabilistic record linkage (e.g., Fellegi-Sunter model, Splink) assigns a match score based on the informativeness of matching fields. Two records sharing a rare surname get a higher score than two records sharing "Smith". A threshold separates links from non-links.

| Aspect | Deterministic (OSI-mapping) | Probabilistic |
|---|---|---|
| **Precision at high confidence** | Same | Same |
| **Recall on fuzzy data** | Lower — misses typos, nicknames, format differences | Higher — scores partial matches |
| **False positive control** | Via link groups (coarse) | Via score threshold (fine-grained) |
| **Transparency** | Fully auditable — each link traceable to field match | Harder to explain — score is composite |
| **Implementation in SQL** | Natural — WHERE clauses, JOIN ON | Unnatural — requires UDFs or external computation |
| **Performance in pg-trickle** | Fast — standard JOIN + WITH RECURSIVE | Slower — scoring functions on every pair |
| **Junk value handling** | None built-in | Can be weighted (common values = low score) |
| **Operator expertise required** | Low — declare which fields are identity | Medium — tune weights and thresholds |

**Assessment for this system:** Deterministic linkage is the right default. The pipeline runs inside PostgreSQL via pg-trickle stream tables, which means identity resolution must be expressible as a SQL view. Probabilistic scoring requires cross-join or blocking + scoring, which is expensive and difficult to express as an incrementally maintained view. The real gap is not the linkage method — it is the lack of safeguards around the deterministic method.

### 4.2 Normalisation Before Matching

Deterministic matching is only as good as the data quality of the identity fields. Two records representing the same person will fail to match if their email is formatted differently:

```
System 1: Alice@Company.COM
System 2: alice@company.com
```

OSI-mapping supports `normalize` expressions on identity fields:

```yaml
fields:
  email:
    strategy: identity
    normalize: "lower(trim(email))"
```

**Considerations:**
- Every identity field should have a normalisation expression
- Common normalisations: `lower()`, `trim()`, phone number formatting (`regexp_replace(phone, '[^0-9+]', '', 'g')`), name transliteration
- Normalisation interacts with junk values: `trim()` may turn ` ` (whitespace-only) into `''` (empty string), which then matches every other empty string
- NULL handling: normalised NULLs should **not** create edges (OSI-mapping already handles this — NULL identity values don't match)

### 4.3 Handling Ambiguity: Certain, Probable, and Uncertain Links

In a real-world dataset, links fall into three buckets:

| Category | Example | Volume | Human Review Needed? |
|---|---|---|---|
| **Certain** | Same tax_id, same email, same external_id | 80–95% | No |
| **Probable** | Same full name + same city, or same phone + similar name | 3–15% | Ideally yes |
| **Uncertain** | Same first name only, or partial phone match | 2–5% | Yes, or reject |

OSI-mapping's current model collapses this into a binary: an edge exists or it doesn't. There is no "maybe" state. This is fine for high-confidence fields (tax_id, email) but inadequate for medium-confidence fields where human judgment would change the outcome.

**Options for expressing confidence:**
1. **Multiple identity fields with link groups** (current): Require composite matches for weaker fields. This indirectly encodes confidence — a composite key is harder to satisfy, so links that pass are more confident.
2. **Tiered identity resolution** (not yet implemented): Run identity resolution in two passes. Pass 1 uses strong fields only (tax_id). Pass 2 uses weaker fields (name + DOB) but only within pass-1 clusters, not across them.
3. **External curation table** (feasible today): Let identity resolution propose links; a human reviews uncertain ones before they take effect. See §6.

### 4.4 Cluster Size Limits

Clusters should not grow without bound. A cluster of 50,000 records is almost certainly wrong — no single real-world entity has 50,000 source records across three systems. Large clusters are a symptom of junk values or over-linking.

**Possible safeguards:**
- **Hard cluster size limit:** If a cluster exceeds N records, freeze it (don't link new records to it) and alert the operator.
- **Cluster growth rate limit:** If a cluster grows by more than M records in a single refresh, flag it.
- **Degree limit on identity edges:** If a single identity value (e.g., one email) would create more than K edges, exclude that value from identity resolution entirely (treat it as junk).

None of these are implemented in OSI-mapping today. The `clusters_merged` metric proposed in [PLAN_ONBOARDING_PROPOSAL.md](PLAN_ONBOARDING_PROPOSAL.md) §3 Gap 2 provides visibility but not automatic protection.

### 4.5 Stability Under Incremental Updates

Record linkage in a batch processing system runs once over the full dataset. In a streaming system backed by pg-trickle, identity resolution runs incrementally — every time a source row is ingested or updated, the connected-components algorithm re-evaluates.

This creates **linkage instability**: a cluster that exists today may split or merge tomorrow based on a single edit to a single source record.

**The dangerous sequence:**
1. System 3 ingests a record with email `alice@co.com` → links to systems 1 and 2 → cluster merges
2. Someone in system 3 "corrects" the email to `alice@company.com` → the bridge is broken → cluster **splits**
3. The golden record forks into two → deltas propagate to all three systems → writeback undoes the previous merge
4. System 3 reverts the email → cluster **re-merges** → writeback again

Each cycle produces writeback in all systems. The records oscillate between merged and split states, with every oscillation touching real APIs.

**Mitigations:**
- **Hysteresis / sticky clusters:** Once two records are linked, require the link to be _absent_ for N consecutive refreshes before splitting. This dampens oscillation.
- **Merge-only policy:** Links can be created but never broken by data changes — only by explicit human override. Radical but effective for preventing oscillation.
- **Rate limiting on cluster changes:** If a cluster merges and splits more than K times in a window, freeze it and alert.

### 4.6 Source System Data Quality

Each source system has its own data quality characteristics. System 1 (CRM) may have clean, human-curated emails. System 3 (a legacy ERP) may have truncated names, default placeholder values, and stale records from 2005.

Adding a low-quality source to the pipeline introduces noise into identity resolution. The noise becomes new edges. Some edges are correct (finding true matches the high-quality sources missed). Others are false positives (junk values that bridge unrelated clusters).

**Considerations:**
- **Source-level identity field eligibility:** Not all identity fields should be used for all sources. System 3's email field might be declared as identity for system 1 but excluded for system 3 if its data quality is poor.
- **Source priority for link strength:** Not all sources' identity fields are equally trustworthy. A tax_id from the government portal is more reliable than a tax_id from a manually-entered CRM field.
- **Data quality scoring:** Before onboarding system 3, run a data quality assessment on its identity fields. Count distinct values, NULL ratios, top-N value distributions (to detect junk values), and format consistency.

### 4.7 Privacy and Compliance

Identity fields are, by definition, personally identifiable information (PII). Record linkage creates cross-references between PII across systems. This has compliance implications:

- **GDPR Article 5(1)(c) — Data minimisation:** Identity fields used for matching must be justified. Using more fields than necessary for linkage may violate minimisation.
- **GDPR Article 17 — Right to erasure:** Deleting a person from system 1 should propagate. But if their cluster spans systems 2 and 3, does deleting from system 1 mean deleting the cluster? Or just the system 1 edge?
- **Cross-system PII exposure:** System 1's tax_id is now visible (through the golden record) to code that writes back to system 2. If system 2 never had access to tax_ids, this may create a new data flow that requires a DPIA (Data Protection Impact Assessment).
- **Audit trail for links:** Who or what created each identity link, and when? This is relevant for both compliance and debugging.

### 4.8 Performance at Scale

Connected-components via `WITH RECURSIVE` has known performance characteristics:

- **Best case:** Sparse graph (few identity edges per record) — O(n) iterations
- **Worst case:** Dense graph (many edges, long chains) — O(n log n) or worse, depending on graph diameter
- **Pathological case:** A single junk value linking 100K records — the recursion depth explodes

pg-trickle's DIFFERENTIAL refresh mode reduces the cost by only processing changed rows. But when system 3 is first added and all its rows are new, the full graph must be re-evaluated. This is a one-time cost at onboarding that can be significant for large datasets (millions of records).

**Considerations:**
- Index identity fields aggressively
- Block on identity values (partition the identity resolution by value ranges) to reduce cross-join cost
- Pre-filter before identity resolution: exclude records with NULL or junk identity values
- Monitor `_id_{target}` refresh time during onboarding

---

## 5. Record Linkage and Onboarding

The three onboarding plans each interact with record linkage differently:

### 5.1 Gating Approach (PLAN_ONBOARDING_PROPOSAL.md)

**How it affects linkage:** System 3's source tables are gated during initial load. Identity resolution does not see system 3's data until the gate lifts. When it does, the full dataset arrives at once — creating all new identity edges in a single refresh.

**Risk profile:**
- ✓ No partial-data identity resolution (source gating prevents it)
- ✗ All new edges activate simultaneously — no opportunity to review individual links before they take effect
- ✗ The `_id_{target}` refresh consumes all new edges at once; if any edge is wrong, the damage is done before the operator can review
- Mode B (conservative) gates delta tables, giving the operator a review window — but the identity resolution itself is already committed

**Linkage-specific consideration:** The gating approach needs a **cluster diff report** — what clusters changed, which system 3 records caused the change, and how many records are in each affected cluster. The `clusters_merged` metric in `pgt_refresh_history` (proposed Gap 2) provides aggregate visibility, but individual link review requires ad-hoc SQL.

### 5.2 Blue/Green Approach (PLAN_ONBOARDING_BLUE_GREEN.md)

**How it affects linkage:** The blue pipeline runs identity resolution independently. The green (production) pipeline is untouched. The operator can compare blue vs green cluster assignments to identify every link change.

**Risk profile:**
- ✓ Production identity resolution is unaffected until cutover
- ✓ Full side-by-side comparison possible (blue clusters vs green clusters)
- ✓ If blue linkage is wrong, discard blue — no production impact
- ✗ No granular link review — the comparison is blue-vs-green at the cluster level, not at the individual-link level
- ✗ Blue pipeline runs in isolation, so linkage quality is validated against a static snapshot, not live data

**Linkage-specific consideration:** Blue/green is the safest approach for **untested identity rules**. Because the production pipeline is never modified, linkage errors in the blue pipeline cannot cause harm. The tradeoff is that you don't discover how linkage behaves under live updates until cutover.

### 5.3 Shadow Mode (PLAN_ONBOARDING_SHADOW_MODE.md)

**How it affects linkage:** System 3 is added to the production identity resolution pipeline immediately. New identity edges take effect in production. The shadow only intercepts writeback, not linkage.

**Risk profile:**
- ✗ Linkage errors affect the production pipeline immediately
- ✗ A false-positive link (junk value bridging two clusters) changes the golden record for all systems before anyone reviews it
- ✓ Extended shadow mode (§10) catches the symptoms — system-3-caused changes to systems 1 and 2 are held for review
- ✓ The `origin` attribution field (§10.5) identifies which changes were caused by system 3 joining, so the operator can trace suspicious changes back to their links

**Linkage-specific consideration:** Shadow mode detects **the consequences** of bad linkage (incorrect deltas) but does not prevent the linkage itself. By the time the operator sees a `system3_caused` field update in the shadow comparison table, the `_id_{target}` view has already merged the clusters and the golden record has already changed. The actual identity error must be traced back from the delta to the cluster to the specific edge that caused the merge.

### 5.4 Comparison of Approaches for Linkage Safety

| Criterion | Gating | Blue/Green | Shadow | Extended Shadow |
|---|---|---|---|---|
| Identity resolution on production data | Yes | No (blue only) | Yes | Yes |
| Linkage errors affect production golden record | Yes (after ungate) | No (until cutover) | Yes (immediately) | Yes (immediately) |
| Operator reviews links before production impact | Mode B only (cluster diff) | Yes (blue vs green) | No — reviews consequences only | No — reviews consequences only |
| Individual link review possible | Ad-hoc SQL | Blue vs green diff | Indirectly via delta attribution | Indirectly via delta attribution |
| Linkage error reversibility | Rollback (re-gate, revert query) | Discard blue | Difficult (golden record already changed) | Difficult (golden record already changed) |
| Handles linkage oscillation | No | No | No | No |
| Detects junk values (over-linking) | Only via cluster metrics | Blue vs green cluster size comparison | Via high-fanout shadow delta counts | Via high-fanout shadow delta counts |
| Detects missed matches (under-linking) | Only if operator queries for duplicate clusters | Blue vs green comparison may reveal | Indirectly — duplicate inserts appear as `new_record` in shadow | Indirectly — duplicate `new_record` rows visible with attribution |

**Key insight:** No onboarding approach directly reviews individual identity links. They all operate at a higher level — clusters, deltas, or pipeline-level diffs. This is the gap that human curation of links would fill.

**Under-linking during onboarding is particularly dangerous.** When system 3 joins, its records should match with existing records in systems 1 and 2. If the identity rules fail to create these links (because of formatting differences, missing fields, or over-strict link groups), the pipeline treats system 3's records as new entities. The delta views for systems 1 and 2 then generate `insert` actions — pushing these "new" entities into systems that _already have them under a different cluster_. The result is duplicate records in systems 1 and 2 that grow with every sync cycle. Extended shadow mode catches these as `new_record` rows with `origin = system3_caused`, giving the operator a chance to investigate before the inserts execute — but only if the operator recognises that a `new_record` might be a missed match rather than a genuinely new entity.

---

## 6. Human Curation of Record Links

### 6.1 The Case For Curation

The conversation in §3–5 converges on a pattern: the system is good at creating links (deterministic matching with transitive closure) and good at detecting consequences of bad links (shadow comparison, cluster metrics), but there is no layer where a human examines individual link decisions before they take effect.

Human curation addresses this by inserting a review step between link proposal and link activation:

```
Source data → Identity matching → Proposed links → HUMAN REVIEW → Accepted links → Cluster computation → Golden record → ...
```

**What would be curated:**
1. **Conflict links** — two records that match on an identity field but disagree substantially on other fields (different names, different addresses). The identifier match alone is not enough context to know if this is a true match or a coincidental shared email.
2. **High-fanout links** — a single identity value that would create more than N edges. These are the junk value candidates.
3. **Cross-system inserts** — the `new_record` classification from PLAN_ONBOARDING_SHADOW_MODE.md §10.6. When system 3 causes a new record to appear in systems 1 and 2, a human should confirm this is a genuine new entity — not a duplicate caused by under-linking.
4. **Cluster re-merges** — when two previously separate clusters are bridged by a system 3 record. These are the highest-risk link decisions because they affect records that were already stable.
5. **Suspected under-links (duplicate clusters)** — two or more clusters whose records are suspiciously similar (near-identical names, overlapping addresses, same phone number with different formatting) but were not matched by the identity rules. These need a human to decide: should they be linked (forced merge via override), or are they genuinely distinct entities? Left unreviewed, each cluster generates cross-system inserts that create duplicates in every writable system.

**What would NOT be curated:**
1. **High-confidence matches** — same tax_id in two systems with consistent names. No human review needed.
2. **Noops** — records that already exist in the same cluster. Nothing changed.
3. **Same-system deduplication** — if two records within the same source system match, that is a data quality issue in the source, not a cross-system linkage decision.

### 6.2 Implementation Approach: Link Override Table

The simplest mechanism is a **link override table** that the curation process writes to, and that the `_id_{target}` view reads from.

```sql
CREATE TABLE inout_link_overrides (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- What this override applies to
    source_a        TEXT NOT NULL,         -- e.g., 'system1'
    external_id_a   TEXT NOT NULL,         -- e.g., 'C-1042'
    source_b        TEXT NOT NULL,         -- e.g., 'system3'
    external_id_b   TEXT NOT NULL,         -- e.g., 'ERP-9001'

    -- The decision
    decision        TEXT NOT NULL,         -- 'link' / 'no_link'
    reason          TEXT,                  -- human-readable justification

    -- Metadata
    decided_by      TEXT NOT NULL,         -- username or 'auto'
    decided_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    identity_field  TEXT,                  -- which field caused the proposed link
    identity_value  TEXT,                  -- the value that matched

    UNIQUE (source_a, external_id_a, source_b, external_id_b)
);
```

**How it integrates with OSI-mapping:**

The `_id_{target}` view's connected-components algorithm creates edges from identity field matches. The link override table acts as a post-filter:

- A `decision = 'link'` override **forces** two records into the same cluster even if no identity field matches (manual link).
- A `decision = 'no_link'` override **prevents** two records from being linked even if an identity field matches (manual split / block).

This requires the engine to incorporate the override table into the `_id_{target}` view's edge set:

```sql
-- Edges from identity fields (existing):
SELECT a._src_id, b._src_id
FROM _fwd_* a JOIN _fwd_* b ON a.email = b.email

UNION ALL

-- Forced links from overrides:
SELECT a._src_id, b._src_id
FROM _fwd_* a
JOIN _fwd_* b
JOIN inout_link_overrides o
  ON  o.source_a = a._source AND o.external_id_a = a._external_id
  AND o.source_b = b._source AND o.external_id_b = b._external_id
  AND o.decision = 'link'

EXCEPT

-- Blocked links from overrides:
SELECT a._src_id, b._src_id
FROM _fwd_* a
JOIN _fwd_* b
JOIN inout_link_overrides o
  ON  o.source_a = a._source AND o.external_id_a = a._external_id
  AND o.source_b = b._source AND o.external_id_b = b._external_id
  AND o.decision = 'no_link'
```

**Note:** The `EXCEPT` for `no_link` is non-trivial in connected components. Removing an edge from a graph does not necessarily split the cluster if there is another path between the two nodes. The `no_link` override would need to be enforced as a post-processing step on cluster assignments, not as edge removal.

### 6.3 Link Curation Workflow

```
Phase 1: Propose (automated)
├── Identity resolution runs normally
├── New edges from system 3 are detected
├── Edges with confidence below threshold → written to curation queue
│   (high-fanout edges, conflict links, cross-system inserts)
└── High-confidence edges → accepted automatically

Phase 2: Review (human)
├── Operator reviews queued links in a UI or dashboard
├── For each proposed link:
│   - See source records side by side
│   - See which identity field caused the match
│   - See the identity value and its frequency (how many records share it)
│   - See other fields (name, address, phone) for context
│   - Decision: Accept (link), Reject (no_link), or Defer
└── Decisions written to inout_link_overrides

Phase 3: Activate (automated)
├── Next identity resolution refresh incorporates overrides
├── Accepted links are added to clusters
├── Rejected links are excluded from clusters
└── Downstream pipeline (resolution, delta, writeback) processes the changes
```

### 6.4 Curation During Onboarding

In the onboarding context, curation fits differently into each approach:

**Gating approach + curation:**
1. Ingest system 3
2. Lift source gate → identity resolution runs
3. Before ungating delta tables → run curation on newly created edges
4. Operator reviews flagged links (high-fanout, cluster re-merges)
5. Override table populated → identity resolution re-runs with overrides
6. Ungate delta tables → writeback with curated linkage

This is a natural fit. The gating window (step 3–5) provides time for curation.

**Blue/green + curation:**
1. Blue pipeline runs identity resolution with system 3
2. Compare blue clusters to green → identify new/changed links
3. Run curation on blue pipeline's links
4. Override table populated in blue → re-run blue identity resolution
5. Cutover when linkage is approved

Also a natural fit. The blue pipeline is a safe environment for curation.

**Shadow mode + curation:**
1. System 3 added to production identity resolution
2. All new edges take effect immediately
3. Shadow comparison detects consequences of bad links
4. Operator traces bad deltas back to links → creates `no_link` overrides
5. Identity resolution re-runs → clusters split → corrected deltas flow

This is reactive, not proactive. Curation happens after the damage (wrong golden record, wrong deltas to systems 1 and 2). Extended shadow mode (§10) mitigates this by holding system-3-caused changes, but the golden record itself is already wrong during the review window.

**Extended shadow mode + curation (recommended workflow):**
1. System 3 added to production identity resolution
2. System-3-caused deltas for systems 1 and 2 are held in shadow tables
3. Run curation on held deltas: trace each `system3_caused` `new_record` or `field_update` back to the link that caused it
4. Override table populated → identity resolution re-runs
5. Shadow tables re-evaluated → convergence improves
6. Go-live when curation is complete and convergence threshold is met

This combines defences: extended shadow mode catches the consequences, curation fixes the causes.

### 6.5 Junk Value Blocklist

A simpler form of curation that does not require per-link review: a **value-level blocklist** that prevents specific identity values from creating edges.

```sql
CREATE TABLE inout_identity_blocklist (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    field_name      TEXT NOT NULL,           -- e.g., 'email'
    blocked_value   TEXT NOT NULL,           -- e.g., 'noemail@company.com'
    reason          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by      TEXT NOT NULL,

    UNIQUE (field_name, blocked_value)
);
```

The `_id_{target}` view filters identity edges against this blocklist:

```sql
-- Edges:
SELECT a._src_id, b._src_id
FROM _fwd_mapping1 a
JOIN _fwd_mapping2 b ON a.email = b.email
WHERE a.email NOT IN (SELECT blocked_value FROM inout_identity_blocklist WHERE field_name = 'email')
```

**When to use:** Always. The blocklist should be populated before onboarding begins, based on data quality analysis of system 3's identity fields. Common patterns to block:

| Pattern | Example |
|---|---|
| Placeholder email | `noemail@*`, `test@*`, `nobody@*` |
| Placeholder phone | `000-000-0000`, `+1-000-0000`, `999-999-9999` |
| Placeholder name | `Unknown`, `N/A`, `Test`, `DO NOT USE` |
| System-generated values | Sequential IDs that look like identity fields |
| High-frequency values | Any value appearing in more than 1% of records |

The blocklist is cheaper and faster than per-link curation. It should be the first line of defence.

### 6.6 Auto-Curation Rules

Between the blocklist (automatic, value-level) and human curation (manual, per-link), there is room for **auto-curation rules** that make link decisions based on heuristics:

```sql
CREATE TABLE inout_link_curation_rules (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_name       TEXT NOT NULL UNIQUE,
    description     TEXT,
    condition_sql   TEXT NOT NULL,     -- SQL boolean expression
    decision        TEXT NOT NULL,     -- 'auto_link' / 'auto_no_link' / 'queue_for_review'
    priority        INT NOT NULL DEFAULT 100,
    enabled         BOOLEAN NOT NULL DEFAULT true
);
```

Example rules:

| Rule | Condition | Decision |
|---|---|---|
| **Block high-fanout** | Identity value appears in >100 records | `auto_no_link` |
| **Accept strong match** | Same tax_id AND same email AND similar name (Levenshtein < 3) | `auto_link` |
| **Queue name conflict** | Same email but names differ by Levenshtein > 5 | `queue_for_review` |
| **Queue large cluster** | Resulting cluster would exceed 20 records | `queue_for_review` |
| **Accept same-source dedup** | Same source, same identity values, all fields identical | `auto_link` |

These rules run after identity resolution proposes edges, filtering them into accepted/rejected/queued before the edges take effect in the connected-components calculation.

---

## 7. Pros and Cons of Human Curation

### 7.1 Advantages

| Advantage | Detail |
|---|---|
| **Catches false positives** | A human can see that "Alice Smith" in CRM and "Alice Chen" in ERP sharing a phone number is a reassigned phone, not the same person. The algorithm cannot. |
| **Prevents junk value cascades** | Before a high-fanout value merges thousands of records, a human flags it. Prevents the most catastrophic failure mode. |
| **Builds institutional knowledge** | The override table becomes a record of decisions and reasons. New team members understand why specific records are linked or split. |
| **Improves identity rules over time** | Repeated curation of the same error pattern (e.g., shared family emails) signals that the identity rule should be adjusted — remove `email` as identity for certain sources, or add a link group. |
| **Regulatory compliance** | For regulated industries (healthcare, finance), automated cross-referencing of PII may require human sign-off. The curation audit trail satisfies this. |
| **Reduces onboarding risk** | Curation during the gating or shadow window catches linkage errors before writeback executes, reducing the blast radius of system 3 joining. |

### 7.2 Disadvantages

| Disadvantage | Detail | Severity |
|---|---|---|
| **Does not scale** | If system 3 has 100K records and 5% produce uncertain links, that is 5,000 links to review. At 30 seconds per link, that is 42 hours of human review. | **High** — the curation queue must be aggressively pre-filtered by auto-curation rules and the blocklist. |
| **Bottleneck on onboarding speed** | The pipeline cannot go live until the curation queue is drained (or triaged). This adds days or weeks to the onboarding timeline. | **Medium** — mitigated by making auto-accept the default for high-confidence links and only queuing uncertain ones. |
| **Human error** | A reviewer accepting a wrong link is worse than the algorithm proposing it — it is now in the override table as an explicit decision, and future corrections must override the override. | **Medium** — mitigated by requiring two reviewers for `no_link` decisions (which are harder to reverse than `link` decisions). |
| **Stale overrides** | A `no_link` override from 2024 may no longer be correct if the underlying data has changed. The override table needs periodic review. | **Low** — add `expires_at` column; re-queue expired overrides. |
| **Requires UI investment** | Effective curation requires a purpose-built interface: side-by-side record comparison, cluster visualisation, bulk actions. A SQL table is not a usable curation interface for operators. | **Medium** — an MVP can use a spreadsheet-style view over the curation queue table, but a production system needs a proper UI. |
| **Incompatible with real-time pipelines** | If the pipeline runs on a 30-second refresh cycle, there is no natural pause for human review. Curation requires either a batch window or an asynchronous hold (gating). | **Medium** — use delta gating or shadow mode to create the review window. |

### 7.3 The Curation Spectrum

Curation is not binary (all-or-nothing). The right level depends on the organisation:

```
← Less curation                                     More curation →

No review        Blocklist     Auto-curation    Human review      Human review
(fully           only          rules +          of uncertain      of ALL links
 automated)                    blocklist        links only        (full manual
                                                                   MDM)
```

**Recommended position for this system:** Blocklist + auto-curation rules + human review of uncertain links. This catches the most dangerous errors (junk values, high-fanout clusters) while keeping the review queue small enough for a single operator.

---

## 8. Integration Design: Where Curation Fits in the Pipeline

### 8.1 Two Possible Architectures

**Architecture A: Curation before identity resolution**

```
Source tables → Forward views → CURATION QUEUE → _id_{target} → downstream
```

Proposed links are computed (by a helper process that evaluates identity matches without committing them) and placed in a queue. Only accepted links are fed to the `_id_{target}` view.

- ✓ Errors never reach the golden record
- ✗ Requires a separate "link proposal" computation outside OSI-mapping
- ✗ The `_id_{target}` view must read from the override table, complicating the SQL
- ✗ Transitive closure may produce different results with partial edges (hard to predict)

**Architecture B: Curation after identity resolution (recommended)**

```
Source tables → Forward views → _id_{target} → CURATION CHECK → downstream
```

Identity resolution runs normally. A curation check process examines the results, flags suspicious clusters, and writes overrides. On the next refresh, the overrides modify the edge set.

- ✓ Uses existing OSI-mapping pipeline without modification
- ✓ Curation operates on complete clusters, not isolated edges (better context for the reviewer)
- ✓ The override table is a simple correction layer
- ✗ The wrong cluster is visible in the golden record for one refresh cycle before correction
- ✗ Requires at least two refresh cycles: one to identify problems, one to apply corrections

**In the context of onboarding:** Architecture B works naturally with gating and extended shadow mode. The delta gating window or the shadow hold provides the time for the curation cycle. The golden record may be "wrong" during the review, but no writeback executes because deltas are held.

### 8.2 Pipeline With Curation

```
                    ┌──────────────────────────────────────────────┐
  System 1 API ──→  │  inout_src_system1_contacts                  │
  System 2 API ──→  │  inout_src_system2_contacts                  │
  System 3 API ──→  │  inout_src_system3_contacts                  │
                    │              ↓                                │
                    │  _fwd_{mapping} (includes blocklist filter)   │
                    │              ↓                                │
                    │  _id_{target} + inout_link_overrides          │
                    │              ↓                                │
                    │  Curation check (post-refresh)                │
                    │  ├── Flag high-fanout clusters                │
                    │  ├── Flag cluster re-merges > threshold       │
                    │  ├── Apply auto-curation rules                │
                    │  └── Queue uncertain links for human review   │
                    │              ↓                                │
                    │  _resolved_{target} → {target}               │
                    │              ↓                                │
                    │  _delta_{mapping} → writeback / shadow        │
                    └──────────────────────────────────────────────┘
```

### 8.3 Interaction With pg-trickle

The curation check runs as a **post-refresh hook** — after `_id_{target}` is refreshed but before (or concurrently with) downstream refresh. This can be achieved via:

1. **pg-trickle's tiered scheduling:** Set `_resolved_{target}` to a lower-priority tier than `_id_{target}`. After `_id_{target}` refreshes, the curation check runs before `_resolved_{target}` is next in the scheduler queue.
2. **Stream table gating:** Gate downstream tables. After `_id_{target}` refresh, run curation. After curation, ungate downstream.
3. **External orchestration:** The curation process runs outside pg-trickle (e.g., as an in-and-out daemon mode) and calls `refresh_stream_table()` manually for downstream tables after curation is complete.

Option 3 (external orchestration) is simplest and does not add requirements to pg-trickle.

---

## 9. Recommendations

### 9.1 Before Onboarding System 3

1. **Audit system 3's identity fields.** Run data quality checks:
   - NULL ratio per identity field
   - Distinct value count
   - Top-20 most common values (junk value candidates)
   - Format consistency (phone formats, email casing)
   - Duplicate identity values within system 3 itself (internal dedup)

2. **Populate the blocklist.** Based on audit results, block known junk values before system 3 enters the pipeline.

3. **Add normalisation expressions.** Ensure every identity field has appropriate `normalize:` in the OSI-mapping YAML — `lower(trim(email))`, phone formatting, name standardisation.

4. **Define link groups for weak fields.** If using name-based matching, require composite keys (first_name + last_name + DOB) rather than individual fields.

5. **Set cluster size alerts.** Configure monitoring to alert if any cluster exceeds a reasonable threshold (e.g., 100 records).

### 9.2 During Onboarding

6. **Use extended shadow mode with curation.** This provides:
   - Immediate visibility into linkage consequences (system-3-caused deltas held in shadow)
   - Time for curation (delta tables are held; no writeback executes)
   - Attribution (which changes were caused by system 3 vs independent)
   - A natural curation queue (the shadow comparison tables contain exactly the records that need review)

7. **Review in priority order:**
   - First: `system3_caused` `new_record` rows (cross-system inserts — highest risk)
   - Second: clusters where `clusters_merged > 0` (re-merges — trace to the bridging link)
   - Third: `system3_caused` `field_update` rows (field changes — inspect for reasonableness)
   - Last: `aligned` rows (already correct — skim for sanity, don't block on review)

8. **Run at least two shadow comparison cycles.** The first cycle catches obvious problems. The second cycle after corrections (overrides applied) confirms convergence.

### 9.3 Ongoing (Post-Onboarding)

9. **Monitor cluster metrics.** Use `clusters_merged` / `clusters_split` from `pgt_refresh_history` to detect unexpected linkage changes during steady-state operation.

10. **Periodically audit the override table.** Expired overrides should be re-evaluated. Overrides that are no longer needed (because the underlying data changed) should be removed.

11. **Feedback loop to identity rules.** If the same pattern keeps appearing in the curation queue (e.g., shared family emails), adjust the OSI-mapping YAML — add a link group, change the identity field, or add a normalisation expression. The goal is for the curation queue to shrink over time as the rules improve.

---

## 10. Open Questions

1. **Should OSI-mapping natively support a link override table?** Currently, implementing overrides requires modifying the generated `_id_{target}` view SQL. It would be cleaner if OSI-mapping's YAML supported an `overrides:` section that the engine incorporates automatically. This is a feature request for OSI-mapping, not pg-trickle.

2. **How should `no_link` overrides interact with transitive closure?** If A–B is blocked but A–C and B–C exist, should A and B still end up in the same cluster (via C)? Probably yes — `no_link` means the direct edge is removed, not that the two records can never be in the same cluster. But some operators may expect `no_link` to mean "these are definitely different entities, keep them apart no matter what," which requires a stronger constraint (forced cluster separation).

3. **Should curation apply to all data types or just contacts?** Companies, deals, and other entity types have the same linkage risks but typically lower volumes. The curation mechanism should be generic (per-target, not per-data-type), but the review priority should focus on the entity types with the highest writeback impact.

4. **What is the minimum viable curation UI?** An MVP might be a read-only view of the curation queue table plus a form for submitting override decisions. A production system would need side-by-side record comparison, cluster graph visualisation, bulk accept/reject, and integration with the shadow mode dashboard from PLAN_ONBOARDING_SHADOW_MODE.md §10.11.

5. **How does curation interact with real-time writeback?** In steady state (post-onboarding), should new links always be auto-accepted, or should the curation queue remain active? If active, there must be a mechanism to hold writeback for clusters with pending curation decisions — essentially permanent shadow mode for uncertain links.

6. **Can probabilistic scoring supplement deterministic linkage without replacing it?** A hybrid model: deterministic matching for the `_id_{target}` view (fast, SQL-native), supplemented by a Python/external scoring step that evaluates uncertain edges and auto-accepts or queues them. This keeps the pg-trickle pipeline simple while adding sophistication where needed.
