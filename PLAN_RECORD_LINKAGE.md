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
- **GDPR Article 17 — Right to erasure (mechanics):** Deleting a person's source record is not sufficient. If that record was the conflict resolution winner for any field in the golden record (e.g., the `coalesce` winner for `email`), deleting the source record must trigger re-resolution of that field from the remaining contributors. If no other source holds the field, the field becomes NULL in the golden record. If the golden record changes, reverse mapping re-runs, and all other systems receive update deltas removing or replacing that field. The erasure propagates through the entire pipeline, not just the source table. Additionally, the override table and the curation history must be purged of any rows referencing the deleted record's `external_id` — otherwise deleted PII persists in audit tables.
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

### 4.9 The Merge-Split Data Loss Trap

When two clusters merge, conflict resolution picks field winners. System 1 has `name = "Alice Smith"` and system 3 has `name = "A. Smith"`. If the conflict resolution strategy (`coalesce` with system 1 priority) picks "Alice Smith", the golden record is "Alice Smith" and a writeback delta updates system 3 to "Alice Smith".

If the merge is later discovered to be wrong and the clusters are split, the system cannot reconstruct the state that existed before the merge:

- The `_id_{target}` view re-runs and assigns separate cluster IDs.
- Conflict resolution re-runs with only each cluster's own records.
- Delta views compare the new desired state against the current `base` snapshot.
- The `base` snapshot for system 3 now shows "Alice Smith" — the value that was written by the previous incorrect writeback.
- The new delta for system 3 is a **noop** (desired = "Alice Smith", base = "Alice Smith").
- The original value "A. Smith" is gone from the pipeline. The incorrect merge permanently propagated its change into system 3.

**The split does not restore data.** To recover, the operator must manually correct system 3's record back to "A. Smith" via in-and-out or directly in the source system. The pipeline provides no rollback mechanism for writeback already executed.

This asymmetry makes false positive handling much harder than it appears. Catching over-links **before writeback executes** (via gating, blue/green, or extended shadow mode) is far cheaper than correcting them afterward.

**Implication for override table design:** When adding a `no_link` override to correct a false positive, the override record should capture the pre-merge field values (if known) and flag the affected `external_id`s for manual data review in each source system. The override alone does not fix already-executed writebacks.

### 4.10 Writeback-Induced Identity Changes

The pipeline is bidirectional: ingestion reads from source systems, writeback writes back to them. This creates a feedback loop unique to bidirectional MDM pipelines that does not exist in read-only golden record systems.

The sequence:
1. System 3 is ingested. A record has no email (`email = NULL`).
2. The golden record has `email = "alice@co.com"` (contributed by system 1).
3. Writeback pushes the golden record to system 3 — system 3's record now has `email = "alice@co.com"`.
4. On the next ingestion cycle, system 3's record is re-ingested with `email = "alice@co.com"`.
5. The `_id_{target}` view now creates a new edge between system 3 and system 2 (which also has `alice@co.com`).
6. A cluster merge occurs that was not possible before the writeback.

**Writeback creates identity signals that didn't exist in the source.** The pipeline becomes self-modifying: each write changes what future ingestion sees, which changes future identity resolution, which changes future writes.

This is generally desirable — it is how the pipeline converges toward a consistent state. But it has hazards:

- A writeback based on a false positive propagates the false identity signal into other systems, causing cascading merges.
- The pipeline may enter an oscillation where writeback alternately adds and removes an identity field, causing repeated merge/split cycles.
- After several writeback cycles, it becomes impossible to determine which identity field values came from the original source and which were written by the pipeline.

**Mitigation:** The `base` column records the state at last ingestion. Before resolution, compare `base` to the current source value. If they differ, the source was modified (either by the pipeline's own writeback or by an independent user edit). Track `writeback_origin` on ingested rows to distinguish pipeline-written values from user-written values. Do not use pipeline-written identity values as independent corroboration — they are echoes, not new evidence.

### 4.11 Same-Source Deduplication as a Side Effect

Identity resolution creates clusters. A cluster can contain multiple records from the same source system if those records match on an identity field. This is usually correct (two CRM records for the same person are duplicates), but has unexpected consequences in writeback.

When `_delta_{mapping}` is computed for system 3, it generates one row per cluster per mapping — the desired state for that cluster in system 3. If two system 3 records are in the same cluster, the delta may emit a `delete` for the "loser" record (the one not chosen by conflict resolution's source row selection).

**The pipeline deletes records within system 3 that it was not explicitly asked to delete.** The operator who configured the pipeline to sync contacts between systems likely did not intend to trigger intra-system deduplication.

**Cases where this occurs:**
- System 3 has two records with the same email. They end up in the same cluster. One is the "winner" (drives the golden record), one is the "loser" (slated for deletion).
- System 1 and system 3 each have a record for the same person. They merge into one cluster. The reverse mapping for system 3 points to system 1's external_id. System 3's record is now the "shadow" and receives a delete delta.

**Mitigation:** The writeback daemon should present same-source deletes as a distinct category requiring explicit operator confirmation, not routine writeback. The curation queue (§6) should flag intra-system delete deltas separately from cross-system update deltas. Connector YAML should support a `prevent_same_source_deletes: true` safeguard for pipelines where intra-system deduplication is out of scope.

### 4.12 Temporal Linkage and Entity Lifecycle

OSI-mapping's identity resolution is point-in-time: it evaluates the current state of all records and assigns clusters based on current field values. There is no concept of a link being valid "from date X to date Y."

This creates correctness problems for several real-world scenarios:

| Scenario | What Happens | Problem |
|---|---|---|
| **Company acquisition** | Company A acquires Company B. System 3 merges them (same tax_id now). Systems 1 and 2 have separate records. | After acquisition, identity resolution links them. Pre-acquisition records that referenced Company B get updated to reference Company A's external_id. Historical records become inaccurate. |
| **Person name change** | A person legally changes their name. System 1 updates to the new name; system 3 still has the old name. | Depending on conflict resolution priority, the new or old name wins. The loser system is updated. If the identity field was name-based, the link may break entirely (under-link) until system 3's data is corrected. |
| **Email reassignment** | A company reassigns `alice@co.com` from Alice to Bob. System 1 updates; system 3 still associates it with Alice. | The email now creates a false positive link between Alice (in system 3) and Bob (in system 1). They merge into one cluster. Data corruption follows. |
| **Record reuse** | System 3 deletes a contact and reuses the same `external_id` for a completely different person. | System 3's ingested record matches all historical identity fields for the old contact. The new person is linked to the old person's cluster across all systems. |

**Mitigation options:**
- **Effective date on identity fields:** Allow identity field matches to be scoped to a time range. Two records match only if their `email` overlapped in time. Not currently supported in OSI-mapping.
- **Tombstone records:** When a source record is deleted, ingest a tombstone row (present but marked deleted) rather than removing it. This preserves the historical cluster membership while flagging the record as inactive.
- **Audit log of cluster changes:** Record every cluster merge/split with timestamps. When a link breaks (and a loser record receives deletion), the operator can inspect the history to understand what changed and when.

### 4.13 Cross-Entity-Type Linkage Dependencies

MDM pipelines typically manage multiple entity types: contacts, companies, deals, orders. These entity types are linked — contacts belong to companies, deals are associated with contacts and companies. OSI-mapping resolves FK references via the `references:` declaration in the YAML.

Linkage errors in one entity type propagate to related entity types through these FK dependencies:

```
Company X (system 1) ←→ Company Y (system 3)  [FALSE POSITIVE — different companies]

  ↓ company linkage is wrong

Contact A references Company X
Contact B references Company Y
  → FK resolution maps both to the same merged company
  → Contact A and Contact B are written back with the same company_id
  → System 3 now shows Contact A under Company Y (wrong)
  → System 1 now shows Contact B under Company X (wrong)
```

The company linkage error propagates to every contact in both companies. The blast radius is not N records — it is N records multiplied by the number of contacts per company.

**Implications:**
- Entity types with more FKs (contacts referencing companies, orders referencing contacts _and_ companies) have higher blast radius from linkage errors in their referenced entities.
- Identity resolution should be run in **dependency order**: entity types with no incoming FKs (companies) should be resolved first, then entity types that reference them (contacts). Running contact identity resolution before company identity resolution means contact FK mappings are computed against an unresolved company graph.
- The `clusters_merged` metric (proposed in PLAN_ONBOARDING_PROPOSAL.md) should be tracked per entity type, not just per target. A `clusters_merged = 1` for companies could affect thousands of contacts.
- Curation priority should weight linkage decisions by downstream FK fan-out: linking two large companies is higher-risk than linking two contacts.

### 4.14 Link Confidence Decay

A link established today may be wrong tomorrow, even if it was correct when created. The most common cause is **the supporting evidence changing in the source data** after the link was established.

```
2024: System 1 has alice@co.com. System 3 has alice@co.com. Link created.
2026: System 3 corrects the email to a.smith@co.com (different person who had been using Alice's old email).
      The email identity edge between system 1 and system 3 is now broken.
      OSI-mapping re-runs identity resolution. The link disappears. The cluster splits.
      Writeback fires to undo the 2-year-old merge.
```

Less obviously, links can decay when the override table becomes stale:

```
2024: Operator creates a 'link' override for System 1 C-1042 ↔ System 3 ERP-9001.
      Reason: same person, different emails at the time.
2026: System 3 ERP-9001 is reassigned to a different person (employee turnover).
      The override still forces the link. Now C-1042 (Alice) is linked to ERP-9001 (Bob).
      The override is wrong but has no expiry. It persists indefinitely.
```

**Mitigation:**
- Add `expires_at` to `inout_link_overrides`. Expired overrides are automatically re-queued for review.
- Add `evidence_snapshot` to `inout_link_overrides` — a JSONB record of the identity field values that justified the override at the time it was created. When the supporting evidence changes (because source data was updated), the system can detect that the override's original justification no longer holds and flag it for re-evaluation.
- Track `last_confirmed_at` on overrides. An override that has not been confirmed in 12+ months should be re-reviewed.
- Monitor cluster membership changes. If a cluster that was manually linked (via override) splits in identity resolution, the split is a signal that the original linking evidence has weakened.

### 4.15 Writeback Rejection as a Linkage Signal

When the writeback daemon attempts to insert a record into system 3 and the API returns an error indicating the record already exists, this is not merely a writeback error — it is direct evidence of an **under-link in the identity resolution**.

The pipeline computed an `insert` delta because, from the golden record's perspective, system 3 does not have this entity. But the source system itself says otherwise. The entity exists in system 3 under a different `external_id` than the pipeline knows about — a record that was not matched during identity resolution.

**The API rejection is evidence of a missed match.** This feedback should flow back to the linkage layer:

```
Writeback daemon → POST /contacts to system 3
System 3 API   → 409 Conflict: "Record with email alice@co.com already exists (id: ERP-9001)"

Expected handling:
  1. Do not retry as a plain writeback error.
  2. Extract the conflicting external_id from the error response (ERP-9001).
  3. Write to the curation queue:
       suspected_under_link:
         cluster_id:       <from the insert delta>
         source_a:         system1  (or whichever has this record)
         source_b:         system3
         external_id_b:    ERP-9001  (extracted from rejection response)
         evidence:         "insert rejected: record already exists"
  4. Fetch system3/ERP-9001 and add it to the shadow comparison.
  5. Route to LLM curation (§7.2) or human review.
  6. If confirmed as under-link: ingest ERP-9001, create link override, re-run identity resolution.
```

This requires the writeback daemon to parse structured error responses from APIs and distinguish "already exists" rejections from other errors (rate limits, validation failures, server errors). Not all APIs return machine-readable conflict responses, but for those that do, this is a high-value signal that costs nothing to collect.

Similarly, a `404 Not Found` response during an update writeback is evidence of a **wrong external_id** — the pipeline believes a record exists in system 3 but it doesn't. This is either a deletion the pipeline didn't know about, or an external_id mismatch caused by an incorrect link. Both cases should surface as curation candidates rather than plain errors.

### 4.16 Transitivity Is an Assumption, Not a Law

OSI-mapping's connected-components algorithm is mathematically correct: if A–B is an edge and B–C is an edge, then {A, B, C} is a cluster. But the underlying assumption — that shared identity field values mean shared real-world identity — is not always transitive in practice.

```
Record A: executive Alice, phone +1-555-1234 (her assistant's number)
Record B: assistant Carol, phone +1-555-1234 (her own number)
Record C: executive Bob,   phone +1-555-1234 (also uses Carol's number)

Edge A–B: phone match  ✓ (same assistant — plausible link, but wrong)
Edge B–C: phone match  ✓ (same assistant — plausible link, but wrong)

Result: cluster {A, B, C} — Alice, Carol, and Bob merged into one entity
Direct edge A–C: does not exist (different people, no shared field)
```

There is no direct false positive between A and C. The merge happens because of a valid-looking chain through B. This is the hardest class of linkage error to detect: there is no single wrong edge to flag — the problem is the combination of two correct-looking edges that happen to pass through a shared intermediary.

**Why this matters differently from other false positives:**
- The curation queue will not flag A–C as a suspicious link, because A and C have no direct edge — only the indirect cluster membership.
- The cluster may look reasonable in isolation: all three records share the same phone number, so the cluster appears internally consistent.
- Splitting requires identifying that B is the hub, not a member, and that A–B and B–C are both coincidental.

**Mitigations:**
- **Hub detection:** If a single record contributes edges to more than N other records in the same cluster, it may be an intermediary rather than a genuine shared identity. Flag it for curation.
- **Cross-field corroboration:** Before accepting a transitive link, require at least one direct field match between the terminal records (A and C must agree on at least one identity field, not just both agree with B). This is stricter than current OSI-mapping semantics but prevents transitive false positives.
- **Link group transitivity control:** Consider whether identity fields should create transitive edges at all, or only within a link group. E.g., phone matches create edges within a link group; two records in the same cluster via different link groups may not actually be the same entity.

### 4.17 Soft Deletes

Most CRMs and ERPs do not hard-delete records. They set a flag — `is_deleted = true`, `archived_at`, `status = 'inactive'` — and the record continues to exist in the database with all its field values intact.

In OSI-mapping's pipeline, a soft-deleted record in a source system is indistinguishable from an active record unless the connector YAML explicitly filters it out. If it is not filtered, it continues to participate in identity resolution:

```
System 3: contact ERP-9001
  name:       "Alice Smith"
  email:      "alice@co.com"
  is_deleted: true   ← soft-deleted 6 months ago, but still in the API response
  updated_at: 2025-09-01

System 1: contact C-1042
  name:       "Alice Chen"
  email:      "alice@co.com"
  is_deleted: false

Identity resolution: same email → same cluster
Golden record: Alice Smith or Alice Chen (depending on conflict resolution priority)
Writeback: system 1 receives update to align with golden record
```

Alice left the organisation 6 months ago. Her soft-deleted record still bridges to an active contact in system 1, updating that contact based on stale, logically-deleted data.

**The problem compounds with company-level soft deletes.** A company that was soft-deleted in system 3 may still link to active companies in systems 1 and 2 via tax_id, pulling those active records into a cluster driven by a defunct entity.

**Mitigations:**
- **Ingest-time filtering:** The connector YAML should support a `filter:` expression that excludes soft-deleted records from identity resolution. Soft-deleted records should be ingested into a separate partition or simply excluded from `inout_src_*` and stored in a separate `inout_src_*_deleted` table for audit purposes only.
- **Active-only identity resolution:** Identity fields for soft-deleted records should not create edges. Only active records should be identity anchors. A soft-deleted record may still appear in the golden record as a historical contributor, but it should not drive cluster membership.
- **Propagate soft deletes:** When a source record is soft-deleted, the ingestion daemon should treat it as a logical delete — writing a delete delta rather than ignoring the `is_deleted` flag.

### 4.18 Multi-Valued Identity Fields

OSI-mapping expects scalar identity fields: one email per record, one tax_id per record. Real-world data frequently violates this assumption.

| Scenario | Source data | OSI-mapping behaviour |
|---|---|---|
| Person with multiple emails | `emails: ["alice@co.com", "a.smith@personal.com"]` | Only one can be declared as the identity field. The other is ignored for matching. Under-linking results. |
| Company with multiple tax IDs | `tax_ids: ["US-12345", "EU-67890"]` (post-acquisition) | Only one tax_id can be identity. The second is invisible to the linker. |
| Phone stored as array | `phones: ["+1-555-1234", "+1-555-5678"]` | Array-valued identity fields are not supported. The field cannot be used for matching. |
| Contact with work + personal email | Both in separate fields | Only the declared field is used. Matching only occurs on whichever field was chosen. |

**The consequence:** Records that should be linked because they share one of N email addresses will not be linked if the matching address is not in the declared identity field slot. This is an architectural under-linking risk that grows with the richness of source data.

**Options:**
1. **Pre-ingestion normalisation:** Flatten multi-valued fields before ingestion. Choose one canonical value per record (e.g., the primary email) and write it to the scalar identity field. Document the selection rule in the connector YAML.
2. **Multiple identity declarations:** Declare multiple fields as identity (e.g., `work_email` and `personal_email`). OSI-mapping creates an edge if either matches. This works if the fields are separate columns, not array elements.
3. **Explode-and-match (not currently supported):** A forward view that unnests array elements and creates one logical row per value, with the same `_src_id`. Identity resolution runs on the exploded view, then collapses back to source rows. This would require OSI-mapping engine support for array-valued identity fields.
4. **LLM-assisted matching (**§7**):** Use embedding similarity to surface candidate under-links that the scalar identity resolution missed, then review with LLM or human.

### 4.19 Cluster Singleton Monitoring

A **singleton cluster** is a cluster containing records from only one source system — a record that matched nothing in any other system. In a well-functioning pipeline after full onboarding, singleton counts should be:

- **Expected for system-exclusive entities:** Some contacts genuinely exist only in system 3. Singletons for these are correct.
- **Suspicious if widespread for an existing system:** If system 1 has 40% singletons after system 3 joins, that means 40% of system 1's contacts have no match in system 3 or system 2. This is either correct (system 1 has unique data) or a sign of under-linking — system 3 has these contacts but the identity rules didn't match them.

Currently there is no metric tracking singleton ratio per source system over time. A rising singleton count in an existing system after onboarding is an early warning of under-linking.

**Recommended monitoring:**

```sql
-- Singleton ratio per source system
SELECT
    source,
    COUNT(*) FILTER (WHERE cluster_size = 1)  AS singletons,
    COUNT(*)                                   AS total,
    ROUND(100.0 *
        COUNT(*) FILTER (WHERE cluster_size = 1) / COUNT(*), 2) AS singleton_pct
FROM (
    SELECT
        source,
        COUNT(*) OVER (PARTITION BY _cluster_id) AS cluster_size
    FROM _id_contact
) sub
GROUP BY source
ORDER BY singleton_pct DESC;
```

**Expected pattern after adding system 3:**
- System 3 singletons: high initially (system 3 has many exclusive records), decreasing as ingestion and identity resolution converge.
- System 1 and 2 singletons: should not change significantly from pre-onboarding baseline. A significant increase means system 3 is not linking to records it should.

This metric should be tracked in `pgt_refresh_history` as a per-source-system aggregate alongside `clusters_merged` / `clusters_split`.

### 4.20 The Bootstrap Problem: The Highest-Risk Refresh

The first time identity resolution runs against the full combined dataset (all three systems, all records), it is not an incremental update — it is a full batch evaluation of the complete identity graph. Every record from system 3 is new. Every potential edge involving system 3 is evaluated simultaneously. The connected-components algorithm runs to completion across the entire graph in a single refresh.

This single refresh is the highest-risk moment in the pipeline's lifetime.

**Why it is uniquely dangerous:**
- **Maximum new edges:** All system 3 identity edges are created at once. Any junk value in system 3 that creates a high-fanout edge links thousands of records in one shot.
- **No incremental visibility:** In steady state, DIFFERENTIAL refresh means each cycle affects a small subset of clusters. A problem (new junk value, wrong identity rule) affects a bounded number of clusters per cycle. At bootstrap, there is no such bound — a single bad rule produces the full extent of its damage immediately.
- **Gating does not prevent the blast:** Source gating prevents identity resolution from running during ingestion. But when the gate lifts, the full resolution runs at once. The blast is deferred, not diminished.
- **Cluster metrics are not comparable:** Pre-bootstrap, the baseline `clusters_merged` count is zero. The first refresh will show a large `clusters_merged` number — but is it large because the data is rich with real matches, or because something is wrong? There is no prior run to compare against.

**Managing the bootstrap refresh:**

The bootstrap refresh requires a different preparation strategy from steady-state change management:

1. **Run identity resolution against a sample first.** Before ungating all of system 3, ungate a random 1–5% sample. Run identity resolution. Inspect the resulting clusters. Do they look reasonable? Is the singleton ratio sensible? Are there any unexpectedly large clusters? Fix any problems before running against the full dataset.
2. **Establish a baseline before adding system 3.** Snapshot the current cluster count, singleton counts per system, and cluster size distribution while the pipeline is healthy (systems 1 and 2 only). This gives a comparison baseline for the post-bootstrap result.
3. **Gate delta tables through bootstrap (Mode B).** Writeback should not execute until the operator has reviewed the bootstrap result. The bootstrap refresh and the first writeback are two separate decisions.
4. **Set conservative cluster size alerts before bootstrap.** Any cluster that exceeds, say, 50 records after the bootstrap refresh should be held and reviewed before writeback fires. This catches junk value merges automatically.
5. **Expect the bootstrap to be slow.** The full graph evaluation is O(n log n) or worse. For millions of records, this may take minutes to hours. Budget for it. Do not set pg-trickle timeouts that would interrupt a slow but legitimate bootstrap refresh.

### 4.21 Asymmetric Identity Field Trust

The document covers source priority for **conflict resolution** — when two sources disagree on a field value, higher-priority sources win. But source priority and identity edge trust are independent concerns, and OSI-mapping currently treats them as one.

A source's identity field value may be authoritative for _winning_ conflicts while being untrustworthy for _creating_ identity edges:

| Source | Tax ID quality | Should win conflicts? | Should create identity edges? |
|---|---|---|---|
| Government portal | Validated, official | Yes | Yes |
| CRM (system 1) | Manually entered, typo-prone | Yes (high data quality otherwise) | **No** — too many entry errors |
| Legacy ERP (system 3) | Imported from spreadsheet in 2018 | Low | **No** — unverified |

If all three sources declare `tax_id: identity`, a typo in system 1's tax_id field (e.g., `12345` instead of `12346`) creates a false edge linking system 1's record to whoever has `12345` in any other system. The conflict resolution priority is irrelevant — the damage is done at the edge-creation stage.

**What is needed:** A per-source-per-field trust configuration that separates "this source's value fills this field in the golden record" from "this source's value creates identity edges." In OSI-mapping YAML terms, this would look like:

```yaml
mappings:
  - name: crm_contacts
    source: crm
    fields:
      - source: tax_id
        target: tax_id
        identity: false    # contributes to the field, but not used for matching
                           # (even though tax_id is declared identity on the target)
```

Until this is supported, the safest workaround is to exclude low-trust sources from the identity field declaration entirely and create a separate high-trust-only mapping for identity purposes — accepting the under-linking cost.

### 4.22 Identity Rule Validation Against Production Data

OSI-mapping's inline test framework validates known cases: given this input, assert this output. It cannot answer the more important pre-onboarding question: **"How well do our identity rules perform against the actual production dataset?"**

Specifically, before the bootstrap refresh (§4.20), you need estimates of:
- **Precision:** Of all the links the identity rules will create when system 3 joins, what fraction are genuine matches?
- **Recall:** Of all the genuine matches that exist between system 3 and systems 1 and 2, what fraction will the identity rules detect?

These cannot be computed without a ground truth sample. The approach:

```
1. Sample
   Take a random sample of ~500 record pairs: 250 from each of
   (system 1 × system 3) and (system 2 × system 3).
   Include pairs that the identity rules WOULD link and pairs they WOULD NOT.

2. Label
   Human reviewers (or LLM, §7) label each pair: genuine_match / non_match.
   Target: ~100 genuine matches out of 500 pairs (adjust sampling to ensure coverage).

3. Evaluate
   Run identity rules against the sample.
   Compute:
     precision = true_positives / (true_positives + false_positives)
     recall    = true_positives / (true_positives + false_negatives)
     F1        = 2 * (precision * recall) / (precision + recall)

4. Threshold decision
   If precision < 90%: the identity rules will create too many false positives.
                       Tighten rules (add link groups, increase field specificity,
                       add to blocklist) before bootstrap.
   If recall   < 70%: the identity rules will miss too many genuine matches.
                       Add normalisation expressions, add identity fields,
                       consider LLM-assisted under-link detection post-bootstrap.
```

**Relationship to LLM curation (§7):** The ground-truth labelling step is a natural use case for LLM-assisted review. The LLM classifies each sampled pair; humans review only the LLM-uncertain cases. This makes the 500-pair evaluation tractable in hours rather than days.

**This validation should be gating:** The bootstrap refresh should not proceed until precision and recall are at acceptable thresholds. An untested identity rule configuration against production data is the single highest-risk gap in the onboarding process.

### 4.23 Schema Drift and Silent Identity Field Loss

Identity resolution depends on identity fields being populated. If a source system's API schema changes — a field is renamed, moved into a nested object, or removed — the connector YAML stops reading it correctly. Affected rows ingest with NULL for that identity field. NULL values do not create identity edges. The identity graph silently degrades.

Unlike a hard failure (connector crashes, ingestion stops), schema drift is **silent degradation**: the pipeline continues running, metrics look normal, but the identity graph is producing fewer links than it should. Previously linked clusters drift apart as `base` snapshots age out and the linking field disappears from new ingestion cycles.

**Concrete sequence:**

```
2026-03-01: System 3 API has field "email"
            → inout_src_system3_contacts.email = "alice@co.com"
            → identity edge: system3 ↔ system1 via email ✓

2026-04-15: System 3 API renames "email" to "primary_email"
            → connector YAML still reads "email" → NULL
            → inout_src_system3_contacts.email = NULL (for all new/updated rows)
            → no identity edges created for any row ingested after 2026-04-15

2026-05-01: Alice updates her phone number in system 3
            → re-ingested with NULL email
            → connected-components re-runs
            → edge system3 ↔ system1 disappears (base row changed, email NULL)
            → cluster splits
            → writeback fires: system 1 receives delete delta for Alice's system3-derived fields
```

The silent split occurs on the next update to any affected record — not immediately after the schema change. Monitoring `_id_{target}` refresh metrics will not show an anomaly until records start being re-ingested with NULL identity fields.

**Mitigations:**
- **Schema validation on ingestion:** The ingestion daemon should validate that declared identity fields are non-NULL for at least X% of ingested records per run. A sudden drop in identity field population rate (e.g., `email` goes from 95% populated to 0%) should raise an alert before any records are committed.
- **Schema change detection:** Compare the shape of the API response against the expected connector YAML schema on every sync run. Alert if declared identity fields are missing from the response.
- **Identity field NULL rate in `pgt_refresh_history`:** Track `null_identity_field_pct` per source per field as a streaming metric. A spike means schema drift or a data quality regression.
- **Connector YAML contract tests:** Before deploying a connector YAML change, run it against a live API sample and verify that identity fields are populated as expected.

### 4.24 Multi-Tenancy Identity Isolation

If the pipeline operates in a multi-tenant context — multiple customers, business units, or organisations sharing the same PostgreSQL database — identity resolution must never link records across tenant boundaries.

Two contacts from different tenants who happen to share an email address are not the same person. But OSI-mapping's identity rules, unless explicitly scoped by tenant, will create an edge between them:

```
Tenant A: contact <alice@consultancy.com>, a consultant working for Tenant A
Tenant B: contact <alice@consultancy.com>, the same consultant working for Tenant B

Identity resolution: same email → same cluster
Golden record: merged across tenants
Writeback: Tenant A's system receives Tenant B's contact data (data leak)
           Tenant B's system receives Tenant A's contact data (data leak)
```

This is not merely a linkage error — it is a security and compliance failure. Tenant data must never cross tenant boundaries, regardless of identity field matches.

**Required mitigations:**

1. **Tenant scoping as a mandatory partition key in identity resolution:**

```yaml
targets:
  contact:
    partition_by: tenant_id    # identity resolution only creates edges
                                # within the same partition value
    fields:
      email: identity
      tax_id: identity
```

This is not currently supported in OSI-mapping. The equivalent workaround is to run separate OSI-mapping instances per tenant (separate schemas or databases), which is operationally expensive but correct.

2. **Forward view filtering:** The `_fwd_{mapping}` views must include a `WHERE tenant_id = current_tenant` predicate. This requires the tenant context to be available at view evaluation time — either as a session variable or baked into the view definition per tenant.

3. **Row-level security on source tables:** PostgreSQL RLS policies on `inout_src_*` tables prevent cross-tenant reads at the database level, providing a defence-in-depth layer even if the OSI-mapping views are misconfigured.

4. **No shared golden record:** The `{target}` analytics view must be per-tenant. A single shared `contact` view would expose all tenants' golden records to any consumer of that view.

**This consideration is binary:** either the system correctly isolates tenants or it silently leaks data. There is no partial correctness. Multi-tenant pipelines must have tenant isolation validated in the test suite before any production deployment.

### 4.25 Match Key Combinatorics and the Accumulator Cluster

OSI-mapping's identity field declarations use OR-logic: a record is linked to a cluster if it matches on email **OR** tax_id **OR** phone **OR** any other declared identity field. Each new identity field added to the YAML is a new axis of potential links.

As the number of identity fields grows, a structural risk emerges that is distinct from any individual field's false positive rate: **accumulator clusters**.

An accumulator cluster forms when a series of individually plausible single-field matches create a large cluster whose members have no direct connection to most other members:

```
Record A: email=alice@co.com,  phone=555-1234,  tax=—
Record B: email=—,             phone=555-1234,  tax=12345
Record C: email=—,             phone=—,         tax=12345,  name=Alice Smith
Record D: email=bob@co.com,    phone=—,         tax=—,      name=Alice Smith
Record E: email=bob@co.com,    phone=555-9999,  tax=—

Each edge is individually plausible:
  A–B: same phone
  B–C: same tax_id
  C–D: same name
  D–E: same email

Cluster {A, B, C, D, E}: five records, no two sharing the same identity field value
```

Alice and Bob share nothing in common, but they are in the same cluster through a chain of coincidental single-field matches across four different identity fields. This is a generalisation of the transitivity problem (§4.16), but caused by the multiplication of identity fields rather than a shared intermediary.

**The risk grows combinatorially.** With 2 identity fields, a record is linked only if it shares one of 2 values. With 5 identity fields, it can enter a cluster through any of 5 different doors. A cluster of 100 records with 5 identity fields has many more potential entry points than a cluster of 100 records with 2 identity fields.

**Mitigations:**
- **Minimum match count:** Require a record to match on at least 2 of N identity fields (not just 1) before creating an edge. This trades recall for precision — some genuine matches that share only one field will be missed, but accumulator clusters become much harder to form. Not currently supported in OSI-mapping; would require engine changes.
- **Link group discipline:** Put each identity field in a link group that requires at least one corroborating field from the same source. A phone match alone is not sufficient; phone + country code, or phone + name initial.
- **Cluster membership diff auditing:** When a record joins a cluster, record which identity field created the edge. If a cluster has members that entered through 4 or 5 different identity fields with no overlap, that cluster is an accumulator candidate.
- **Progressive identity field rollout:** Don't declare all 5 identity fields at once during onboarding. Declare the 2 highest-confidence fields first (tax_id, email). Evaluate the resulting clusters. Add a third field (phone) only after confirming the 2-field clusters are correct. Each addition is a controlled experiment, not a simultaneous change to the full identity graph.

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

## 7. LLM-Assisted Curation

### 7.1 The Scalability Problem LLMs Solve

The core weakness of human curation is throughput. At 30 seconds per link decision, 5,000 uncertain links take 42 hours of human time. Auto-curation rules (§6.6) reduce this queue by handling the easy cases (high-fanout → block, strong multi-field match → accept), but the remaining cases — the ones where context matters — still need judgment.

LLMs are well-suited to this specific judgment task. Each link curation decision is a self-contained, context-rich question: "Given these two records, are they the same real-world entity?" The input is structured (field names and values), the output is a classification (link / no_link / uncertain), and the reasoning can be explained.

### 7.2 What LLMs Can Do

**Task 1: Classify uncertain link proposals**

For each edge flagged by auto-curation rules as `queue_for_review`, an LLM examines the two records and decides:

```
Record A (System 1 — CRM):
  name: "Alice Smith"
  email: "alice@company.com"
  phone: "+1-555-1234"
  address: "123 Main St, Springfield, IL"

Record B (System 3 — ERP):
  name: "A. Smith"
  email: "alice@company.com"
  phone: null
  address: "123 Main Street, Springfield"

Identity field matched: email
Question: Are these the same person?

LLM reasoning: The email addresses match exactly. The names are compatible
("A. Smith" is a plausible abbreviation of "Alice Smith"). The addresses
match after normalisation ("St" vs "Street"). Phone is missing in System 3,
which provides no evidence either way. High confidence these are the same entity.

Decision: link
Confidence: 0.95
```

This is a task that requires:
- Understanding that "A. Smith" is an abbreviation of "Alice Smith" (not a different person)
- Understanding that "St" and "Street" are the same
- Weighing the significance of a missing phone number (neutral, not negative)
- Combining weak signals into an overall judgment

SQL-based auto-curation rules cannot do this. Levenshtein distance on names would give a low score for "A. Smith" vs "Alice Smith" (edit distance 6), potentially rejecting a correct match. An LLM understands the semantic relationship.

**Task 2: Detect under-linked entities (missed matches)**

LLMs can scan separate clusters for records that should have been linked but weren't, because the identity fields don't match exactly:

```
Cluster 17: {System 1: "Alice Smith" <alice@company.com>}
Cluster 42: {System 3: "Alice R. Smith" <asmith@company.com>}

No identity field match:
  - email: "alice@company.com" ≠ "asmith@company.com"
  - name: "Alice Smith" ≠ "Alice R. Smith" (not identity field — uses coalesce strategy)

LLM reasoning: These are likely the same person. "asmith@company.com" is a
common corporate email pattern for "Alice Smith". The name "Alice R. Smith"
includes a middle initial. Both are at the same company domain. Recommend
reviewing for manual link.

Decision: queue_for_review (suspected under-link)
```

This is the hardest task for rule-based systems. There is no deterministic criterion that matches `alice@company.com` to `asmith@company.com` — it requires understanding corporate email conventions. An LLM can surface these as candidates for a human to confirm (or auto-link at high confidence).

**Task 3: Identify junk values**

LLMs can classify identity values as genuine or junk without a pre-populated blocklist:

```
Identity value: "info@company.com" (appears in 340 records across 3 systems)
LLM reasoning: "info@" is a generic departmental email, not a personal identifier.
Linking 340 records on this value would create a false mega-cluster.
Decision: add to blocklist
```

```
Identity value: "john.smith@gmail.com" (appears in 12 records across 2 systems)
LLM reasoning: This is a personal email. However, "John Smith" is a very
common name. 12 records is plausible for coincidental sharing of a common
name's email. Recommend reviewing the 12 records individually.
Decision: queue_for_review
```

**Task 4: Explain curation decisions**

Even when the final decision-maker is human, an LLM pre-processes each case with an explanation. This reduces the 30-second human review to a 5-second confirmation:

```
Proposed link: System 1 C-1042 ↔ System 3 ERP-9001
LLM recommendation: LINK (confidence: 0.92)
Reason: Same email address. Names differ ("Alice Smith" vs "A. Smith") but
are compatible — likely abbreviation. Addresses are in the same city. No
contradicting evidence.

[✓ Accept]  [✗ Reject]  [? Defer]
```

### 7.3 Architecture: Where the LLM Fits

The LLM sits in the curation layer, between identity resolution and the override table. It does not replace identity resolution — it reviews its output.

```
_id_{target} refreshes
        ↓
Auto-curation rules run (§6.6)
        ↓
┌─────────────────────────────────────────────┐
│ Cases routed to LLM:                         │
│  • queue_for_review edges                    │
│  • Suspected under-links (cluster pairs)     │
│  • New junk value candidates                 │
│                                              │
│ LLM outputs:                                 │
│  • auto_link (high confidence)  → override   │
│  • auto_no_link (high confidence) → override │
│  • uncertain → human queue                   │
│  • blocklist_candidate → blocklist           │
└─────────────────────────────────────────────┘
        ↓
Human reviews only LLM-uncertain cases
        ↓
Override table updated → next refresh incorporates
```

**The LLM does NOT:**
- Modify the `_id_{target}` view or its SQL
- Run inside PostgreSQL or pg-trickle
- Have write access to source tables
- Make irreversible decisions without operator configuration

**The LLM DOES:**
- Read record pairs from the curation queue
- Produce a classification + confidence score + natural language reasoning
- Write to the override table (if configured for auto-accept above a confidence threshold)
- Write to the human review queue (if confidence is below threshold)

### 7.4 Implementation: LLM Curation Daemon

A new in-and-out daemon mode that processes the curation queue:

```python
# Pseudocode for LLM curation loop
for edge in curation_queue.where(status='pending_llm'):
    record_a = fetch_record(edge.source_a, edge.external_id_a)
    record_b = fetch_record(edge.source_b, edge.external_id_b)

    prompt = build_prompt(record_a, record_b, edge.identity_field, edge.identity_value)
    response = llm.complete(prompt)

    decision = parse_decision(response)  # link / no_link / uncertain
    confidence = parse_confidence(response)

    if confidence >= config.auto_accept_threshold:  # e.g., 0.90
        # Write directly to override table
        write_override(edge, decision, reason=response.reasoning, decided_by='llm')
        edge.status = 'resolved_by_llm'
    else:
        # Queue for human with LLM's reasoning attached
        edge.llm_recommendation = decision
        edge.llm_confidence = confidence
        edge.llm_reasoning = response.reasoning
        edge.status = 'pending_human'
```

### 7.5 The Prompt

The prompt structure for link classification:

```
You are reviewing a proposed identity link in a Master Data Management system.
Two records from different source systems matched on an identity field and may
represent the same real-world entity. Your job is to assess whether they are
truly the same entity.

MATCHED IDENTITY FIELD: {field_name}
MATCHED VALUE: {value}

RECORD A (Source: {source_a}):
{formatted_fields_a}

RECORD B (Source: {source_b}):
{formatted_fields_b}

Consider:
1. Are the non-identity fields consistent with being the same entity?
   (Names compatible? Addresses in the same area? Dates reasonable?)
2. Could the matched identity value be coincidental?
   (Common name? Shared/generic email? Reassigned phone number?)
3. Are there any fields that contradict the match?
   (Different gender? Incompatible dates of birth? Different countries?)

Respond with:
- decision: "link" | "no_link" | "uncertain"
- confidence: 0.0 to 1.0
- reasoning: one paragraph explaining your assessment
```

For under-link detection (scanning separate clusters):

```
You are reviewing two separate clusters in a Master Data Management system.
These records did NOT match on any identity field, but they may represent the
same real-world entity. Your job is to assess whether they should be linked.

CLUSTER {cluster_a} records:
{formatted_records_a}

CLUSTER {cluster_b} records:
{formatted_records_b}

Consider:
1. Could these be the same entity despite the identity fields not matching?
   (Typos? Nicknames? Different email formats? Name changes?)
2. How strong is the evidence for a match?
3. What is the risk if they are falsely linked?

Respond with:
- decision: "suggest_link" | "no_link" | "uncertain"
- confidence: 0.0 to 1.0
- reasoning: one paragraph explaining your assessment
```

### 7.6 Confidence Calibration

The LLM's confidence score must be calibrated against actual outcomes. Uncalibrated, LLMs tend toward overconfidence — a model that says "0.95 confidence" may only be correct 80% of the time.

**Calibration approach:**
1. During initial deployment, set `auto_accept_threshold` to 1.0 (effectively disabling auto-accept — all decisions go to human review).
2. Humans review all LLM recommendations. Track accuracy: what percentage of LLM "link" decisions at confidence 0.9+ were accepted by the human? What percentage of "no_link" decisions?
3. Once the observed accuracy matches the stated confidence (e.g., 95% of 0.95-confidence decisions are correct), lower the auto-accept threshold to that level.
4. Continue monitoring. If accuracy drops (e.g., due to a new source system with different data patterns), raise the threshold.

**Expected outcome:** After calibration, the LLM handles 80–95% of the curation queue automatically. The human reviews only the 5–20% where the LLM is genuinely uncertain. This reduces the 42-hour review queue to 2–8 hours.

### 7.7 LLM for Under-Link Detection at Scale

Under-link detection (Task 2 in §7.2) is the hardest curation problem because it requires comparing clusters that the identity rules _didn't_ connect. Naively, this is O(n²) on the number of clusters — impractical for large datasets.

**Approach: Embedding-based candidate retrieval + LLM verification**

1. **Embed each cluster** into a vector using a text embedding model (records concatenated into a text representation).
2. **Nearest-neighbour search** using vector similarity (cosine distance) to find cluster pairs that are close in embedding space but not linked by identity rules.
3. **LLM reviews** only the top-K nearest candidates per cluster — a tractable number.

```
Step 1: Embed
  Cluster 17 → vector [0.23, -0.14, ...]  (from "Alice Smith alice@company.com 123 Main St")
  Cluster 42 → vector [0.22, -0.15, ...]  (from "Alice R. Smith asmith@company.com 123 Main Street")

Step 2: Nearest neighbours
  Cluster 17's nearest unlinked cluster: Cluster 42 (cosine similarity: 0.97)

Step 3: LLM review
  "Are Cluster 17 and Cluster 42 the same entity?" → "Yes, likely — recommend link"
```

This scales to millions of clusters because embedding + ANN search is O(n log n), and the LLM only reviews the small number of high-similarity candidates.

### 7.8 Can LLMs Replace Human Curation?

**Short answer:** For routine link decisions, yes. For high-stakes or ambiguous decisions, no.

| Curation Task | LLM Can Replace Human? | Why |
|---|---|---|
| Classifying clear matches (abbreviation, formatting differences) | **Yes** | LLMs handle semantic similarity well. After calibration, accuracy matches or exceeds a distracted human reviewer. |
| Detecting junk values | **Yes** | LLMs recognise generic/placeholder values reliably. |
| Classifying clear non-matches (contradicting fields) | **Yes** | LLMs detect contradictions (different DOB, different gender) consistently. |
| Ambiguous matches (similar but not clearly same entity) | **Partially** | LLM provides a recommendation + reasoning, but the final decision should be human. The cost of a wrong link (data corruption) exceeds the cost of human review time. |
| Under-link detection (missed matches) | **Partially** | The embedding + LLM approach surfaces candidates, but confirming a merge across clusters changes the golden record for potentially hundreds of downstream records. Human sign-off is appropriate. |
| Regulatory-sensitive links | **No** | In healthcare and finance, automated PII cross-referencing may require documented human judgment. An LLM decision may not satisfy audit requirements. |
| Novel data patterns | **No** | When a new source system has data patterns the LLM hasn't seen (industry-specific IDs, non-Latin scripts, domain-specific abbreviations), the LLM's confidence is unreliable until re-calibrated. |

**Practical model:** LLM handles the long tail of moderately uncertain decisions. Humans handle the small number of genuinely ambiguous cases and the regulatory-sensitive ones. The LLM-to-human escalation path is always available.

### 7.9 Risks of LLM-Assisted Curation

| Risk | Detail | Mitigation |
|---|---|---|
| **Hallucinated reasoning** | The LLM may produce plausible-sounding reasoning for an incorrect decision. A human reviewer trusting the reasoning without checking the data could approve a bad link. | Always display both the LLM reasoning AND the raw record data. The reasoning is a convenience, not a substitute for the data. |
| **Overconfidence** | LLMs may report high confidence on decisions they should be uncertain about. Without calibration, the auto-accept threshold lets bad decisions through. | Mandatory calibration period (§7.6). Never auto-accept before calibration. |
| **PII in prompts** | The LLM prompt contains record data, which is PII. Sending PII to an external LLM API (OpenAI, Anthropic) may violate data residency or privacy requirements. | Use a self-hosted model (e.g., local Llama, Mistral) or an API with a Data Processing Agreement and no training on customer data. This is a deployment constraint, not an architectural one. |
| **Cost** | At $0.01–0.10 per LLM call (depending on model and token count), 5,000 curation decisions cost $50–500. Acceptable for onboarding; may be costly for continuous steady-state curation. | Batch calls. Use a smaller/cheaper model for clear cases, escalate to a larger model for ambiguous ones. |
| **Inconsistency** | The same record pair presented twice may get different LLM responses (non-deterministic generation). This is confusing if a human overrides an LLM decision and the LLM later contradicts itself. | Set temperature to 0. Cache decisions. Once a decision is made (by LLM or human), store it in the override table — don't re-evaluate. |
| **Bias amplification** | If the LLM has biases about name patterns (e.g., assuming names from certain cultures are more likely to be the same person), it may systematically over-link or under-link records from those groups. | Audit LLM decisions by demographic segment. Compare acceptance rates across name origins, geographies, and source systems. |

### 7.10 Updated Curation Spectrum

With LLM-assisted curation, the spectrum from §7.3 expands:

```
← Less curation                                                         More curation →

No review       Blocklist    Auto-rules    LLM-assisted     LLM + human     Human review
(fully          only         + blocklist   (auto-accept     (LLM pre-       of ALL links
 automated)                                above threshold) processes,       (full manual)
                                                            human confirms
                                                            uncertain)
```

**Updated recommendation for this system:** Blocklist + auto-curation rules + LLM-assisted curation with human review of LLM-uncertain cases. This achieves the quality of human curation at the throughput of automation for the ~85% of cases where the LLM is confident.

---

## 8. Pros and Cons of Human Curation

### 8.1 Advantages

| Advantage | Detail |
|---|---|
| **Catches false positives** | A human can see that "Alice Smith" in CRM and "Alice Chen" in ERP sharing a phone number is a reassigned phone, not the same person. The algorithm cannot. |
| **Prevents junk value cascades** | Before a high-fanout value merges thousands of records, a human flags it. Prevents the most catastrophic failure mode. |
| **Builds institutional knowledge** | The override table becomes a record of decisions and reasons. New team members understand why specific records are linked or split. |
| **Improves identity rules over time** | Repeated curation of the same error pattern (e.g., shared family emails) signals that the identity rule should be adjusted — remove `email` as identity for certain sources, or add a link group. |
| **Regulatory compliance** | For regulated industries (healthcare, finance), automated cross-referencing of PII may require human sign-off. The curation audit trail satisfies this. |
| **Reduces onboarding risk** | Curation during the gating or shadow window catches linkage errors before writeback executes, reducing the blast radius of system 3 joining. |

### 8.2 Disadvantages

| Disadvantage | Detail | Severity |
|---|---|---|
| **Does not scale** | If system 3 has 100K records and 5% produce uncertain links, that is 5,000 links to review. At 30 seconds per link, that is 42 hours of human review. | **High** — the curation queue must be aggressively pre-filtered by auto-curation rules and the blocklist. |
| **Bottleneck on onboarding speed** | The pipeline cannot go live until the curation queue is drained (or triaged). This adds days or weeks to the onboarding timeline. | **Medium** — mitigated by making auto-accept the default for high-confidence links and only queuing uncertain ones. |
| **Human error** | A reviewer accepting a wrong link is worse than the algorithm proposing it — it is now in the override table as an explicit decision, and future corrections must override the override. | **Medium** — mitigated by requiring two reviewers for `no_link` decisions (which are harder to reverse than `link` decisions). |
| **Stale overrides** | A `no_link` override from 2024 may no longer be correct if the underlying data has changed. The override table needs periodic review. | **Low** — add `expires_at` column; re-queue expired overrides. |
| **Requires UI investment** | Effective curation requires a purpose-built interface: side-by-side record comparison, cluster visualisation, bulk actions. A SQL table is not a usable curation interface for operators. | **Medium** — an MVP can use a spreadsheet-style view over the curation queue table, but a production system needs a proper UI. |
| **Incompatible with real-time pipelines** | If the pipeline runs on a 30-second refresh cycle, there is no natural pause for human review. Curation requires either a batch window or an asynchronous hold (gating). | **Medium** — use delta gating or shadow mode to create the review window. |

### 8.3 The Curation Spectrum

Curation is not binary (all-or-nothing). The right level depends on the organisation:

```
← Less curation                                     More curation →

No review        Blocklist     Auto-curation    LLM-assisted      Human review
(fully           only          rules +          (§7) + human      of ALL links
 automated)                    blocklist        for uncertain     (full manual
                                                                   MDM)
```

**Recommended position for this system:** Blocklist + auto-curation rules + LLM-assisted curation with human review of LLM-uncertain cases (see §7.10).

---

## 9. Integration Design: Where Curation Fits in the Pipeline

### 9.1 Two Possible Architectures

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

### 9.2 Pipeline With Curation

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

### 9.3 Interaction With pg-trickle

The curation check runs as a **post-refresh hook** — after `_id_{target}` is refreshed but before (or concurrently with) downstream refresh. This can be achieved via:

1. **pg-trickle's tiered scheduling:** Set `_resolved_{target}` to a lower-priority tier than `_id_{target}`. After `_id_{target}` refreshes, the curation check runs before `_resolved_{target}` is next in the scheduler queue.
2. **Stream table gating:** Gate downstream tables. After `_id_{target}` refresh, run curation. After curation, ungate downstream.
3. **External orchestration:** The curation process runs outside pg-trickle (e.g., as an in-and-out daemon mode) and calls `refresh_stream_table()` manually for downstream tables after curation is complete.

Option 3 (external orchestration) is simplest and does not add requirements to pg-trickle.

---

## 10. Recommendations

### 10.1 Before Onboarding System 3

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

### 10.2 During Onboarding

6. **Use extended shadow mode with LLM-assisted curation.** This provides:
   - Immediate visibility into linkage consequences (system-3-caused deltas held in shadow)
   - Time for curation (delta tables are held; no writeback executes)
   - Attribution (which changes were caused by system 3 vs independent)
   - A natural curation queue (the shadow comparison tables contain exactly the records that need review)
   - LLM pre-processing of the curation queue (§7) to reduce human review time by ~85%

7. **Review in priority order:**
   - First: `system3_caused` `new_record` rows (cross-system inserts — highest risk)
   - Second: clusters where `clusters_merged > 0` (re-merges — trace to the bridging link)
   - Third: `system3_caused` `field_update` rows (field changes — inspect for reasonableness)
   - Last: `aligned` rows (already correct — skim for sanity, don't block on review)

8. **Run at least two shadow comparison cycles.** The first cycle catches obvious problems. The second cycle after corrections (overrides applied) confirms convergence.

### 10.3 Ongoing (Post-Onboarding)

9. **Monitor cluster metrics.** Use `clusters_merged` / `clusters_split` from `pgt_refresh_history` to detect unexpected linkage changes during steady-state operation.

10. **Periodically audit the override table.** Expired overrides should be re-evaluated. Overrides that are no longer needed (because the underlying data changed) should be removed.

11. **Feedback loop to identity rules.** If the same pattern keeps appearing in the curation queue (e.g., shared family emails), adjust the OSI-mapping YAML — add a link group, change the identity field, or add a normalisation expression. The goal is for the curation queue to shrink over time as the rules improve.

---

## 11. Open Questions

1. **Should OSI-mapping natively support a link override table?** Currently, implementing overrides requires modifying the generated `_id_{target}` view SQL. It would be cleaner if OSI-mapping's YAML supported an `overrides:` section that the engine incorporates automatically. This is a feature request for OSI-mapping, not pg-trickle.

2. **How should `no_link` overrides interact with transitive closure?** If A–B is blocked but A–C and B–C exist, should A and B still end up in the same cluster (via C)? Probably yes — `no_link` means the direct edge is removed, not that the two records can never be in the same cluster. But some operators may expect `no_link` to mean "these are definitely different entities, keep them apart no matter what," which requires a stronger constraint (forced cluster separation).

3. **Should curation apply to all data types or just contacts?** Companies, deals, and other entity types have the same linkage risks but typically lower volumes. The curation mechanism should be generic (per-target, not per-data-type), but the review priority should focus on the entity types with the highest writeback impact.

4. **What is the minimum viable curation UI?** An MVP might be a read-only view of the curation queue table plus a form for submitting override decisions. A production system would need side-by-side record comparison, cluster graph visualisation, bulk accept/reject, and integration with the shadow mode dashboard from PLAN_ONBOARDING_SHADOW_MODE.md §10.11.

5. **How does curation interact with real-time writeback?** In steady state (post-onboarding), should new links always be auto-accepted, or should the curation queue remain active? If active, there must be a mechanism to hold writeback for clusters with pending curation decisions — essentially permanent shadow mode for uncertain links.

6. **Can probabilistic scoring supplement deterministic linkage without replacing it?** A hybrid model: deterministic matching for the `_id_{target}` view (fast, SQL-native), supplemented by a Python/external scoring step that evaluates uncertain edges and auto-accepts or queues them. This keeps the pg-trickle pipeline simple while adding sophistication where needed.

7. **What is the right LLM deployment model for curation?** Self-hosted models avoid PII concerns but require GPU infrastructure. API-based models (with DPAs) are simpler to deploy but add latency and cost. For onboarding (batch, non-real-time), API-based is likely acceptable. For steady-state continuous curation, self-hosted may be required for cost and latency reasons.

8. **Should LLM curation decisions be treated as first-class overrides or as soft suggestions?** If LLM decisions go directly to the override table, they have the same authority as human decisions and persist until explicitly removed. If they are treated as suggestions, they need human confirmation but reduce the cognitive load per decision. The choice depends on the organisation's risk tolerance and the LLM's calibrated accuracy.
