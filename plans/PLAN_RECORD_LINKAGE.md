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

### 4.26 Identity Field and Conflict Resolution Contradiction

The same field is often both an identity field (used to create edges for matching) and a resolved field (its final value in the golden record is determined by conflict resolution). These are independent computations, but their outputs can contradict each other.

Consider:
```
System 1: email = alice@co.com     (priority 1 — high)
System 2: email = alice@corp.com   (priority 2 — medium)
System 3: email = alice@co.com     (priority 3 — low)

Identity resolution: system 1 and system 3 share alice@co.com → same cluster ✓
Conflict resolution: system 1 wins (highest priority) → golden record email = alice@co.com ✓

Now change system 1's priority:
System 2 is re-ranked to priority 1 (system 2 has better data quality overall)

Identity resolution: unchanged — system 1 and system 3 still share alice@co.com
Conflict resolution: system 2 wins → golden record email = alice@corp.com

Result: The cluster was formed because system 1 and system 3 share alice@co.com.
        But the golden record's email is now alice@corp.com — neither of the values
        that justified the link.
        Reverse mapping writes alice@corp.com back to system 1 and system 3.
        On the next ingestion cycle, system 1 has alice@corp.com; system 3 has alice@corp.com.
        The original linking edge (alice@co.com) is gone.
        Whether the cluster holds depends on whether any other identity field still connects them.
```

**The edge that justified the cluster can be overwritten by conflict resolution in the same pipeline refresh cycle.** If the overwritten value was the only basis for the link, the cluster will split on the next ingestion cycle after writeback executes — a writeback-induced instability (related to §4.10) caused specifically by the interaction between identity matching and conflict resolution winning values.

**Mitigation:** Identity fields used for matching should have either `strategy: identity` (meaning their value is locked — they are never overwritten by conflict resolution) or their conflict resolution winner should be constrained to values that preserve the linking invariant. In OSI-mapping, `strategy: identity` fields already behave this way to some degree, but the interaction between identity matching and conflict resolution on the same field needs explicit documentation and testing.

### 4.27 Intra-Source Deduplication as a Prerequisite

Cross-system identity resolution assumes each source system is internally consistent — no two records within the same source share the same identity field value. If they do, both records participate in matching and both create edges from the same source into the identity graph. The connected-components algorithm treats them as valid, distinct contributors.

**Concrete problem:**
```
System 3 has two records with email = alice@co.com:
  ERP-9001: Alice Smith,  email=alice@co.com,  created 2020
  ERP-9002: Alice Jones,  email=alice@co.com,  created 2024  ← data entry error

System 1 has one record:
  C-1042:   Alice Smith,  email=alice@co.com

Identity resolution creates edges:
  ERP-9001 ↔ C-1042  (email match)
  ERP-9002 ↔ C-1042  (email match)
  ERP-9001 ↔ ERP-9002 (email match — both in system 3)

Cluster: {ERP-9001, ERP-9002, C-1042}

Conflict resolution must pick a winner for the golden record.
Which of ERP-9001 and ERP-9002 is the "real" Alice from system 3?
Typically: whichever was most recently modified, or whichever has higher priority row selection.
But: the loser (say ERP-9002, Alice Jones) receives a delete delta — the pipeline
     is now orchestrating a deletion within system 3 it was not asked to perform.
```

Both the cluster composition and the conflict resolution outcome are wrong because the cross-system identity resolution silently absorbed an intra-source duplicate that should have been resolved first.

**Intra-source deduplication must run before cross-source identity resolution.** This is not just a data quality recommendation — it is a logical prerequisite for correct pipeline behaviour. Specifically:

1. **Pre-ingestion:** Source system should be clean before onboarding. The data quality audit (§10.1 recommendation 1) should include a scan for intra-source identity field duplicates. Any found should be resolved in the source system before ingestion begins.
2. **Ingestion-time validation:** The ingestion daemon should alert if identity fields have duplicate values within a single sync run. Not a hard block (the source data cannot be changed by in-and-out), but a loud warning.
3. **Post-ingestion monitoring:** Query `inout_src_system3_contacts` for duplicate identity field values after each sync run. A rising count signals a data quality regression in the source system.

```sql
-- Detect intra-source duplicates on identity field
SELECT email, COUNT(*) AS cnt
FROM inout_src_system3_contacts
WHERE email IS NOT NULL
GROUP BY email
HAVING COUNT(*) > 1
ORDER BY cnt DESC;
```

### 4.28 New Identity Field Mid-Lifecycle (Configuration Bootstrap Hazard)

Adding a new identity field to an existing, stable pipeline is operationally equivalent to the bootstrap problem (§4.20) — but it is far more dangerous because the operator is less likely to be on guard.

Adding a third identity field (e.g., `phone`) to a pipeline that has been running stably with `email` and `tax_id` for two years feels like a small configuration change. In reality, it is a full re-evaluation of the identity graph against a new dimension. Every record is reconsidered for links on the new field. Junk values in `phone` that would have been caught during an onboarding review are now introduced silently into a production pipeline.

**The hazard sequence:**
```
Before change: 12,450 clusters, steady state, no issues

YAML change: add phone: identity

After ALTER QUERY + next pg-trickle refresh:
  phone = "000-000-0000" appears in 340 records across systems 1, 2, and 3
  → 340 records merge into one cluster
  → previously stable clusters for 340 records split and re-merge
  → delta tables emit changes for all 340 records across all systems
  → writeback begins

Time from config change to writeback: minutes (one pg-trickle refresh cycle)
Time operator had to review: zero
```

Unlike onboarding a new source system — where the gating workflow (PLAN_ONBOARDING_PROPOSAL.md) provides a structured review window — adding a new identity field has no equivalent protective workflow today. The `ALTER QUERY` mechanism updates the `_id_{target}` stream table immediately; there is no pending query (§3 Gap 3 in PLAN_ONBOARDING_PROPOSAL.md) to stage the change.

**Required treatment:** Any change to the identity field declarations in the OSI-mapping YAML must be treated as a bootstrap event and follow the same preparation protocol as §4.20:

1. Run the precision/recall evaluation (§4.22) for the new field against a labelled sample before deploying.
2. Audit the new field for junk values (§3.3, §6.5) and populate the blocklist before the field goes live.
3. Use Mode B gating (all delta tables gated) during the first refresh with the new field active.
4. Review cluster change metrics (`clusters_merged`) in `pgt_refresh_history` before ungating.
5. Only then ungate and allow writeback.

The `pending_query` mechanism proposed in PLAN_ONBOARDING_PROPOSAL.md §3 Gap 3 would directly support this workflow — storing the updated query without activating it until the operator explicitly approves.

### 4.29 Source System Internal Merge Breaking external_id References

Source systems like Salesforce, HubSpot, and most CRMs have their own internal deduplication. When a user merges two contacts inside HubSpot, one `external_id` is declared the winner and the other is permanently deleted — typically with a redirect or a merge record in the source system's own audit log. In-and-out previously ingested both records.

**What the pipeline sees:**
```
Before HubSpot internal merge:
  inout_src_system3_contacts:
    ERP-9001: Alice Smith  (ingested 2026-01-15)
    ERP-9002: Alice Jones  (ingested 2026-01-15)

HubSpot user merges ERP-9002 into ERP-9001 on 2026-03-20.
ERP-9002 is now a deleted/redirected record in HubSpot.

Next ingestion cycle (2026-03-21):
  in-and-out fetches ERP-9002 → 404 Not Found (or 301 redirect to ERP-9001)
  Ingestion daemon interprets 404 as: this record was deleted
  → inout_src_system3_contacts: ERP-9002 marked deleted / removed
  → identity resolution re-runs without ERP-9002's edges
  → delta tables may emit delete deltas for any records whose cluster membership
     depended on ERP-9002's identity fields
```

If ERP-9002 was the bridging record between a system 3 cluster and systems 1 and 2, its removal causes the entire cluster to split — not because the entity was deleted, but because the source system merged two records and kept one. The surviving record (ERP-9001) contains ERP-9002's data, but in-and-out doesn't know that — it just sees ERP-9001 as unchanged and ERP-9002 as gone.

**Mitigations:**
- **Detect redirects, not just 404s:** When an API returns a 301/redirect or a merge reference for a previously-ingested `external_id`, treat it as a merge event, not a deletion. Update `inout_src_*` to associate ERP-9002's historical edges with ERP-9001.
- **Merge event webhook:** Some source systems (Salesforce) emit webhook events for internal merges. The ingestion daemon should subscribe to these and handle them as first-class events — not as deletions discovered indirectly during polling.
- **Soft-delete tolerance:** Before treating a 404 as a hard delete, check whether the record appears under a different `external_id` via identity field lookup. If a record with the same email now exists under a new ID, it was merged, not deleted.

### 4.30 Legitimate One-to-Many Identity Field Sharing

The blocklist (§6.5) handles **meaningless** identity values — placeholders that should never create edges. But some identity values are **meaningful and correctly shared** by multiple distinct real-world entities:

| Shared value | Reason for sharing | Entities | Should create edges? |
|---|---|---|---|
| `+1-212-555-1000` | Goldman Sachs main switchboard | Thousands of employees | **No** — different people |
| `123 Main St` | Large office building | Dozens of companies | **No** — different organisations |
| `legal@lawfirm.com` | Legal representative for multiple companies | All client companies | **No** — different legal entities |
| `alice@family.com` | Family shared email | Alice Smith and Bob Smith | **No** — different people, different entities |
| `hr@company.com` | Shared HR inbox | Multiple employees who use it | **No** — different people |

These are not junk values. Blocking them globally removes legitimate information from the pipeline — `+1-212-555-1000` is the verified phone number of Goldman Sachs and should be stored as a field value. The problem is specifically using it as an identity **edge-creation trigger**, not as a data value.

**The distinction from junk values:**
- **Junk values** (§3.3): Meaningless, never correct as identity signals. Block globally from edge creation.
- **Legitimate shared values:** Meaningful, correct as field values, but not valid as identity signals for the specific records that have them. The block must be per-record, not per-value.

**Current gap:** The blocklist blocks a value across the entire pipeline. There is no mechanism to say "this record's phone number happens to be a shared switchboard — do not use it for identity matching, but do store it as the phone field value."

**Possible approaches:**

1. **Per-record identity field exclusion flag:** Add a metadata column to source tables — `_identity_exclude_fields TEXT[]` — populated by the connector based on API metadata (some CRMs expose a "shared number" flag). The forward view uses this to null out excluded fields before identity resolution sees them.

2. **Identity field value frequency threshold (dynamic, not static blocklist):** Instead of a fixed blocklist, compute the frequency of each identity field value across all sources during the pre-bootstrap data quality audit. Values appearing in more than 0.1% of all records are automatically excluded from edge creation (but not from field storage). This is a dynamic, per-deployment blocklist rather than a global one.

3. **Source-specific value blocking:** Extend the blocklist table with an optional `source` column. `+1-212-555-1000` is only blocked for sources where it appears in more than 10 records — a Goldman Sachs CRM export would trigger this; a healthcare provider's database wouldn't.

```sql
ALTER TABLE inout_identity_blocklist
ADD COLUMN source TEXT,  -- NULL = block for all sources
ADD COLUMN min_occurrence_count INT DEFAULT 1;
-- When source IS NOT NULL, only block for that source's records
-- When min_occurrence_count > 1, auto-compute from data rather than
-- requiring manual entry
```

### 4.31 Record Resurrection (Delete + Re-Create With New ID)

A record deleted from system 3 and later re-created in the same organisation gets a new `external_id`. From the pipeline's perspective, the old and new records are entirely unrelated entities.

**The sequence:**
```
2025-01-10: ERP-9001 (Alice Smith, alice@co.com) ingested into pipeline.
            Cluster formed: {C-1042 (sys1), ERP-9001 (sys3)}.

2025-06-01: ERP-9001 deleted in system 3 (Alice left the company).
            Ingestion marks ERP-9001 as deleted.
            Cluster {C-1042, ERP-9001} loses its system 3 member.
            _delta_system1 emits noop (C-1042 still exists, unchanged by the split).

2026-01-15: Alice returns. System 3 creates ERP-7712 (Alice Smith, alice@co.com).
            Ingestion creates new row for ERP-7712.
            Identity resolution: ERP-7712 ↔ C-1042 via email → cluster re-merges. ✓ (correct)

BUT:
            Between June 2025 and January 2026, did _delta_system1 emit an insert
            for a "new" Alice (from cluster containing only C-1042)?  Depends on
            whether the golden record changed or a noop was emitted.

            If a different system (system 2) had an entry for Alice under a third
            external_id, and that entry was used to push data toward system 1 and
            system 3 during the gap — on re-create, ERP-7712 may receive a writeback
            immediately (correct) but with field values from the gap period that
            may differ from what system 3 stored when Alice originally left.
```

The deeper problem arises when the resurrection happens with a *different* email or phone — the new record does not re-link to the old cluster:

```
ERP-9001 (alice@co.com) deleted.
ERP-7712 (a.smith@co.com) created — new email format after IT policy change.

Identity resolution: ERP-7712 ≠ C-1042 (different email, no other shared fields)
Result:
  Cluster 1: {C-1042} — system 1 now has Alice as a singleton
  Cluster 2: {ERP-7712} — system 3's resurrection is a new singleton

_delta_system1: insert Alice (from cluster 2's golden record) into system 1
→ system 1 now has TWO Alice records: C-1042 (original) and a new duplicate.
```

**This is an under-link caused by identity field change at the source, not by a bad identity rule.** It is not detectable by the pipeline without additional context about the source's internal identity continuity.

**Mitigations:**
- **Resurrection detection via identity field lookup:** When a new record is ingested with identity fields that closely match a recently-deleted record (same name + different email, or same phone + different name), flag it as a possible resurrection and route to curation queue.
- **Source system resurrection signals:** Some APIs include a `merged_from` or `previous_id` field when a record is recreated. The ingestion daemon should consume this as a forced link override — ERP-7712 is the same entity as ERP-9001.
- **Re-ingestion window:** When a record is deleted, do not immediately remove it from the identity graph. Keep a ghost row (marked deleted) for a configurable window (e.g., 90 days). If a record with similar identity fields appears within the window, treat it as a resurrection candidate before treating the deletion as final.

### 4.32 N-System Scaling Hazard

The document is framed around a 3-system pipeline. The hazard magnitude grows with each additional system onboarded.

**Why N matters:**

When system 2 was added to a system 1 pipeline, it could only create edges to system 1's clusters. When system 3 was added, it could create edges to clusters in both systems 1 and 2. When system 4 is added, it can create edges to clusters spanning all three existing systems — including clusters that were formed by previous onboardings and whose correctness may have been validated months ago.

| Systems onboarded | Max clusters a new system can touch | Bootstrap re-merge blast radius |
|---|---|---|
| Adding system 2 | Clusters from system 1 only | Low |
| Adding system 3 | Clusters across systems 1–2 | Medium (documented in PLAN_ONBOARDING_*.md) |
| Adding system 4 | Clusters across systems 1–3, including merged clusters | **Higher** — bridges clusters that were formed by prior onboardings |
| Adding system N | Clusters across all N-1 systems | **Compounds with each prior onboarding** |

Each prior onboarding produced cluster merges. Those merged clusters are now larger than the original singleton clusters. A single false-positive edge from system N can merge two previously-validated multi-system clusters — creating a cascade that undoes previously reviewed identity resolution decisions.

**Specific risks that grow with N:**
- **Re-merge blast radius:** A false positive in system N bridges two clusters that each span N-1 systems. The resulting merged cluster spans N systems. Writeback fires to all N systems.
- **Cluster validation becomes harder:** After 5 onboardings, each cluster may have records from 5 systems. Verifying that a cluster is correct requires checking all 5 systems' records. This does not scale to manual review.
- **Junk value super-clusters:** A junk value (e.g., a generic phone number) that was present in 3 systems and blocked before onboarding may reappear in system N under a different representation. The blocklist entry for the normalised value may not cover the new variant. The junk value bridges all N systems in one refresh.
- **Performance degrades:** The connected-components algorithm's worst-case performance worsens as the graph becomes denser with each new system. A junk value in system N that links 1,000 records from each of the N systems creates a clique of N×1,000 records — the recursion depth and join cost explode.

**Recommended scaling practice:**
- The pre-onboarding precision/recall evaluation (§4.22) becomes *more* important, not less, as N grows. Each onboarding risks more than the previous one.
- Cluster size limits (§4.4) should be tightened as N grows. A cluster of 20 records is suspicious with 3 systems; it may still be legitimate with 5 systems if each contributes 4 records. Recalibrate limits after each onboarding.
- The progressive identity field rollout (§4.25) becomes mandatory for high-N pipelines — adding all identity fields at once when joining system 5 creates O(N×fields) new edge evaluation combinations.

### 4.33 Confidence Not Exposed in Golden Record Output

A cluster formed via a validated tax_id match (high confidence) and a cluster formed via a name+city match (low confidence) produce identical `contact` view output. Downstream consumers cannot distinguish them.

This matters for:

- **BI dashboards:** A report built on the `contact` golden record treats all records as equally trustworthy. Low-confidence merges silently distort aggregate metrics (e.g., revenue per customer counts a cluster of 4 loosely-linked records as one customer).
- **ML models:** A model trained on golden records learns from both high- and low-confidence identity decisions. Low-confidence merges introduce label noise. The model cannot weight records by identity confidence.
- **Downstream integrations:** A system 4 that reads from the golden record to populate its own database has no way to limit ingestion to high-confidence entities.
- **Audit and compliance:** An auditor reviewing why system 2 received an update for a given record cannot determine from the golden record how confident the pipeline was that the update was correct.

**Proposed addition to golden record output:**

```sql
-- Additional columns on the {target} / _resolved_{target} view:
_link_confidence     TEXT,    -- 'high' / 'medium' / 'low' / 'manual'
_link_basis          TEXT[],  -- identity fields that formed the cluster
                              -- e.g., ARRAY['tax_id', 'email']
_cluster_size        INT,     -- number of source records in this cluster
_cluster_source_count INT,    -- number of distinct source systems in cluster
_has_pending_curation BOOL,   -- true if any edge in this cluster is in
                              -- the curation queue or has an override
```

`_link_confidence` derivation:
- `manual`: cluster was formed or modified by a human override in `inout_link_overrides`
- `high`: all links formed via strong unique identifiers (tax_id, SSN, DUNS)
- `medium`: links formed via email or composite key (name+DOB)
- `low`: links formed via weak fields (phone, name alone) or transitively through medium/low edges

These columns are computable from `_id_{target}` metadata without changes to the OSI-mapping engine — they can be added to the bridge layer or as a wrapper view over the generated `{target}` view.

### 4.34 Third-Party Data Enrichment as Identity Field Source

When identity field values are populated by external enrichment services (Clearbit, ZoomInfo, Apollo, etc.) rather than from the authoritative source systems, those values create identity edges not grounded in actual source data.

**The problem:**

```
System 1 (CRM): contact C-1042 — Alice Smith, email=alice@co.com
Enrichment (ZoomInfo): adds phone=+1-555-1234 to C-1042 in the CRM

System 3 (ERP): contact ERP-9001 — Bob Jones, phone=+1-555-1234
                (Bob uses the same number as Alice — office landline)

Identity resolution: C-1042 and ERP-9001 share phone → same cluster
Golden record: Alice Smith merged with Bob Jones

ZoomInfo was wrong about Alice's phone (or used a shared office number).
The enrichment error propagated directly into cluster composition.
```

The enrichment-sourced value is indistinguishable from a system-originated value in the identity resolution input. The pipeline treats them identically — an edge is an edge, regardless of how the matching field value got there.

**Additional risks:**
- **Enrichment staleness:** ZoomInfo updates its database on its own schedule. A phone enriched in 2022 may be stale by 2026. Stale enrichment values that no longer match reality create edges that should not exist, and make identity oscillation (§4.5) more likely as enrichment values change on enrichment refresh cycles.
- **Enrichment coverage gaps:** Enrichment services often have better coverage for some geographies, industries, or company sizes than others. Uneven coverage means identity resolution quality is uneven in ways that are invisible from the pipeline's perspective.
- **Circular enrichment:** The pipeline writes to system 3 via writeback. System 3's enrichment service reads system 3's data. The enrichment service adds enriched values back to the pipeline via ingestion. The pipeline now has enriched values derived from its own writeback outputs as identity evidence. The enrichment is an echo, not independent data.

**Mitigations:**
- **Track field provenance:** Add a `_field_source` metadata column to forward views (or to `inout_src_*` tables). Record whether each field value came from the authoritative source API or from an enrichment overlay. The `_id_{target}` view can be configured to use only source-originated values for identity matching, even if enriched values are stored for display.
- **Treat enriched identity fields as lower trust:** Enrichment-sourced identity fields should be limited to supplementary matching — they can break ties or increase confidence in an existing link, but they should not independently create new cluster edges. Implement as a lower-priority identity tier that only fires when the primary (source-originated) identity fields have not already linked the records.
- **Enrichment field separation:** Store enriched values in separate columns (`email_enriched`, `phone_enriched`) rather than overwriting the source-originated columns. Identity resolution uses only the authoritative columns; downstream consumers choose which to display.

### 4.35 Identity Regression Testing

§4.22 covers precision/recall evaluation before onboarding. Once the pipeline is live, there is no equivalent protection for ongoing changes to the OSI-mapping YAML — adding a new identity field, changing conflict resolution priority, updating a normalisation expression, or adjusting link groups.

Each of these changes silently re-evaluates the identity graph. Previously-validated clusters may split. Previously-separate clusters may merge. The operator has no mechanism today to detect that a YAML change altered cluster assignments in unexpected ways.

**What is needed:** An identity regression test suite that:

1. **Snapshots cluster assignments** before a YAML change: for each `(source, external_id)` pair, record the `_cluster_id` it resolves to.
2. **Re-evaluates** after the YAML change.
3. **Diffs the snapshots**: which records changed cluster? Which clusters split, merged, or were newly created?
4. **Classifies the diff** against previously curated decisions: did any known-correct link disappear? Did any known-incorrect link reappear?

```sql
-- Snapshot cluster state before YAML change
CREATE TABLE inout_cluster_snapshot_before AS
SELECT source, external_id, _cluster_id, snapshot_ts = NOW()
FROM _id_contact;

-- After deploying YAML change, compare:
SELECT
    b.source,
    b.external_id,
    b._cluster_id AS cluster_before,
    a._cluster_id AS cluster_after,
    CASE
        WHEN b._cluster_id = a._cluster_id THEN 'unchanged'
        WHEN b._cluster_id != a._cluster_id THEN 'reclassified'
        WHEN a._cluster_id IS NULL           THEN 'lost'
    END AS change_type
FROM inout_cluster_snapshot_before b
FULL OUTER JOIN _id_contact a
    USING (source, external_id)
WHERE b._cluster_id IS DISTINCT FROM a._cluster_id;
```

**Relationship to the override table:** Any record whose cluster assignment changed and which has a corresponding entry in `inout_link_overrides` should be flagged immediately — a YAML change invalidated a previous manual curation decision. The override must be re-evaluated against the new cluster state.

**This regression test should be a mandatory gate on all OSI-mapping YAML deployments**, just as schema migrations require review before applying to production. The cluster diff report is the output of this gate.

### 4.36 Hierarchical Entity Identity

OSI-mapping clusters entities using connected-components: records are either in the same cluster (same entity) or in different clusters (different entities). There is no intermediate state: "related but distinct."

Real-world entity hierarchies violate this binary. A global headquarters and its regional subsidiary are distinct legal entities that must remain separate in the pipeline — but they share many identity signals:

| Signal | Shared? | Reason |
|---|---|---|
| Website domain | Often yes | `siemens.com` for both Siemens AG and Siemens USA |
| Company name prefix | Yes | "Siemens AG" and "Siemens USA Inc." |
| Parent tax ID | Sometimes | Consolidated tax filers |
| Office address | Partially | Regional offices in the same building |
| Switchboard phone | Often | Central number used by all subsidiaries |

If these shared signals are declared as identity fields, the parent and all subsidiaries merge into one cluster. All subsidiaries' contacts, deals, and orders appear under one golden record entity. FK resolution produces incorrect cross-subsidiary references.

If these signals are excluded from identity fields, genuine duplicates that should be linked (two separate CRM entries for Siemens AG) are missed.

**The correct concept is an entity relationship table**, distinct from cluster membership:

```sql
CREATE TABLE inout_entity_relationships (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cluster_id_a    TEXT NOT NULL,   -- parent cluster
    cluster_id_b    TEXT NOT NULL,   -- child/related cluster
    relationship    TEXT NOT NULL,   -- 'parent_of' / 'subsidiary_of' /
                                     -- 'acquired_by' / 'formerly_known_as'
    valid_from      DATE,
    valid_to        DATE,
    source          TEXT,            -- 'system3' / 'manual' / 'enrichment'
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Two clusters can be related without being merged. The FK resolution layer can use relationships to correctly route contacts: a contact that belongs to Siemens USA in the CRM should not receive Siemens AG's `company_id` during writeback, even though the two clusters are related.

**This is a gap in OSI-mapping's current model.** The only current mechanisms are: merge (one cluster) or separate (two unrelated clusters). An `entity_relationship` construct is a meaningful extension that would reduce both false merges (from over-linking subsidiaries) and missed FK resolution (from under-linking related entities).

### 4.37 Cross-Jurisdiction Regulatory Identifier Collision

`tax_id` and similar regulatory identifiers exist within jurisdiction namespaces. Two organisations with the same identifier string in different jurisdictions are unrelated. Without namespace qualification, identity resolution treats them as the same entity.

**Real collision examples:**

| System 1 source | System 3 source | Identifier | Same entity? |
|---|---|---|---|
| UK Company number `12345678` | US EIN normalised to `12345678` | `tax_id = "12345678"` | No — different registries |
| French SIRET `12345678901234` (14 digits) | German VAT `DE12345678` truncated to digits `12345678` | `tax_id = "12345678"` | No — different formats |
| US SSN `123-45-6789` normalised to `123456789` | Colombian NIT `123456789` | `tax_id = "123456789"` | No — different countries |

The normalisation expressions that strip punctuation and whitespace (designed to handle formatting differences within a jurisdiction) make cross-jurisdiction collisions *more* likely by removing the format cues that distinguish identifier types.

**Required: jurisdiction-qualified identity fields.**

The correct identity field for a company is not `tax_id` but `(jurisdiction, tax_id)` — a composite that uniquely identifies an entity within its registry. In OSI-mapping YAML terms:

```yaml
fields:
  tax_id_jurisdiction:
    strategy: identity
    link_group: tax_key
  tax_id_value:
    strategy: identity
    link_group: tax_key
```

This requires `tax_id_jurisdiction` to be populated correctly — which in turn requires the ingestion connector to extract or infer the jurisdiction from each source record. For sources that mix jurisdictions without a jurisdiction field, the forward view must derive it from the record's other fields (country, address, identifier format).

**Implication for normalisation:** Normalisation expressions must be jurisdiction-aware. A UK company number is 8 digits; zero-padding is expected. A US EIN is 9 digits. Applying the same `regexp_replace` normalisation to both destroys the format information that would have prevented the collision.

### 4.38 Derived Source Data as False Corroboration

When system 3 was originally populated by exporting data from system 1 — a common scenario in legacy migrations, system consolidations, and "we copied the spreadsheet" onboardings — systems 1 and 3 share identity field values not because they independently observed the same entity, but because one was a copy of the other.

**Why this matters for identity resolution:**

Identity resolution implicitly assumes that a match between two sources is **independent corroboration** — two separate systems agreeing on a value is stronger evidence than one system alone. This assumption underpins the chain-of-confidence logic: if three sources all have `email = alice@co.com`, that is strong evidence.

But if system 3 got `alice@co.com` from system 1 via a data export, there is only **one independent observation** of that email. The apparent three-source agreement is actually one source observed once and then copied twice. Any data quality error in system 1's original record is inherited verbatim by system 3 — and the identity resolution sees the error corroborated by both systems.

**Concrete consequence:**
```
System 1: alice@co.com (original — correct)
System 3: alice@co.com (copied from system 1 in 2022 export)
System 2: alice@co.com (independent observation — correct)

Apparent evidence: 3-way agreement on email = very high confidence link
Actual evidence: 2-way agreement (system 1 original + system 2 independent)
                 System 3 adds no information — it is an echo of system 1.

Now: System 1 corrects Alice's email to a.smith@co.com.
     System 3 still has alice@co.com (not yet updated from the pipeline).
     System 2 still has alice@co.com.
     
Identity resolution: system 3 ↔ system 2 edge still active via alice@co.com.
System 1's record leaves that cluster.
System 3's stale copy maintains a link that no longer reflects any independent source.
```

**Mitigations:**
- **Document source lineage at onboarding:** During the data quality audit (§10.1 recommendation 1), explicitly identify whether system 3 is an independent data source or derived from another system in the pipeline. If derived, note which fields were copied and from which source.
- **Treat derived fields with lower identity trust:** Fields known to be copied from another pipeline source should be declared with lower identity weight (or excluded from identity matching entirely) until they have been independently updated in the source system. This can be tracked via `_field_source` provenance (§4.34).
- **Time-based corroboration decay:** A match between system 1 and system 3 where system 3's value has not changed since the original export should be treated as weaker evidence than a match where both systems have updated the value independently. The `base` column timestamps provide a proxy for this.

### 4.39 Non-Latin Script and Transliteration Inconsistency

The normalisation section (§4.2) covers `lower()`, `trim()`, and phone formatting. These are effective for ASCII-range data. They are ineffective for identity fields in non-Latin scripts.

**The problem:**

The same entity's name may appear in different scripts or transliterations across source systems, all representing the same string but matching on zero identity field values:

| System | Name value | Script |
|---|---|---|
| System 1 (EU CRM) | `Müller` | Latin with umlaut |
| System 2 (US CRM) | `Mueller` | ASCII transliteration (German convention) |
| System 3 (Internal) | `Muller` | Simplified (umlaut dropped) |

```
lower('Müller')  = 'müller'
lower('Mueller') = 'mueller'
lower('Muller')  = 'muller'

All three are different strings. Identity resolution: three separate singletons.
```

For non-Latin scripts, the problem is more severe:

| System | Name value | Issue |
|---|---|---|
| System 1 | `北京市朝阳区` | Chinese characters (address) |
| System 2 | `Beijing Chaoyang` | Pinyin transliteration |
| System 3 | `Peking, Chaoyang District` | Historical/Western romanisation |

These three represent the same address. Standard normalisation cannot bridge them. No SQL `WHERE` clause will produce a match.

**Why this matters for this pipeline:** Any deployment handling data from China, Japan, Korea, the Middle East, Eastern Europe, or any non-ASCII-first market will have this problem. It is a systematic false-negative generator for international identity resolution.

**Mitigations:**

1. **Unicode normalisation (NFC/NFD):** PostgreSQL's `normalize()` function handles composed vs decomposed Unicode forms. `Müller` (single `ü` character) and `Müller` (u + combining umlaut) are the same string after NFC normalisation, even if they differ at the byte level. This should be in every normalisation expression:
   ```sql
   normalize(lower(trim(name)), NFC)
   ```

2. **Transliteration at ingest time:** The connector YAML or forward view applies transliteration before storing identity field values. Chinese names are stored in both their original script and a Pinyin equivalent. Arabic names are stored in both Arabic script and a romanised form. Identity matching uses the romanised form; display uses the original.

3. **LLM-assisted matching for cross-script candidates (§7.7):** After deterministic matching runs (and produces singletons for non-Latin records), the embedding-based under-link detection pipeline (§4.19, §7.7) uses semantic similarity to surface candidates. LLMs handle cross-script and cross-transliteration name matching well — "北京" and "Beijing" are close in embedding space regardless of script.

4. **Per-source character encoding audit pre-onboarding:** Before ingesting system 3, run a character distribution analysis on identity fields. If a significant fraction of values contain non-ASCII characters, the normalisation strategy must account for them before the bootstrap refresh. Discovering transliteration inconsistencies after the bootstrap is much more expensive to fix.

---

### 4.40 High-Frequency Non-Discriminating Identity Values (Token Pollution)

Some values that legitimately appear in identity fields carry near-zero discriminating power because they are genuinely shared by many distinct entities:

- Catch-all email addresses: `info@company.com`, `billing@company.com`, `noreply@company.com`
- Shared office main phone: the main switchboard number for a large organisation
- Generic addresses: `1600 Amphitheatre Pkwy` — valid, but thousands of employees share it
- Tax IDs of a parent holding company attributed to all subsidiaries

When such a value appears in a link group, it becomes a hub. The `WITH RECURSIVE` query forms a star cluster of potentially thousands of records that are false positives. The values are syntactically valid, pass normalisation and format checks, and no individual record is wrong. The problem exists only at the distribution level.

**Detection:**

```sql
-- Flag any email value appearing in more than N records (run per identity field, per source)
SELECT email, count(*) AS record_count
FROM inout_src_hubspot_contacts
WHERE email IS NOT NULL
GROUP BY email
HAVING count(*) > 5
ORDER BY record_count DESC;
```

**Mitigations:**

- **Frequency-based blocklist:** Any identity field value appearing in more than a threshold fraction of records (e.g. >0.1%) should be excluded from link group matching. Persist in `identity_value_blocklist(field, value, reason, added_at)`. The blocklist must be reviewed at each connector onboarding.
- **Cardinality monitoring:** Alert when any single identity field value clusters more than 50 records.
- **Connector YAML hint:** A `require_selective: true` flag triggers per-field distribution checks at ingest.
- **Pre-bootstrap gate:** Run distribution analysis on all identity fields before ungating system 3. Any value used by more than 10 records must be reviewed before opening the main gate.

---

### 4.41 The Per-Field Independence Assumption (Single-Signal Cluster Formation)

OSI-mapping evaluates each link group independently: if two records share email → they are linked; if they share phone → they are linked. Transitivity then merges everything those links touch. There is no mechanism to require that at least N of M fields must agree before a link is asserted.

**The trap in action:**

```
Record A: email=x,  phone=null,  company=Acme
Record B: email=x,  phone=555,   company=null
Record C: email=null, phone=555, company=Widgets

A–B link: email match (valid)
B–C link: phone match (valid)
A–C: no shared signal, but transitively in the same cluster
```

A and C are now in the same cluster even though they share nothing. A is pulled into C's identity solely via a two-hop transit through B. Each individual link is justified; the resulting cluster is not backed by direct evidence between its most distant members.

**Why this compounds over time:** Every additional link group added to the OSI-mapping YAML increases graph connectivity — not just the precision of each individual link. A system with five link groups can form clusters through four-hop transitive chains where the end records share no signals whatsoever.

**Mitigations:**

- When designing link groups in OSI-mapping YAML, prefer fewer, higher-quality fields over many fields.
- Compute a "direct corroboration count" for each cluster pair: how many distinct identity fields are shared between every pair of records in the cluster. Expose this in the monitoring dashboard.
- Set human curation review thresholds for clusters whose member pairs have zero direct corroboration (linked solely by transitive intermediaries).
- Before adding a new link group, simulate the resulting cluster size distribution in a staging environment — the expected cluster size impact is not linear.

---

### 4.42 Surrogate Key Leakage into Identity Fields

A common connector-authoring mistake is declaring internal surrogate keys as cross-system identity fields:

```yaml
# WRONG: these are system-internal keys with no semantic meaning across systems
identity_fields:
  - salesforce_id       # Only meaningful inside Salesforce
  - internal_row_id     # Sequential integer assigned independently by each system
  - record_guid         # UUID generated locally, not shared
```

**The problem has two failure modes:**

**Mode 1 — Zero links (usually):** Because each system generates its own surrogate keys independently, values never match. No cross-system links are produced. The field pollutes the link group count and adds compute overhead with no benefit.

**Mode 2 — Catastrophic false positives (when namespaces collide):** If two systems use sequential integers, record `id=4891` in CRM A links to record `id=4891` in CRM B — completely wrong. This is particularly dangerous for Contacts tables where both systems start at ID 1 and increment. In a 50,000-record dataset, every record in A will merge with a record in B, producing 50,000 false-positive clusters.

UUID v1 (time-based) carries a non-negligible collision probability across systems running simultaneously on similar hardware. UUID v4 is safe in practice but the authoring error should still be caught.

**Mitigations:**

- **Connector review checklist:** "Is this identity field semantically meaningful to an external observer who has no access to the source system? If not, it is a surrogate key and must not be declared as an identity field."
- **CI validation:** An identity field with cardinality equal to the total record count is definitionally a surrogate key. Assert in the validation pipeline that identity fields used for cross-system linking have duplicate values across at least one source (i.e. they are non-unique in at least one context — meaning multiple records may share the same email, etc.).
- **Schema annotation:** Add `internal_key: true` to connector YAML fields that are system-internal references. The validation pipeline rejects these if they appear in `identity_fields`.

---

### 4.43 Bulk Import and Historical Backfill Hazard

The bootstrap problem (§4.20) covers the risk of the very first data load when the identity graph is empty. This section covers a distinct, often more dangerous hazard: **mid-lifecycle bulk imports** — data migrations, historical backfills, post-acquisition CRM consolidations — that occur while writeback is actively running.

**Why mid-lifecycle bulk imports are more dangerous than bootstrap:**

1. The existing cluster graph participates. New records form edges not only with each other but with all existing records.
2. Writeback is uninhibited. As each batch of records arrives and triggers identity re-evaluation, cluster changes fire writeback to all connected systems immediately.
3. Data quality corrections cannot be applied retroactively. If bad values in the import batch cause false merges, the writeback has already propagated to all systems before the corrections are applied.

**The sequence of harm:**

```
t=0:  bulk import begins — 200,000 records written to inout_src_*
t=1:  _id_contact re-evaluates — transient super-clusters form
t=2:  _delta_contact fires — thousands of writeback events generated
t=3:  writeback propagates to all systems
t=4:  import data quality corrections applied — false merges identified
t=5:  corrections require manual reconciliation across all systems
```

**Distinguishing from the bootstrap problem (§4.20):**
- Bootstrap: first load, identity graph is empty, writeback systems are gated, no propagation risk.
- Bulk import: mid-lifecycle, active graph, writeback running uninhibited.

**Mitigations:**

- Gate all ingest deltas before the import begins: `SELECT pgtrickle.gate_source('inout_src_*', 'bulk_import_lock');`
- Apply a full data quality pass to the import batch in a staging table before committing to `inout_src_*`: deduplication, format normalisation, blocklist checks.
- Run the identity resolution preview (§4.22) against the staged batch to inspect cluster size changes before committing.
- Ungate only after the cluster distribution has stabilised for at least one full refresh cycle with no cluster size alerts.
- Treat every medium-to-large data migration as equivalent to a new-system onboarding. Apply Mode B gating (PLAN_ONBOARDING_PROPOSAL.md) even for existing systems receiving bulk data.

---

### 4.44 Signal Coverage Asymmetry and the Fragile Transitivity Chain

In a three-source deployment, identity field coverage is rarely uniform across sources:

| Field        | Source A (CRM) | Source B (Support) | Source C (Finance) |
|---|---|---|---|
| email        | 92%            | 78%                | 3%                 |
| phone        | 45%            | 81%                | 12%                |
| company_name | 87%            | 23%                | 95%                |

No single field provides high coverage across all three sources. Cross-source identity therefore depends on transitivity: A–B via email, B–C via phone, so A–C by transitivity — even though A and C share no direct identity signal.

**The fragile transitivity chain has three failure modes:**

1. **Source removal collapses the chain.** If source B is removed, all A–C links dissolve. A and C share no direct signals. An entire region of the identity graph collapses silently, without any error — records simply become singletons.

2. **New signal causes mass-merge.** Adding source D with high email coverage across all three sources suddenly creates direct A–D, B–D, C–D links via email, and by transitivity, merges previously-isolated records. The resulting cluster growth event is proportional to source D's coverage — potentially linking the entire record population.

3. **Asymmetric link confidence.** The A–B link (via 92%/78% email coverage) is structurally stronger than B–C (via 23%/12% shared coverage). There is no way to model this confidence asymmetry: the graph treats all transitive edges as equivalent.

**Mitigations:**

- **Coverage matrix audit at onboarding:** For every (source, identity_field) pair, compute coverage %. Document which source pairs have direct shared coverage vs. transitively-only coverage. The transitively-only pairs are structurally fragile.
- **Redundancy requirement:** Before going live, verify that at least two source pairs have direct coverage on at least one common identity field. If the only connectivity between two sources is transitive via a third, document this explicitly and monitor it.
- **Fragile-link detection:** Identify clusters where records from source A and source C share no direct identity signal — linked only through source-B intermediaries. Flag these for human review.
- **New-source impact simulation:** Before onboarding a new source, simulate the link groups it will create and estimate the number of new cluster merges. A high-coverage source bringing a previously-missing field is the highest-risk new-source type.

---

### 4.45 Identifier Temporal Reuse (The Recycled Identifier Problem)

Identity fields are assumed to refer permanently to one entity. In practice, identifiers are recycled by the issuing authority long after their original owner stops using them:

| Identifier type | Recycling mechanism | Typical lifespan before reassignment |
|---|---|---|
| Mobile phone number | Carrier reassignment after 90–180 days inactivity | 3–12 months |
| Email domain (company) | Domain acquired after bankruptcy; new owner receives all old emails | 6–24 months |
| Company registration number | Some registries reassign dissolved company numbers | Jurisdiction-dependent |
| IP address / device fingerprint | DHCP, carrier-grade NAT, device resale | Minutes to months |

**The failure mode:**

Person A held phone `+44 7700 900123` until 2023. Person B was issued the same number in 2024. System 3 ingests Person B's record with `+44 7700 900123`. OSI-mapping links B's record to Person A's cluster because the phone number matches an existing identity field value.

Person B is now merged with Person A's golden record. Writeback may update Person A's account across all systems with Person B's data, and vice versa. There is no data quality error detectable at the record level — both records have valid, properly formatted phone numbers.

**Why this differs from §4.12 (temporal lifecycle):** §4.12 covers an entity that changes its identity field values over time. This section covers a static record whose identity field value was previously held by a different entity. The record itself is current and correct.

**Mitigations:**

- **Recency-weighted identity fields:** Phone and email used for cross-system matching should carry a `last_confirmed_at` timestamp. An identity field value that has not appeared in any ingest for more than N months should carry reduced confidence and eventually be excluded from active matching. This is an extension of §4.14 (link confidence decay) applied specifically to identifier reuse risk.
- **Velocity alert on new-record-to-existing-cluster matches:** If a newly created record immediately links to a cluster whose members were created significantly earlier, flag for human review. Legitimate matches for brand-new records linking to old clusters exist (returning customers) but the age gap is a useful heuristic.
- **Phone number portability and carrier validation:** For phone numbers specifically, carrier lookup APIs can confirm current assignment. This is expensive to run inline but viable as a nightly validation job for identity fields that anchor large clusters.
- **Document recycle risk per field type in connector YAML:** A `recycle_risk: high|medium|low` annotation on identity fields informs the confidence decay schedule in §4.14.

---

### 4.46 NULL and Empty-String Pollution in Identity Fields

Two pre-normalisation corruption classes that silently produce mass false-positive links before any identity logic runs:

**Class 1: Empty string treated as a meaningful value**

Many source systems and connectors write `''` (empty string) for unknown, N/A, or absent identity field values rather than `NULL`. PostgreSQL evaluates:

```sql
WHERE a.email = b.email  -- NULL = NULL is FALSE (correct)
WHERE a.email = b.email  -- '' = '' is TRUE (wrong)
```

Every record with an empty-string email is in the same link group. In a 100,000-record Contacts table where 30% of records have no email address but the connector writes `''` for missing values, 30,000 records instantly form one cluster.

**Class 2: Integer type coercion destroys leading zeros**

Some connectors (or intermediate ETL steps) store phone numbers as `bigint` or `integer` instead of `text`. Leading zeros are stripped silently:

| Original value | Stored as bigint | Effect |
|---|---|---|
| `0044 7700 900123` (UK international) | `447700900123` | Format change only |
| `020 7946 0123` (UK London) | `2079460123` | Leading zero dropped |
| `0612345678` (Netherlands mobile) | `612345678` | Leading zero dropped; becomes 9-digit |

The stripped value now fails to match the correctly formatted value from another system. This is a systematic false-negative generator for any country where national phone numbers begin with 0 (Netherlands, France, Germany, UK domestic format, Australia, etc.).

The inverse also occurs: if System A stores `020 7946 0123` and System B stored the same number as integer `2079460123`, normalisation (`regexp_replace(phone, '\D', '', 'g')`) produces `02079460123` and `2079460123` respectively — still no match.

**Mitigations:**

- **Connector validation rule:** Reject empty strings for identity fields at ingest time. The connector YAML or forward view should coerce `''` to `NULL` before the record reaches `inout_src_*`.
  ```sql
  -- In _fwd_{connector}_{type} view:
  NULLIF(trim(email), '') AS email
  ```
- **Phone number storage contract:** All phone number identity fields must be stored as `text`, never as numeric types. Assert in the connector CI pipeline that phone columns have `text` or `varchar` type.
- **International format normalisation:** Store phones in E.164 format (`+cc-number`) at ingest. This requires knowing the country code, which the connector must supply or infer. A missing country code is preferable to a dropped leading zero — missing produces a null match (no link); dropped zero produces a wrong match (false link).
- **Null-rate and empty-string-rate monitoring:** Alert when the empty-string rate for any identity field exceeds 0% in a new source, and when it changes by more than 1% between syncs.

---

### 4.47 Test, Synthetic, and Demo Record Contamination

Source systems expose test records, sandbox contacts, and demo data through the same API endpoint as production data. These are records created by developers, QA engineers, sales demo scripts, and onboarding workflows. They are structurally identical to real records and arrive with syntactically valid identity field values.

**Examples of contaminating test records:**

| Field | Test value | Contamination effect |
|---|---|---|
| email | `test@example.com`, `qa@yourcompany.com` | Links all test records together; `example.com` domain is IANA-reserved and unusable but syntactically valid |
| phone | `555-0100`, `07700 900000` (UK Ofcom test range) | Clusters all records with test phone numbers |
| name | `Test Contact`, `DO NOT DELETE`, `ZZZZ Placeholder` | Harms name-based fuzzy matching benchmarks |
| company | `Test Company`, `Acme Corp` (fictional name in widespread use) | Acme Corp appears in millions of CRM records across unrelated customers |

**Why this is distinct from token pollution (§4.40):** Token pollution concerns real values (a real company's shared fax number) that are legitimately high-frequency. Test record contamination concerns synthetic values that should not be present in production data at all. The detection mechanism differs:

- Token pollution: frequency analysis (real value, high cardinality)
- Test contamination: pattern matching (known test patterns, IANA reserved domains, regex for known test strings)

**Compounding effect with frequency blocklist (§4.40):** If enough connectors include test records with `test@example.com`, the value's frequency rises above the blocklist threshold. The blocklist now suppresses it — but silent suppression means the test records become singletons rather than clustering together, making them harder to identify and purge.

**Mitigations:**

- **Test record filter at connector level:** Each connector YAML should declare filter expressions applied before records reach `inout_src_*`:
  ```yaml
  exclude_filter: "email LIKE '%@example.com' OR email LIKE '%@test.%' OR name ILIKE 'test %' OR name ILIKE '% test'"
  ```
- **IANA reserved domain blocklist:** `example.com`, `example.net`, `example.org`, `test`, `localhost` should be unconditionally excluded from email identity matching.
- **Pre-onboarding test record audit:** Before bootstrapping system 3, count records matching known test patterns. Any significant count (>0.1% of total) must be filtered or purged from the source before the bootstrap.
- **Post-bootstrap test record detection:** Run pattern scan after bootstrap. Test records that slipped through are identifiable by inspection of the singleton cluster population (§4.19) — singletons with test-pattern values are likely test records, not genuine under-links.

---

### 4.48 The Writeback-Induced Corroboration Feedback Loop

This section describes a subtle variant of §4.38 (derived source data). §4.38 covers a one-time onboarding event where System 3 was pre-populated from a System 1 export. This section covers an ongoing, live mechanism that operates continuously through the writeback pipeline.

**The loop:**

```
t=0:  System A has alice@co.com. System B has no email for Alice.
t=1:  Identity resolution: A and B are in the same cluster via name + phone.
t=2:  Writeback: MDM writes alice@co.com to System B (sourced from System A).
t=3:  System B next sync: alice@co.com is now present in System B's ingest.
t=4:  Identity resolution: alice@co.com appears in both A and B.
      Confidence: "email corroborated by two independent sources" ← FALSE.
t=5:  Writeback now has a higher-confidence basis to propagate Alice's identity
      to any new record that matches on email alone (previously required name+phone).
```

Each writeback cycle in which a field is written to a target system and then re-ingested strengthens the apparent multi-source corroboration of that field. The MDM acts as an amplifier: it takes a single-source observation, distributes it to N systems, and then re-ingests those N copies as N independent observations.

**Why this is dangerous at scale:**

In a steady-state three-system deployment running hourly syncs, a single confident false merge in week 1 can, by week 4, have produced email corroboration across all three systems — making it appear triple-confirmed when the original source was a single mistaken record. The error is self-reinforcing and increasingly difficult to unwind.

**Distinction from §4.10:** §4.10 covers the case where writeback causes the *target system* to update a field to a new value, which then re-ingests and changes the link graph. §4.48 covers the case where the target system simply stores the written-back value and re-ingests it without changing it — inflating apparent corroboration with no field update occurring.

**Mitigations:**

- **Writeback provenance tagging:** Tag each identity field value with `source_origin: 'writeback'` or `source_origin: 'native'` at ingest time. In the forward view, values written back by the MDM carry a `writeback_echo: true` flag. Identity resolution excludes these from corroboration counting — they are single-source observations regardless of how many systems now hold them.
  ```sql
  -- In _fwd_hubspot_contacts:
  CASE WHEN updated_by_source = 'mdm_writeback' THEN NULL ELSE email END AS email
  ```
- **Writeback field exclusion from identity matching:** The most conservative mitigation: fields written back by the MDM are excluded from identity field evaluation entirely. Identity is resolved only on fields the source system populated natively.
- **Corroboration audit query:** Regularly run a report that identifies clusters where all sources' identity field values share the same first-seen-at timestamp via writeback (i.e., all copies arrived at approximately the same time following a writeback event). These are echo clusters masquerading as corroborated clusters.

---

### 4.49 GDPR Purpose Limitation for Identity Matching (Art. 5(1)(b))

§4.7 covers the mechanics of GDPR erasure on identity fields. This section covers a prior legal question: **whether using a given identity field as a match key is a lawful processing operation at all**, given the original collection basis.

**The problem:**

GDPR Art. 5(1)(b) requires that personal data collected for one purpose not be used for incompatible purposes without a new legal basis. Different data flows have different collection contexts:

| Source system | Typical legal basis | Example data collected |
|---|---|---|
| CRM (System 1) | Legitimate interest / contract | email, phone, company name |
| Support ticket system (System 2) | Contract (support service) | email, name, device info |
| Finance / billing (System 3) | Contract (payment) | name, billing address, VAT number |

Using an email collected under the support ticket basis **as an identity key** to link to the marketing CRM record is a processing operation. That processing requires a legal basis compatible with the original collection. `Legitimate interest` for identity matching is generally arguable, but:

1. The Data Protection Impact Assessment (DPIA) must explicitly cover identity matching as a processing purpose.
2. If System 3's billing address was collected under contractual basis for invoicing, using it to cross-reference marketing records may fall outside that basis.
3. If a subject withdraws consent for marketing, the MDM must not use their marketing-consented email as a bridge to link their support or billing records — the withdrawal may dissolve the identity link's legal basis.

**The data minimisation question (Art. 5(1)(c)):**

If a unique match can be achieved with email alone, is it lawful to also store phone + company + address as identity keys? GDPR data minimisation requires that processing use the minimum data adequate for the purpose. Storing five identity fields when two would suffice may not satisfy minimisation.

**Practical implications for this pipeline:**

- The DPIA for the MDM deployment must list each identity field in use, the source system it originates from, the legal basis under which it was collected, and the argument for why using it as a match key is compatible with that basis.
- An `identity_fields` section in the connector YAML should carry a `legal_basis` annotation:
  ```yaml
  identity_fields:
    - field: email
      legal_basis: contract
      dpia_reference: "DPIA-2025-003 §4.2"
    - field: phone
      legal_basis: legitimate_interest
      dpia_reference: "DPIA-2025-003 §4.3"
  ```
- Consent withdrawal must trigger suppression of that field from identity matching — not just from the golden record output — so that the withdrawn field can no longer create or sustain a cluster link. This integrates with the erasure mechanics in §4.7 but operates at the matching layer, not only the storage layer.
- Consult legal counsel before deploying cross-system identity matching across sources with materially different collection bases (e.g. support + marketing + billing).

---

### 4.50 Locale-Sensitive Field Normalisation (DOB Ambiguity and the Turkish İ Problem)

§4.2 covers standard normalisation: `lower()`, `trim()`, phone formatting, unicode NFC. Two locale-sensitive failure modes fall outside that scope and require explicit handling.

**Failure mode 1 — Date-of-birth format ambiguity (MM/DD vs DD/MM)**

Date-of-birth (DOB) is sometimes used as a secondary identity field, particularly in healthcare, finance, and government-adjacent deployments. Different source systems format dates inconsistently:

| System | DOB stored as | Interpreted as |
|---|---|---|
| CRM (US) | `01/02/1980` | January 2, 1980 |
| Support (EU) | `01/02/1980` | February 1, 1980 |

These are **the same string** — they will match in a WHERE clause. But they represent different entity-confirming facts. If identity resolution uses DOB to break a tie between two otherwise-similar records, a US-format date matching an EU-format date produces a false-positive link with no error surfaced anywhere.

Conversely, if System A stores `1980-02-01` (ISO 8601) and System B stores `01/02/1980` (EU format), these represent the same date but produce a string mismatch — false negative.

**Failure mode 2 — Turkish dotted-I / dotless-i case folding**

Standard PostgreSQL `lower()` is locale-agnostic. In a C or en_US locale:

```sql
lower('I') = 'i'   -- correct for most languages
lower('İ') = 'i̇'  -- Unicode lowercase of LATIN CAPITAL LETTER I WITH DOT ABOVE
```

In the Turkish and Azerbaijani languages, the uppercase of `i` (dotless-i) is `I` (dotless), and the uppercase of `İ` (dotted-i) is the same `İ`. Running `lower()` in a PostgreSQL `C` locale on Turkish names:

- `IŞIK` → `ışık` (correct Turkish lowercase)
- `lower('IŞIK')` in C locale → `iŞIK` (partially lowercased, broken)

This silently destroys identity field matching for Turkish names stored in differently-cased forms across source systems.

**Mitigations:**

- **ISO 8601 at ingest:** All date values in identity fields must be coerced to `YYYY-MM-DD` at the connector forward view layer. The connector is responsible for knowing its source's date format convention.
  ```sql
  -- In _fwd_{connector}_contacts, for a US-format source:
  to_char(to_date(dob, 'MM/DD/YYYY'), 'YYYY-MM-DD') AS dob
  ```
  If format is ambiguous (could be MM/DD or DD/MM), the field must not be used as an identity field until the format is confirmed.
- **DOB as a confirming field, not a primary key:** DOB has low entropy and high format ambiguity. Design identity rules so DOB is only used to disambiguate within a candidate pair already linked by a higher-quality field (e.g. email + DOB agrees → confidently the same person; DOB alone → never link).
- **Locale-aware `lower()` for Turkish/Azerbaijani data:** Use `lower(x COLLATE "tr_TR")` or normalise at ingest by applying `translate(upper(x), 'İI', 'ii')` before standard lowercasing. Document the locale assumption in the connector YAML.
- **Character set audit at onboarding:** If source data contains Turkish, Arabic, or any language with locale-variant case rules, the normalisation pipeline must explicitly handle it before bootstrap.

---

### 4.51 Intentional Dual-Relationship Records (The Same Person, Two Hats)

§4.30 covers legitimate one-to-many identity field sharing: the same value (phone number, email) intentionally belongs to multiple distinct entities (a shared corporate mailbox). This section covers a different scenario: **one person who intentionally has two separate records representing two distinct business relationships with the same organisation**.

**Common examples:**

- A consultant who is both a *vendor contact* (in procurement/finance) and a *customer lead* (in the CRM)
- A doctor who is a *patient* of the clinic they also work at
- A company founder who is a *personal customer* of their own company's product AND a *B2B contact* for enterprise sales

In each case, the two records have the same email, the same phone, and often the same name. They are, by every identity field criterion, the same person. But they **must not be merged** — the business contexts are intentionally separate, different teams manage them, different data governance rules apply, and merging produces a broken golden record that is neither a vendor contact nor a customer lead.

**Why this is harder than §4.16 (transitivity) or §4.30 (one-to-many sharing):**

The two records do not share a *community* value — they share the person's own direct identity fields. The block-merge curation override is the correct mechanism, but it requires the deploying team to:

1. Know these dual-relationship records exist before the bootstrap
2. Create the override before the first identity resolution runs
3. Maintain the override permanently — it must survive YAML redeployments (§4.35), schema migrations, and curation database restores

If the override is created *after* the first writeback has already propagated a merged golden record to all systems, the operator must manually unwind the merged data in every system.

**Mitigations:**

- **Onboarding questionnaire item:** "Does this source system contain records for individuals who also appear in other source systems in a different capacity?" If yes, enumerate the cases and pre-create block-merge overrides before bootstrap.
- **Relationship-type field as a structural cue:** If the source system carries a `record_type` or `relationship_type` field (Vendor, Customer, Employee), the identity rule can be conditioned: only match records of the same `relationship_type`. Records of different types enter the curation queue for human review before linking.
- **Explicit entity-type separation in OSI-mapping YAML:** If the two relationship types are sufficiently distinct, model them as separate entity types (e.g. `vendor_contact` vs `customer_contact`) with separate `_id_*` resolution pipelines. They can still be linked via a `cross_type_relationship` table, but they cannot accidentally merge through transitivity.

---

### 4.52 Curation Override Loss and Silent Reversion

The human curation system (§6) stores block-merge and force-merge overrides in a dedicated table. Those overrides are the primary mechanism by which the MDM respects human judgment over algorithmic identity resolution. This section covers what happens when that override table is damaged or emptied.

**Scenarios that cause override loss:**

| Scenario | Mechanism | Detection |
|---|---|---|
| Database restore to earlier snapshot | Override table restored to pre-override state | None (silent) |
| `TRUNCATE identity_overrides` in migration script | All overrides deleted | None (silent) |
| Override table excluded from backup | Overrides not restored after DB failure | None (silent) |
| Connector re-bootstrap reassigns `external_id` values | Existing overrides reference stale external_ids | Overrides silently stop matching |
| YAML redeployment changes entity type names | Override table uses old entity type keys | Same as above |

**Consequence:** Every blocked merge becomes a real merge. Every forced merge dissolves. All human curation effort is silently undone. The identity graph reverts to the unconstrained algorithmic state. Writeback then propagates the reverted state to all systems — potentially distributing data to systems whose records were manually separated for compliance or business reasons.

The silent nature is the critical hazard: there is no error, no warning, no cluster-size alert that specifically signals "override table was emptied."

**Mitigations:**

- **Override count monitoring:** Persist the count of active overrides after each curation action. Alert if the count decreases by more than a threshold outside of an operator-initiated bulk action. A sudden drop from 847 to 0 is automatically suspicious.
- **Override table inclusion in backup SLA:** Explicitly include `identity_overrides`, `identity_blocklist`, and all curation tables in the DB backup runbook. Mark them as safety-critical: a restore without these tables is not a valid restore.
- **Override table referential integrity check on deploy:** The CI/CD pipeline for YAML redeployments should run a check: for each override row, verify that its `external_id` and entity type still exist in the current schema. Stale overrides should surface as a deployment warning, not silently stop matching.
- **Override re-export before any destructive migration:** Before any operation that could affect the override table (migration, restore, schema rename), export the full override table to a versioned backup. The export is also a human-readable audit record of all curation decisions.
- **Cluster diff on next refresh:** After any database restore or bulk migration, compare the current cluster assignments to the last known-good snapshot (§4.35). A large number of reclassified records after a low-change data period is a strong signal of override loss.

---

### 4.53 Source-Level Pre-Deduplication Asymmetry

Different source systems enforce different uniqueness constraints at the platform level, before data ever reaches the MDM ingestion pipeline:

| Source | Platform-enforced uniqueness | Implication |
|---|---|---|
| Salesforce | Unique email constraint optional; standard duplicate rules can block exact-match creates at API level | MDM may see zero or minimal duplicates on the dimensions Salesforce deduplicates |
| HubSpot | Unique email per portal (contacts) | Contacts with duplicate email are merged or blocked at HubSpot level |
| Generic CRM / internal system | No uniqueness constraints; any number of records with identical emails can coexist | MDM sees raw, undeduped data |
| Finance/ERP | Often no CRM-style deduplication; vendor/customer IDs are unique but contact emails are not | Mixed: structural IDs are clean; free-text identity fields are not |

**Why this creates an identity resolution problem:**

The MDM's identity resolution operates on the assumption that records in `inout_src_*` are the raw population of entities. When source A delivers already-deduplicated records (because Salesforce silently merged them) and source B delivers raw duplicates, the cross-source link graph is inconsistent:

- Source A's cluster has one record per entity (already resolved inside Salesforce)
- Source B's cluster may have 3–5 records per entity (unresolved duplicates)
- The MDM links them all via transitivity — producing a cluster that is 3–5× larger than it should be
- When the MDM writes back the golden record to source A, Salesforce may reject it because the golden record now combines data from duplicate contacts that Salesforce's own rules would have blocked

**The reverse problem:** If a source applies strict deduplication and two people share an email address (e.g. a shared family email), the platform may have already merged them into one record. The MDM ingests one record and cannot detect that it represents two distinct people — the pre-deduplication has destroyed the information needed for correct resolution.

**Mitigations:**

- **Document platform deduplication behavior per source at connector onboarding:** Does the source enforce email uniqueness? Phone uniqueness? At what point in the write path? Record this in the connector YAML.
- **Deduplication-aware confidence scoring:** Records from sources with platform-enforced uniqueness carry higher per-record identity confidence (that email was checked for duplicates at write time). Records from unconstrained sources carry lower confidence. Weight accordingly in §4.3 ambiguity handling.
- **Pre-bootstrap duplicate audit per source:** Before ingesting any source into the MDM, count duplicate identity field values within that source. High within-source duplicate rates are a signal that the source requires intra-source deduplication first (§4.27) before cross-source matching produces useful results.

---

### 4.54 Partial-Ingest Transient Identity Instability

A full sync of a source system does not always arrive as a single atomic operation. Connectors paginate API responses, respect rate limits, and may be interrupted mid-sync by timeouts, network errors, or API quota exhaustion. When identity resolution runs on a partial ingest, the resulting cluster assignments may differ materially from those produced when the full ingest completes.

**The two-pass problem:**

```
t=0:  Connector begins full sync of system 3 (200,000 records)
t=1:  Connector ingests records 1–50,000 (rate limit hit)
t=2:  pg-trickle DIFFERENTIAL refresh runs on the partial ingest
      → Identity resolution produces cluster assignments based on 50,000 records
      → Writeback fires for all cluster changes triggered by this partial set
t=3:  Connector resumes, ingests records 50,001–200,000
t=4:  pg-trickle DIFFERENTIAL refresh runs on the full ingest
      → Identity resolution re-evaluates with all 200,000 records
      → Cluster assignments differ from t=2; further writeback events fire
```

The cluster assignment at t=2 is not a valid intermediate state — it is an artifact of ingest ordering. Records in the second half that would have bridged two clusters were absent at t=2, so the clusters were kept separate, writeback fired with "these are distinct entities", then at t=4 they merge and writeback fires the opposite.

**Why this is worse than the bootstrap problem (§4.20):** The bootstrap (§4.20) happens once and from an empty graph. Partial-ingest instability can happen on any regular sync cycle. If a sync is interrupted frequently (flaky API, aggressive rate limiting), writeback oscillates on every cycle for affected records.

**Relationship to §4.5 (stability under incremental updates):** §4.5 covers oscillation caused by conflicting field values. §4.54 covers oscillation caused by incomplete data — the field values are consistent, but the graph is evaluated in two passes due to operational constraints.

**Mitigations:**

- **Sync completion marker before identity refresh:** The connector writes a `sync_completed_at` timestamp to a control table when it finishes a full sync successfully. pg-trickle's identity refresh is gated on this timestamp being newer than the last refresh. An interrupted sync does not trigger identity resolution.
  ```sql
  -- Checked before _id_{target} refresh materialises:
  SELECT 1 FROM inout_sync_status
  WHERE connector = 'system3'
    AND sync_completed_at > last_refresh_at
    AND sync_status = 'complete';
  ```
- **Minimum-completeness threshold:** Even without a completion marker, if fewer than 80% of the expected record count has arrived (estimated from the previous full sync), suppress the identity refresh until the next sync cycle.
- **Writeback hold during active sync:** While a sync is in-progress, mark the delta views as held. The bridge layer (§2) does not propagate changes to `inout_dst_*` while `sync_in_progress = true` for the relevant source.
- **Connector reliability monitoring:** Track the fraction of syncs that complete successfully vs. are interrupted. Sources with high interruption rates require either more aggressive connector reliability work (retry logic, checkpoint resumption) or gating policy.

---

### 4.55 Golden Record Field Conflict Resolution for Non-Identity Fields

§4.26 covers the specific contradiction between a field being an identity key and a conflict-resolution target simultaneously. This section covers the separate, general problem of what value populates the golden record when cluster members disagree on ordinary non-identity fields such as `job_title`, `company_name`, `billing_address`, or `phone_number`.

**The problem:**

A cluster of three records representing the same contact may carry:

| Source | first_name | job_title | phone |
|---|---|---|---|
| CRM (Salesforce) | Alice | VP of Sales | +1 415 555 0100 |
| Support (HubSpot) | Alice | VP Sales | +1 415 555 0100 |
| Finance | A. Smith | Director | +1 415 555 0101 |

Which `job_title` and `phone` go in the golden record? The MDM must implement a policy. The choice of policy is not neutral — it directly affects what gets written back to all systems.

**Common policies and their failure modes:**

| Policy | Description | Failure mode |
|---|---|---|
| Most-recent-wins | Use value from most recently updated record | A stale Finance record updated yesterday overwrites a more accurate CRM record |
| Source-priority | Prefer source A over B over C | Finance (low trust) wins because it was updated most recently in its own priority tier |
| Most-populated | Use non-null value from highest-priority source | `NULL` in Salesforce is treated as "no data" but actually means "intentionally cleared" |
| Majority vote | Use value held by the most sources | Works only for 3+ sources; two sources with different values deadlocks at 50% |
| Longest value | Use the longest non-null string | `"VP of Sales (EMEA, APAC, LATAM, Global Accounts)"` wins over `"VP of Sales"` — not necessarily more accurate |

**Why this interacts with identity resolution specifically:**

1. **Writeback of the wrong field value can create new false-positive links.** The golden record writes `phone = +1 415 555 0101` (from Finance) to Salesforce. On the next ingest, Salesforce now carries the Finance phone number — creating a cross-source phone corroboration that did not exist before and may not be accurate.
2. **Conflict resolution policy determines the corroboration feedback loop velocity (§4.48).** Most-recent-wins causes the freshest source to dominate writeback, which then echoes back into all sources. Source-priority causes one source to persistently dominate.
3. **NULL-as-intentional-clear vs NULL-as-missing is undetectable.** If a Salesforce admin deliberately cleared a phone number because it is wrong, `most-populated` policy overwrites the deliberate erasure with the wrong value from another source.

**Mitigations:**

- **Explicit conflict resolution policy per field in the connector/mapping YAML:**
  ```yaml
  field_resolution:
    job_title:
      policy: source_priority
      source_order: [salesforce, hubspot, finance]
    phone:
      policy: most_recent
      max_age_days: 90  # ignore values older than 90 days
    billing_address:
      policy: source_priority
      source_order: [finance, salesforce, hubspot]
  ```
- **NULL-semantics annotation:** Fields where NULL means "intentionally cleared" (not "unknown") must be annotated `null_means_cleared: true`. The conflict resolution logic treats an explicit NULL from a high-priority source as a definitive signal rather than falling through to the next source.
- **Golden record provenance tracking:** The golden record table should carry `{field}_source` and `{field}_updated_at` columns so that each field's origin is auditable. When a wrong value appears in a downstream system, the source of that value can be traced without examining the full cluster history.
- **Conflict rate monitoring:** For each field in the golden record, track the fraction of clusters that have conflicting values. A rising conflict rate on `email` or `phone` is a signal that source data quality is diverging — or that writeback is creating artificial values that are now being re-ingested as alternatives.

---

### 4.56 API Constraint Violations Silently Corrupting Round-Trip Identity

When the MDM writes a golden record field back to a target system, the target system may silently truncate, transform, or reject the value due to its own schema constraints. On the next ingest, the modified value no longer matches the original — producing a false negative in identity resolution.

**Truncation is the most common and most dangerous form:**

```
MDM golden record:  email = "alice.verylongname.with.subdomain@corporation.co.uk"  (55 chars)
Target system:      email column is VARCHAR(40)
Written back as:    "alice.verylongname.with.subdomain@cor"  (silently truncated)
Re-ingested:        "alice.verylongname.with.subdomain@cor"
OSI-mapping:        'alice.verylongname.with.subdomain@cor' ≠ 'alice.verylongname.with.subdomain@corporation.co.uk'
Result:             False negative — one record leaves the cluster
```

The target system does not return an error — it stores what it can and returns a success response. The writeback daemon logs a successful write. On the next ingest, a subtly different value arrives and identity resolution silently splits the cluster.

**Other constraint violation modes:**

| Constraint type | Example | Identity effect |
|---|---|---|
| Character set narrowing | Target system is Latin-1-only; MDM writes UTF-8 `Müller`; stored as `M?ller` | False negative (normalised forms differ) |
| Case enforcement | Target system uppercases all names; `alice` becomes `ALICE`; normalisation handles this if lowercased before comparison | Usually safe post-normalisation *if* normalisation runs before comparison |
| Special character stripping | `alice+tag@co.com` → `alicetag@co.com` or `alice@co.com` | Either false negative or false positive depending on stripping rule |
| Domain or format validation | Target system validates email format strictly; rejects writeback; old value persists | On re-ingest: old value; identity field diverges across systems |
| Field-level encryption | Target system encrypts PII at rest and returns ciphertext in API responses | Ciphertext ingested as identity value → matches nothing → false negative |

**Mitigations:**

- **Pre-writeback constraint validation:** Before writing a field value to a target system, validate it against the target system's known constraints. The connector YAML should declare target-side constraints:
  ```yaml
  writeback_fields:
    email:
      max_length: 80        # target system VARCHAR(80)
      charset: utf8mb4      # MySQL 4-byte UTF-8
      case: preserve        # do not coerce
  ```
  A value that fails validation should be flagged for human review rather than truncated silently.
- **Round-trip hash check:** After writeback, re-fetch the written field from the target system and compare it to what was written. A mismatch is a constraint violation. Log it, alert, and exclude the corrupted field value from identity matching until it is corrected.
- **Target-side constraint audit at connector onboarding:** Before enabling writeback for system 3, query its schema for field length, character set, and format constraints. Document them in the connector YAML. Any identity field that the target system applies transformation rules to must not be used for cross-system matching via that system's re-ingested copy.

---

### 4.57 Cluster Merge Evidence Auditability

In regulated industries — healthcare, financial services, legal, government — the question "why were these two records determined to represent the same entity?" is not merely operational; it is a compliance requirement. Regulators, patients, customers, and auditors may require a documented chain of evidence for any merge decision.

OSI-mapping's identity resolution is a view pipeline: `_fwd_*` → `_id_*` (WITH RECURSIVE connected-components) → `_resolved_*`. The pipeline produces a correct answer given the current data, but it does not produce a log. There is no record of:

- Which identity field value caused the edge between record A and record B
- When that edge was first established
- Whether the edge was established deterministically (exact match) or via a curation override
- What the cluster membership was at any prior point in time

**Why the view-only architecture makes this hard:**

The `_id_{target}` view is re-evaluated on every refresh. The cluster assignment for any given record today tells you the current result, not the path. If the data that caused a merge was later corrected (e.g. a shared email was a data error and has been removed), the original merge reason is gone — but the downstream effects may persist in all systems' histories.

**The two audit questions that must be answerable:**

1. **Forward audit:** "What fields caused Record A to merge with Record B?" Required for: compliance review, debugging incorrect merges, explaining MDM decisions to affected data subjects (GDPR Art. 22 — automated processing).
2. **Reverse audit:** "What was the cluster membership of Record A on date X?" Required for: retrospective investigation, legal discovery, incident response.

**Mitigations:**

- **Edge table:** Materialise the identity edges (not just the cluster assignments) as a persisted table updated on each refresh:
  ```sql
  CREATE TABLE identity_edges (
    record_a_id       text NOT NULL,
    record_b_id       text NOT NULL,
    link_field        text NOT NULL,   -- which identity field produced this edge
    link_value        text NOT NULL,   -- the shared value
    first_seen_at     timestamptz NOT NULL DEFAULT now(),
    last_confirmed_at timestamptz NOT NULL DEFAULT now(),
    source            text,            -- 'algorithmic' | 'curation_override'
    PRIMARY KEY (record_a_id, record_b_id, link_field)
  );
  ```
- **Cluster assignment history:** On each refresh, snapshot any cluster assignments that changed into a `cluster_assignment_history` table (record_id, cluster_id, valid_from, valid_to). This enables the reverse audit query: "what cluster was this record in on 2025-01-15?"
- **Override decisions carry a mandatory reason field:** Human curation overrides (§6) must require a `reason` text field. The reason is stored in the override table and forms part of the audit record for any merge affected by that override.
- **Audit report generation:** Implement a query template that, given two record IDs, produces a human-readable audit report: "These records were merged because they share email `alice@co.com`, first observed on 2025-03-12. The link was confirmed by human curation on 2025-03-14 (operator: j.smith, reason: 'verified by phone')."

---

### 4.58 Stale Permanence of Curation Blocks (Over-Blocking)

§4.52 covers the loss of curation overrides and silent reversion to the algorithmic default. This section covers the opposite failure: **curation blocks that persist indefinitely and become incorrect over time**.

A block-merge override is typically placed because two records with matching identity field values are known to represent different entities. But entities change:

| Scenario | Why the block was placed | Why it may later be wrong |
|---|---|---|
| Two employees with the same name and work email pattern | Different people: Alice Smith (Sales) and Alice Smith (Engineering) | One leaves; their email is reassigned to the other (or to a new hire with an alias) |
| Parent company and subsidiary sharing a tax ID | Distinct legal entities at time of blocking | Subsidiary is acquired and fully merged into parent; now they are the same entity |
| Ex-partners sharing an address | Distinct people living at the same address | One moves; the other retains the address; new records match correctly but the block prevents it |
| Test record and production record with same email | Test record is genuinely different | Test record is deleted; a legitimate production record is created with the same email |

**The structural problem:** Curation overrides have no built-in expiry mechanism. A block placed three years ago by an operator who has since left the company carries equal weight to a block placed yesterday. The override table grows monotonically. Over time, an increasing fraction of blocks may be invalid, silently preventing correct merges.

This is the **false-negative equivalent of unchecked false positives**: incorrect blocks cause the same harm as incorrect matches — data is fragmented across systems, golden records are incomplete, writeback cannot synchronise fields across the full intended scope.

**Mitigations:**

- **Block expiry and review scheduling:** Every override row should carry a `review_due_at` date. Blocks on volatile data (email, address) should expire sooner than blocks on stable data (government identifiers, date of birth). An automated report surfaces overrides past their review date for operator action.
  ```sql
  ALTER TABLE identity_overrides ADD COLUMN review_due_at date;
  ALTER TABLE identity_overrides ADD COLUMN last_reviewed_at timestamptz;
  ALTER TABLE identity_overrides ADD COLUMN reviewed_by text;
  ```
- **Block trigger inventory:** When a block is created, record *why* the two records matched (which identity field value they shared). If that field value is later absent from both records — because the shared email was corrected — the block should be automatically flagged for review: its triggering condition no longer exists.
- **Periodic block validity check:** A scheduled job re-evaluates each active block: do the two blocked records still share any identity field values? If they share no current identity field values, the block is no longer preventing any merge — but it may be masking the correct link to a third record. Flag these as dormant blocks for review.
- **Block count as a leading indicator:** A steadily increasing block count with a low review rate is a leading indicator of accumulated stale blocks. Track `blocks_created / blocks_reviewed / blocks_expired` over time.

---

### 4.59 Mixed Entity Granularity Across Source Systems

OSI-mapping's `_id_{entity_type}` view assumes one record per entity occurrence in each source system. In practice, different source systems model the same underlying entity at different granularities:

**The granularity mismatch problem:**

| Source system | Model | Record count for "Alice at Acme Corp" |
|---|---|---|
| CRM (Salesforce) | One Contact record per person | 1 |
| Support (Zendesk) | One Contact per person | 1 |
| Finance / billing | One BillingContact per person-account relationship | 3 (Alice has 3 active subscriptions) |
| Event platform | One Attendee per person-event registration | 12 (Alice has attended 12 events) |
| Healthcare EHR | One Encounter per visit | 47 (47 clinical encounters over 5 years) |

When the Finance system is ingested, Alice appears as 3 records — one per subscription. All three carry her email and phone. Identity resolution links all three to her CRM record via email — correct. But now her cluster has 5 members (1 CRM + 1 Support + 3 Finance), and the golden record must pick values from a cluster whose Finance contribution has 3× the weight of the others.

**Two distinct failure modes:**

**Mode 1 — Artificial cluster inflation.** The Finance records have identical identity fields but different `subscription_id` values. They are not duplicates (§4.27) — they represent distinct business objects. Intra-source deduplication would wrongly merge them. But identity resolution can't tell that they are intentionally distinct; it sees three records with the same email and treats them as candidates for the same cluster.

**Mode 2 — Cross-granularity writeback.** When the MDM writes back to Finance, it writes to all three BillingContact records. If Alice updates her email in the CRM, the golden record propagates the new email to all three Finance records — correct. But if Alice's Finance record 2 has a different phone number than records 1 and 3 (a work phone assigned to one subscription), the golden record's conflict resolution policy overwrites that subscription-specific phone silently.

**Why the standard OSI-mapping model doesn't handle this:**

The `_id_{target}` connected-components algorithm assigns all linked records to a single cluster ID. There is no concept of "this record is a role-occurrence of an entity rather than an entity occurrence." The cluster is flat.

**Mitigations:**

- **Entity granularity annotation at connector onboarding:** Every connector YAML must declare whether its records are entity records (one per person) or occurrence records (one per event/relationship/transaction):
  ```yaml
  record_granularity: entity     # one record represents one entity
  # or
  record_granularity: occurrence # one record represents one event or relationship
  occurrence_key: subscription_id  # field that distinguishes occurrences
  ```
- **Occurrence-granularity sources excluded from cluster-size-limit rules (§4.4):** The cluster size limit exists to catch accumulator clusters. But an event-platform source with 47 attendance records per person will routinely breach the limit. The limit must account for expected occurrence frequency.
- **Occurrence deduplication in the forward view:** For occurrence-granularity sources, deduplicate on the identity fields in the forward view before the identity resolution layer sees the data:
  ```sql
  -- _fwd_eventplatform_attendees: one row per person, not per attendance
  SELECT DISTINCT ON (email) email, phone, name, max(last_seen_at) AS last_seen_at
  FROM raw_attendees
  GROUP BY email, phone, name
  ```
- **Writeback scope restriction:** For occurrence-granularity sources, writeback should update only identity fields shared across all occurrence records for that entity (or the most recent occurrence) — not all occurrence records.

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
- ✓ [Extended shadow mode](PLAN_ONBOARDING_SHADOW_MODE_EXTENDED.md) catches the symptoms — system-3-caused changes to systems 1 and 2 are held for review
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
| Bulk import / backfill safety (§4.43) | Mode B gating provides window; pre-bootstrap quality pass required | Blue pipeline is safest: bulk load into blue, validate before cutover | System-3-caused deltas held; but golden record may absorb bad bulk data | Same as shadow; held deltas give correction window |
| Partial-ingest instability (§4.54) | Sync completion marker required before ungate | Blue pipeline isolated; partial ingests do not affect production | No protection — partial ingest triggers identity refresh immediately | Same exposure as shadow; sync completion marker required |
| Fragile transitivity chain exposure (§4.44) | Full system 3 dataset arrives at once; worst-case chain formation | Chain effects visible in blue vs green cluster diff before cutover | Chain effects fire in production immediately; operator sees downstream consequences only | Same as shadow; but chain effects on systems 1 and 2 are held for review |

**Key insight:** No onboarding approach directly reviews individual identity links. They all operate at a higher level — clusters, deltas, or pipeline-level diffs. This is the gap that human curation of links would fill.

**Under-linking during onboarding is particularly dangerous.** When system 3 joins, its records should match with existing records in systems 1 and 2. If the identity rules fail to create these links (because of formatting differences, missing fields, or over-strict link groups), the pipeline treats system 3's records as new entities. The delta views for systems 1 and 2 then generate `insert` actions — pushing these "new" entities into systems that _already have them under a different cluster_. The result is duplicate records in systems 1 and 2 that grow with every sync cycle. [Extended shadow mode](PLAN_ONBOARDING_SHADOW_MODE_EXTENDED.md) catches these as `new_record` rows with `origin = system3_caused`, giving the operator a chance to investigate before the inserts execute — but only if the operator recognises that a `new_record` might be a missed match rather than a genuinely new entity.

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
3. **Cross-system inserts** — the `new_record` classification from [PLAN_ONBOARDING_SHADOW_MODE_EXTENDED.md](PLAN_ONBOARDING_SHADOW_MODE_EXTENDED.md) §5. When system 3 causes a new record to appear in systems 1 and 2, a human should confirm this is a genuine new entity — not a duplicate caused by under-linking.
4. **Cluster re-merges** — when two previously separate clusters are bridged by a system 3 record. These are the highest-risk link decisions because they affect records that were already stable.
5. **Suspected under-links (duplicate clusters)** — two or more clusters whose records are suspiciously similar (near-identical names, overlapping addresses, same phone number with different formatting) but were not matched by the identity rules. These need a human to decide: should they be linked (forced merge via override), or are they genuinely distinct entities? Left unreviewed, each cluster generates cross-system inserts that create duplicates in every writable system.
6. **Dual-relationship candidates (§4.51)** — two records where all identity fields match but the records represent intentionally distinct business relationships (vendor AND customer; employee AND patient). These must be explicitly reviewed before the bootstrap because a post-writeback block requires manual unwinding across all systems. The review question is not "are these the same person?" but "should these records be managed as a unit?"
7. **Recycled-identifier candidates (§4.45)** — a newly created record that immediately matches the identity fields of a cluster whose members were created significantly earlier. Could be a legitimate returning customer or a recycled phone/email. The age gap between the new record and the existing cluster is the flag.

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

    -- Override lifecycle (§4.52, §4.58)
    review_due_at    DATE,                 -- when this override should be re-evaluated
    last_reviewed_at TIMESTAMPTZ,          -- most recent human review
    reviewed_by      TEXT,                 -- reviewer for audit trail

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

This is reactive, not proactive. Curation happens after the damage (wrong golden record, wrong deltas to systems 1 and 2). [Extended shadow mode](PLAN_ONBOARDING_SHADOW_MODE_EXTENDED.md) mitigates this by holding system-3-caused changes, but the golden record itself is already wrong during the review window.

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
| IANA reserved domains | `@example.com`, `@example.net`, `@example.org`, `@test`, `@localhost` |
| Placeholder phone | `000-000-0000`, `+1-000-0000`, `999-999-9999` |
| Placeholder name | `Unknown`, `N/A`, `Test`, `DO NOT USE` |
| System-generated values | Sequential IDs that look like identity fields |
| High-frequency values | Any value appearing in more than 1% of records |
| Empty strings | `''` — must be coerced to NULL before matching (`NULLIF(trim(field), '')`) — see §4.46 |

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
| **Corroboration loop amplification (§4.48)** | The writeback-induced corroboration feedback loop (§4.48) causes an email written by the MDM to appear as independently confirmed by two sources. To the LLM, this looks like multi-source corroboration and inflates its confidence. The LLM cannot distinguish a native value from a writeback echo. | Tag writeback-origin fields with `writeback_echo: true` at ingest (§4.48). Strip echo-tagged fields from the record representation fed to the LLM prompt, or annotate them explicitly: `email (writeback-echo — single source)`. |
| **GDPR purpose limitation on LLM calls (§4.49)** | Sending a record pair to an LLM for curation is a processing operation. If the two records were collected under different legal bases (e.g., support ticket email vs. marketing CRM email), cross-referencing them — even for identity matching — may require a compatible legal basis. The LLM call itself is the processing act, not just storage. | Confirm with legal counsel that the curation processing purpose is covered by the DPIA. Self-hosted LLMs reduce the data transfer dimension of this concern. Do not send records to an external API if they contain health, financial, or legally privileged information without explicit DPA and legal review. |

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

3. **Add normalisation expressions.** Ensure every identity field has appropriate `normalize:` in the OSI-mapping YAML — `lower(trim(email))`, phone formatting, name standardisation. Coerce empty strings to NULL (`NULLIF(trim(field), '')`) for all identity fields (§4.46).

4. **Run a character encoding and locale audit (§4.39, §4.50).** If identity fields contain non-ASCII characters, add Unicode NFC normalisation. Identify any sources using non-Latin scripts or regional transliteration. Confirm the date format convention (MM/DD vs DD/MM) for any source using DOB or date-based identity fields, and coerce all dates to ISO 8601 at the forward view layer before bootstrap.

5. **Define link groups for weak fields.** If using name-based matching, require composite keys (first_name + last_name + DOB) rather than individual fields.

6. **Check for surrogate key leakage (§4.42).** For every field declared in `identity_fields` in the connector YAML, confirm it is semantically meaningful to an external observer. Sequential integers and system-internal GUIDs must not be declared as identity fields. Run a cardinality check: any identity field with cardinality equal to total record count is a surrogate key.

7. **Filter test and sandbox records (§4.47).** Count records matching known test patterns (`@example.com`, `test@*`, `DO NOT DELETE`, `555-01xx` phone ranges). Any count > 0 must be addressed with a connector-level `exclude_filter` before bootstrap.

8. **Document platform deduplication behaviour per source (§4.53).** Record whether the source system enforces uniqueness on email, phone, or other identity fields at the platform level. High within-source duplicate rates require intra-source deduplication (§4.27) before cross-source matching.

9. **Annotate entity granularity (§4.59).** For each connector, declare `record_granularity: entity` or `record_granularity: occurrence` in the YAML. Occurrence-granularity sources require deduplication in the forward view before identity resolution.

10. **Inventory dual-relationship records (§4.51).** Ask: does this source contain records for individuals who also appear in other sources in a different capacity (vendor AND customer; employee AND patient)? If yes, enumerate cases and pre-create block-merge overrides before bootstrap.

11. **Define conflict resolution policy per non-identity field (§4.55).** Before bootstrap, specify `source_priority` or `most_recent` policy for each field in the golden record YAML. Annotate fields where NULL means deliberately cleared, not unknown (`null_means_cleared: true`).

12. **Document target-system field constraints for writeback (§4.56).** For each system that will receive writeback, query its schema for field length, character set, and format constraints. Declare them in the connector YAML. Any identity field that the target system transforms must not be used for cross-system matching via that system's re-ingested copy.

13. **Set cluster size alerts.** Configure monitoring to alert if any cluster exceeds a reasonable threshold (e.g., 100 records).

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

10. **Periodically audit the override table.** Review overrides whose `review_due_at` date has passed (§4.58). Check for dormant blocks — overrides where the two records no longer share any identity field values and the block is no longer preventing any merge. Remove or renew. Track the `blocks_created / blocks_reviewed / blocks_expired` ratio over time: a growing block count with low review rate indicates stale-block accumulation.

11. **Monitor override table count (§4.52).** Alert if the active override count drops by more than a threshold outside of an operator-initiated bulk action. A sudden drop from hundreds to zero indicates override table damage (migration, restore, or accidental truncation). Verify override table inclusion in the database backup and restore runbook.

12. **Run writeback round-trip hash checks (§4.56).** Periodically re-fetch written-back identity field values from target systems and compare them to what was written. Any mismatch indicates a field constraint violation (truncation, charset narrowing, stripping) that is silently creating false negatives in identity matching. Add the affected field to the connector's constraint documentation.

13. **Audit for writeback-induced corroboration (§4.48).** Regularly query for clusters where all sources' copies of an identity field value arrived at approximately the same time following a writeback event — these are echo clusters masquerading as multi-source corroboration. Use the `writeback_echo` provenance flag (if implemented) to identify which identity field instances are MDM-originated rather than natively observed.

14. **Feedback loop to identity rules.** If the same pattern keeps appearing in the curation queue (e.g., shared family emails), adjust the OSI-mapping YAML — add a link group, change the identity field, or add a normalisation expression. The goal is for the curation queue to shrink over time as the rules improve.

---

## 11. Open Questions

1. **Should OSI-mapping natively support a link override table?** Currently, implementing overrides requires modifying the generated `_id_{target}` view SQL. It would be cleaner if OSI-mapping's YAML supported an `overrides:` section that the engine incorporates automatically. This is a feature request for OSI-mapping, not pg-trickle.

2. **How should `no_link` overrides interact with transitive closure?** If A–B is blocked but A–C and B–C exist, should A and B still end up in the same cluster (via C)? Probably yes — `no_link` means the direct edge is removed, not that the two records can never be in the same cluster. But some operators may expect `no_link` to mean "these are definitely different entities, keep them apart no matter what," which requires a stronger constraint (forced cluster separation).

3. **Should curation apply to all data types or just contacts?** Companies, deals, and other entity types have the same linkage risks but typically lower volumes. The curation mechanism should be generic (per-target, not per-data-type), but the review priority should focus on the entity types with the highest writeback impact.

4. **What is the minimum viable curation UI?** An MVP might be a read-only view of the curation queue table plus a form for submitting override decisions. A production system would need side-by-side record comparison, cluster graph visualisation, bulk accept/reject, and integration with the shadow mode dashboard from [PLAN_ONBOARDING_SHADOW_MODE_EXTENDED.md](PLAN_ONBOARDING_SHADOW_MODE_EXTENDED.md) §10.

5. **How does curation interact with real-time writeback?** In steady state (post-onboarding), should new links always be auto-accepted, or should the curation queue remain active? If active, there must be a mechanism to hold writeback for clusters with pending curation decisions — essentially permanent shadow mode for uncertain links.

6. **Can probabilistic scoring supplement deterministic linkage without replacing it?** A hybrid model: deterministic matching for the `_id_{target}` view (fast, SQL-native), supplemented by a Python/external scoring step that evaluates uncertain edges and auto-accepts or queues them. This keeps the pg-trickle pipeline simple while adding sophistication where needed.

7. **What is the right LLM deployment model for curation?** Self-hosted models avoid PII concerns but require GPU infrastructure. API-based models (with DPAs) are simpler to deploy but add latency and cost. For onboarding (batch, non-real-time), API-based is likely acceptable. For steady-state continuous curation, self-hosted may be required for cost and latency reasons.

8. **Should LLM curation decisions be treated as first-class overrides or as soft suggestions?** If LLM decisions go directly to the override table, they have the same authority as human decisions and persist until explicitly removed. If they are treated as suggestions, they need human confirmation but reduce the cognitive load per decision. The choice depends on the organisation's risk tolerance and the LLM's calibrated accuracy.

9. **Who owns the sync completion marker — the connector or pg-trickle?** §4.54 proposes that pg-trickle's identity refresh be gated on a `sync_completed_at` marker written by the connector. Should this be a first-class pg-trickle feature (a `require_sync_complete: true` gate on a stream table) or an external orchestration responsibility handled outside the scheduler? If external, how is it integrated with pg-trickle's tiered scheduling?

10. **How should occurrence-granularity sources be modelled in OSI-mapping YAML (§4.59)?** Currently there is no `record_granularity` concept in OSI-mapping. The workaround is deduplication in the forward view. Should OSI-mapping natively support an `occurrence_key` annotation that causes the `_fwd_*` layer to deduplicate automatically before identity resolution? Or is forward-view deduplication sufficient as a connector-authoring convention?

11. **What is the right architecture for the identity edge table and cluster assignment history (§4.57)?** The view pipeline currently produces no log. The edge table and history could be: (a) maintained by a post-refresh trigger on `_id_{target}`, (b) computed by a separate materialisation job outside pg-trickle, or (c) a native pg-trickle feature that snapshots cluster assignments on each refresh. Option (c) would require a pg-trickle extension. Options (a) and (b) are implementable today but add pipeline complexity. What is the right tradeoff?

12. **Where should conflict resolution policy per field be defined — OSI-mapping YAML or the bridge layer (§4.55)?** Field conflict resolution determines what value populates the golden record and therefore what gets written back. It could live in the OSI-mapping connector YAML (alongside the identity field definitions), in the bridge layer SQL (as CASE expressions over ranked sources), or in a dedicated resolution config file. Each has different maintenance implications. Currently there is no standardised location.
