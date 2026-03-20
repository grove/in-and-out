# Impact Report: Extending Writeback Beyond HTTP APIs

**Date:** 19 March 2026
**Scope:** Analysis of the consequences of adding support for non-HTTP writeback targets — Kafka, NATS, relational databases, and other message/event systems — evaluated against the current GOAL.md specification.

**Companion document:** See `REPORT_IMPACT_NON_HTTP_INGESTION.md` for the equivalent analysis on the ingestion side.

---

## 1. Executive Summary

The writeback tool (Tool 2) is designed to push MDM-refined data into external HTTP APIs. Its 34 requirements assume an HTTP request-response model: the tool sends a payload, receives a response with a status code and body, captures generated IDs, and detects conflicts via pre-flight reads and conditional writes.

Extending writeback to non-HTTP targets introduces a fundamentally different delivery model. HTTP APIs are **synchronous, stateful, and queryable** — you send a request, get a response, and can read back what you wrote. Message brokers are **asynchronous, append-only, and non-queryable** — you produce a message and receive at best an acknowledgement that the broker accepted it, with no way to read back the current state of a "record." Relational databases sit between the two — synchronous and queryable, but with different write semantics (SQL DML instead of HTTP verbs).

The impact is more severe than on the ingestion side. Approximately **18 of 34 writeback requirements** are HTTP-specific or rely on HTTP semantics that don't transfer. Several core features — conflict detection, client-side patching, response capture, partial-success batch handling — are architecturally impossible against append-only message targets.

This report catalogues every point of friction, assesses its severity, and proposes the minimal design changes needed to keep the door open.

---

## 2. Target Types Under Consideration

| Target Type | Examples | Write Model | Confirmation Model | Queryable? |
|---|---|---|---|---|
| HTTP API (current) | HubSpot, Salesforce, Stripe | Request-response (POST/PUT/PATCH/DELETE) | Synchronous — HTTP status code + response body | Yes (GET) |
| Message broker (log) | Kafka, Redpanda | Produce message to topic/partition | Broker ack (offset assigned) — no target-system confirmation | No |
| Message broker (JetStream) | NATS JetStream | Publish to stream/subject | Broker ack (sequence assigned) | No (can replay, but not query by key) |
| Message broker (ephemeral) | NATS Core, Redis Pub/Sub | Publish to subject/channel | Fire-and-forget or basic ack | No |
| Relational database | MySQL, SQL Server, Oracle | SQL INSERT/UPDATE/DELETE | Synchronous — row-affected count, RETURNING clause | Yes (SELECT) |
| Document database | MongoDB, DynamoDB | Insert/update/delete document | Synchronous — operation result with generated ID | Yes (find/get) |
| Graph database | Neo4j | Cypher MERGE/CREATE/DELETE | Synchronous — operation result | Yes (MATCH) |

---

## 3. What Already Works (Target-Agnostic)

The following writeback requirements are independent of the write transport and will carry forward without modification:

| Area | Requirement(s) | Why It's Agnostic |
|---|---|---|
| Desired-state input table model | T2 #7 | The input is always PostgreSQL logical replication or polling — transport-agnostic by design |
| Near real-time trigger mechanism | T2 #10 | Trigger is PostgreSQL-side (logical replication / polling) — not affected by the write target |
| REPLICA IDENTITY FULL | T2 #22 | PostgreSQL configuration — independent of write target |
| Pre-write data transformation | T2 #17 | Reshaping data is needed for all targets, not just HTTP |
| Pre-write payload validation | T2 #23 | Validating the payload before sending applies to any target |
| Write ordering for the same record | T2 #28 | Ordering is an internal queue concern, not a transport concern |
| Dependency ordering within a batch | T2 #26 | Topological sort is an internal concern |
| Dry-run / preview mode | T2 #27 | "Don't send, just log" works for any transport |
| Dead-letter queue | T2 #24 | Failed writes go to DLQ regardless of how they failed |
| Writeback circuit breaker | T2 #25 | Concept applies to any target — triggers may differ |
| Replication slot health monitoring | T2 #32 | PG-side monitoring — independent of write target |
| Write batch composition | T2 #33 | Batch assembly is an internal concern |
| Cluster merge & split propagation | T2 #34 | Internal action semantics — target-agnostic |
| Smart writes (change detection) | T2 #2 | Diff computation happens locally before the write |
| Separate processing paths per op | T2 #15 | Architectural decision, not transport-bound |

**Assessment:** Approximately 15 of 34 requirements (~44%) are target-agnostic. This is a weaker starting position than the ingestion side (which was ~60% transport-agnostic), because the writeback tool's value proposition — conflict detection, identity capture, audit logging — is deeply intertwined with the HTTP response model.

---

## 4. What Breaks or Doesn't Map

### 4.1 The Fundamental Problem: Message Brokers Are Not Queryable

The most severe architectural mismatch is between writeback requirements that assume the target system can be **read before or after a write**, and message brokers which are **write-only from the producer's perspective**.

The following capabilities require reading the target system's current state:

| Capability | Why It Needs a Read | Consequence for Message Targets |
|---|---|---|
| **Conflict detection (#3, #30)** | Must fetch the current state to compare against the expected base | **Impossible** — Kafka/NATS have no "current state of a record" concept. A topic is a log, not a table. |
| **Base-aware updates (#4)** | Must read current state for three-way merge | **Impossible** — same reason |
| **Client-side patching (#5)** | Must fetch current state, compute diff, send minimal update | **Impossible** — message targets accept complete messages, not patches |
| **Delete safety guard (#31)** | Must verify record still exists and matches expected state | **Impossible** — no query API |
| **Identity mapping (#8)** | Must capture generated ID from the target system's response | **Partially applicable** — brokers don't generate domain-relevant IDs; they assign offsets/sequence numbers, which are transport metadata, not business identifiers |
| **External reference field writeback (#16)** | Populates a field on the target system's record | **N/A** — messages don't have mutable fields that other systems read by key |
| **Duplicate insert prevention (#14)** | Verifies against the target before retrying | **Cannot query target** — must rely on idempotent message keys instead |

**This is not a gap that can be bridged with clever engineering.** If the target is append-only and non-queryable, then conflict detection, base-aware merging, and client-side patching are architecturally excluded. The writeback tool for message targets is a fundamentally simpler (but different) component: it is a **state-change publisher**, not a **state synchroniser**.

### 4.2 Detailed Requirement-by-Requirement Impact

#### T2 #1 — Per-Datatype Mapping
**Current:** Map MDM table to HTTP API endpoint/datatype.
**Impact:** For Kafka, the mapping is to a topic (and optionally a partition key strategy). For NATS, to a subject. For databases, to a table. The mapping concept transfers, but the target coordinates are transport-specific.
**Severity:** Low — config schema needs a target-type discriminator, but the concept is the same.

#### T2 #3 — Conflict Resolution & Prevention
**Current:** Pre-flight state checks, OCC, HTTP conditional requests (If-Match, ETags).
**Impact:** **Does not apply to message brokers.** Kafka/NATS are append-only logs — there is no "current state" to conflict with. For relational databases, conflict detection maps to `SELECT ... FOR UPDATE` or `WHERE version = ?` — different mechanics but the same concept. For document databases, conditional writes (e.g., DynamoDB's `ConditionExpression`) are analogous to HTTP conditional requests.
**Severity:** High for message targets (entire feature absent); Low for database targets (mechanics differ but concept transfers).

#### T2 #4 — Base-Aware Updates
**Current:** Three-way merge against current state in the target system.
**Impact:** **N/A for message targets.** Kafka/NATS messages are immutable once produced. For databases, achievable via SQL-level comparison.
**Severity:** High for message targets; Low for databases.

#### T2 #5 — Client-Side Patching
**Current:** Lookup-diff-write cycle (GET → diff → PATCH).
**Impact:** **N/A for message targets.** For databases, translates to `UPDATE ... SET field1 = $1 WHERE pk = $2` — the concept of "partial update" exists but is expressed in SQL, not as a PATCH verb.
**Severity:** High for message targets; Low for databases.

#### T2 #6 — CRDT Support
**Current:** Leverage target system's CRDT structures for conflict-free updates.
**Impact:** **Potentially relevant for some databases** (e.g., some distributed databases have CRDT support). **N/A for message brokers** — there's no state to merge. Kafka-backed CRDT implementations exist but are application-layer, not something the writeback tool would drive.
**Severity:** Medium — niche, already optional.

#### T2 #8 — Identity Mapping
**Current:** Capture generated ID from HTTP response (e.g., `201 Created` with body containing the new ID).
**Impact:** **Message brokers don't generate business IDs.** A Kafka producer receives an offset confirmation — that's transport metadata, not the external system's ID for the record. If the writeback publishes a message and a downstream consumer creates the record, the generated ID flows back through a *separate* channel (another topic, a callback, etc.) — not in the broker's produce response.
For databases, `INSERT ... RETURNING id` gives you the generated ID synchronously — directly analogous to HTTP.
**Severity:** High for message targets (identity capture requires a feedback channel that doesn't exist in the produce path); Low for databases.

#### T2 #11 — Politeness & Rate Limiting
**Current:** HTTP rate limiting (429 handling, backoff, request throttling).
**Impact:** For Kafka, rate limiting is managed by producer backpressure (`buffer.memory`, `max.block.ms`, batching config) — the broker doesn't return 429s. For NATS, JetStream has flow control and `max_bytes` on streams. For databases, connection pool limits and statement timeouts serve a similar role.
**Severity:** Medium — the concept applies (don't overwhelm the target) but the mechanisms are entirely different.

#### T2 #12 — API Asymmetry Handling
**Current:** Mapping between read/write schema differences in the same HTTP API.
**Impact:** **Partially N/A for message brokers.** You don't read and write the same record via a broker — you just publish messages. Schema asymmetry exists (producer schema vs. consumer expectation) but is managed through a schema registry, not through declarative field mapping in the writeback tool. For databases, column mapping between the MDM canonical form and the target table schema is directly analogous.
**Severity:** Low for databases; Medium for message targets (different mechanism).

#### T2 #13 — Response Capture & Audit Logging
**Current:** Capture full HTTP response (status code, headers, body) after every write.
**Impact:** For Kafka, a successful produce returns `RecordMetadata` (topic, partition, offset, timestamp) — useful but much less information. There are no headers, no status code, no body. For NATS, a JetStream publish ack returns stream name, sequence, and duplicate flag — even less. For databases, `INSERT ... RETURNING` or affected-row counts are the response. The concept of audit logging transfers, but the audit record structure must be transport-specific.
**Severity:** Medium — audit logging is essential but the captured data differs fundamentally.

#### T2 #14 — Duplicate Insert Prevention / Idempotency Guards
**Current:** Verify against write log that the entity hasn't already been delivered.
**Impact:** For Kafka, idempotency is available natively via producer `idempotence=true` and transactional producers — this is a transport configuration, not application logic. For NATS JetStream, deduplicate using `Nats-Msg-Id` header. For databases, unique constraints (or `INSERT ... ON CONFLICT DO NOTHING`) handle it at the target level.
**Severity:** Medium — the problem is universal, but the solution shifts from application-layer write-log checking to transport-native idempotency features.

#### T2 #16 — External Reference Field Writeback
**Current:** Populate a target system field with the MDM `cluster_id`.
**Impact:** **N/A for message brokers** — messages are immutable and don't have "fields that other applications query by." For databases, this is a simple column write — directly applicable.
**Severity:** High for message targets (no equivalent concept); None for databases.

#### T2 #18 — Datatype-Specific Operation Configuration
**Current:** Per-datatype HTTP methods, URL patterns, headers, payload structures.
**Impact:** For Kafka, per-datatype config maps to: topic name, serialisation format, partition key expression, headers. For NATS, to: subject pattern, serialisation. For databases, to: target table, column mapping, SQL template per operation type. The concept is the same — per-datatype write instructions — but the config structure is entirely transport-specific.
**Severity:** Medium — requires transport-specific config schema sections.

#### T2 #19 — Upsert Write Strategy
**Current:** Route to a target API's upsert endpoint.
**Impact:** For Kafka, upsert is inherent — producing a message with the same key overwrites the previous value in a compacted topic. For databases, `INSERT ... ON CONFLICT DO UPDATE` (PostgreSQL), `MERGE` (SQL Server, Oracle), or `REPLACE INTO` (MySQL). The concept transfers but the implementation is native to each target.
**Severity:** Low — implementation differs, concept is universal.

#### T2 #20 — Archive Action Type
**Current:** Trigger a soft-delete in the target system.
**Impact:** **N/A for message brokers** — you can produce a "status: archived" message, but you can't mutate existing records. For databases, this is an `UPDATE ... SET archived = true` — directly applicable.
**Severity:** High for message targets (no state mutation); None for databases.

#### T2 #21 — Transaction-Level Atomicity
**Current:** Process same-MDM-transaction records as an atomic unit.
**Impact:** For Kafka, transactional producers (`initTransactions()`, `beginTransaction()`, `commitTransaction()`) provide exactly-once semantics across multiple topic-partitions — directly analogous. For databases, `BEGIN / COMMIT` wraps multiple DML statements atomically — even stronger than HTTP (where no cross-request atomicity exists). For NATS, no native transaction support.
**Severity:** Low for Kafka and databases (native support exists); High for NATS Core.

#### T2 #25 — Writeback Circuit Breaker
**Current:** Triggers on sustained HTTP 500s/503s.
**Impact:** For Kafka, circuit-breaker triggers would be: sustained `ProducerFencedException`, `OutOfOrderSequenceException`, or broker unreachability. For databases, sustained connection failures or deadlock storms. The concept transfers; the trigger conditions are transport-specific.
**Severity:** Low — concept is universal, triggers differ.

#### T2 #29 — Partial-Success Batch Response Handling
**Current:** Parse HTTP 200/207 mixed responses to classify per-record outcomes.
**Impact:** For Kafka, a batch produce either succeeds entirely or fails entirely (with transactional producers). There is no partial-success response to parse. For databases, a multi-statement transaction either commits or rolls back — again, no partial success in the HTTP 207 sense. Partial success is an HTTP API quirk.
**Severity:** High as an HTTP-specific concern — but the solution for other transports is simpler (not harder): they don't have the problem.

#### T2 #30 — Conflict Detection — Resolution Path
**Current:** Declared strategies (dead-letter, last-writer-wins, skip-and-warn) when target state differs from expected.
**Impact:** **N/A for message brokers** — no target state to conflict with. For databases, conflict detection maps to SQL-level version checks. The resolution strategies are still useful for database targets.
**Severity:** High for message targets; Low for databases.

#### T2 #31 — Delete Safety Guard
**Current:** Verify record exists and matches expected state before deleting.
**Impact:** **N/A for message brokers** — you can't delete a message in a topic. For databases, achievable via `DELETE ... WHERE pk = $1 AND version = $2` with row-count check.
**Severity:** High for message targets; Low for databases.

---

## 5. Classification Summary

### Requirements grouped by target-type compatibility

**Fully target-agnostic (work as-is for all targets):** 15 requirements
T2 #2, #7, #10, #15, #17, #22, #23, #24, #26, #27, #28, #32, #33, #34, #35 (cross-cutting)

**Work for databases, N/A for message brokers:** 9 requirements
T2 #3, #4, #5, #8, #16, #19, #20, #30, #31

**Concept transfers but mechanics differ across all transports:** 7 requirements
T2 #1, #11, #13, #14, #18, #21, #25

**HTTP-specific (no equivalent for non-HTTP targets):** 3 requirements
T2 #6 (CRDT — niche), #12 (API asymmetry — HTTP-specific framing), #29 (partial-success batch — HTTP 207 quirk)

### Severity matrix

| Target Type | Requirements that work as-is | Requirements needing adaptation | Requirements that are N/A | Net fit |
|---|---|---|---|---|
| HTTP API (current) | 34/34 | 0 | 0 | 100% |
| Relational database | 31/34 | 3 (mechanics differ) | 0 | ~91% |
| Document database | 29/34 | 5 (mechanics differ) | 0 | ~85% |
| Kafka (transactional) | 15/34 | 7 (concept transfers) | 12 (N/A) | ~44% |
| NATS JetStream | 15/34 | 5 (concept transfers) | 14 (N/A) | ~41% |
| NATS Core (ephemeral) | 15/34 | 3 (concept transfers) | 16 (N/A) | ~38% |

---

## 6. The Two Worlds: State Synchroniser vs. Event Publisher

The writeback tool as currently specified is a **state synchroniser**: it reads the desired state, compares it to the current state in the target, resolves conflicts, and writes the minimum delta to bring the target into alignment.

Writing to a message broker is fundamentally different. It is **event publishing**: the tool reads a state change from the desired-state table and publishes it as a message. There is no read-before-write, no conflict detection, no last-written-state comparison — just produce and confirm.

These are two legitimately different tools with different value propositions:

| Dimension | State Synchroniser (HTTP / DB) | Event Publisher (Kafka / NATS) |
|---|---|---|
| Core operation | Read target state → compute delta → write delta | Serialise desired-state → produce message |
| Conflict detection | Yes — central value prop | No — not applicable |
| Identity mapping | Yes — capture generated IDs | No — broker doesn't generate business IDs |
| Idempotency | Write-log + pre-flight check | Transport-native (idempotent producer, dedup headers) |
| Response capture | Rich (status code, body, headers) | Minimal (ack with offset/sequence) |
| Partial updates | PATCH / minimal diff | Full message replacement (on compacted topics) or append |
| Deletion | DELETE request or soft-delete update | Tombstone message (null value for key on compacted topic) or status-change message |
| Audit trail | Last-written-state table tracking confirmed writes | Produce-confirmation log tracking broker acks |
| Transaction atomicity | Application-level (transaction groups, dependency ordering) | Transport-native (Kafka transactions) or unavailable (NATS) |

### Implications for architecture

There are two viable architectural approaches:

**Option A: One tool, two adapter types.** The writeback engine contains a core orchestration layer (desired-state consumption, batch composition, ordering, DLQ) with pluggable write adapters. An HTTP adapter implements the full state-synchronisation flow. A Kafka adapter implements the simpler event-publishing flow, skipping inapplicable steps. The engine uses a capabilities declaration to decide which steps to run.

**Option B: Two separate tools.** The state synchroniser (HTTP/DB writeback) remains as specified. A separate, simpler "Event Publisher" tool reads desired-state tables and produces messages. It reuses the same input contract (desired-state tables, same MDM Contract) but has a much smaller feature set — no conflict detection, no identity mapping, no response capture. It has its own codebase, requirements, and configuration schema.

| | Option A (single tool, adapters) | Option B (separate tools) |
|---|---|---|
| **Pros** | Shared infrastructure (scheduling, DLQ, monitoring); single operational surface; easier to add new targets | Simpler per-tool; no feature gating; event publisher can be very lightweight; less risk of over-abstracting |
| **Cons** | Complex capability gating; risk that the adapter interface is designed around HTTP and can't cleanly accommodate message targets; "one tool that does two very different things" | Duplicated infrastructure (DLQ, monitoring, batch composition); two operational surfaces; two config schemas; two CLIs |

---

## 7. Impact on the MDM Contract and Shared Infrastructure

### Desired-state table model

The desired-state table model (T2 #7) works for all targets with one extension: message targets need a **routing key expression** (Kafka partition key, NATS subject suffix) that is not required for HTTP targets. This can be an optional column or a config-level expression that computes the routing key from the record's data.

For message targets, the `action` semantics shift:
- `insert` → Produce a "created" message
- `update` → Produce an "updated" message (or a full-state message on a compacted topic)
- `delete` → Produce a tombstone message (null value for the key, on compacted topics) or a "deleted" event
- `archive` → Produce an "archived" status message (not a state mutation)
- `merge` / `split` → Produce corresponding identity-change events
- `noop` → Skip (same as current)

The desired-state table needs no structural changes — only the interpretation of actions differs.

### Identity mapping tables

For HTTP/database targets, the identity mapping table captures the link between `cluster_id` and the generated external ID. For message targets, there is no generated external ID — the `cluster_id` or a derived key *is* the message key. The mapping table is either unused or trivially populated (key = cluster_id).

This means the identity mapping table should be declared as **optional per connector**. Empty or absent mapping tables must not cause errors in the MDM layer or other shared components.

### Sync-run log

The sync-run log schema works for all targets. For message targets, `records_written` would mean "messages successfully produced and acked," and `records_errored` would mean "produce failures." The same log serves both.

### Audit logging (response capture)

The current response capture (T2 #13) expects HTTP-like responses. For non-HTTP targets, the "response" structure must be transport-specific:

| Target | Captured "Response" |
|---|---|
| HTTP | Status code, headers, body |
| Kafka | Topic, partition, offset, timestamp |
| NATS JetStream | Stream, sequence, duplicate flag |
| Database | Affected row count, RETURNING row (if applicable), SQL state |

The audit table must use a JSONB column for the response data rather than structured HTTP-specific columns, so it can accommodate any transport's confirmation metadata.

### Dead-letter queue

DLQ is target-agnostic. The dead-letter entry includes the original desired-state record, the error detail, and diagnostic context — all of which are the same regardless of how the write failed. The only difference is what the "error response" looks like (Kafka `ProducerException` vs. HTTP 422 response body).

---

## 8. Impact on the Error Taxonomy

The Cross-Cutting Concerns error classification table is HTTP-centric:

| Current Class | Current Examples | Non-HTTP Equivalents |
|---|---|---|
| Retryable transient | Network timeout, 503, connection reset | Kafka: `NetworkException`, `NotLeaderForPartitionException`. NATS: connection lost. DB: connection reset, lock timeout. |
| Rate limit | 429 with Retry-After | Kafka: `ProducerFencedException` (not exactly rate-limit, but requires backoff). NATS: JetStream slow consumer. DB: connection pool exhaustion. |
| Auth error | 401, expired token | Kafka: `AuthenticationException`, `AuthorizationException`. NATS: auth timeout. DB: auth failure (ORA-01017, SQLSTATE 28000). |
| Data / validation error | 422, 400, schema mismatch | Kafka: `SerializationException` (schema registry mismatch). DB: constraint violation, type error. |
| Config error | Missing required field, unknown endpoint | Kafka: unknown topic. NATS: unknown stream. DB: unknown table or column. |

The taxonomy categories are transport-neutral and work well. Only the examples column needs expansion.

---

## 9. Recommended Architectural Changes

### 9.1 Introduce a Write Adapter Abstraction

Define the write interface at a level above HTTP:

```
WriteAdapter (abstract):
  - connect(credentials) → Connection
  - write(operation: insert|update|delete|archive|merge|split, record, metadata) → WriteResult
  - write_batch(operations[]) → BatchWriteResult
  - lookup(external_id) → Optional<CurrentState>  # Optional — not all targets support this
  - disconnect()
  - capabilities() → { supports_lookup, supports_conflict_detection, supports_partial_update, supports_transactions, supports_generated_ids, delivery_guarantee }

WriteResult:
  - status: success | failed | conflict
  - generated_id: Optional<string>
  - response_metadata: JSONB  # Transport-specific confirmation data
  - error_detail: Optional<JSONB>

BatchWriteResult:
  - per_record_results: WriteResult[]
  - atomic: boolean  # Whether the batch was processed atomically
```

The `lookup` method is optional — message broker adapters would return `None` or raise `NotSupported`. The engine skips conflict detection, base-aware updates, and delete safety guards when `supports_lookup` is false.

### 9.2 Declare Transport Capabilities in Config

```yaml
target:
  transport: http | kafka | nats | database
  capabilities:
    supports_lookup: true           # Can read current state from target
    supports_conflict_detection: true
    supports_partial_update: true    # PATCH equivalent
    supports_transactions: false     # Atomic multi-record writes
    supports_generated_ids: true     # Target generates IDs on insert
    delivery_guarantee: at-least-once
```

For well-known transports, most capabilities can be inferred from the transport type with connector-level overrides.

### 9.3 Scope HTTP-Specific Requirements

Mark the following requirements as transport-conditional:

| Requirement | Applicability |
|---|---|
| T2 #3 Conflict Resolution | `[Requires: supports_lookup]` |
| T2 #4 Base-Aware Updates | `[Requires: supports_lookup]` |
| T2 #5 Client-Side Patching | `[Requires: supports_lookup, supports_partial_update]` |
| T2 #6 CRDT Support | `[Requires: supports_crdt]` |
| T2 #8 Identity Mapping | `[Requires: supports_generated_ids]` |
| T2 #12 API Asymmetry Handling | `[HTTP]` |
| T2 #16 External Reference Field | `[Requires: supports_lookup]` |
| T2 #29 Partial-Success Batch | `[HTTP]` |
| T2 #30 Conflict Resolution Path | `[Requires: supports_lookup]` |
| T2 #31 Delete Safety Guard | `[Requires: supports_lookup]` |

### 9.4 Generalise Response Capture

Redefine T2 #13 (Response Capture & Audit Logging) to use a JSONB response column rather than HTTP-specific structured columns:

- HTTP: `{"status_code": 201, "headers": {...}, "body": {...}}`
- Kafka: `{"topic": "contacts", "partition": 3, "offset": 14502, "timestamp": "..."}`
- Database: `{"rows_affected": 1, "returning": {...}, "sql_state": "00000"}`

This is a schema design decision, not a requirement change — but it must be made before the audit table schema is finalised.

### 9.5 Add Message Target–Specific Concepts

For message broker targets, a few concepts need to be added that don't exist in the HTTP model:

| Concept | Description | Where It Lives |
|---|---|---|
| **Routing key expression** | Computes the Kafka partition key or NATS subject from record data | Per-datatype config |
| **Serialisation format** | Avro, Protobuf, JSON — with optional schema registry integration | Per-connector config |
| **Compaction semantics** | Whether the target topic is compacted (enabling "last value per key" = state) or append-only (log of events) | Per-datatype config — this determines whether an `update` replaces or appends |
| **Tombstone convention** | How to represent deletion: null-value message, or explicit "deleted" event payload | Per-datatype config |
| **Schema registry** | Integration with Confluent Schema Registry or equivalent for schema evolution | Per-connector config |

### 9.6 Expand the Simulator Contract

For message broker targets, the simulator must be an embedded broker (Testcontainers) or a mock that can:
- Accept produced messages and store them in memory
- Return the expected ack metadata (offset, sequence)
- Simulate failure conditions (broker unavailable, auth failure, produce timeout)
- Allow assertions on the produced messages (count, content, ordering, keys)

For database targets, the simulator is a test database instance with the expected schema pre-loaded.

---

## 10. Impact on the Implementation Plan

| Step | Impact |
|---|---|
| **#1 Configuration Design** | Config schema needs a `transport` discriminator and transport-specific sections (topic names, serialisation, SQL templates) alongside the current HTTP operation definitions. |
| **#2 Database Architecture** | Audit/response capture tables should use JSONB for response data from day one, not HTTP-structured columns. |
| **#3 Simulator Framework** | Simulator contract must be defined at a transport-agnostic level. HTTP stub is one implementation; embedded Kafka and test databases are others. |
| **#5 Writeback Engine** | The engine must be designed around the write adapter interface, not around HTTP request building. The HTTP adapter is the first implementation, but the engine loop must not import HTTP concepts. |
| **#6 Operational CLI** | Minor: CLI commands for inspecting write results must display transport-specific response metadata from JSONB, not assume HTTP columns. |
| **#7 Connector SDK** | Connector authoring contract must define the adapter interface, not just the HTTP operation config. |

---

## 11. Cost-Benefit Assessment

### If we make no changes now

- The HTTP-only writeback implementation proceeds unimpeded.
- The engine will be built around HTTP request construction, HTTP response parsing, and HTTP conflict detection.
- When a non-HTTP target is needed, the engine's core write loop must be refactored to extract a write adapter interface from the hardcoded HTTP flow.
- The audit table schema (HTTP-structured) must be migrated to JSONB.
- The config schema must be extended with transport-specific sections.
- Estimated rework: **significant** — more intrusive than the ingestion side because the write loop (conflict detect → transform → validate → write → capture response → update state) assumes HTTP at every step.

### If we make the minimal changes now (recommended)

- **Define the write adapter interface** in the architecture phase, even with only HTTP as the first adapter.
- **Use JSONB for response metadata** in the audit table from day one — costs nothing extra.
- **Add transport applicability markers** to the ~10 requirements that are HTTP-conditional.
- **Add a target capabilities concept** to the config schema design.
- **No code changes** — these are specification and schema design decisions.
- Estimated cost: **trivial** — a few hours of document and schema design work.
- When the first non-HTTP target is needed: implement a new write adapter, add transport-specific config section, done. No engine refactoring.

### If we design the full adapter abstraction now

- Design and implement the write adapter interface before building the HTTP adapter.
- Higher upfront cost but enables rapidly adding Kafka, database, and NATS targets later.
- Risk: Over-engineering without a second adapter to validate the abstraction.
- This is more justified on the writeback side than the ingestion side, because the write loop has more steps that need to be conditional on capabilities (conflict detection, identity capture, response parsing).

---

## 12. Comparison with Ingestion-Side Impact

| Dimension | Ingestion (REPORT_IMPACT_NON_HTTP_INGESTION) | Writeback (this report) |
|---|---|---|
| Transport-agnostic share | ~60% | ~44% |
| Requirements that break entirely | ~6 of 46 (13%) | ~12 of 34 for message targets (35%) |
| Architectural severity | Medium — connector model needs a transport abstraction | High — the core write loop assumes read-before-write |
| Easiest non-HTTP target | Database polling (closest to HTTP pull model) | Relational database (~91% compatibility) |
| Hardest non-HTTP target | NATS Core (ephemeral, no replay) | NATS Core (ephemeral, no transactions, no query) |
| Single biggest gap | Shared HTTP Logic strategy bullet | Conflict detection requires queryable target |
| Recommended approach | Minimal spec changes now; transport adapter in architecture | Same, plus JSONB audit schema and capabilities gating |

The **writeback side has a harder problem** because the tool's core value proposition — conflict-safe state synchronisation — is fundamentally tied to the target being queryable. For message targets, a large percentage of the tool's sophistication (conflict detection, base-aware merging, client-side patching, delete safety guards) simply does not apply. The tool becomes a much simpler "transform-validate-produce" pipeline.

---

## 13. Conclusion

The writeback tool is **more tightly HTTP-coupled than the ingestion tool** (~56% of requirements are HTTP-specific or HTTP-centric, vs. ~40% on the ingestion side). The coupling is also deeper — it's not just transport mechanics (like pagination), but the tool's core value proposition (conflict detection, identity capture) that depends on HTTP response semantics.

Adding **relational database targets** is relatively straightforward (~91% requirement compatibility). SQL DML provides synchronous, queryable, transactional writes — closely analogous to HTTP APIs. The effort is primarily in config schema and SQL query generation.

Adding **message broker targets** is a more fundamental extension. It changes what the tool *does*, not just how it does it. For Kafka/NATS, the tool is not a state synchroniser — it is an event publisher. This is a simpler but genuinely different component.

The **recommended path** is the same as for ingestion: make the minimal specification and schema design changes now (write adapter interface, JSONB audit columns, capability gating, transport applicability markers) to avoid painting the architecture into an HTTP-only corner. The single highest-leverage decision is using **JSONB for response/audit metadata** — this is a schema choice made once in the Database Architecture step that either enables or blocks generic audit logging for all future transports.
