# Impact Report: Extending Ingestion Beyond HTTP APIs

**Date:** 19 March 2026
**Scope:** Analysis of the consequences of adding support for non-HTTP ingestion sources — Kafka, NATS, relational databases (via polling or CDC), and other message/event systems — evaluated against the current GOAL.md specification.

---

## 1. Executive Summary

The current GOAL.md is deeply coupled to HTTP as the ingestion transport. The word "HTTP" appears in the project title's objective, the Strategy section, and 20+ individual requirements. While many architectural decisions (JSONB storage, per-datatype tables, the MDM contract, observability, error classification) are transport-agnostic and will carry forward unchanged, the **connector runtime model**, **authentication framework**, **progress tracking**, and approximately **15 specific requirements** assume a request-response HTTP paradigm that does not map to message brokers or database CDC streams.

Adding non-HTTP sources is feasible without a ground-up rewrite, but it requires **introducing a transport abstraction layer** early in the architecture. If this abstraction is deferred until after the HTTP-only implementation hardens, the refactoring cost rises significantly — the HTTP assumptions will have leaked into engine internals, test infrastructure, configuration schema, and operational tooling.

This report catalogues every point of friction, rates its severity, and proposes the minimal architectural changes needed to keep the door open.

---

## 2. Source Types Under Consideration

| Source Type | Examples | Data Delivery Model | Progress Tracking Primitive |
|---|---|---|---|
| HTTP API (current) | HubSpot, Salesforce, Stripe | Request-response; tool pulls data | High-water mark (timestamp or cursor) |
| Message broker (log-based) | Kafka, Redpanda | Persistent subscription; broker pushes | Consumer group offset per partition |
| Message broker (at-most-once) | NATS Core | Ephemeral subscription; broker pushes | None (no replay guarantee) |
| Message broker (JetStream) | NATS JetStream | Persistent subscription; broker pushes | Stream sequence number + consumer ack |
| Relational database (polling) | MySQL, Oracle, SQL Server | Tool pulls via SQL query | High-water mark (column value) |
| Relational database (CDC) | PostgreSQL logical replication, Debezium | Persistent stream; DB pushes change events | LSN (Log Sequence Number) or slot position |
| Event bus / webhook relay | AWS EventBridge, Google Pub/Sub | Persistent subscription; push delivery | Message ID + ack |

---

## 3. What Already Works (Transport-Agnostic)

The following design decisions in GOAL.md are independent of how data arrives and will work without modification for any source type:

| Area | Requirement(s) | Why It's Agnostic |
|---|---|---|
| Per-datatype tables with JSONB | T1 #1, #2 | The storage schema cares about the data shape, not how it was fetched |
| Standard metadata columns | T1 #2 | `external_id`, `data`, `raw`, `_ingested_at`, `_sync_run_id`, `_raw_hash`, `_deleted`, etc. are all source-independent |
| Tombstone records on deletion | T1 #32 | Deletion representation is a storage concern |
| Soft-delete resurrection | T1 #41 | Re-activation logic is storage-layer |
| Change history / selectable history mode | T1 #15, #30 | Append vs. overwrite is a write-side decision |
| Schema change tracking | T1 #31 | Detecting field drift in the incoming payload applies to any source |
| Intra-sync deduplication | T1 #33 | Dedup by primary key before write — source-independent |
| Out-of-order event handling | T1 #35 | More relevant for streams than HTTP, actually — already well-specified |
| Timestamp normalisation | T1 #45 | Applies to any timestamp, regardless of transport |
| Multi-connector fan-in policy | T1 #46 | Table-ownership rules don't depend on transport |
| Read-only datatype support | T1 #23 | Ingestion-only flag is metadata |
| Relationship datatypes | T1 #22 | How relationships are stored is transport-agnostic |
| Per-datatype concurrency control | T1 #36 | Advisory lock mechanism works for any source |
| Connector validation mode | T1 #43 | Concept applies to all sources; validation steps differ per transport |
| Source unavailability handling | T1 #44 | Conceptually identical across transports |
| The entire MDM Contract | All | Communication is through PostgreSQL — transport is invisible to the MDM layer |
| All Cross-Cutting Concerns | All | Error taxonomy, observability, PII, retention, health endpoints — all transport-agnostic |
| The entire Writeback tool | All T2 | Writeback is HTTP-only by design; adding ingestion sources doesn't affect it |

**Assessment:** Roughly 60% of the specification is already transport-neutral. This is a strong foundation.

---

## 4. What Breaks or Doesn't Map

### 4.1 Strategy-Level Issues

#### S-1: "Shared HTTP Logic" — Too Narrow

**Current text:** _"Since most external systems communicate via HTTP APIs, the underlying execution engine will share HTTP client logic (auth, pagination, retries) across both tools."_

**Problem:** This frames the shared engine layer as an HTTP client library. For Kafka, you need a Kafka consumer client; for databases, a connection pool and query executor (or a CDC slot listener). The concept of shared logic is sound — scheduling, checkpointing, error classification, observability — but it must be described as a transport-agnostic orchestration layer with pluggable transport adapters, not as shared HTTP plumbing.

**Severity:** High — this is the architectural keystone. If the engine is built as an HTTP client wrapper, every non-HTTP source requires engine surgery.

#### S-2: "Simulator-First Testability" — HTTP Stub Only

**Current text:** Describes _"a configurable HTTP stub/mock server."_

**Problem:** For Kafka, you need an embedded broker (e.g., Testcontainers with a Kafka image). For databases, you need a test database instance or a CDC event replay mechanism. The concept is correct — every connector must be testable without a live system — but the simulator contract must be defined at a higher level of abstraction than "HTTP stub server."

**Severity:** Medium — the concept survives, but the implementation contract and Implementation Plan step #3 need rewording.

#### S-3: Execution Model — HTTP Server Assumed

**Current text:** _"The ingestion tool maintains a persistent HTTP server for webhook reception and manages scheduled polling loops internally."_

**Problem:** Non-HTTP push sources don't use webhooks. A Kafka connector maintains a consumer group subscription; a database CDC connector maintains a replication slot. The daemon's internal architecture must support multiple concurrent connection types, not just "HTTP server + poll scheduler."

**Severity:** Medium — the daemon model is correct (long-lived process), but its internal structure is over-specified for HTTP only.

#### S-4: Graceful Shutdown — HTTP-Specific Language

**Current text:** _"complete in-flight HTTP requests and commit the current page or batch's data before exiting."_

**Problem:** For a Kafka consumer, graceful shutdown means: stop fetching new messages, process all messages already polled, commit offsets, close the consumer cleanly. For a CDC listener, it means: process all buffered WAL events, confirm the LSN, release the replication slot. The principle is identical, the mechanics differ. The requirement should describe transport-neutral drain semantics.

**Severity:** Low — the intent is clear and correct; only the wording is HTTP-specific.

---

### 4.2 Tool 1 Requirements — HTTP-Only or HTTP-Centric

The following requirements are written for HTTP connectors and either **do not apply**, **need transport guards**, or **need a parallel equivalent** for non-HTTP sources:

| # | Requirement | HTTP Coupling | Impact for Non-HTTP |
|---|---|---|---|
| **#3** | Full & Incremental Modes | Assumes HTTP "full fetch" is always possible | **Kafka:** Full sync = replay from offset 0, which may be impossible if topic retention has expired. **DB CDC:** Full sync = snapshot query + switch to CDC stream. The fallback guarantee ("full sync is always available") is not universally true. |
| **#5** | Deletion Verification (targeted lookup) | Assumes source has a queryable GET-by-ID endpoint | **Kafka:** No query API exists — deletions are tombstone messages on the topic. **NATS:** Same. **DB:** Can query, but the pattern differs (SQL `SELECT` vs. HTTP `GET`). Requirement must be scoped: "for sources that support point lookups." |
| **#7** | Webhook Lifecycle Management | HTTP webhooks only | **Completely N/A** for Kafka, NATS, DB CDC. Kafka uses consumer group coordination; NATS uses subscription management; DB CDC uses replication slot management. Each has its own lifecycle, but none of them are "webhooks." |
| **#8** | Full-State Resolution from Events | Assumes HTTP follow-up fetch | **Kafka/NATS:** If the message contains the full state, no follow-up is needed. If it contains only a notification, there must be a queryable source to resolve full state — which may not exist for a pure message broker. Requirement must be conditional: "when the event payload is partial and a queryable source endpoint exists." |
| **#9** | Parameterized Sources (list + detail lookup) | Assumes HTTP list endpoint returning IDs, then HTTP detail endpoint | **N/A** for message brokers — messages arrive complete or they don't. **DB polling:** Equivalent would be a query returning PKs followed by per-PK detail queries, which is unusual. This is an HTTP API pattern. |
| **#11** | Declarative Authentication Schemes | OAuth2, API keys, JWT — all HTTP auth | **Kafka:** Uses SASL (PLAIN, SCRAM-SHA-256/512, GSSAPI/Kerberos), mTLS, or OAUTHBEARER (which is OAuth2., but configured differently). **NATS:** Uses NKeys, JWT credentials, or TLS client certs. **DB:** Uses connection-string credentials, LDAP, Kerberos, or IAM. The auth framework needs a transport-level concept, not just HTTP header/query injection. |
| **#12** | Pagination Support | HTTP pagination (offset, cursor, link-header) | **N/A** for message brokers — data arrives in a continuous stream partitioned by topic/partition, not pages. **DB polling:** Uses SQL `LIMIT`/`OFFSET` or keyset pagination — similar concept but different mechanics. |
| **#13** | Circuit Breakers (empty response) | Triggered by HTTP response patterns (empty pages, shrinking result sets) | **Kafka:** "Empty" = no messages in partition, which is normal for a caught-up consumer — not a circuit-breaker event. Circuit breaker semantics for streams should be: sustained unexpected gaps in sequence numbers, or consumer lag growing without bounds. Different failure modes require different triggers. |
| **#16** | Linked/Nested Object Resolution | Assumes HTTP follow-up lookups | Only works if the source system has a queryable API. A Kafka topic containing parent records with embedded child IDs has no query endpoint to resolve those IDs against. The tool would need to either (a) wait for the child records to arrive on a separate topic, or (b) have a queryable sidecar source configured alongside the stream source. |
| **#18** | Politeness & Rate Limiting | HTTP rate limiting (debouncing, backoff) | **Kafka/NATS:** Rate limiting is reversed — the *broker* controls delivery rate via consumer fetch configuration, not the client. Backpressure is managed by pausing consumption, not by debouncing requests. **DB CDC:** Not applicable — the replication stream delivers at its own pace. Rate limiting in the HTTP sense is irrelevant. |
| **#19** | Shared Event Receivers / Fan-Out | HTTP webhook endpoint routing | **N/A** — Kafka topics, NATS subjects, and DB replication slots are already per-source-stream. Fan-out is handled differently: one Kafka consumer may read multiple topics, dispatching by topic name — not by HTTP endpoint routing. |
| **#21** | Declarative Field / Property Selection | Assumes request-time field filtering (HTTP query params) | **Kafka/NATS:** Messages arrive with whatever fields the producer included — you cannot request specific fields. Projection is done *after* receipt, not before. **DB polling:** Field selection maps to a SQL `SELECT` clause, which is similar but syntactically different. |
| **#24** | Custom Pre-Request Auth Flows | HTTP pre-request token acquisition | **Kafka:** Auth is at connection establishment time (SASL handshake), not per-request. **DB:** Auth is at connection time. The per-request auth model doesn't apply. |
| **#25** | Webhook Event Deduplication | Tracks processed webhook event IDs | The concept applies to Kafka (at-least-once delivery means duplicates), but the mechanism differs: Kafka consumer offsets + idempotent writes vs. tracking HTTP event IDs. |
| **#26** | Ownership Scoping for Webhook Cleanup | HTTP webhook-specific | **Completely N/A** — Kafka consumer groups, NATS subscriptions, and DB replication slots have different ownership/cleanup semantics. |
| **#27** | Configurable Response Expressions | Parses HTTP response envelopes | **Kafka:** Messages have a key, value, headers, and metadata — no "envelope" to unwrap. **DB CDC:** Change events have a defined structure (old/new row, operation type). Expression parsing applies to HTTP JSON responses, not to typed message formats. |
| **#34** | Webhook Signature Verification | HMAC over HTTP body | **Kafka:** Security is at the transport level (SASL, mTLS), not per-message. **NATS:** Same. This requirement is HTTP-webhook-only. |
| **#38** | Pagination Drift Protection | HTTP pagination artifact | **N/A** — Message brokers and CDC streams don't paginate. |
| **#39** | API Version Management | HTTP API versioning | **Kafka:** Schema evolution is handled by a schema registry (e.g., Confluent Schema Registry) with subject-version compatibility, not API versions. **DB CDC:** Schema changes are detected via replication protocol, not versioned endpoints. |
| **#42** | Webhook HTTP Server Hardening | HTTP server security | **N/A** for non-HTTP sources. Kafka/NATS/DB have their own security models (TLS, SASL, network policies). |

**Summary:** 20 of 46 Tool 1 requirements are HTTP-specific or HTTP-centric. Of those:
- **6 are completely N/A** for non-HTTP (#7, #9, #19, #26, #34, #42): They would simply not apply.
- **8 need transport-conditional scoping** (#3, #5, #8, #12, #13, #18, #21, #38): The core concept exists but the HTTP-specific formulation doesn't transfer.
- **6 need parallel transport-specific equivalents** (#11, #16, #24, #25, #27, #39): The same problem must be solved but with different mechanics.

---

### 4.3 High-Water Mark Semantics Diverge

Requirement T1 #40 (High-Water Mark Storage) is one of the best-specified requirements and is close to transport-neutral, but the atomicity guarantee it describes is HTTP-specific:

> _"The watermark must be updated atomically with the data write in the same database transaction."_

This works when both the data and the watermark live in PostgreSQL — the ingestion tool writes data and advances the watermark in one `COMMIT`. But:

| Source Type | Watermark Primitive | Can It Be Atomic with PG Write? |
|---|---|---|
| HTTP API | Timestamp or cursor string | **Yes** — watermark written to PG alongside data in same tx |
| Kafka | Consumer offset per partition | **No** — offset commit is a Kafka protocol operation, separate from the PG tx. Two-phase: write to PG, then commit offset. On crash between the two, messages are re-delivered (at-least-once). |
| NATS JetStream | Stream sequence + ack | **No** — ack is a NATS protocol operation. Same two-phase problem as Kafka. |
| Database (polling) | Column value (e.g., `updated_at`) | **Yes** — if watermark is stored in PG, both are PG transactions (though in different databases) |
| Database (CDC / logical replication) | LSN (Log Sequence Number) | **No** — LSN confirmation is sent back to the source DB's replication protocol, not part of the target PG transaction |

**Consequence:** For non-HTTP push sources, the tool must adopt an **at-least-once** delivery model: write data to PG first, then confirm progress to the source system. On crash, some messages will be re-delivered and the deduplication requirement (T1 #33) becomes load-bearing — it cannot be a nice-to-have, it must be the primary correctness mechanism.

The watermark table should still be used (it's a useful operational record), but its atomicity guarantee must be downgraded to "best-effort" for sources where progress confirmation is a remote operation.

---

### 4.4 "Full Sync" Is Not Universally Available

GOAL.md treats full sync as a reliable fallback:

> *T1 #3: "implement robust full-sync mechanisms … when [incremental APIs] do not [exist]"*

This assumption holds for HTTP APIs (you can always re-fetch from page 1) and database polling (you can always `SELECT * FROM …`). It does **not** hold for:

- **Kafka with finite retention:** Once messages age past the retention window, they are deleted. Replaying from offset 0 only gives you what's still on the topic, not the full historical dataset. If the ingestion tool has never seen a record that was produced and expired before it started consuming, that record is permanently lost.
- **NATS Core (non-JetStream):** Messages are fire-and-forget. There is no replay capability whatsoever.
- **CDC with a non-snapshotting slot:** Some CDC configurations don't support an initial snapshot — they only deliver changes from the moment the slot is created.

**Consequence:** The circuit breaker logic (T1 #13) and deletion verification (T1 #5) depend on the assumption that the tool has a complete picture of the source. For retention-limited sources, the tool can only assert: "I have seen every record that was available since I started consuming." The concept of "mass false deletions from an empty result set" doesn't apply — silence is the normal state of a caught-up consumer.

A **source capability declaration** should be part of the connector config:
- `supports_full_sync: true | false`
- `supports_point_lookup: true | false`
- `supports_deletion_events: true | false`
- `delivery_guarantee: at-least-once | at-most-once | exactly-once`

These flags would gate which requirements apply.

---

### 4.5 Authentication Models Are Incompatible

The authentication framework (T1 #11, #24) is built around HTTP auth patterns:

| Pattern | HTTP | Kafka | NATS | Database |
|---|---|---|---|---|
| OAuth2 / refresh tokens | Yes | OAUTHBEARER (similar but connection-scoped) | No | IAM token auth (some cloud DBs) |
| API key (header/query) | Yes | No | No | No |
| JWT | Yes | No | Yes (different format — NKey-signed) | No |
| Session token (pre-request) | Yes | No | No | No |
| SASL (PLAIN, SCRAM, GSSAPI) | No | Yes | No | Some (Kerberos) |
| mTLS (client certificate) | Rare | Yes | Yes | Yes |
| NKeys | No | No | Yes | No |
| Connection string credentials | No | No | No | Yes |

**Consequence:** The declarative auth config must support a **connection-level auth** concept in addition to the current **per-request auth** concept. For HTTP, auth is injected per-request (headers, query params). For Kafka/NATS/databases, auth is established at connection time and maintained for the session's lifetime. The credential store supports both models (it's just credentials), but the config schema's auth section needs expansion.

---

### 4.6 Simulator Infrastructure

The Simulator-First Testability strategy and Implementation Plan step #3 describe an HTTP stub server as the test infrastructure. For non-HTTP sources:

| Source Type | Simulator Approach |
|---|---|
| Kafka | Embedded Kafka broker (e.g., Testcontainers with `confluentinc/cp-kafka`), or a mock using `kafka-python`'s `MemoryRecordsBuilder` |
| NATS | Embedded NATS server (NATS server binary is tiny and starts in milliseconds — easiest to test of all) |
| Database (polling) | Test database instance (Testcontainers with PostgreSQL/MySQL/etc.) |
| Database (CDC) | Test database with logical replication enabled + a WAL event producer |

**Consequence:** The simulator contract must abstract over the transport. Instead of "start an HTTP server that responds to requests," the contract should be "start a fixture that produces records in the expected transport format and supports the source's lifecycle operations." An HTTP stub server is one implementation; an embedded Kafka broker is another; a test database is a third.

---

## 5. Impact on the Writeback Tool

The writeback tool is unaffected. Its entire design is about writing to external HTTP APIs, which is the correct scope. If future requirements emerge for writing back to Kafka topics or database tables (rather than HTTP endpoints), that would be a separate extension — but it's not implied by adding non-HTTP *ingestion* sources.

The MDM Contract is also unaffected because both tools communicate exclusively through PostgreSQL. The MDM layer doesn't care how data arrived in the ingestion tables.

---

## 6. Recommended Architectural Changes

These changes would preserve the option to add non-HTTP sources without requiring an engine rewrite, while making no functional change to the current HTTP-only scope.

### 6.1 Introduce a Transport Adapter Abstraction

Define a core connector interface at a higher level of abstraction than HTTP:

```
Connector (abstract):
  - connect(credentials) → Connection
  - sync(mode: full | incremental, checkpoint) → Stream<Record>
  - checkpoint() → WatermarkValue
  - disconnect()
  - capabilities() → { supports_full_sync, supports_point_lookup, supports_deletion_events, delivery_guarantee }

Record:
  - external_id: string
  - data: JSON
  - raw: bytes | JSON
  - metadata: { timestamp, sequence, operation_type }
```

The HTTP adapter implements this by orchestrating pagination, request/response cycles, and follow-up lookups. A Kafka adapter would implement it by wrapping a consumer group. A database adapter would implement it by executing queries or consuming a CDC stream.

The engine only depends on this interface — never on HTTP directly.

### 6.2 Scope HTTP-Specific Requirements

Add a transport applicability marker to requirements that only apply to HTTP connectors:

- Requirements that are HTTP-only should be explicitly marked (e.g., `[HTTP]` prefix or a conditional clause: "For HTTP connectors, …").
- Requirements that have transport-specific variants should state the general principle first, then the HTTP-specific implementation.

The affected requirements: #5, #7, #8, #9, #11, #12, #13, #16, #18, #19, #21, #24, #25, #26, #27, #34, #38, #39, #42.

### 6.3 Acknowledge At-Least-Once for Push Sources

For sources where progress confirmation is a remote protocol operation (Kafka offset commit, NATS ack, CDC LSN confirmation), the ingestion tool operates in at-least-once mode. This makes T1 #33 (Intra-Sync Deduplication) the primary correctness mechanism, not just an optimisation. This should be stated explicitly so the dedup implementation is engineered for correctness, not just as a performance improvement.

### 6.4 Add a Source Capability Declaration

The connector config should include a capabilities block that gates which requirements apply:

```yaml
capabilities:
  transport: http | kafka | nats | database-poll | database-cdc
  supports_full_sync: true
  supports_point_lookup: true
  supports_deletion_events: false
  delivery_guarantee: at-least-once
```

Circuit breakers, deletion verification, linked-object resolution, and full-sync fallback would all check these flags before activating.

### 6.5 Generalise the Simulator Contract

Redefine the simulator as a transport-agnostic "source fixture" interface rather than an HTTP stub server. The HTTP stub server is one implementation; an embedded Kafka broker is another. The contract should define:

- How to start and stop the fixture
- How to pre-load it with test data
- How to trigger events (new record, update, delete)
- How to simulate failure conditions (unreachable, slow, auth failure)

### 6.6 Expand the Auth Framework

Add a **connection-level auth** concept alongside the current per-request-level auth:

| Auth Level | When Credentials Apply | Examples |
|---|---|---|
| Per-request (current) | Injected into every outbound HTTP request | OAuth2 bearer token, API key header |
| Connection-level (new) | Established once at connection time, maintained for session lifetime | Kafka SASL, NATS NKeys, database connection credentials, mTLS |

Both levels reference the same credential store; the difference is in when and how they are applied.

---

## 7. Cost-Benefit Assessment

### If we make no changes now

- The HTTP-only implementation proceeds unimpeded.
- When a non-HTTP source is needed, the engine must be refactored to extract a transport abstraction from the hardcoded HTTP logic.
- HTTP assumptions will have leaked into: the config schema, the scheduler, the checkpoint logic, the error classifier, the simulator framework, the CLI, and the test suite.
- Estimated rework: **significant** — touches the core engine loop, the connector config schema, and the test infrastructure.

### If we make the minimal changes now

- **Rename "Shared HTTP Logic"** to describe transport-agnostic orchestration (wording change only).
- **Add transport applicability markers** to ~19 requirements (annotation only — no functional change).
- **Define the connector interface** at the right abstraction level in the Implementation Plan (design document addition).
- **Add a capabilities declaration concept** to the config schema design step.
- **No code changes** — these are specification-level adjustments.
- Estimated cost: **trivial** — a few hours of document refinement.
- Estimated payoff: When the first non-HTTP source is needed, the work is: "implement a new transport adapter" rather than "refactor the engine architecture."

### If we design the full transport abstraction now

- Design and implement the adapter interface before building the HTTP connector.
- The HTTP connector becomes the first adapter, proving the abstraction.
- Non-HTTP connectors become routine adapter implementations.
- Estimated additional upfront cost: **moderate** — a few extra days of design work in the architecture phase.
- Risk: Over-engineering an abstraction without a second consumer to validate it.

---

## 8. Conclusion

The current specification is **approximately 60% transport-agnostic** by happy accident — the storage model, the MDM contract, the cross-cutting concerns, and the writeback tool are all clean. The remaining **~40% is tightly HTTP-coupled**, concentrated in the Strategy section, the ingestion connector model, and about 20 individual requirements.

The most cost-effective path is the **minimal changes now** approach (Section 7, option 2): adjust the specification language to avoid painting the architecture into an HTTP-only corner, without adding any non-HTTP requirements or building unused abstractions. This preserves the current development scope while ensuring the first non-HTTP source doesn't trigger an architectural rework.

The single highest-leverage change is introducing the **transport adapter interface** concept (Section 6.1) in the architecture design phase. If the engine is built against that interface from day one — even with HTTP as the only adapter — adding Kafka, NATS, or database sources later is a connector implementation task, not an engine rewrite.
