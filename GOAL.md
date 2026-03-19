# Goal: Declarative MDM Synchronization Tools ("In-and-Out")

This project aims to research and build two separate but related tools that act as the automated ingestion and output layers of a Master Data Management (MDM) system, built on top of PostgreSQL 18.3.

## The Strategy
- **Declarative Integrations:** Instead of writing code for every new integration, the tools interpret configuration files (e.g., YAML/JSON) to define how data is read or written.
- **Shared HTTP Logic:** Since most external systems communicate via HTTP APIs, the underlying execution engine will share HTTP client logic (auth, pagination, retries) across both tools.
- **Sync Modes:** The primary mode of operation is **Incremental Synchronization**, utilizing high-water marks (e.g., `updated_at`). However, both tools must gracefully handle **Full Synchronizations** when external APIs lack mechanisms to query incremental changes or during initial loads.

## Tool 1: The Ingestion Tool
**Objective:** Extract data from external HTTP APIs and synchronize it into PostgreSQL 18.3.

**Key Requirements:**
1. **Per-Datatype Tables:** One dedicated table per distinct datatype discovered in the external system.
2. **Flexible Schema (JSONB):** The database schema avoids brittle structured columns, relying instead on PostgreSQL's `JSONB` to store the native data payload alongside operational metadata (e.g., timestamps, hashes).
3. **Full & Incremental Modes:** Support incremental fetching when APIs permit, but implement robust full-sync mechanisms (e.g., diffing/hashing existing records against new full payloads) when they do not.
4. **Deletion Tracking:** Detect objects that have been deleted from the source system. During full syncs this is done by diffing against previously known records; during incremental syncs this may rely on deletion events or tombstone markers if the API provides them.
5. **Deletion Verification:** Before marking a record as deleted, perform an explicit targeted lookup against the source system to confirm the object is truly gone. This guards against false positives caused by faulty pagination, transient API errors, or incomplete result sets.
6. **Near Real-Time Ingestion (Event-Driven):** Support webhooks, event streams, or similar notification mechanisms to fetch changes as soon as they occur. Because external systems have varying capabilities, this must gracefully fall back to polling if needed.
7. **Webhook Lifecycle Management:** Actively maintain webhook registrations over time — including initial registration, periodic renewal/re-registration, health checks, and cleanup of stale subscriptions.
8. **Full-State Resolution from Events:** When a webhook or event stream delivers a partial or notification-only payload (e.g., "object X changed"), the tool must perform a follow-up lookup to retrieve the full current state of the object.
9. **Single Source-of-Truth Table:** Combine webhook/event data with the last known persisted state to produce and maintain one authoritative source-of-truth table per datatype. This table always reflects the best-known current state of every object.
10. **Politeness & Rate Limiting:** The ingestion process must strictly enforce rate limiting, batching, and backoff strategies to prevent accidental Denial of Service (DoS) attacks on external systems, preserving their stability regardless of change volume (e.g., debouncing webhook triggers).

## Tool 2: The Synchronization Tool (Writeback)
**Objective:** Read refined data from PostgreSQL and synchronize it back into external HTTP APIs.

**Key Requirements:**
1. **Per-Datatype Mapping:** Map the MDM-produced relational table to its respective external system endpoint datatype using declarative configuration.
2. **Smart Writes:** The writeback should exclusively target records that have changed, prioritizing incremental pushes. If an API requires bulk full writes, it should handle that smoothly.
3. **Conflict Resolution & Prevention:** The external system may receive conflicting writes from other applications or users. The tool must provide strong conflict prevention (e.g., Optimistic Concurrency Control, pre-flight state checks, or HTTP conditional requests) before committing data.
4. **Base-Aware Updates:** The writeback input may include a "base" — the subset of properties representing the version of the entity the update was computed from. This enables three-way merge and precise conflict detection against the current state in the target system.
5. **Client-Side Patching:** When the target system supports partial updates (PATCH), the tool should support a lookup-diff-write cycle: fetch the current state, compute a minimal client-side diff against the desired state, and submit only the changed fields.
6. **CRDT Support:** If the target system supports CRDT (Conflict-free Replicated Data Type) structures (e.g., counters, sets, registers), the writeback tool should leverage those data structures to apply updates without conflicts.
7. **Desired-State Input Table:** The MDM produces a desired-state table per target datatype with the following structure:
   - **`action`** column: one of `insert`, `update`, `delete`, or `noop`.
   - **`cluster_id`** column: the MDM merge-group identifier (origin identity).
   - **Primary key column(s):** the external system's identifier(s) for the record. Present for `update` and `delete` actions; absent for `insert` (only `cluster_id` is known before creation).
   - `noop` rows indicate the desired state is already applied and can be skipped.
8. **Identity Mapping:** When creating new objects (`insert`), the external system generates an ID. The tool must capture these generated IDs and persist the link between the originating `cluster_id` and the generated external identity in dedicated mapping tables, making them available for future operations.
9. **Last-Written-State Tables:** The tool must store and expose the last successfully written state of each record as queryable tables. This provides an audit trail and serves as the "base" for future diff computations.
10. **Near Real-Time Writeback:** The tool should listen for finalized changes produced by the MDM (e.g., via PostgreSQL triggers, logical replication, or event queues) and proactively push these updates to the external systems as soon as possible.
11. **Politeness & Rate Limiting:** While striving for near real-time updates, the writeback process must remain gentle and respect external system limits. It must enforce strict rate limiting, request batching/chunking, and exponential backoff to ensure we do not bombard the target with a high volume of concurrent write operations (avoiding DoS).

## Implementation Plan (Draft)
1. **Configuration Design:** Design a YAML/JSON configuration schema to define HTTP API integrations mapping.
2. **Database Architecture:** Design the PostgreSQL schema utilizing `JSONB` for raw payloads and metadata columns for sync state tracking.
3. **Ingestion Engine:** Build the extractor that reads configs, fetches HTTP data (incremental/full), and writes to the DB.
4. **Writeback Engine:** Build the synchronizer that reads from the DB and pushes to external HTTP APIs with conflict resolution.
