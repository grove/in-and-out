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

## Tool 2: The Synchronization Tool (Writeback)
**Objective:** Read refined data from PostgreSQL and synchronize it back into external HTTP APIs.

**Key Requirements:**
1. **Per-Datatype Mapping:** Map the MDM-produced relational table to its respective external system endpoint datatype using declarative configuration.
2. **Smart Writes:** The writeback should exclusively target records that have changed, prioritizing incremental pushes. If an API requires bulk full writes, it should handle that smoothly.
3. **Conflict Resolution & Prevention:** The external system may receive conflicting writes from other applications or users. The tool must provide strong conflict prevention (e.g., Optimistic Concurrency Control, pre-flight state checks, or HTTP conditional requests) before committing data.

## Implementation Plan (Draft)
1. **Configuration Design:** Design a YAML/JSON configuration schema to define HTTP API integrations mapping.
2. **Database Architecture:** Design the PostgreSQL schema utilizing `JSONB` for raw payloads and metadata columns for sync state tracking.
3. **Ingestion Engine:** Build the extractor that reads configs, fetches HTTP data (incremental/full), and writes to the DB.
4. **Writeback Engine:** Build the synchronizer that reads from the DB and pushes to external HTTP APIs with conflict resolution.
