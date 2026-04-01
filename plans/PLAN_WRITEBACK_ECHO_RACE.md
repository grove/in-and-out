# Plan: Writeback-Echo Race Condition

**Status:** Analysis — no code changes yet  
**Date:** 2026-04-01  
**Affects:** `engine/writeback`, `engine/ingestion/webhooks`, `simulator`

---

## 1. Problem

When the writeback engine inserts a new record into a target system (e.g.
HubSpot, Tripletex, the demo simulator), the target fires a webhook back to
the ingest endpoint **before the writeback engine has finished writing the
`inout_ops_identity_map` row** that links the MDM cluster ID to the
newly-assigned external ID.

The window exists because the write sequence is:

```
Writeback engine                             Simulator / real CRM
─────────────────────────────────────────    ──────────────────────────────────
POST /contacts  ──────────────────────────►
                                             create record (id = 42)
                                             schedule webhook (async task)
                                             ↓
                                             POST /webhook  ───────────►  Engine
                                             ← await webhook response       ↓
                                                                       ingest record 42
                                             respond 201 ◄─────────────
◄────────────────────── 201 {id: 42}
extract returned_id = "42"
INSERT inout_ops_identity_map
  cluster_id X → external_id 42            (too late — ingest already ran)
INSERT inout_ops_lwstate
```

In the **simulator** the race is **guaranteed** because
`dispatch_nowait` calls `asyncio.create_task(dispatch(...))` and the first
`await` inside the insert handler yields to the webhook task before the 201
response is sent.  The full webhook round-trip completes before the simulator
even sends the HTTP response back to the writeback engine.

In **real CRMs** the race is probabilistic: webhook delivery is async and
typically arrives 100 ms–5 s after the 201.  For fast connections or heavily
loaded systems with slow identity-map writes the window is still real.

---

## 2. Harm Pathways

### 2.1 Spurious new cluster (most severe)

If a linkage job runs between the webhook ingest (step 6 above) and the
identity-map write (step 12):

1. Linkage sees `external_id = 42` in the source table with no identity-map
   entry → creates a new cluster Y for it.
2. Writeback engine writes `inout_ops_identity_map`: cluster X → external_id 42.
3. Two clusters now claim ownership of the same source record (cluster X via
   the identity map, cluster Y via linkage).  Downstream materialization
   produces duplicate golden records or silently drops one.

### 2.2 Writeback loop (duplicate insert)

1. Webhook ingest lands.  Linkage / golden-record materialisation reruns (if
   triggered on ingest events).
2. Materialisation sees no lwstate for cluster X yet → schedules another
   writeback insert for cluster X.
3. Writeback engine is mid-flight writing the identity map for the first insert.
4. The second insert fires and creates a second record in the target (id = 99).
5. Writeback engine finishes: writes identity map for 42, AND then a second
   run writes it for 99.  One is silently orphaned.

### 2.3 LWState drift

The webhook ingest puts the full API response body into the source table.  The
writeback engine then writes `inout_ops_lwstate` with *what it sent*, which may
differ from the API's representation (e.g. server-generated timestamps,
normalised fields).  The next update comparison calculates a spurious diff and
re-pushes fields that did not actually change.

---

## 3. Options

### Option A — Delay webhook dispatch in the simulator (workaround)

Add a configurable `webhook_dispatch_delay_ms` to the simulator's webhook
config.  If the create request carries an `X-InAndOut-Source: writeback`
header, sleep before dispatching.

- **Pros:** Zero engine changes; immediate demo fix.
- **Cons:** Timing-based; does not fix real CRM behaviour; requires
  simulator-specific code path per operation source.

### Option B — Write-ahead pending-insert marker + token embedding

Before the writeback engine issues the insert HTTP call:
1. Write a row to `inout_ops_pending_writes(connector, datatype, cluster_id,
   token, expires_at)`.
2. Embed `"__inandout_write_token": "<token>"` in the insert payload.

After the 201 response, in a single DB transaction:
3. Write `inout_ops_identity_map` (cluster_id → external_id).
4. Write `inout_ops_lwstate`.
5. Update `inout_ops_pending_writes` with `status='done', external_id=42`.

In the **webhook handler**, before triggering linkage:
6. Check for `__inandout_write_token` in the payload.
7. If the token matches a row in `inout_ops_pending_writes` → record is a
   **writeback echo**; do idempotent ingest but **skip linkage trigger**.

For **notification-only connectors** (HubSpot-style, no payload body):
6b. Check `inout_ops_pending_writes` for a recently completed insert whose
    `external_id` matches the notification's `objectId`.  Same suppression.

- **Pros:** Correct for both simulator and real CRMs; no timing dependency.
- **Cons:** Requires one extra table; writeback payload now carries
  `__inandout_write_token`; CRMs must reflect all fields (most do for
  full-payload connectors; notification-only needs the 6b path).

### Option C — Identity-map check in webhook handler (lightweight guard)

After identity-map write, the record is marked.  In the webhook handler, after
routing but **before** triggering linkage:

1. Query `inout_ops_identity_map WHERE connector=$c AND datatype=$d AND
   target_external_id=$id`.
2. If a row exists: the record came from a prior writeback → ingest normally
   (idempotent) but skip linkage / materialisation trigger.
3. If no row exists: ingest normally **and** set a short re-check sentinel
   (e.g. 2 s) — if identity-map row appears within that window it was a racing
   echo; if not, it is a genuine external create.

- **Pros:** No schema changes beyond what already exists; cheap read path.
- **Cons:** Does not close the window — the identity-map row may not exist yet
  when the check runs (that is the race condition itself).  Acts as a
  mitigation for the "second echo" but leaves the initial race unclosed without
  a re-check / retry loop.

### Option D — Atomic insert + identity-map write via advisory lock

Wrap the entire insert-HTTP-call + identity-map-write sequence in a
per-`(connector, datatype, cluster_id)` advisory lock.  The webhook handler
acquires the same lock before triggering linkage.  If it cannot acquire the
lock, the writeback is in-flight → defer webhook linkage.

- **Pros:** Closes the window completely; no extra table.
- **Cons:** Advisory locks across two async coroutines (writeback daemon and
  webhook handler) are complex and lock contention could stall the webhook
  response path.

---

## 4. Recommended Approach

**Two-phase:** Option B (authoritative production fix) + Option A (immediate
simulator demo fix until B is implemented).

### 4.1 Immediate — Simulator delay (Option A)

Add `webhook_delay_ms: 300` to the per-connector webhook config (or a
global default).  The simulator sleeps before dispatching the task when the
creating request carries `X-InAndOut-Source: writeback`.

The writeback engine adds `X-InAndOut-Source: writeback` to all outgoing
insert/update/delete HTTP calls.  The simulator inspects this header and sets
a delay accordingly.  Pure demo-path change.

### 4.2 Production — Pending-write table + token embedding (Option B)

#### Schema

```sql
CREATE TABLE inout_ops_pending_writes (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    connector    text        NOT NULL,
    datatype     text        NOT NULL,
    cluster_id   text        NOT NULL,
    token        text        NOT NULL UNIQUE,
    status       text        NOT NULL DEFAULT 'pending',  -- 'pending' | 'done'
    external_id  text,           -- filled after 201
    created_at   timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL DEFAULT now() + interval '60 seconds'
);
CREATE INDEX ON inout_ops_pending_writes (connector, datatype, external_id)
    WHERE status = 'done';
CREATE INDEX ON inout_ops_pending_writes (token);
```

Expired rows are swept by the existing ops-table housekeeping job.

#### Writeback engine changes (`writeback/engine.py`)

```
before HTTP insert:
  token = str(uuid4())
  INSERT inout_ops_pending_writes (connector, datatype, cluster_id, token)

inject into payload:
  payload["__inandout_write_token"] = token

after 201:
  in single transaction:
    INSERT inout_ops_identity_map (cluster_id, external_id)
    INSERT inout_ops_lwstate (...)
    UPDATE inout_ops_pending_writes SET status='done', external_id=<id>
      WHERE token = token
```

The `__inandout_write_token` field should be stripped from the payload by the
linter / field-exclusion layer when persisting to the source table, and must be
included in `writeback.payload_schema` exclusions to prevent validation errors.

#### Webhook handler changes (`ingestion/webhooks.py`)

After signature verification and routing, add a guard before the linkage
trigger:

```python
write_token = payload.get("__inandout_write_token")
if write_token:
    pending = await _lookup_pending_write(pool, connector.name, datatype, write_token)
    if pending:
        # writeback echo — ingest idempotently but skip linkage trigger
        await engine.ingest_record(..., skip_linkage=True)
        return JSONResponse({"status": "echo_suppressed"}, status_code=200)

# For notification-only connectors:
ext_id = payload.get(ext_id_field)
if ext_id:
    echo = await _lookup_pending_write_by_external_id(pool, connector.name, datatype, ext_id)
    if echo:
        await engine.ingest_record(..., skip_linkage=True)
        return JSONResponse({"status": "echo_suppressed"}, status_code=200)
```

`skip_linkage=True` is propagated to `ingest_record` to bypass the
post-ingest linkage / materialisation event publish.  The record is still
upserted (idempotent; harmless) so that future polls do not re-fetch it as
stale.

---

## 5. What `skip_linkage` means in practice

The ingest path today always upserts the record in the source table and may
publish an `INGEST_COMPLETED` event that downstream linkage/materialisation
subscribes to.  With `skip_linkage=True`:

- Source-table upsert proceeds as normal (idempotent).
- `INGEST_COMPLETED` event is NOT published for this record.
- The identity-map row was already written by the writeback engine → next
  linkage run (triggered by a future non-echo event or the periodic cycle)
  will see `external_id` already mapped to `cluster_id` and skip re-clustering.

---

## 6. Edge Cases

| Scenario | Handling |
|---|---|
| Writeback succeeds but `pending_writes` update fails | `expires_at` ensures the row is cleaned up; worst case a legitimate webhook is suppressed once |
| Network error: 201 never arrives back | Pending row expires in 60 s; no linkage suppression after that |
| Real CRM does not reflect `__inandout_write_token` back | Notification-only path (6b) handles this via `external_id` match on `pending_writes status='done'` |
| Two simultaneous inserts for same `(connector, datatype)` | Each has its own `token` and `cluster_id`; no collision |
| `__inandout_write_token` leaked into the target's UI | Field should be stripped from the simulator's display layer (`k.startswith("__")`) — already done |

---

## 7. Out of Scope

- Writeback **updates** and **deletes** — the echo race still exists but the
  harm is much less severe (upsert of an existing record does not create a new
  cluster).  The `X-InAndOut-Source` header suppression in Option A covers
  updates/deletes in the simulator without any additional logic.
- Full distributed locking (Option D) — over-engineered for the current
  traffic profile; revisit if the linkage cycle becomes fully event-driven with
  sub-second latency requirements.
