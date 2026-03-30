# Plan: Multi-Replica Work Coordination

> **Date:** March 30, 2026
> **Status:** Draft
> **Scope:** Design and implement efficient work distribution across multiple ingestion and writeback replicas
> **Related:** [GOAL.md](../GOAL.md) (Multi-Instance & High Availability), Federation module (`engine/src/inandout/federation/`)

---

## 1. Problem Statement

Today, every replica starts a polling loop for **every** connector/datatype pair. Before doing work, each replica attempts a `SELECT ... FOR UPDATE SKIP LOCKED` on `inout_ops_sync_lock` (ingestion) or `pg_try_advisory_lock` (writeback). If the lock is already held, the cycle is skipped.

With 8 ingestion replicas and 10 connector/datatype pairs, every poll interval produces:

- 80 lock attempts (8 replicas × 10 pairs)
- 10 succeed, 70 are wasted `sync_lock_skipped` no-ops
- Cost scales as O(replicas × pairs) with no benefit beyond 1 replica per pair

This is correct — at-most-one-worker-per-pair is guaranteed — but it wastes database connections, pollutes logs, and provides no mechanism for load-aware distribution. Adding replicas adds contention without improving throughput once replica count exceeds pair count.

### Constraints

1. **Webhook affinity**: The ingestion daemon hosts an HTTP server that external systems POST to. Webhook events arrive at whatever replica the load balancer routes to. The receiving replica may not "own" the connector/datatype that the webhook targets.
2. **No external coordination service**: The design must use PostgreSQL as the sole coordination backend (consistent with GOAL.md's "stateless aside from PostgreSQL" principle).
3. **Graceful rebalancing**: Adding or removing replicas must not cause missed sync cycles, double-processing, or data loss.
4. **Backward compatibility**: A single-replica deployment must work with zero configuration changes.

---

## 2. Design Overview

Replace the current "every replica tries every pair" model with a **lease-based work assignment** coordinated through a PostgreSQL assignment table. Each replica claims a subset of work items; only claimed items are polled.

```
┌──────────────────────────────────────────────────────────┐
│                  inout_ops_work_assignment                │
│                                                          │
│  connector  │ datatype  │ tool      │ owner_id  │ lease  │
│  ───────────┼───────────┼───────────┼───────────┼──────  │
│  hubspot    │ contacts  │ ingestion │ replica-a │ 60s    │
│  hubspot    │ companies │ ingestion │ replica-b │ 60s    │
│  hubspot    │ deals     │ ingestion │ (vacant)  │        │
│  salesforce │ contacts  │ ingestion │ replica-a │ 60s    │
│  salesforce │ leads     │ ingestion │ replica-c │ 60s    │
│  ...        │           │           │           │        │
└──────────────────────────────────────────────────────────┘

              ┌───────────┐
              │ Replica A │ → polls only hubspot:contacts, salesforce:contacts
              │ Replica B │ → polls only hubspot:companies
              │ Replica C │ → polls only salesforce:leads
              │ Replica D │ → sits idle, ready for failover or new pairs
              └───────────┘
```

The existing `inout_ops_sync_lock` / advisory lock mechanism is **retained** as a safety net. The assignment table provides efficient distribution; the lock prevents double-processing if assignment state is momentarily stale.

---

## 3. Coordination Strategies

We support three strategies, selectable via tool config. The default is `lease` for multi-replica deployments. Single-replica deployments continue to work unchanged with `none`.

### 3.1 Strategy: `none` (current behavior, default for single replica)

Every replica loops over all pairs. Coordination is lock-based only. No assignment table. This is the existing behavior, preserved for backward compatibility and the simplest deployment model.

- **When to use**: Single replica, or development/testing.
- **Config**: `coordination.strategy: none` or simply omit the coordination section.

### 3.2 Strategy: `hash` (static partitioning)

Each replica is assigned an ordinal index (0..N-1) and a total replica count. A pair is owned by the replica whose index matches `hash(connector:datatype) % replica_count`. The replica only starts polling loops for its assigned pairs.

```
coordinator:
  strategy: hash
  replica_index: ${INOUT_REPLICA_INDEX}   # 0-based, injected by StatefulSet ordinal
  replica_count: ${INOUT_REPLICA_COUNT}   # total replicas
```

- **Advantages**: Zero database overhead for assignment. Deterministic. Instant startup — no coordination round needed.
- **Disadvantages**: Uneven distribution if pair count is not a multiple of replica count. Requires restart or config reload on scale-up/down. Does not account for pair cost variations (a full-sync pair is more expensive than an incremental one).
- **When to use**: Kubernetes StatefulSets where ordinal is stable. Works well when pairs >> replicas and costs are roughly uniform.

### 3.3 Strategy: `lease` (dynamic assignment, recommended)

Pairs are claimed dynamically from a shared assignment table using `FOR UPDATE SKIP LOCKED`. Each claim has a TTL (lease duration). Replicas renew leases while actively processing; expired leases are reclaimed by other replicas.

This is the recommended strategy for production multi-replica deployments.

```
coordinator:
  strategy: lease
  lease_duration: 120s          # how long a claim lasts without renewal
  rebalance_interval: 60s       # how often to check for unclaimed work
  max_pairs_per_replica: 0      # 0 = unlimited (take any unclaimed pair)
```

- **Advantages**: Automatic rebalancing on scale-up/down. Handles replica crashes (lease expires, pair is reclaimed). No need for stable ordinals. Cost-aware extension possible (weighted leases).
- **Disadvantages**: Small database overhead (one query per rebalance interval). Slightly slower startup (must claim before polling). Short window where a pair is unowned during failover (bounded by lease duration).
- **When to use**: Any multi-replica deployment. Especially when replicas are ephemeral (Deployments, not StatefulSets) or pair costs vary.

---

## 4. Detailed Design: Lease Strategy

### 4.1 Assignment Table

```sql
CREATE TABLE inout_ops_work_assignment (
    connector       TEXT NOT NULL,
    datatype        TEXT NOT NULL,
    tool            TEXT NOT NULL,             -- 'ingestion' or 'writeback'
    owner_id        TEXT,                      -- instance_id of the claiming replica (NULL = unclaimed)
    leased_at       TIMESTAMPTZ,              -- when the lease was acquired
    lease_until     TIMESTAMPTZ,              -- when the lease expires
    renewed_at      TIMESTAMPTZ,              -- last renewal timestamp
    priority        INT NOT NULL DEFAULT 0,   -- higher = claimed first (for cost-aware scheduling)

    PRIMARY KEY (connector, datatype, tool)
);

CREATE INDEX idx_work_assignment_unclaimed
    ON inout_ops_work_assignment (tool)
    WHERE owner_id IS NULL OR lease_until < NOW();
```

### 4.2 Work Item Registration

On startup, each daemon scans its loaded connector configs and ensures all (connector, datatype) pairs have a row in `inout_ops_work_assignment`, using `INSERT ... ON CONFLICT DO NOTHING`. This handles new connectors being added.

```sql
INSERT INTO inout_ops_work_assignment (connector, datatype, tool, priority)
VALUES (%s, %s, %s, %s)
ON CONFLICT (connector, datatype, tool) DO NOTHING;
```

### 4.3 Lease Acquisition

A background coroutine (`_lease_manager_loop`) runs every `rebalance_interval` seconds. It claims unclaimed or expired pairs:

```sql
-- Claim one unclaimed (or expired) pair at a time, highest priority first
UPDATE inout_ops_work_assignment
SET owner_id   = %s,
    leased_at  = NOW(),
    lease_until = NOW() + %s::interval,
    renewed_at = NOW()
WHERE (connector, datatype, tool) = (
    SELECT connector, datatype, tool
    FROM inout_ops_work_assignment
    WHERE tool = %s
      AND (owner_id IS NULL OR lease_until < NOW())
    ORDER BY priority DESC, leased_at ASC NULLS FIRST
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING connector, datatype;
```

The lease manager repeats this in a loop until no more rows are returned or `max_pairs_per_replica` is reached, then sleeps for `rebalance_interval`.

### 4.4 Lease Renewal

Each active polling loop renews its lease after every successful sync cycle:

```sql
UPDATE inout_ops_work_assignment
SET lease_until = NOW() + %s::interval,
    renewed_at  = NOW()
WHERE connector = %s AND datatype = %s AND tool = %s AND owner_id = %s;
```

If the UPDATE affects 0 rows (another replica stole the lease during a long sync), the polling loop stops for that pair — the lock mechanism prevents double-processing during the transition.

### 4.5 Lease Release

On graceful shutdown (SIGTERM), the daemon releases all its leases:

```sql
UPDATE inout_ops_work_assignment
SET owner_id = NULL, lease_until = NULL
WHERE owner_id = %s AND tool = %s;
```

On crash, leases expire naturally after `lease_duration`. Other replicas reclaim them on their next rebalance cycle.

### 4.6 Rebalancing (Work Stealing)

When replicas have uneven loads (e.g., 3 replicas with 5/3/2 pairs), a voluntary rebalancing step redistributes work. On each rebalance cycle, a replica checks:

```
my_pair_count = count of pairs I own
avg_pair_count = total_pairs / alive_replicas   (from inout_ops_federation)

if my_pair_count > avg_pair_count + 1:
    release (my_pair_count - ceil(avg_pair_count)) pairs
    (release the ones with lowest priority first)
```

Released pairs become available for under-loaded replicas to claim. This converges to an even distribution within 2–3 rebalance cycles.

### 4.7 Dynamic Polling Loop Management

Today, polling loops are spawned at startup in a task group and run forever. With lease-based assignment, the daemon needs to **start and stop polling loops dynamically** as leases are acquired and released.

```python
class LeaseCoordinator:
    """Manages lease acquisition and dynamic polling loop lifecycle."""

    def __init__(self, pool, tool: str, instance_id: str, config: CoordinatorConfig):
        self._pool = pool
        self._tool = tool
        self._instance_id = instance_id
        self._config = config
        self._active_tasks: dict[tuple[str, str], anyio.abc.TaskGroup] = {}

    async def run(self, task_group: anyio.abc.TaskGroup, loop_factory):
        """Main loop: acquire leases, start/stop polling loops."""
        while not draining:
            newly_claimed = await self._claim_unclaimed_pairs()
            for connector, datatype in newly_claimed:
                task_group.start_soon(loop_factory(connector, datatype))

            lost_leases = await self._detect_lost_leases()
            for connector, datatype in lost_leases:
                self._cancel_polling_loop(connector, datatype)

            await self._rebalance_if_overloaded()
            await anyio.sleep(self._config.rebalance_interval)
```

### 4.8 Interaction with Existing Lock Mechanism

The assignment table and the sync lock serve different purposes:

| Concern | Assignment table | Sync lock |
|---|---|---|
| **Purpose** | Efficient distribution (avoid wasted work) | Correctness (at-most-one) |
| **Scope** | Which replica _should_ process a pair | Which replica _is_ processing right now |
| **Failure mode** | Stale assignment → two replicas try → lock resolves it | Lock leaked → TTL expires → reclaimed |
| **Required** | No (graceful degradation to lock-only) | Yes (always) |

The assignment table is an **optimization layer**. If it is unavailable (migration not yet run, table dropped), the system falls back to the current lock-only behavior. This is the backward compatibility guarantee.

---

## 5. Webhook Routing

Webhooks introduce a unique challenge: external systems POST events to whichever replica the load balancer selects, but that replica may not own the connector/datatype for the event.

### 5.1 Current Behavior

Every ingestion replica runs the webhook HTTP server. The webhook handler calls `engine.run_sync_single_record()` which acquires a sync lock, processes the single record, and releases the lock. This works regardless of assignment because the lock is the coordination point.

### 5.2 With Lease-Based Assignment

Two approaches, in order of preference:

**Option A: Process locally (recommended)**

The webhook handler continues to process events on whichever replica receives them. Single-record syncs are lightweight and short-lived. The sync lock ensures correctness. The assignment table is not consulted for webhook-triggered work.

This means a replica may process webhook events for pairs it doesn't "own" for polling. This is fine — the lock prevents conflict, and the cost is one short-lived lock acquisition per event.

**Option B: Forward to owner**

The receiving replica looks up the owner in the assignment table and forwards the webhook payload via an internal HTTP call. This adds latency, complexity, and a failure mode (owner is down). Not recommended unless webhook volume is extremely high and processing cost is significant.

**Recommendation**: Option A. Webhooks are low-volume, high-urgency events. Processing them locally with lock-based coordination is simpler and more reliable.

---

## 6. Configuration

### 6.1 Tool Config Changes

Add a `coordinator` section to both `IngestionToolConfig` and `WritebackToolConfig`:

```yaml
# ingestion.yaml / writeback.yaml
coordinator:
  strategy: lease              # none | hash | lease
  lease_duration: 120s         # lease strategy only
  rebalance_interval: 60s      # lease strategy only
  max_pairs_per_replica: 0     # 0 = unlimited
  # hash strategy only:
  replica_index: ${INOUT_REPLICA_INDEX}
  replica_count: ${INOUT_REPLICA_COUNT}
```

### 6.2 Config Data Model

```python
@dataclass
class CoordinatorConfig:
    strategy: Literal["none", "hash", "lease"] = "none"
    lease_duration: str = "120s"
    rebalance_interval: str = "60s"
    max_pairs_per_replica: int = 0
    replica_index: int | None = None
    replica_count: int | None = None
```

### 6.3 K8s Deployment Changes

Switch from Deployment to StatefulSet only if using `hash` strategy (for stable ordinals). For `lease` strategy, Deployment is preferred since replicas are interchangeable.

```yaml
# ingestion HPA (new)
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: inandout-ingest
  namespace: inandout
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: inandout-ingest
  minReplicas: 1
  maxReplicas: 8
  metrics:
    - type: Pods
      pods:
        metric:
          name: sync_lag_seconds    # max lag across owned pairs
        target:
          type: AverageValue
          averageValue: "300"       # scale up if avg lag > 5 minutes
```

---

## 7. Observability

### 7.1 New Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `inout_coordinator_leases_held` | Gauge | `tool`, `instance_id` | Number of active leases held by this replica |
| `inout_coordinator_leases_unclaimed` | Gauge | `tool` | Total unclaimed pairs (should converge to 0) |
| `inout_coordinator_claim_attempts` | Counter | `tool`, `result` (acquired/skipped) | Lease acquisition attempts |
| `inout_coordinator_rebalance_releases` | Counter | `tool` | Pairs voluntarily released during rebalancing |
| `inout_coordinator_lease_expirations` | Counter | `tool` | Leases that expired without renewal (replica crash) |

### 7.2 Readiness Endpoint

Update `/ready` to include assignment state:

```json
{
  "status": "ready",
  "coordination": {
    "strategy": "lease",
    "owned_pairs": 3,
    "instance_id": "ingest-7f4b2-abc12"
  },
  "connectors": [
    {"connector": "hubspot", "datatype": "contacts", "status": "syncing"},
    {"connector": "hubspot", "datatype": "companies", "status": "idle"}
  ]
}
```

### 7.3 Log Enrichment

All structured log entries from a polling loop already include `connector` and `datatype`. Add `coordination_strategy` and `lease_expires_in` to the bound logger context for lease-managed loops.

### 7.4 Grafana Dashboard

A "Coordination" panel showing:
- Pair-to-replica assignment heatmap (who owns what)
- Unclaimed pair count over time (should be 0 in steady state)
- Lease acquisition latency histogram
- Rebalance event timeline

---

## 8. Migration Path

### Phase 1: Assignment table + lease coordinator (engine changes)

1. **C1**: Add `inout_ops_work_assignment` migration.
2. **C2**: Add `CoordinatorConfig` to `config/tool.py` and connector schema.
3. **C3**: Implement `LeaseCoordinator` in `engine/src/inandout/engine/coordinator.py`.
4. **C4**: Implement `HashCoordinator` in the same module.
5. **C5**: Add coordinator metrics.

### Phase 2: Daemon integration

6. **C6**: Refactor `ingestion/daemon.py` to use `LeaseCoordinator` for dynamic polling loop management when `strategy != none`.
7. **C7**: Refactor `writeback/daemon.py` similarly.
8. **C8**: Upgrade writeback lock mechanism to use row-level locks with TTL (matching ingestion), replacing bare advisory locks.

### Phase 3: Deployment & observability

9. **C9**: Add ingestion HPA manifest (`k8s/hpa-ingest.yaml`).
10. **C10**: Update readiness endpoints with coordination state.
11. **C11**: Add Grafana dashboard panel for coordination.
12. **C12**: Update deployment documentation in the book.

### Phase 4: Validation

13. **C13**: Integration test: 3 replicas, 6 pairs → verify even distribution and no double-processing.
14. **C14**: Chaos test: kill a replica mid-sync → verify lease expiry and failover within `lease_duration`.
15. **C15**: Scale test: add/remove replicas → verify rebalancing converges within 3 cycles.

---

## 9. Failure Scenarios

| Scenario | Behavior |
|---|---|
| **Replica crashes mid-sync** | Sync lock released when connection drops. Lease expires after `lease_duration`. Another replica claims the pair on next rebalance. Worst-case gap = `lease_duration`. |
| **Database temporarily unavailable** | All replicas pause (no lock, no lease renewal). When DB recovers, replicas resume with their existing leases if still valid, or reclaim. |
| **Assignment table missing** (migration not run) | Coordinator logs a warning and falls back to `strategy: none` (lock-only, current behavior). |
| **All replicas start simultaneously** | Each claims pairs via `FOR UPDATE SKIP LOCKED` — PostgreSQL serializes claims. No thundering herd on the assignment table itself. |
| **Replica count > pair count** | Excess replicas have no leases and sit idle. They acquire work immediately when new connectors are added or a peer fails. |
| **Long-running full sync exceeds lease duration** | The polling loop renews the lease after each checkpoint (not just after completion). The renewal interval (`_LOCK_HEARTBEAT_INTERVAL_SECS`) must be shorter than `lease_duration`. |
| **Network partition isolates a replica** | Isolated replica cannot renew leases (DB unreachable). Leases expire. Remaining replicas reclaim. When partition heals, the isolated replica discovers its leases are gone and re-claims available pairs. |

---

## 10. Decision Log

| Decision | Rationale | Alternatives Considered |
|---|---|---|
| Lease-based as recommended strategy | Works with Deployments (no stable ordinals). Handles crash recovery. Self-balancing. | Hash (requires StatefulSet), external coordinator (violates PostgreSQL-only constraint) |
| Retain existing sync lock as safety net | Lease assignment is an optimization, not a correctness mechanism. Belt-and-suspenders. | Replace locks with leases entirely (too risky — assignment staleness could cause double-processing) |
| Process webhooks locally regardless of assignment | Simplicity. Webhooks are low-volume. Lock ensures correctness. | Forward to owner (adds latency, failure modes, internal service mesh) |
| Assignment table per-tool, not shared | Ingestion and writeback scale independently. Separate tables would also work but a single table with a `tool` column is simpler. | Separate tables per tool (unnecessary complexity) |
| `max_pairs_per_replica: 0` default | Let the system self-balance. Operator can cap if a replica is resource-constrained. | Require explicit limit (forces operator to do math) |

---

## 11. Open Questions

1. **Priority assignment**: Should pair priority be manual (operator sets it) or automatic (based on record count, sync frequency, or historical duration)? Initial implementation: manual via `priority` column, defaulting to 0.

2. **Weighted lease cost**: A full-sync pair that takes 30 minutes should count as "heavier" than a 5-second incremental pair. Should the rebalancer account for estimated cost, or is pair-count balancing sufficient? Initial implementation: pair-count only. Cost-weighted rebalancing deferred to a future iteration.

3. **Lease duration tuning**: Too short → unnecessary failover churn. Too long → slow recovery after crash. The 120s default assumes sync cycles are typically under 60s and the rebalance interval is 60s. May need per-pair tuning for very long full syncs.

4. **Connector hot-reload interaction**: When SIGHUP triggers a config reload and new connector/datatype pairs appear, the daemon must register them in the assignment table and claim them. When pairs are removed from config, the daemon should release their leases. This should be handled in the same hot-reload codepath that currently rescans connector YAML files.
