# Troubleshooting Runbook

## Quick Diagnostics

```bash
# Check daemon health
curl http://localhost:9090/health
curl http://localhost:9090/ready

# View recent sync runs
psql $INOUT_DATABASE_URL -c "
  SELECT connector, datatype, mode, status, started_at, finished_at,
         records_inserted, records_updated, records_deleted
  FROM inout_ops_sync_run
  ORDER BY started_at DESC
  LIMIT 20"

# Check circuit breaker state
psql $INOUT_DATABASE_URL -c "
  SELECT connector, datatype, state, opened_at, fail_count
  FROM inout_ops_circuit_breaker
  WHERE state != 'closed'"

# View dead letter queue
psql $INOUT_DATABASE_URL -c "
  SELECT connector, datatype, external_id, error, failed_at
  FROM inout_dead_letter
  ORDER BY failed_at DESC
  LIMIT 10"
```

---

## Ingestion Issues

### Symptom: No data ingested

**Check 1: Verify connector is running**
```bash
psql $INOUT_DATABASE_URL -c "
  SELECT connector, datatype, MAX(started_at) as last_run
  FROM inout_ops_sync_run
  GROUP BY connector, datatype
  ORDER BY last_run DESC"
```

**Check 2: Check for circuit breaker**
```bash
inandout control status --connector <name>
```

**Check 3: Review logs**
```bash
# Docker Compose
docker compose logs ingest | grep error

# Kubernetes
kubectl logs -n inandout deployment/ingest | grep error
```

**Resolution:**
- If circuit breaker open: `inandout control reset-circuit-breaker --connector <name>`
- If auth errors: verify credentials in environment
- If connection errors: check network connectivity to API

---

### Symptom: Sync lag increasing

**Check sync duration**
```bash
psql $INOUT_DATABASE_URL -c "
  SELECT connector, datatype,
         AVG(EXTRACT(EPOCH FROM (finished_at - started_at))) as avg_duration_secs
  FROM inout_ops_sync_run
  WHERE finished_at > NOW() - INTERVAL '1 day'
  GROUP BY connector, datatype
  ORDER BY avg_duration_secs DESC"
```

**Resolution:**
- Increase polling interval if the API is slow
- Enable bulk export mode for large datasets
- Check for rate limiting (429 responses)
- Reduce `page_size` if responses are timing out

---

### Symptom: Webhooks not received

**Check 1: Verify webhook registration**
```bash
psql $INOUT_DATABASE_URL -c "
  SELECT connector, webhook_id, callback_url, status, registered_at
  FROM inout_ops_webhook_subscriptions"
```

**Check 2: Test webhook endpoint**
```bash
curl -X POST http://localhost:9090/webhook/<connector> \
  -H "Content-Type: application/json" \
  -d '{"event": "test"}'
```

**Resolution:**
- Re-register webhook — check connector registration config
- Verify the callback URL is publicly accessible
- Check that webhook secret matches the external system

---

### Symptom: Many records in dead letter queue

**Check error distribution**
```bash
psql $INOUT_DATABASE_URL -c "
  SELECT error, COUNT(*) as count
  FROM inout_dead_letter
  GROUP BY error
  ORDER BY count DESC"
```

**Resolution:**
- 422 errors: review field mappings in connector config
- 401 errors: refresh credentials
- Replay after fixing: `inandout control replay-dead-letter --connector <name> --datatype <type>`

---

### Symptom: Circuit breaker keeps opening

**Check HTTP errors**
```bash
curl http://localhost:9090/metrics | grep inout_http_errors_total
```

**Resolution:**
- 500 errors: target API may be down — wait for recovery
- Auth errors: refresh OAuth tokens
- Manual reset: `inandout control reset-circuit-breaker --connector <name>`

---

### Symptom: Duplicate records created

**Check deduplication**
```bash
psql $INOUT_DATABASE_URL -c "
  SELECT external_id, COUNT(*) as count
  FROM inout_src_<connector>_<datatype>
  WHERE _deleted = false
  GROUP BY external_id
  HAVING COUNT(*) > 1"
```

**Resolution:**
- Check webhook deduplication (event_id tracking)
- Verify `primary_key` config is correct
- Check for race conditions between webhook and poll

---

## Writeback Issues

### Symptom: Changes not written back

**Check 1: Verify replication slot is active**
```bash
psql $INOUT_DATABASE_URL -c "
  SELECT slot_name, active, confirmed_flush_lsn
  FROM pg_replication_slots
  WHERE slot_name LIKE 'inout_%'"
```

**Check 2: Check desired-state tables**
```bash
psql $INOUT_DATABASE_URL -c "
  SELECT COUNT(*) as pending_writes
  FROM inout_dst_<connector>_<datatype>
  WHERE _status = 'pending'"
```

**Resolution:**
- If slot inactive: restart the writeback daemon
- If no pending rows: check the OSI-Mapping bridge layer
- If slot lag high: increase writeback throughput

---

### Symptom: Many writeback conflicts

**Sample a conflict**
```bash
psql $INOUT_DATABASE_URL -c "
  SELECT connector, datatype, external_id, conflict_fields,
         base_state, current_state, desired_state
  FROM inout_ops_writeback_result
  WHERE result_type = 'conflict'
  ORDER BY processed_at DESC
  LIMIT 1"
```

**Resolution:**
- Many conflicts on unrelated fields: add field coupling config
- Legitimate external changes: adjust `conflict_resolution` strategy in connector

---

### Symptom: Duplicate writes to target API

**Check write deduplication**
```bash
psql $INOUT_DATABASE_URL -c "
  SELECT external_id, COUNT(*) as write_count
  FROM inout_ops_writeback_result
  WHERE processed_at > NOW() - INTERVAL '1 hour'
    AND result_type = 'success'
  GROUP BY external_id
  HAVING COUNT(*) > 1"
```

**Resolution:**
- Verify `inout_dst_*_lwstate` is updated correctly after each write
- Check idempotency guards in `insert` operations
- Review retry logic for failed writes

---

## Database Issues

### Symptom: Database connection errors

```bash
psql $INOUT_DATABASE_URL -c "SELECT 1"

psql $INOUT_DATABASE_URL -c "
  SELECT count(*) as active_connections
  FROM pg_stat_activity
  WHERE datname = 'inandout'"
```

**Resolution:** increase `pool_size` in config; verify PostgreSQL `max_connections`.

---

### Symptom: Replication slot lag growing

```bash
psql $INOUT_DATABASE_URL -c "
  SELECT slot_name,
         pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as lag
  FROM pg_replication_slots
  WHERE slot_name LIKE 'inout_%'"
```

**Resolution:** writeback daemon may be stopped or stuck; increase processing rate.

---

### Symptom: Table bloat / disk space

```bash
psql $INOUT_DATABASE_URL -c "
  SELECT schemaname, tablename,
         pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
  FROM pg_tables
  WHERE tablename LIKE 'inout_%'
  ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
  LIMIT 20"
```

**Resolution:**
- Run housekeeping — check retention windows
- `VACUUM ANALYZE inout_<table>`
- Consider partitioning full-history tables

---

## Performance Issues

### Symptom: High CPU usage

```bash
psql $INOUT_DATABASE_URL -c "
  SELECT query, calls, mean_exec_time
  FROM pg_stat_statements
  WHERE query LIKE '%inout_%'
  ORDER BY mean_exec_time DESC
  LIMIT 10"
```

**Resolution:** add GIN indexes on frequently-queried JSONB columns; reduce poll frequency.

---

### Symptom: TLS/SSL errors

**Resolution:**
- Update CA certificates: `apt-get update && apt-get install ca-certificates`
- Check for expired certificates on the target API
- Verify hostname in `base_url` matches the certificate CN/SAN

---

## Data Quality Issues

### Symptom: Schema mismatches

```bash
curl http://localhost:9090/metrics | grep inout_schema_changes_total
```

**Resolution:**
- Review `_schema_version` in source tables
- Check API changelog for breaking changes
- Update connector field mappings

---

### Symptom: Missing records

**Check deletion tracking**
```bash
psql $INOUT_DATABASE_URL -c "
  SELECT COUNT(*) as deleted_count
  FROM inout_src_<connector>_<datatype>
  WHERE _deleted = true"
```

**Resolution:**
- Verify pagination strategy matches API behavior
- Check deletion detection logic (`tombstone` vs `diff` strategy)
- Force a full sync to reconcile: `inandout control force-full-sync --connector <name> --datatype <type>`

---

## Emergency Procedures

### Stop all processing

```bash
# Graceful drain
inandout control drain

# Docker Compose hard stop
docker compose down
```

### Clear stuck locks

```sql
UPDATE inout_ops_sync_lock
SET locked_until = NULL, locked_by = ''
WHERE locked_until < NOW();
```

### Reset watermark (data loss risk)

```bash
inandout control force-full-sync --connector <name> --datatype <type>
```

### Drop replication slot (data loss risk)

```sql
SELECT pg_drop_replication_slot('inout_writeback');
```

Then restart the writeback daemon — it will recreate the slot on startup.

---

## Escalation

If the issue persists after following this runbook:

1. Collect diagnostics: logs (last 1000 lines), metrics snapshot, database schema version, connector config (redact secrets).
2. Open a GitHub issue with: symptom description, steps taken, diagnostic output, expected vs. actual behaviour.
3. Include a minimal reproduction if possible.
