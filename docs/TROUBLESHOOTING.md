# Troubleshooting Runbook

## Quick Diagnostics

```bash
# Check daemon health
curl http://localhost:8081/health
curl http://localhost:8081/ready

# View recent sync runs
psql $DATABASE_URL -c "
  SELECT connector, datatype, mode, status, started_at, finished_at, 
         records_inserted, records_updated, records_deleted
  FROM inout_ops_sync_run 
  ORDER BY started_at DESC 
  LIMIT 20"

# Check circuit breaker state
psql $DATABASE_URL -c "
  SELECT connector, datatype, state, opened_at, fail_count
  FROM inout_ops_circuit_breaker
  WHERE state != 'closed'"

# View dead letter queue
psql $DATABASE_URL -c "
  SELECT connector, datatype, external_id, error, failed_at
  FROM inout_dead_letter
  ORDER BY failed_at DESC
  LIMIT 10"

# Check connector health
psql $DATABASE_URL -c "
  SELECT connector, datatype, status, last_healthy_at, marked_unhealthy_at
  FROM inout_ops_connector_health"
```

## Ingestion Issues

### Symptom: No data ingested

**Check 1: Verify connector is running**
```bash
psql $DATABASE_URL -c "
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
# Docker
docker logs inandout-ingestion | grep error

# Kubernetes
kubectl logs -n inandout deployment/ingestion | grep error
```

**Resolution:**
- If circuit breaker open: `inandout control reset-circuit-breaker --connector <name>`
- If auth errors: Verify credentials in environment
- If connection errors: Check network connectivity to API

### Symptom: Sync lag increasing

**Check 1: View sync lag metric**
```bash
curl http://localhost:9090/metrics | grep inout_sync_lag_seconds
```

**Check 2: Check sync duration**
```bash
psql $DATABASE_URL -c "
  SELECT connector, datatype, 
         AVG(EXTRACT(EPOCH FROM (finished_at - started_at))) as avg_duration_secs
  FROM inout_ops_sync_run
  WHERE finished_at > NOW() - INTERVAL '1 day'
  GROUP BY connector, datatype
  ORDER BY avg_duration_secs DESC"
```

**Resolution:**
- Increase polling interval if API is slow
- Enable bulk export mode for large datasets
- Check for rate limiting (429 responses)
- Reduce page_size if responses are timing out

### Symptom: Webhooks not received

**Check 1: Verify webhook registration**
```bash
psql $DATABASE_URL -c "
  SELECT connector, webhook_id, callback_url, status, registered_at
  FROM inout_ops_webhook_subscriptions"
```

**Check 2: Test webhook endpoint**
```bash
curl -X POST http://localhost:8080/webhook/<connector> \
  -H "Content-Type: application/json" \
  -d '{"event": "test"}'
```

**Check 3: Verify signature validation**
- Review logs for "webhook_signature_invalid"
- Check that webhook secret matches external system

**Resolution:**
- Re-register webhook: Check connector registration config
- Verify callback URL is publicly accessible
- Check firewall rules allow inbound from API provider

### Symptom: Many records in dead letter queue

**Check 1: Review dead letter errors**
```bash
psql $DATABASE_URL -c "
  SELECT error, COUNT(*) as count
  FROM inout_dead_letter
  GROUP BY error
  ORDER BY count DESC"
```

**Check 2: Sample failed record**
```bash
psql $DATABASE_URL -c "
  SELECT connector, datatype, external_id, payload, error, failed_at
  FROM inout_dead_letter
  LIMIT 1"
```

**Resolution:**
- If validation errors: Check field mappings in connector config
- If 422 errors: Review API schema requirements
- If 401 errors: Refresh credentials
- Replay after fixing: `inandout control replay-dead-letter --connector <name> --datatype <type>`

### Symptom: Circuit breaker keeps opening

**Check 1: View failure pattern**
```bash
psql $DATABASE_URL -c "
  SELECT status, COUNT(*) as count
  FROM inout_ops_sync_run
  WHERE connector = '<name>' 
    AND started_at > NOW() - INTERVAL '1 hour'
  GROUP BY status"
```

**Check 2: Check HTTP errors**
```bash
curl http://localhost:9090/metrics | grep inout_http_errors_total
```

**Resolution:**
- If 500 errors: Target API may be down, wait for recovery
- If empty responses: Review circuit breaker thresholds
- If auth errors: Refresh OAuth tokens
- Manual reset: `inandout control reset-circuit-breaker --connector <name>`

### Symptom: Duplicate records created

**Check 1: Verify deduplication working**
```bash
psql $DATABASE_URL -c "
  SELECT external_id, COUNT(*) as count
  FROM inout_<connector>_<datatype>
  WHERE _deleted = false
  GROUP BY external_id
  HAVING COUNT(*) > 1"
```

**Resolution:**
- Check webhook deduplication (event_id tracking)
- Verify primary_key config is correct
- Check for race conditions between webhook and poll

## Writeback Issues

### Symptom: Changes not written back

**Check 1: Verify replication slot active**
```bash
psql $DATABASE_URL -c "
  SELECT slot_name, active, confirmed_flush_lsn
  FROM pg_replication_slots
  WHERE slot_name LIKE 'inout_%'"
```

**Check 2: Check desired-state tables**
```bash
psql $DATABASE_URL -c "
  SELECT COUNT(*) as pending_writes
  FROM inout_dst_<connector>_<datatype>
  WHERE _processed_at IS NULL"
```

**Resolution:**
- If slot inactive: Restart writeback daemon
- If no pending rows: Check OSI-Mapping bridge layer
- If slot lag high: Increase writeback throughput

### Symptom: Many writeback conflicts

**Check 1: Review conflict resolution**
```bash
psql $DATABASE_URL -c "
  SELECT resolution, COUNT(*) as count
  FROM inout_ops_writeback_result
  WHERE result_type = 'conflict'
    AND processed_at > NOW() - INTERVAL '1 day'
  GROUP BY resolution"
```

**Check 2: Sample conflict**
```bash
psql $DATABASE_URL -c "
  SELECT connector, datatype, external_id, conflict_fields, 
         base_state, current_state, desired_state
  FROM inout_ops_writeback_result
  WHERE result_type = 'conflict'
  ORDER BY processed_at DESC
  LIMIT 1"
```

**Resolution:**
- If many conflicts on unrelated fields: Add field coupling config
- If legitimate external changes: Adjust conflict_resolution strategy
- If stale base state: Verify ingestion is running frequently

### Symptom: Writeback performance slow

**Check 1: Check write duration**
```bash
curl http://localhost:9091/metrics | grep inout_sync_duration_seconds
```

**Check 2: Check batch size**
```bash
psql $DATABASE_URL -c "
  SELECT connector, datatype, COUNT(*) as batch_size
  FROM inout_dst_<connector>_<datatype>
  WHERE _processed_at IS NULL
  GROUP BY connector, datatype"
```

**Resolution:**
- Enable batch writes if API supports
- Increase concurrency limits
- Check for rate limiting (429 responses)

### Symptom: Duplicate writes to target API

**Check 1: Check write deduplication**
```bash
psql $DATABASE_URL -c "
  SELECT external_id, COUNT(*) as write_count
  FROM inout_ops_writeback_result
  WHERE processed_at > NOW() - INTERVAL '1 hour'
    AND result_type = 'success'
  GROUP BY external_id
  HAVING COUNT(*) > 1"
```

**Resolution:**
- Verify last-written-state table is updated correctly
- Check idempotency guards in insert operations
- Review retry logic for failed writes

## Database Issues

### Symptom: Database connection errors

**Check 1: Test connection**
```bash
psql $DATABASE_URL -c "SELECT 1"
```

**Check 2: Check connection pool**
```bash
psql $DATABASE_URL -c "
  SELECT count(*) as active_connections
  FROM pg_stat_activity
  WHERE datname = 'inandout'"
```

**Resolution:**
- Increase pool_size in config
- Check PostgreSQL max_connections setting
- Verify network connectivity

### Symptom: Replication slot lag growing

**Check 1: Check slot lag**
```bash
psql $DATABASE_URL -c "
  SELECT slot_name, 
         pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as lag
  FROM pg_replication_slots
  WHERE slot_name LIKE 'inout_%'"
```

**Resolution:**
- Writeback daemon may be stopped or stuck
- Increase writeback processing rate
- Check for blocked transactions

### Symptom: Table bloat / disk space

**Check 1: Check table sizes**
```bash
psql $DATABASE_URL -c "
  SELECT schemaname, tablename, 
         pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
  FROM pg_tables
  WHERE tablename LIKE 'inout_%'
  ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
  LIMIT 20"
```

**Resolution:**
- Run housekeeping: Check retention windows
- VACUUM tables: `VACUUM ANALYZE inout_<table>`
- Consider partitioning history tables

## Performance Issues

### Symptom: High CPU usage

**Check 1: Identify expensive queries**
```bash
psql $DATABASE_URL -c "
  SELECT query, calls, mean_exec_time
  FROM pg_stat_statements
  WHERE query LIKE '%inout_%'
  ORDER BY mean_exec_time DESC
  LIMIT 10"
```

**Resolution:**
- Add indexes on frequently queried columns
- Reduce polling frequency
- Optimize JSONB queries with GIN indexes

### Symptom: High memory usage

**Check 1: Check Python process memory**
```bash
# Docker
docker stats inandout-ingestion

# Linux
ps aux | grep inandout
```

**Resolution:**
- Reduce page_size for large responses
- Enable streaming mode
- Check for memory leaks in long-running processes

## Network Issues

### Symptom: Timeouts to external API

**Check 1: Test connectivity**
```bash
curl -I https://api.example.com
```

**Check 2: Review timeout config**
```yaml
connection:
  timeout: "60s"  # Increase if API is slow
```

**Resolution:**
- Increase timeout in connector config
- Check for network latency issues
- Verify target API is responsive

### Symptom: TLS/SSL errors

**Resolution:**
- Update CA certificates: `apt-get update && apt-get install ca-certificates`
- Check for expired certificates
- Verify hostname in base_url matches certificate

## Data Quality Issues

### Symptom: Schema mismatches

**Check 1: View schema changes**
```bash
curl http://localhost:9090/metrics | grep inout_schema_changes_total
```

**Resolution:**
- Review `_schema_version` column in source tables
- Check API changelog for breaking changes
- Update connector field mappings

### Symptom: Missing records

**Check 1: Check deletion tracking**
```bash
psql $DATABASE_URL -c "
  SELECT COUNT(*) as deleted_count
  FROM inout_<connector>_<datatype>
  WHERE _deleted = true"
```

**Check 2: Check pagination termination**
- Review pagination config
- Check for pagination drift

**Resolution:**
- Verify pagination strategy matches API behavior
- Check deletion detection logic
- Force full sync to reconcile

## Emergency Procedures

### Stop All Processing

```bash
# Graceful drain
inandout control drain

# Force stop
kill -9 <pid>
```

### Clear Stuck Locks

```bash
psql $DATABASE_URL -c "
  UPDATE inout_ops_sync_lock
  SET locked_until = NULL, locked_by = ''
  WHERE locked_until < NOW()"
```

### Reset Watermark (Data Loss Risk!)

```bash
# Only for non-delta-only sources
inandout control force-full-sync --connector <name> --datatype <type>
```

### Drop Replication Slot (Data Loss Risk!)

```sql
SELECT pg_drop_replication_slot('inout_writeback');
```

Then restart writeback daemon to recreate slot.

## Escalation

If issue persists after following runbook:

1. Collect diagnostics:
   - Logs (last 1000 lines)
   - Metrics snapshot
   - Database schema version
   - Connector config (redact secrets)

2. Create GitHub issue with:
   - Symptom description
   - Steps taken
   - Diagnostic output
   - Expected vs actual behavior

3. Include minimal reproduction if possible
