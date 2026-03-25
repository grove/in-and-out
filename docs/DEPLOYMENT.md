# Deployment Playbook

## Overview

This playbook covers deploying in-and-out ingestion and writeback tools in production environments using Docker and Kubernetes.

## Prerequisites

- PostgreSQL 18.3+ with logical replication enabled
- Docker 20.10+ or Kubernetes 1.24+
- Network access to target APIs
- Credential storage (environment variables, Vault, AWS Secrets Manager)

## Architecture

```
┌─────────────────┐
│  External APIs  │
└────────┬────────┘
         │
    ┌────▼────────────────┐
    │ Ingestion Daemon    │ ← Webhooks + Polling
    │ (Port 8080)         │
    └────────┬────────────┘
             │
    ┌────────▼────────────┐
    │   PostgreSQL 18.3   │
    │   Source Tables     │
    │   Desired State     │
    └────────┬────────────┘
             │
    ┌────────▼────────────┐
    │ Writeback Daemon    │ ← Logical Replication
    │ (No external port)  │
    └────────┬────────────┘
             │
    ┌────────▼────────┐
    │  External APIs  │
    └─────────────────┘
```

## Database Setup

### 1. Enable Logical Replication

Edit `postgresql.conf`:
```ini
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

Restart PostgreSQL.

### 2. Create Database and User

```sql
CREATE DATABASE inandout;
CREATE USER inandout_app WITH PASSWORD 'secure_password_here';
GRANT ALL PRIVILEGES ON DATABASE inandout TO inandout_app;

-- Grant replication privilege for writeback
ALTER USER inandout_app WITH REPLICATION;
```

### 3. Run Migrations

```bash
export DATABASE_URL='postgresql://inandout_app:password@localhost:5432/inandout'
inandout db migrate
```

## Docker Deployment

### 1. Build Image

```bash
docker build -t inandout:latest .
```

### 2. Create Network and Database

```bash
docker network create inandout-net

docker run -d \
  --name postgres \
  --network inandout-net \
  -e POSTGRES_DB=inandout \
  -e POSTGRES_USER=inandout \
  -e POSTGRES_PASSWORD=secure_password \
  -v postgres-data:/var/lib/postgresql/data \
  postgres:18.3
```

### 3. Run Migrations

```bash
docker run --rm \
  --network inandout-net \
  -e DATABASE_URL='postgresql://inandout:secure_password@postgres:5432/inandout' \
  inandout:latest \
  inandout db migrate
```

### 4. Deploy Ingestion Daemon

```bash
docker run -d \
  --name inandout-ingestion \
  --network inandout-net \
  -p 8080:8080 \
  -e DATABASE_URL='postgresql://inandout:secure_password@postgres:5432/inandout' \
  -e HUBSPOT_CLIENT_ID='...' \
  -e HUBSPOT_CLIENT_SECRET='...' \
  -v $(pwd)/config:/config:ro \
  inandout:latest \
  inandout ingest daemon --config /config/tool.yaml
```

### 5. Deploy Writeback Daemon

```bash
docker run -d \
  --name inandout-writeback \
  --network inandout-net \
  -e DATABASE_URL='postgresql://inandout:secure_password@postgres:5432/inandout' \
  -e SALESFORCE_CLIENT_ID='...' \
  -e SALESFORCE_CLIENT_SECRET='...' \
  -v $(pwd)/config:/config:ro \
  inandout:latest \
  inandout writeback daemon --config /config/tool.yaml
```

## Kubernetes Deployment

### 1. Create Namespace

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: inandout
```

### 2. Database Secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: postgres-credentials
  namespace: inandout
type: Opaque
stringData:
  DATABASE_URL: "postgresql://user:pass@postgres-service:5432/inandout"
```

### 3. API Credentials Secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: api-credentials
  namespace: inandout
type: Opaque
stringData:
  HUBSPOT_CLIENT_ID: "..."
  HUBSPOT_CLIENT_SECRET: "..."
  SALESFORCE_CLIENT_ID: "..."
  SALESFORCE_CLIENT_SECRET: "..."
```

### 4. ConfigMap for Connectors

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: connector-configs
  namespace: inandout
data:
  tool.yaml: |
    # Main tool configuration
    database: "${DATABASE_URL}"
    # ...
  
  hubspot.yaml: |
    # HubSpot connector configuration
    # ...
```

### 5. Ingestion Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ingestion
  namespace: inandout
spec:
  replicas: 1  # DO NOT scale horizontally - use distributed locks
  selector:
    matchLabels:
      app: ingestion
  template:
    metadata:
      labels:
        app: ingestion
    spec:
      containers:
      - name: ingestion
        image: inandout:latest
        command: ["inandout", "ingest", "daemon", "--config", "/config/tool.yaml"]
        ports:
        - containerPort: 8080
          name: webhook
        - containerPort: 9090
          name: metrics
        - containerPort: 8081
          name: health
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: postgres-credentials
              key: DATABASE_URL
        envFrom:
        - secretRef:
            name: api-credentials
        volumeMounts:
        - name: config
          mountPath: /config
          readOnly: true
        livenessProbe:
          httpGet:
            path: /health
            port: 8081
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8081
          initialDelaySeconds: 10
          periodSeconds: 5
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
      volumes:
      - name: config
        configMap:
          name: connector-configs
```

### 6. Service for Webhooks

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ingestion-webhook
  namespace: inandout
spec:
  type: LoadBalancer
  selector:
    app: ingestion
  ports:
  - port: 443
    targetPort: 8080
    name: webhook
```

### 7. Writeback Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: writeback
  namespace: inandout
spec:
  replicas: 1  # DO NOT scale horizontally - uses logical replication
  selector:
    matchLabels:
      app: writeback
  template:
    metadata:
      labels:
        app: writeback
    spec:
      containers:
      - name: writeback
        image: inandout:latest
        command: ["inandout", "writeback", "daemon", "--config", "/config/tool.yaml"]
        ports:
        - containerPort: 9091
          name: metrics
        - containerPort: 8082
          name: health
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: postgres-credentials
              key: DATABASE_URL
        envFrom:
        - secretRef:
            name: api-credentials
        volumeMounts:
        - name: config
          mountPath: /config
          readOnly: true
        livenessProbe:
          httpGet:
            path: /health
            port: 8082
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8082
          initialDelaySeconds: 10
          periodSeconds: 5
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
      volumes:
      - name: config
        configMap:
          name: connector-configs
```

## Observability

### Prometheus Metrics

Both daemons expose Prometheus metrics on ports 9090 (ingestion) and 9091 (writeback).

**Key Metrics:**
- `inout_sync_lag_seconds` - Time since last successful sync
- `inout_sync_duration_seconds` - Per-datatype sync duration histogram
- `inout_http_request_duration_seconds` - Per-endpoint request latency
- `inout_records_processed_total` - Record throughput counter
- `inout_circuit_breaker_state` - Circuit breaker status
- `inout_dead_letter_depth` - Failed records awaiting resolution
- `inout_conflicts_detected_total` - Writeback conflict counter

### Grafana Dashboards

Import pre-built dashboards from `observability/grafana/`:
- `ingestion-overview.json` - Sync health, throughput, errors
- `writeback-overview.json` - Write operations, conflicts, latency
- `connector-details.json` - Per-connector drill-down

### Log Aggregation

Both tools emit structured JSON logs to stdout. Configure your log aggregation:

**Fluentd/Fluent Bit:**
```yaml
<filter inandout.**>
  @type parser
  key_name log
  <parse>
    @type json
  </parse>
</filter>
```

**Datadog:**
```yaml
logs:
  - type: docker
    service: inandout
    source: python
```

### Tracing

OpenTelemetry traces are exported to configured OTLP endpoint:

```yaml
# config/tool.yaml
observability:
  tracing:
    enabled: true
    otlp_endpoint: "http://jaeger:4318"
    sample_rate: 0.1
```

## Operations

### Hot-Reload Configuration

```bash
# Send SIGHUP to daemon
kill -HUP <pid>

# Or via control table
inandout control reload-config
```

### Force Full Sync

```bash
inandout control force-full-sync --connector hubspot --datatype contacts
```

### Pause/Resume Processing

```bash
# Pause
inandout control pause --connector salesforce --datatype accounts

# Resume
inandout control resume --connector salesforce --datatype accounts
```

### Circuit Breaker Reset

```bash
inandout control reset-circuit-breaker --connector hubspot
```

### GDPR Data Purge

```bash
inandout control gdpr-purge \
  --connector hubspot \
  --datatype contacts \
  --external-id "contact-12345"
```

### View Sync Status

```bash
# Interactive mode
inandout interactive --database-url $DATABASE_URL

# Or SQL query
psql $DATABASE_URL -c "SELECT * FROM inout_ops_sync_run ORDER BY started_at DESC LIMIT 10"
```

### Replay Dead Letter

```bash
inandout control replay-dead-letter \
  --connector hubspot \
  --datatype contacts \
  --external-id "contact-12345"
```

## Scaling Considerations

### Ingestion Daemon

- **Horizontal Scaling**: ❌ Not supported - use distributed locks
- **Vertical Scaling**: ✅ Increase memory/CPU for high-volume connectors
- **Multiple Connectors**: ✅ Run separate instances with different connector subsets

### Writeback Daemon

- **Horizontal Scaling**: ❌ Not supported - uses single logical replication slot
- **Vertical Scaling**: ✅ Increase resources for high write volume
- **Partitioning**: ✅ Can partition by connector using filtered replication

### Database

- **Connection Pooling**: Configured via `database.pool_size` (default: 20)
- **Read Replicas**: Supported via `database.read_pool_url` for queries
- **Partitioning**: Time-based partitioning on history tables recommended for high volume

## High Availability

### Ingestion

- Run 2+ instances with distributed locks
- Only one instance will hold lock per connector/datatype
- Lossy failover: in-flight sync lost on crash, next instance resumes from last watermark

### Writeback

- Run active-passive with PostgreSQL failover
- Standby monitors replication slot, takes over on primary failure
- Connection pooling ensures graceful reconnection

### Database

- PostgreSQL HA with streaming replication
- Logical replication slot survives primary failover
- Advisory locks cleared automatically on connection loss

## Security

### Credential Management

**Environment Variables (Basic):**
```bash
export HUBSPOT_CLIENT_ID='...'
export HUBSPOT_CLIENT_SECRET='...'
```

**HashiCorp Vault:**
```yaml
auth:
  credential_ref: vault:secret/data/hubspot/oauth
```

**AWS Secrets Manager:**
```yaml
auth:
  credential_ref: aws:secretsmanager:us-east-1:hubspot-oauth
```

### Network Security

- Ingestion webhook endpoint: Expose via HTTPS with TLS
- Health endpoints: Keep internal, not public
- Metrics endpoints: Restrict to monitoring systems
- Database: Use TLS, restrict to application network

### Webhook Signature Verification

Always configure webhook signatures:
```yaml
webhooks:
  signature:
    algorithm: hmac-sha256
    header: "X-Webhook-Signature"
    credential_ref: webhook_secret
```

## Monitoring

### Health Checks

- `/health` - Liveness probe (process alive)
- `/ready` - Readiness probe (actively processing)

Both must respond in <1s.

### Alerts

Recommended alert rules:

1. **Sync Lag**: `inout_sync_lag_seconds > 3600` (1 hour)
2. **Circuit Breaker Open**: `inout_circuit_breaker_state == 1`
3. **Dead Letter Growth**: `rate(inout_dead_letter_depth[5m]) > 0`
4. **Conflict Rate High**: `rate(inout_conflicts_detected_total[5m]) > threshold`
5. **Replication Slot Lag**: `inout_replication_slot_lag_bytes > 1GB`

### Dashboards

Import from `observability/grafana/`:
- Sync health and throughput
- Error rates and circuit breaker state
- Writeback conflicts and dead letter depth
- Per-datatype latency histograms

## Troubleshooting

See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for detailed runbook.

### Common Issues

**Ingestion daemon won't start:**
- Check DATABASE_URL connection
- Verify schema migrations ran
- Check connector YAML validation

**Webhooks not received:**
- Verify webhook registration succeeded
- Check network path from external API
- Verify signature validation

**Writeback stuck:**
- Check replication slot exists and active
- Verify desired-state tables populated
- Check for circuit breaker open state

**High memory usage:**
- Reduce page_size in pagination config
- Enable streaming mode for large responses
- Check for memory leaks in linked object resolution

## Backup and Recovery

### Database Backup

```bash
# Full backup
pg_dump $DATABASE_URL > backup.sql

# Restore
psql $DATABASE_URL < backup.sql
```

### Configuration Backup

Store connector YAML files in version control (Git).

### Disaster Recovery

1. Restore database from backup
2. Redeploy daemons with same config
3. Watermarks preserved - ingestion resumes from last position
4. Writeback resumes from replication slot position

## Upgrades

### Zero-Downtime Upgrade

1. **Database migration:** Run migrations before deploying new code
2. **Rolling restart:** Update one daemon at a time
3. **Verify health:** Check `/ready` endpoint before proceeding

### Breaking Changes

- Review CHANGELOG for breaking changes
- Test in staging environment first
- Have rollback plan ready

## Performance Tuning

### Connection Pool

```yaml
database:
  pool_size: 20          # Max concurrent connections
  pool_timeout: 30s       # Wait time for connection
  read_pool_size: 10      # Read replica pool (optional)
```

### Rate Limiting

Start conservative, increase based on monitoring:
```yaml
rate_limit:
  requests_per_second: 5.0   # Start low
  burst: 10                   # Allow small bursts
```

### Concurrency

```yaml
ingestion:
  linked_objects:
    - max_concurrency: 10    # Concurrent child fetches
```

### Batch Size

```yaml
list:
  pagination:
    offset:
      page_size: 100          # Start with 100, increase to 1000 if supported
```

## Support

- GitHub: https://github.com/grove/in-and-out
- Documentation: https://in-and-out.readthedocs.io
- Issues: https://github.com/grove/in-and-out/issues
