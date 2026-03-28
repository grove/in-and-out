# Deployment Playbook

This playbook covers deploying in-and-out in production environments using Docker and Kubernetes.

## Prerequisites

- PostgreSQL 15 or 16 with logical replication enabled
- Docker 20.10+ or Kubernetes 1.24+
- Network access to target external APIs
- Credential storage strategy (environment variables, Vault, AWS Secrets Manager, etc.)

## Architecture

```
┌─────────────────┐
│  External APIs  │
└────────┬────────┘
         │
    ┌────▼────────────────┐
    │ Ingestion Daemon    │ ← Webhooks + Polling  (port 9090)
    └────────┬────────────┘
             │
    ┌────────▼────────────┐
    │   PostgreSQL        │
    │   inout_src_* tables│
    │   inout_dst_* tables│
    └────────┬────────────┘
             │
    ┌────────▼────────────┐
    │ Writeback Daemon    │ ← Logical Replication
    └────────┬────────────┘
             │
    ┌────────▼────────────┐
    │  External APIs      │
    └─────────────────────┘
```

## Database Setup

### 1. Enable Logical Replication

Edit `postgresql.conf`:

```ini
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

Restart PostgreSQL after editing.

### 2. Create Database and User

```sql
CREATE DATABASE inandout;
CREATE USER inandout_app WITH PASSWORD 'secure_password_here';
GRANT ALL PRIVILEGES ON DATABASE inandout TO inandout_app;

-- Grant replication privilege for the writeback daemon
ALTER USER inandout_app WITH REPLICATION;
```

### 3. Run Migrations

Always run migrations before starting daemons. See [Database & Migrations](./database.md).

```bash
inandout db upgrade
```

---

## Docker Deployment

The repo ships multi-stage Dockerfiles at `engine/Dockerfile` and `simulator/Dockerfile`.
Build context must be the **workspace root** (required for `uv.lock`).

### 1. Build Image

```bash
docker build -t inandout:latest -f engine/Dockerfile .
```

### 2. Create Network and Database

```bash
docker network create inandout

docker run -d \
  --name postgres \
  --network inandout \
  -e POSTGRES_DB=inandout \
  -e POSTGRES_USER=inandout \
  -e POSTGRES_PASSWORD=secure_password \
  -v postgres-data:/var/lib/postgresql/data \
  postgres:16-alpine
```

### 3. Run Migrations

```bash
docker run --rm \
  --network inandout \
  -e INOUT_DATABASE_URL='postgresql://inandout:secure_password@postgres:5432/inandout' \
  -v $(pwd)/engine/config:/config:ro \
  inandout:latest \
  inandout db upgrade --config /config/ingestion.yaml
```

### 4. Deploy Ingestion Daemon

```bash
docker run -d \
  --name inandout-ingest \
  --network inandout \
  -p 9090:9090 \
  -e INOUT_DATABASE_URL='postgresql://inandout:secure_password@postgres:5432/inandout' \
  -e HUBSPOT_CLIENT_ID='...' \
  -e HUBSPOT_CLIENT_SECRET='...' \
  -v $(pwd)/connectors:/connectors:ro \
  -v $(pwd)/engine/config:/config:ro \
  inandout:latest \
  inandout ingest run --config /config/ingestion.yaml
```

### 5. Deploy Writeback Daemon

```bash
docker run -d \
  --name inandout-writeback \
  --network inandout \
  -e INOUT_DATABASE_URL='postgresql://inandout:secure_password@postgres:5432/inandout' \
  -e SALESFORCE_CLIENT_ID='...' \
  -e SALESFORCE_CLIENT_SECRET='...' \
  -v $(pwd)/connectors:/connectors:ro \
  -v $(pwd)/engine/config:/config:ro \
  inandout:latest \
  inandout writeback run --config /config/writeback.yaml
```

For local development, use `just up` which wraps the `docker-compose.yml` stack.

---

## Kubernetes Deployment

The `k8s/` directory contains a complete Kustomize manifest set. Apply it with:

```bash
kubectl apply -k k8s/
```

The key manifests are:

| File | Purpose |
|------|---------|
| `k8s/namespace.yaml` | Namespace `inandout` |
| `k8s/secret.yaml` | Database URL and API credentials |
| `k8s/configmap.yaml` | `ingestion.yaml` / `writeback.yaml` connector configs |
| `k8s/migrate-job.yaml` | One-off migration Job |
| `k8s/ingest-deployment.yaml` | Ingestion Deployment (replicas: 1) |
| `k8s/writeback-deployment.yaml` | Writeback Deployment (replicas: 1) |
| `k8s/hpa.yaml` | HorizontalPodAutoscaler |
| `k8s/servicemonitor.yaml` | Prometheus ServiceMonitor |

> **Scaling note**: Both daemons rely on distributed locks and/or logical
> replication slots. Do **not** scale them horizontally beyond `replicas: 1`
> without understanding the locking implications.

### Secrets

Create a secret with the database URL and any API credentials before applying:

```bash
kubectl create secret generic inandout-credentials \
  --namespace inandout \
  --from-literal=INOUT_DATABASE_URL='postgresql://user:pass@postgres:5432/inandout' \
  --from-literal=HUBSPOT_CLIENT_ID='...' \
  --from-literal=HUBSPOT_CLIENT_SECRET='...'
```

---

## Observability

Both daemons expose Prometheus metrics on port 9090 (ingestion) and 9090 (writeback,
typically remapped to host port 9091).

**Key metrics:**

| Metric | Description |
|--------|-------------|
| `inout_sync_lag_seconds` | Time since last successful sync |
| `inout_sync_duration_seconds` | Per-datatype sync duration histogram |
| `inout_http_request_duration_seconds` | Per-endpoint HTTP latency |
| `inout_records_processed_total` | Record throughput counter |
| `inout_circuit_breaker_state` | Circuit breaker status |
| `inout_dead_letter_depth` | Failed records awaiting resolution |
| `inout_conflicts_detected_total` | Writeback conflict counter |

Import pre-built Grafana dashboards from `observability/grafana/dashboards/`.

For the local observability stack (Prometheus + Grafana + Alertmanager), run:

```bash
just up-obs
```

Grafana is available at `http://localhost:3000`.

### Log Aggregation

Both daemons emit structured JSON logs to stdout. Configure your log shipper to
parse the `log` field as JSON (structlog format).

### Tracing

OpenTelemetry traces are exported to the configured OTLP endpoint. Enable in
the runtime config:

```yaml
observability:
  tracing:
    enabled: true
    otlp_endpoint: "http://jaeger:4318"
    sample_rate: 0.1
```
