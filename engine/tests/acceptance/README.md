# Acceptance Tests

Acceptance tests run against real external APIs and are **skipped by default** unless
the required environment variables are set.

## Warning

These tests make real API calls and may:
- Consume API rate-limit quota
- Mutate data if the API key has write permissions
- Incur costs on usage-based APIs

Always use sandbox/test credentials and **never run against production** without explicit approval.

## Running Acceptance Tests

### HubSpot

```bash
export INOUT_ACCEPTANCE_HUBSPOT_API_KEY="pat-xxx-..."
uv run pytest tests/acceptance/test_hubspot_acceptance.py -m acceptance -v
```

### Salesforce

```bash
export INOUT_ACCEPTANCE_SF_CLIENT_ID="..."
export INOUT_ACCEPTANCE_SF_CLIENT_SECRET="..."
export INOUT_ACCEPTANCE_SF_INSTANCE_URL="https://myorg.salesforce.com"
uv run pytest tests/acceptance/test_salesforce_acceptance.py -m acceptance -v
```

### All acceptance tests

```bash
uv run pytest tests/acceptance/ -m acceptance -v
```

### Deselect acceptance tests in normal runs

```bash
uv run pytest -m "not acceptance"
```

## Environment Variables

| Variable | Description |
|---|---|
| `INOUT_ACCEPTANCE_HUBSPOT_API_KEY` | HubSpot Private App API key |
| `INOUT_ACCEPTANCE_SF_CLIENT_ID` | Salesforce Connected App Client ID |
| `INOUT_ACCEPTANCE_SF_CLIENT_SECRET` | Salesforce Connected App Client Secret |
| `INOUT_ACCEPTANCE_SF_INSTANCE_URL` | Salesforce instance URL (e.g. `https://myorg.salesforce.com`) |
