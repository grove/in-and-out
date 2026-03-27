# in-and-out — Feature Backlog

| ID | Area | Summary | Status | Plan |
|---|---|---|---|---|
| FEAT-WH-01 | Webhook lifecycle manager | Per-route subscription registration | `planned` | [PLAN_WEBHOOK_REGISTRATION.md](PLAN_WEBHOOK_REGISTRATION.md) |
| FEAT-WH-02 | Webhook lifecycle manager | `register_body_extra` placeholder substitution | `planned` | [PLAN_WEBHOOK_REGISTRATION.md](PLAN_WEBHOOK_REGISTRATION.md) |
| FEAT-WH-03 | Webhook HTTP receiver | Custom header auth (non-HMAC connectors) | `planned` | [PLAN_WEBHOOK_REGISTRATION.md](PLAN_WEBHOOK_REGISTRATION.md) |
| FEAT-WH-04 | Webhook fan-out router | Null-value delete payload handling | `planned` | [PLAN_WEBHOOK_REGISTRATION.md](PLAN_WEBHOOK_REGISTRATION.md) |
| FEAT-SIM-01 | Simulator dispatcher | Registration-based webhook dispatch format | `planned` | [PLAN_WEBHOOK_REGISTRATION.md](PLAN_WEBHOOK_REGISTRATION.md) |
| FEAT-EXT-01 | Alert dispatcher | Pluggable alerting channels — currently Slack and PagerDuty are hardcoded in `alerting/dispatcher.py`; a `AlertChannel` protocol + entry-point registry would allow Teams, Opsgenie, SMS, etc. without forking | `idea` | — |
| FEAT-EXT-02 | Schema registry | Pluggable schema registry backend — currently schemas are persisted as local JSON files only; a `SchemaRegistryBackend` protocol would allow remote/centralised registries (Confluent Schema Registry, Apicurio, etc.) | `idea` | — |
| FEAT-EXT-03 | Schema drift handler | `on_schema_drift` callback hook — currently new columns are auto-migrated and orphans logged/pruned; a callback would allow orgs to gate schema changes behind a human-approval workflow or CI pipeline | `idea` | — |
| FEAT-EXT-04 | Dead-letter replay policy | `DeadLetterPolicy.should_replay` is already wired for writeback replay; extend the same check to ingestion dead-letter replay when that CLI path is built | `idea` | — |
