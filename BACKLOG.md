# in-and-out — Feature Backlog

| ID | Area | Summary | Status | Plan |
|---|---|---|---|---|
| FEAT-WH-01 | Webhook lifecycle manager | Per-route subscription registration | `done` | [PLAN_WEBHOOK_REGISTRATION.md](PLAN_WEBHOOK_REGISTRATION.md) |
| FEAT-WH-02 | Webhook lifecycle manager | `register_body_extra` placeholder substitution | `done` | [PLAN_WEBHOOK_REGISTRATION.md](PLAN_WEBHOOK_REGISTRATION.md) |
| FEAT-WH-03 | Webhook HTTP receiver | Custom header auth (non-HMAC connectors) | `done` | [PLAN_WEBHOOK_REGISTRATION.md](PLAN_WEBHOOK_REGISTRATION.md) |
| FEAT-WH-04 | Webhook fan-out router | Null-value delete payload handling | `done` | [PLAN_WEBHOOK_REGISTRATION.md](PLAN_WEBHOOK_REGISTRATION.md) |
| FEAT-SIM-01 | Simulator dispatcher | Registration-based webhook dispatch format | `done` | [PLAN_WEBHOOK_REGISTRATION.md](PLAN_WEBHOOK_REGISTRATION.md) |
| FEAT-WH-05 | Webhook HTTP receiver | HMAC signature encoding variant (`base64` vs `hex_prefix`) | `done` | [PLAN_WEBHOOK_REGISTRATION.md](PLAN_WEBHOOK_REGISTRATION.md) |
| FEAT-WH-06 | Webhook lifecycle manager | `register_headers_extra` — delivery headers configured at registration | `done` | [PLAN_WEBHOOK_REGISTRATION.md](PLAN_WEBHOOK_REGISTRATION.md) |
| FEAT-WH-07 | Webhook lifecycle manager | Events-array single registration (all events in one POST) | `done` | [PLAN_WEBHOOK_REGISTRATION.md](PLAN_WEBHOOK_REGISTRATION.md) |
| FEAT-WH-08 | Webhook fan-out router | Header-based fan-out discriminator | `done` | [PLAN_WEBHOOK_REGISTRATION.md](PLAN_WEBHOOK_REGISTRATION.md) |
| FEAT-EXT-01 | Alert dispatcher | Pluggable alerting channels — currently Slack and PagerDuty are hardcoded in `alerting/dispatcher.py`; a `AlertChannel` protocol + entry-point registry would allow Teams, Opsgenie, SMS, etc. without forking | `idea` | — |
| FEAT-EXT-02 | Schema registry | Pluggable schema registry backend — currently schemas are persisted as local JSON files only; a `SchemaRegistryBackend` protocol would allow remote/centralised registries (Confluent Schema Registry, Apicurio, etc.) | `idea` | — |
| FEAT-EXT-03 | Schema drift handler | `on_schema_drift` callback hook — currently new columns are auto-migrated and orphans logged/pruned; a callback would allow orgs to gate schema changes behind a human-approval workflow or CI pipeline | `idea` | — |
| FEAT-EXT-04 | Dead-letter replay policy | `DeadLetterPolicy.should_replay` is already wired for writeback replay; extend the same check to ingestion dead-letter replay when that CLI path is built | `idea` | — |
