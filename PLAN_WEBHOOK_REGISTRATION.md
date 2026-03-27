# Plan: Registration-based webhook support

Items: FEAT-WH-01, FEAT-WH-02, FEAT-WH-03, FEAT-WH-04, FEAT-SIM-01  
Tracker: [BACKLOG.md](BACKLOG.md)  
Driver: Tripletex connector

---

## Background

Two webhook delivery models exist in the wild:

**Fan-out (passive)** — the engine registers one static callback URL with the
source; the source routes all events to it. Discriminating by event type is done
inside the engine via `fan_out.routes[].discriminator_value`. HubSpot uses this.

**Registration-based (active)** — the engine must POST to the source's
subscription API once per event type, supplying the callback URL and any
required body fields. Tripletex uses this. On delivery the source calls each
registered URL separately; the payload contains an `event` field identifying
which subscription fired.

The config model (`WebhookRegistrationConfig`) and the connector YAML
(`connectors/tripletex.example.yaml`) already express the registration-based
model. The _implementation_ of the four affected components is not yet complete.

---

## FEAT-WH-01 — Per-route registration

**What it does**: When `registration.per_route_registration = true`, the
lifecycle manager iterates `fan_out.routes` and POSTs once per route.

**What exists today**: `WebhookRegistrationConfig.per_route_registration: bool`
is parsed but the lifecycle manager issues a single POST regardless.

**File**: `src/inandout/webhooks/lifecycle.py` (or wherever `register()` lives)

**Change**:
```python
if config.registration.per_route_registration:
    sub_ids = []
    for route in config.fan_out.routes:
        sub_id = await _register_one(route, config, credential_fn)
        sub_ids.append(sub_id)
    return sub_ids          # one id per route
else:
    return [await _register_one(None, config, credential_fn)]
```

**Storage**: The persisted state must hold a list of subscription IDs so each
can be DELETEd individually on deregistration.

---

## FEAT-WH-02 — `register_body_extra` placeholder substitution

**What it does**: Resolves `${route_event}` and `${credential:<ref>}` in the
extra body fields added to each subscription POST.

**Depends on**: FEAT-WH-01 (called inside `_register_one`)

**Tripletex YAML** (`connectors/tripletex.example.yaml`):
```yaml
registration:
  register_body_extra:
    event: "${route_event}"
    authHeaderName: X-Simulator-Auth
    authHeaderValue: "${credential:tripletex_webhook_secret}"
```

**File**: `src/inandout/webhooks/lifecycle.py`

**Change**: Resolver function, called per route before the POST:
```python
def resolve_extra(extra: dict, route_event: str, credential_fn) -> dict:
    out = {}
    for k, v in extra.items():
        if v == "${route_event}":
            out[k] = route_event
        elif v.startswith("${credential:") and v.endswith("}"):
            ref = v[len("${credential:"):-1]
            out[k] = credential_fn(ref)
        else:
            out[k] = v
    return out
```

`credential_fn` is `lambda ref: os.environ.get(f"INOUT_CREDENTIAL_{ref.upper()}", "")`.

---

## FEAT-WH-03 — Custom header auth at receive time

**What it does**: Validates the header-based auth that Tripletex sends on every
delivery instead of an HMAC signature.

**Config** (`src/inandout/config/webhooks.py`):
```python
auth_header_name: str | None = None           # already in model
auth_header_credential_ref: str | None = None  # already in model
```

**File**: `src/inandout/webhooks/receiver.py` (or wherever signature validation
lives in the HTTP handler)

**Logic** (three-way branch):
```python
if config.signature:
    _verify_hmac(request, config.signature)            # existing path
elif config.auth_header_name and config.auth_header_credential_ref:
    expected = credential_fn(config.auth_header_credential_ref)
    received = request.headers.get(config.auth_header_name, "")
    if not hmac.compare_digest(received, expected):    # constant-time
        raise HTTPException(status_code=401)
# else: no auth configured — accept (IP allowlist handles it)
```

**Security note**: Use `hmac.compare_digest` to avoid timing attacks even
though Tripletex tokens are not secrets of the same sensitivity as HMAC keys.

---

## FEAT-WH-04 — Null-value delete payload

**What it does**: Handles Tripletex delete payloads where `"value": null` and
the deleted record's ID is at the top level.

**Payload shape**:
```json
{ "subscriptionId": 42, "event": "customer.delete", "id": 10001, "value": null }
```

**File**: wherever the fan-out router extracts the record from an inbound
webhook payload (likely `src/inandout/webhooks/fanout.py`)

**Change**:
1. After discriminating the event type, check `payload.get("value") is None`.
2. If null, treat operation as `delete`.
3. Extract identity from `FanOutRoute.notification_external_id_field` (default
   `"id"`) at the top level of `payload`.
4. Build `{pk_field: extracted_id}` as the minimal record dict; skip record
   normalisation.

This keeps the happy path (non-null `value`) unchanged.

---

## FEAT-SIM-01 — Registration-based dispatch in the simulator

**What it does**: Makes the simulator send a Tripletex-shaped payload when
`per_route_registration = true`, so the engine's fan-out discriminator matches.

**File**: `src/inandout/simulator/webhooks.py` — `WebhookDispatcher.dispatch()`

**Payload today** (HubSpot fan-out style):
```json
{ "objectId": 1001, "subscriptionType": "contact.creation", ... }
```

**Payload for registration-based** (Tripletex style):
```json
{ "subscriptionId": 0, "event": "customer.create", "id": 1001, "value": { ... } }
```

**Change**:
```python
if config.registration and config.registration.per_route_registration:
    route_event = _operation_to_route_event(config, operation)
    payload = {
        "subscriptionId": 0,
        "event": route_event,
        "id": record_id,
        "value": None if operation == "delete" else record_data,
    }
else:
    payload = _hubspot_style_payload(config, operation, record_id, record_data)
```

`_operation_to_route_event` maps `("create"|"update"|"delete", datatype)` to
the matching `fan_out.routes[].match` value (e.g. `"customer.create"`).

---

## Delivery order

All five items are tightly coupled; the recommended implementation order is:

1. **FEAT-WH-01** — skeleton of `_register_one`, loop, ID storage
2. **FEAT-WH-02** — plug placeholder resolver into `_register_one`
3. **FEAT-WH-03** — auth branch in receiver (independent, can go in parallel)
4. **FEAT-WH-04** — null-value guard in fan-out router (independent)
5. **FEAT-SIM-01** — simulator payload shape (integrates last, after 1–4 are
   testable end-to-end with a real Tripletex sandbox)

---

## Additional patterns (SuperOffice CRM + Shopify)

These four items generalise the webhook subsystem beyond Tripletex to cover
other registration-based connectors. They are independent of FEAT-WH-01–05.

### FEAT-WH-05 — HMAC signature encoding variant

**Problem**: The current `SignatureConfig` specifies `algorithm` but not how the
resulting digest is serialised in the HTTP header.

| Connector | Header | Format |
|---|---|---|
| GitHub | `X-Hub-Signature-256` | `sha256=<hex>` |
| HubSpot | `X-HubSpot-Signature-v3` | raw base64 |
| SuperOffice | `X-SuperOffice-Signature` | raw base64 |
| Shopify | `X-Shopify-Hmac-Sha256` | raw base64 |

**Config model** (`src/inandout/config/webhooks.py`):
```python
class SignatureEncoding(StrEnum):
    hex_prefix = "hex_prefix"   # "sha256=deadbeef..."
    base64     = "base64"       # "abc123=="

class SignatureConfig(BaseModel):
    ...
    encoding: SignatureEncoding = SignatureEncoding.hex_prefix
```

**Receiver change**: after computing `hmac.new(secret, body, digestmod).digest()`,
branch on `encoding`:
```python
if config.encoding == SignatureEncoding.base64:
    computed = base64.b64encode(digest).decode()
else:
    computed = f"{config.algorithm.value.replace('-', '')}={digest.hex()}"
```

**Scope**: `SignatureConfig` model + receiver validation only. No lifecycle
manager changes.

---

### FEAT-WH-06 — `register_headers_extra`

**Problem**: SuperOffice's webhook subscription definition has a `Headers`
property — a dict of headers that SuperOffice will include in every delivery
POST it sends. This lets you configure SuperOffice to add an auth header to its
outbound webhooks, which the receiver then verifies via FEAT-WH-03.

Example SuperOffice registration body:
```json
{
  "Name": "Contact Handler",
  "Events": ["contact.created", "contact.changed", "contact.deleted"],
  "TargetUrl": "https://engine.example.com/webhooks/superoffice",
  "Secret": "...",
  "State": "Active",
  "Headers": {"X-Inout-Auth": "${credential:superoffice_webhook_token}"}
}
```

**Config model** (`WebhookRegistrationConfig`):
```python
register_headers_extra: dict[str, str] = {}
```

**Lifecycle manager**: same placeholder resolver as `register_body_extra`
(FEAT-WH-02) — resolve `${credential:<ref>}` values before POSTing. Include
the resolved dict under the field name given by a sibling config key
`register_headers_field: str = "Headers"`.

---

### FEAT-WH-07 — Events-array single registration

**Problem**: SuperOffice can register all event types in a single webhook
subscription (`Events: ["contact.created", "contact.changed", "contact.deleted"]`).
Neither `per_route_registration: true` (one POST per route) nor the default
(one POST, no events list) covers this.

This is a third registration mode: **one POST, events array derived from
`fan_out.routes`**.

**Config model** (`WebhookRegistrationConfig`):
```python
register_events_field: str | None = None
# When set, the lifecycle manager collects fan_out.routes[].match into a list
# and includes it under this key in the single registration POST body.
# e.g. register_events_field = "Events"
```

**Lifecycle manager logic** (mutually exclusive with `per_route_registration`):
```python
if config.registration.per_route_registration:
    # FEAT-WH-01: one POST per route
elif config.registration.register_events_field:
    events = [r.match for r in config.fan_out.routes]
    body = {config.registration.register_events_field: events, ...}
    sub_id = await _register_one(body, config)
else:
    # Legacy: single registration, no events list
```

**Connector YAML** (SuperOffice example):
```yaml
registration:
  register_path: /api/v1/Webhook
  register_events_field: Events
  id_response_path: WebhookId
  register_body_extra:
    State: Active
    Type: webhook
    Name: "in-and-out contact handler"
```

---

### FEAT-WH-08 — Header-based fan-out discriminator

**Problem**: SuperOffice includes the event name in the `X-SuperOffice-Event`
HTTP header (`contact.changed`). Route matching from a header avoids parsing
the JSON body before auth, and is cleaner for connectors that put no
discriminator in the body.

**Config model** (`FanOutConfig`):
```python
discriminator: str | None = None        # body field (existing, now optional)
discriminator_header: str | None = None # HTTP header alternative
```

At least one of the two must be set. If both are set, `discriminator_header`
takes precedence (header is cheaper — no body parse needed).

**Fan-out router change**:
```python
if config.fan_out.discriminator_header:
    event_value = request.headers.get(config.fan_out.discriminator_header)
else:
    payload = await request.json()
    event_value = _get_nested(payload, config.fan_out.discriminator)
```

**Connector YAML** (SuperOffice):
```yaml
webhooks:
  path: /webhooks/superoffice
  signature:
    algorithm: hmac-sha256
    header: X-SuperOffice-Signature
    credential_ref: superoffice_webhook_secret
    encoding: base64
  fan_out:
    discriminator_header: X-SuperOffice-Event
    routes:
      - match: contact.created
        datatype: contacts
      - match: contact.changed
        datatype: contacts
      - match: contact.deleted
        datatype: contacts
```
