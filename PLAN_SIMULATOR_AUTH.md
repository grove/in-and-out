# Plan: Authentication in the simulator

> Status: **Draft**
> Goal: the engine should authenticate when calling the simulator's connector
> API endpoints (list, lookup, writeback, OAuth token), and the simulator
> should verify those credentials — the same way the real provider would.
> The browser UI does not need authentication.

---

## Design principle

**Auth is read from the existing connector YAML config — no new config
required.**  The simulator already has `connector.auth` for every connector.
It should resolve the same `credential_ref` env vars the engine resolves and
reject requests that don't match.  This means a misconfigured `credential_ref`
or wrong header name in the connector YAML causes a 401 from the simulator
during development, before anything ever contacts a real API.

If the relevant credential env var is **not set**, the simulator logs a warning
at startup and skips the check for that connector (fail-open, dev convenience).

---

## Scope

| Surface | In scope? |
|---|---|
| `GET/POST /{c}/{d}/…` — list, lookup, writeback routes called by the engine | **Yes** |
| `POST {token_url}` — fake OAuth2 token endpoint called by the engine | **Yes** |
| `GET/PUT/DELETE /{c}/admin/{d}/{id}` — admin CRUD used by test scripts | No |
| `GET /ui/…`, SSE `/events` — browser UI | No |

---

## Auth types to support

Every auth type in `connector.auth` that the engine can be configured to use:

| `auth.type` | Engine sends | Simulator checks |
|---|---|---|
| `oauth2` (client credentials) | `Authorization: Bearer <token>` (obtained from token endpoint) | Token endpoint validates `client_id` + `client_secret`; issued token is `sim_token_{connector_name}`; all other routes reject any other Bearer value |
| `api_key` (header) | `{auth.api_key.name}: <secret>` | Header value matches `INOUT_CREDENTIAL_<credential_ref>` |
| `api_key` (query) | `?{auth.api_key.name}=<secret>` | Query param value matches `INOUT_CREDENTIAL_<credential_ref>` |
| `basic` | `Authorization: Basic <base64(user:pass)>` | Username + password match resolved credentials |
| `none` / absent | Nothing | No check |

---

## Implementation

### `_make_auth_dependency(connector)` in `route_builder.py`

```python
def _make_auth_dependency(connector: ConnectorConfig):
    """Return a FastAPI Depends-able coroutine that enforces connector auth,
    or None if auth is not configured / credential not in env."""
    auth = connector.auth
    if auth is None:
        return None

    if auth.type == "oauth2":
        expected_token = f"sim_token_{connector.name}"

        async def _dep_oauth2(authorization: str | None = Header(default=None)):
            if authorization != f"Bearer {expected_token}":
                raise HTTPException(
                    401,
                    detail=f"Expected 'Authorization: Bearer {expected_token}'",
                    headers={"WWW-Authenticate": "Bearer"},
                )

        return _dep_oauth2

    if auth.type == "api_key" and auth.api_key:
        secret = _resolve_credential(auth.credential_ref)
        if secret is None:
            return None  # env var not set — warn at startup, skip check
        loc = auth.api_key.location  # "header" | "query"
        name = auth.api_key.name

        if loc == "header":
            async def _dep_api_key_header(request: Request):
                if request.headers.get(name) != secret:
                    raise HTTPException(401, detail=f"Expected header {name}=<secret>")
            return _dep_api_key_header

        if loc == "query":
            async def _dep_api_key_query(request: Request):
                if request.query_params.get(name) != secret:
                    raise HTTPException(401, detail=f"Expected query param {name}=<secret>")
            return _dep_api_key_query

    if auth.type == "basic":
        username = _resolve_credential(getattr(auth, "username_ref", None))
        password = _resolve_credential(getattr(auth, "password_ref", None))
        if username is None or password is None:
            return None

        async def _dep_basic(credentials: HTTPBasicCredentials = Depends(HTTPBasic(auto_error=False))):
            if credentials is None:
                raise HTTPException(401, headers={"WWW-Authenticate": "Basic"})
            ok = (
                secrets.compare_digest(credentials.username, username)
                and secrets.compare_digest(credentials.password, password)
            )
            if not ok:
                raise HTTPException(401, headers={"WWW-Authenticate": "Basic"})
        return _dep_basic

    return None  # unrecognised type — accept anything
```

### Attach to routes in `build_connector_router()`

```python
auth_dep = _make_auth_dependency(connector)
deps = [Depends(auth_dep)] if auth_dep else []

# applied to every list / single-record / writeback route:
router.add_api_route(path, handler, methods=[method], dependencies=deps)
```

### OAuth2 token endpoint validation

Currently the fake token endpoint issues a token unconditionally.  When
`oauth2.client_id_ref` and `oauth2.client_secret_ref` are set, validate
the incoming `client_id` + `client_secret` form fields:

```python
@router.post(token_path)
async def _token(
    client_id: str = Form(...),
    client_secret: str = Form(...),
):
    expected_id     = _resolve_credential(connector.auth.oauth2.client_id_ref)
    expected_secret = _resolve_credential(connector.auth.oauth2.client_secret_ref)
    if expected_id and not secrets.compare_digest(client_id, expected_id):
        raise HTTPException(401, "invalid_client")
    if expected_secret and not secrets.compare_digest(client_secret, expected_secret):
        raise HTTPException(401, "invalid_client")
    return {
        "access_token": f"sim_token_{connector.name}",
        "token_type": "bearer",
        "expires_in": 3600,
    }
```

### Startup warning when credential is absent

In `app.py` startup, after loading connectors:

```python
for connector in connectors:
    auth = connector.auth
    if auth and auth.credential_ref:
        if _resolve_credential(auth.credential_ref) is None:
            logger.warning(
                "simulator_auth_credential_missing",
                connector=connector.name,
                credential_ref=auth.credential_ref,
                hint=f"Set INOUT_CREDENTIAL_{auth.credential_ref.upper()} to enable auth checking",
            )
```

---

## Config additions needed

None.  All required information is already in `connector.auth`.  The simulator
reuses the same `INOUT_CREDENTIAL_<REF>` env var convention the engine uses.

The `required_scopes` per-datatype declarations (`DatatypeScopes.read` /
`DatatypeScopes.write`) were added to `DatatypeConfig` in a separate commit and
are already available — they are the data source for the scope enforcement
described below.

---

## OAuth2 scope enforcement (Layer 1c)

> **Precondition**: Layer 1 (credential verification) is in place.  Scope
> enforcement is layered on top — a request that fails the Bearer token check
> never reaches scope validation.  A connector with no `required_scopes`
> declared on any datatype skips scope checks entirely (opt-in).

### Why it belongs in the plan

The existing plan's Layer 1 checks *who* the caller is (valid token), but not
*what they're allowed to do*.  With `required_scopes` now in the connector
schema, the simulator can check *what scopes the token was issued with* and
enforce the same per-datatype, per-operation rules that the real provider would.

This makes two important test scenarios possible:
1. **Insufficient scope on writeback** — token was issued with only read scopes;
   writeback should get `403 {"error": "insufficient_scope"}`, not a silent
   success or a 401.
2. **Subset deployment** — a project deploys only the `contacts` datatype.
   The engine requests only `crm.objects.contacts.read`; an attempt to call the
   `companies` list endpoint should get a 403, confirming the scope request was
   correctly computed.

---

### Data flow

```
POST /token  (scope=crm.objects.contacts.read crm.objects.contacts.write)
    │
    ▼
OAuthTokenStore.issue(granted_scopes=[...])
    │  returns sim_token_hubspot_<nonce>
    ▼
GET /crm/v3/objects/contacts          ← checks token.granted_scopes ⊇ {contacts.read}  → 200
PATCH /crm/v3/objects/contacts/{id}   ← checks token.granted_scopes ⊇ {contacts.write} → 200
GET /crm/v3/objects/companies         ← checks token.granted_scopes ⊇ {companies.read} → 403
```

---

### `OAuthTokenStore` changes (extends Layer 1b design)

The Layer 1b `OAuthTokenStore` already tracks issued tokens.  Add `granted_scopes`
to `IssuedToken`:

```python
@dataclass
class IssuedToken:
    access_token: str
    refresh_token: str
    issued_at: float = field(default_factory=time.monotonic)
    expires_in: int = 3600
    revoked: bool = False
    granted_scopes: list[str] = field(default_factory=list)  # NEW

    def has_scope(self, required: str) -> bool:
        """Return True if *required* is in the granted scope set."""
        return required in self.granted_scopes
```

`OAuthTokenStore.issue()` gains a `scopes` parameter:

```python
def issue(self, scopes: list[str] = (), expires_in: int = 3600) -> IssuedToken:
    tok = IssuedToken(
        ...,
        granted_scopes=list(scopes),
    )
    ...
```

---

### Updated token endpoint

The token endpoint parses the `scope` form field (space-separated, standard
OAuth2 wire format) and passes it to the store:

```python
@router.post(token_path)
async def _token(
    grant_type: str = Form(...),
    client_id: str | None = Form(default=None),
    client_secret: str | None = Form(default=None),
    scope: str | None = Form(default=None),         # NEW
    refresh_token: str | None = Form(default=None),
):
    store: OAuthTokenStore = request.app.state.oauth_stores[connector.name]
    requested_scopes = scope.split() if scope else []

    if grant_type == "client_credentials":
        # ... validate client_id / client_secret (unchanged from Layer 1b) ...
        tok = store.issue(scopes=requested_scopes)       # pass scopes through
        return {
            "access_token": tok.access_token,
            "token_type": "bearer",
            "scope": " ".join(tok.granted_scopes),       # echo back
            "refresh_token": tok.refresh_token,
            "expires_in": tok.expires_in,
        }
```

**The simulator grants exactly what was requested** — it does not narrow or
expand the scope.  Scope negotiation (e.g. provider refusing an unknown scope)
is out of scope for now.

---

### Per-route scope check in `build_connector_router()`

After the Bearer token is validated (Layer 1), check that the token's
`granted_scopes` contains the required scope for this specific route.  This is
a second dependency, or a combined dependency that checks both:

```python
def _make_scope_check(required_scope: str, store: OAuthTokenStore):
    """Return a FastAPI dependency that enforces *required_scope*."""
    async def _dep(request: Request):
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            # Layer 1 will already reject this; defensive only.
            return
        token = auth_header.removeprefix("Bearer ")
        issued = store.get(token)          # new store.get() method → IssuedToken | None
        if issued is None:
            return  # Layer 1 handles invalid tokens
        if not issued.has_scope(required_scope):
            raise HTTPException(
                403,
                detail={
                    "error": "insufficient_scope",
                    "required_scope": required_scope,
                    "granted_scopes": issued.granted_scopes,
                },
            )
    return _dep
```

Attached per-route in `build_connector_router()`:

```python
# List / lookup (read operations)
read_scope = dt_cfg.required_scopes.read[0] if (
    dt_cfg.required_scopes and dt_cfg.required_scopes.read
) else None
read_deps = [Depends(_make_scope_check(read_scope, store))] if read_scope else []

# Write operations (insert / update / delete / archive / upsert)
write_scope = dt_cfg.required_scopes.write[0] if (
    dt_cfg.required_scopes and dt_cfg.required_scopes.write
) else None
write_deps = [Depends(_make_scope_check(write_scope, store))] if write_scope else []
```

Note: the plan uses the first declared scope as "the" required scope per
operation class.  Multi-scope AND-logic (all must be present) is a trivial
extension but deferred.

---

### Interaction with the fault injection admin API (Layer 1b)

The `PUT /{c}/admin/oauth/scopes` fault injection endpoint (from Layer 1b)
manipulates the *granted_scopes* on every active token:

```python
# Override granted scopes on all active tokens — simulates a re-issued token
# with reduced permissions (e.g. admin revoked write access via the provider portal).
PUT /{c}/admin/oauth/scopes
body: {"granted": ["crm.objects.contacts.read"]}
```

This replaces `IssuedToken.granted_scopes` in-place without revoking the token,
so the engine continues to use the same Bearer value but suddenly gets 403 on
write operations.  The engine should detect and surface this clearly.

---

### Behaviour matrix

| Scenario | `required_scopes` on datatype? | Scope in token request? | Result |
|---|---|---|---|
| OAuth2 connector, no `required_scopes` | No | Any / none | No scope check — existing Layer 1 token validity check only |
| OAuth2 connector, scope declared | Yes | Correct scope | 200 |
| OAuth2 connector, scope declared | Yes | Missing scope | 403 `insufficient_scope` |
| OAuth2 connector, scope declared | Yes | Empty (engine bug: no `scope` sent) | 403 `insufficient_scope` on read/write routes |
| `api_key` connector | n/a | n/a | No scope check (scopes are an OAuth2 concept) |

---

### No changes needed outside `route_builder.py`

- `OAuthTokenStore` lives in `simulator/oauth_store.py` (new file)  
- `route_builder.py` — token endpoint + per-route scope dependency  
- No changes to `app.py`, `seed.py`, `webhooks.py`, or any config model  
- The `DatatypeScopes` model is already in `inandout.schema.connector`

---

The simulator already has a custom `/docs` endpoint per connector
(`_make_swagger_endpoint` in `app.py`) that injects JavaScript into the page
body.  The same mechanism will **pre-authorise the Swagger UI automatically**
using the resolved credentials, so "Try it out" works without the developer
ever opening the Authorize dialog.

The Swagger UI JavaScript API exposes `ui.preauthorizeApiKey(schemeName, value)`
and `ui.preauthorizeBasic(schemeName, user, pass)`.  We inject a snippet that
calls these after the UI finishes loading.

### What the simulator does

#### 1 — Declare `securitySchemes` in the OpenAPI spec

Override `get_openapi()` on each connector sub-app to add the scheme that
matches `connector.auth`:

```python
# oauth2 — declare a Bearer security scheme
"securitySchemes": {
    "bearerAuth": {"type": "http", "scheme": "bearer"}
}

# api_key (header)
"securitySchemes": {
    "ApiKeyHeader": {"type": "apiKey", "in": "header", "name": "<header_name>"}
}

# api_key (query)
"securitySchemes": {
    "ApiKeyQuery": {"type": "apiKey", "in": "query", "name": "<param_name>"}
}
```

Each route that has an auth dependency gets `"security": [{"<schemeName>": []}]`
added to its `openapi_extra`.

#### 2 — Inject a `preauthorize` snippet in `_make_swagger_endpoint`

`_make_swagger_endpoint(connector_name, title)` gains a third argument:
`preauth_script: str | None`.  The snippet is built in `app.py` based on the
resolved credential:

```python
def _build_preauth_script(connector: ConnectorConfig) -> str | None:
    auth = connector.auth
    if auth is None:
        return None

    secret = _resolve_credential(auth.credential_ref)  # same helper as route_builder

    if auth.type == "oauth2":
        token = f"sim_token_{connector.name}"
        # Wait for SwaggerUIBundle to be ready, then pre-authorise.
        return f"""
<script>
(function poll() {{
  if (window.ui && ui.preauthorizeApiKey) {{
    ui.preauthorizeApiKey('bearerAuth', '{token}');
  }} else {{
    setTimeout(poll, 100);
  }}
}})();
</script>"""

    if auth.type == "api_key" and auth.api_key and secret:
        scheme = "ApiKeyHeader" if auth.api_key.location == "header" else "ApiKeyQuery"
        return f"""
<script>
(function poll() {{
  if (window.ui && ui.preauthorizeApiKey) {{
    ui.preauthorizeApiKey('{scheme}', '{secret}');
  }} else {{
    setTimeout(poll, 100);
  }}
}})();
</script>"""

    return None
```

If the credential env var is not set, `_build_preauth_script` returns `None`
and the Swagger UI loads without pre-authorisation — a warning was already
printed at startup.

### Result

- Developer opens `/{connector}/docs` → Swagger shows the padlock as locked (authorised)
- "Try it out" → Execute sends the correct header/token immediately
- No manual Authorize dialog interaction needed, ever
- Credentials are the real resolved values, so the same request the Swagger UI
  makes is exactly what the engine makes — identical auth headers




---

## Threat model

| Context | Risk |
|---|---|
| Local dev | None — loopback only; auth is pure friction |
| CI (engine container ↔ simulator container) | Test scripts call the admin API; a leaked key could mutate test state |
| Shared / staging | Other developers or internet exposure; admin and UI must be protected |

---

## Surface map (current state — all unprotected)

| Endpoint group | Callers | Risk |
|---|---|---|
| `GET/PUT/DELETE /{c}/admin/{d}/{id}` | Human, CI scripts | Arbitrary record mutation / bulk wipe |
| `POST /{c}/admin/{d}/{id}/restore` | Human | Resurrect deleted records |
| `DELETE /ui/_registrations/{c}/{sub_id}` | Browser | Remove engine subscriptions mid-run |
| `GET /ui/…`, SSE `/events` | Browser | Data leakage |
| `GET/POST /{c}/{d}/…` (list, lookup, writeback) | Engine | Accepts requests with wrong / missing credentials silently — config bugs go undetected |
| `POST {token_url}` (fake OAuth) | Engine | Issues tokens to any caller unconditionally |

---

## Layer 1 — Connector API credential verification (P0 — highest value)

The simulator currently accepts any `Authorization` header without inspection.
Making it verify the value catches connector config bugs (wrong
`credential_ref`, wrong header name) at simulator test time with a 401 before
anything touches a real API.

### Implementation

Add `_make_auth_dependency(connector: ConnectorConfig)` in `route_builder.py`:

```python
def _make_auth_dependency(connector: ConnectorConfig):
    auth = connector.auth
    if auth is None:
        return None

    if auth.type == "oauth2":
        expected = f"sim_token_{connector.name}"
        async def dep(authorization: str = Header(None)):
            if not authorization or authorization != f"Bearer {expected}":
                raise HTTPException(401, f"Expected Bearer {expected}")
        return dep

    if auth.type == "api_key" and auth.api_key:
        resolved = _resolve_credential(auth.credential_ref)
        loc = auth.api_key.location   # "header" | "query"
        name = auth.api_key.name
        if loc == "header":
            async def dep(request: Request):
                if request.headers.get(name) != resolved:
                    raise HTTPException(401, f"Expected {name}: <secret>")
            return dep
        if loc == "query":
            async def dep(request: Request):
                if request.query_params.get(name) != resolved:
                    raise HTTPException(401, f"Expected query param {name}=<secret>")
            return dep

    return None   # unknown auth type — accept anything
```

Attach the dependency to every list / lookup / writeback route for that
connector:

```python
deps = [Depends(auth_dep)] if auth_dep else []
router.add_api_route(path, handler, methods=[method], dependencies=deps)
```

The fake OAuth token endpoint also validates `client_id` / `client_secret` from
the `oauth2.client_id_ref` / `oauth2.client_secret_ref` credential refs when
those are set, rather than issuing a token unconditionally.

**Config additions needed:** none.  `connector.auth` already carries all
required fields.

---

## Layer 1b — Auth fault injection (P1)

The base credential check in Layer 1 makes token/key values opaque strings.
This section adds a **per-connector fault injection admin API** so that tests
can simulate real-world auth failure scenarios: expired tokens, revoked sessions,
credential rotation mid-run.

This is useful for two distinct auth patterns present in the current connectors:

| Connector | Auth type | Test scenarios |
|---|---|---|
| HubSpot, Salesforce | `oauth2` client-credentials | Token expiry, revocation mid-run, refresh token flow, invalid grant |
| Tripletex | `api_key` / `Authorization` header | Session token revocation, credential rotation (Tripletex sessions are per-company and can be invalidated server-side at any time) |

---

### `OAuthTokenStore` (new module: `simulator/oauth_store.py`)

```python
import time
import secrets
from dataclasses import dataclass, field

@dataclass
class IssuedToken:
    access_token: str
    refresh_token: str
    issued_at: float = field(default_factory=time.monotonic)
    expires_in: int = 3600          # seconds; 0 = never expires
    revoked: bool = False

    def is_expired(self) -> bool:
        if self.expires_in == 0:
            return False
        return (time.monotonic() - self.issued_at) > self.expires_in

    def is_valid(self) -> bool:
        return not self.revoked and not self.is_expired()


class OAuthTokenStore:
    """In-process OAuth2 token ledger for one connector."""

    def __init__(self, connector_name: str) -> None:
        self._connector = connector_name
        self._tokens: dict[str, IssuedToken] = {}   # access_token → record
        self._refresh_map: dict[str, str] = {}      # refresh_token → access_token

    def issue(self, expires_in: int = 3600) -> IssuedToken:
        tok = IssuedToken(
            access_token=f"sim_token_{self._connector}_{secrets.token_hex(8)}",
            refresh_token=f"sim_refresh_{self._connector}_{secrets.token_hex(8)}",
            expires_in=expires_in,
        )
        self._tokens[tok.access_token] = tok
        self._refresh_map[tok.refresh_token] = tok.access_token
        return tok

    def validate(self, access_token: str) -> bool:
        tok = self._tokens.get(access_token)
        return tok is not None and tok.is_valid()

    def revoke(self, access_token: str) -> bool:
        tok = self._tokens.get(access_token)
        if tok:
            tok.revoked = True
            return True
        return False

    def revoke_all(self) -> int:
        count = sum(1 for t in self._tokens.values() if not t.revoked)
        for t in self._tokens.values():
            t.revoked = True
        return count

    def expire_all(self) -> int:
        """Backdate issued_at so every token appears expired."""
        count = 0
        for t in self._tokens.values():
            if not t.revoked and t.expires_in > 0:
                t.issued_at = 0.0
                count += 1
        return count

    def refresh(self, refresh_token: str, expires_in: int = 3600) -> IssuedToken | None:
        access_token = self._refresh_map.get(refresh_token)
        if access_token is None:
            return None
        old = self._tokens.get(access_token)
        if old is None:
            return None
        old.revoked = True          # old access token is no longer valid
        new_tok = self.issue(expires_in=expires_in)
        return new_tok

    def list_all(self) -> list[dict]:
        return [
            {
                "access_token": t.access_token,
                "issued_at": t.issued_at,
                "expires_in": t.expires_in,
                "expired": t.is_expired(),
                "revoked": t.revoked,
                "valid": t.is_valid(),
            }
            for t in self._tokens.values()
        ]

    def clear(self) -> None:
        self._tokens.clear()
        self._refresh_map.clear()
```

One `OAuthTokenStore` instance per OAuth2 connector, stored on the FastAPI app
state (e.g. `app.state.oauth_stores: dict[str, OAuthTokenStore]`).

---

### Updated token endpoint (replaces the static version in Layer 1)

```python
@router.post(token_path)
async def _token(
    grant_type: str = Form(...),
    client_id: str | None = Form(default=None),
    client_secret: str | None = Form(default=None),
    refresh_token: str | None = Form(default=None),
):
    store: OAuthTokenStore = request.app.state.oauth_stores[connector.name]

    if grant_type == "client_credentials":
        # Validate client credentials when env vars are set
        expected_id     = _resolve_credential(connector.auth.oauth2.client_id_ref)
        expected_secret = _resolve_credential(connector.auth.oauth2.client_secret_ref)
        if expected_id and not secrets.compare_digest(client_id or "", expected_id):
            raise HTTPException(401, detail="invalid_client")
        if expected_secret and not secrets.compare_digest(client_secret or "", expected_secret):
            raise HTTPException(401, detail="invalid_client")
        tok = store.issue()
        return {"access_token": tok.access_token, "token_type": "bearer",
                "refresh_token": tok.refresh_token, "expires_in": tok.expires_in}

    if grant_type == "refresh_token":
        if not refresh_token:
            raise HTTPException(400, detail="refresh_token required")
        tok = store.refresh(refresh_token)
        if tok is None:
            raise HTTPException(401, detail="invalid_grant")
        return {"access_token": tok.access_token, "token_type": "bearer",
                "refresh_token": tok.refresh_token, "expires_in": tok.expires_in}

    raise HTTPException(400, detail=f"unsupported_grant_type: {grant_type}")
```

The route guard changes from string equality to a store lookup:

```python
async def dep(authorization: str | None = Header(default=None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, headers={"WWW-Authenticate": "Bearer"})
    token = authorization.removeprefix("Bearer ")
    store: OAuthTokenStore = request.app.state.oauth_stores[connector.name]
    if not store.validate(token):
        raise HTTPException(401, detail="Token invalid, expired, or revoked",
                            headers={"WWW-Authenticate": "Bearer"})
```

---

### Fault injection admin endpoints for OAuth2 connectors

All under `/{connector}/admin/oauth/`:

| Method | Path | Effect |
|---|---|---|
| `GET` | `/{c}/admin/oauth/tokens` | List all issued tokens with status |
| `POST` | `/{c}/admin/oauth/revoke` | Body: `{"token": "…"}` — revoke one token |
| `POST` | `/{c}/admin/oauth/revoke_all` | Revoke every active token |
| `POST` | `/{c}/admin/oauth/expire_all` | Backdate all tokens so is_expired() → True |
| `DELETE` | `/{c}/admin/oauth/tokens` | Wipe the store (clean state between tests) |

These endpoints respect the admin API key (Layer 2) when it is set.

**Engine resilience tests these enable:**

```python
# Revoke mid-run — engine must detect 401 and re-authenticate
client.post("/hubspot/admin/oauth/revoke_all")
# ... trigger an engine sync
# Engine should call /token again and resume without error

# Expiry simulation — engine must honour expires_in and refresh proactively
client.post("/hubspot/admin/oauth/expire_all")
# ... engine makes any API call → 401 → calls /token with refresh_token → succeeds

# Invalid refresh token — engine must fall back to full client_credentials re-auth
client.delete("/hubspot/admin/oauth/tokens")  # wipe store — refresh_tokens no longer known
client.post("/hubspot/admin/oauth/expire_all")
# ... engine's refresh attempt gets 401 invalid_grant → full re-auth
```

---

### Fault injection for `api_key` connectors (covers Tripletex)

Tripletex uses a company-scoped session token that can be revoked server-side
at any time.  The fault injection for `api_key` type is simpler — a boolean
"block" flag per connector, plus an optional credential override:

New `sim/api_key_store.py` (or a dict on app state):

```python
@dataclass
class ApiKeyState:
    blocked: bool = False         # if True, all requests return 401
    override: str | None = None   # if set, expected value is this instead of the env var
```

Admin endpoints:

| Method | Path | Effect |
|---|---|---|
| `POST` | `/{c}/admin/auth/block` | Start returning 401 for all API calls (session revoked) |
| `POST` | `/{c}/admin/auth/unblock` | Stop blocking |
| `PUT`  | `/{c}/admin/auth/credential` | Body: `{"value": "new_secret"}` — rotate the expected credential value mid-run |
| `DELETE` | `/{c}/admin/auth/credential` | Revert to env-var value |

The `_dep_api_key_header` / `_dep_api_key_query` functions check `ApiKeyState.blocked`
and use `ApiKeyState.override or _resolve_credential(auth.credential_ref)` as the
expected value.

**Engine resilience tests for Tripletex:**

```python
# Session revocation — engine should surface a clear error rather than silently fail
client.post("/tripletex/admin/auth/block")
# ... trigger a sync → engine should log 401, stop the run gracefully

# Credential rotation — engine should pick up updated INOUT_CREDENTIAL_TRIPLETEX_SESSION
client.put("/tripletex/admin/auth/credential", json={"value": "new_session_token"})
# Update the env var / credential in the engine config
# ... next sync should succeed with new credential
```

---

### UI token panel (connector detail page)

For OAuth2 connectors the existing `/ui/{connector}/{datatype}` table page gets
a new HTMX fragment at the top:

```
GET /{c}/ui/_oauth_tokens          → renders a small table: token (truncated), issued, expires, status
```

Refreshed automatically every 10 s via `hx-trigger="every 10s"`.  Shows the
real token strings (truncated to last 8 chars for readability) so the developer
can see whether the engine has re-authenticated after a forced revocation.  For
`api_key` connectors the fragment shows the current block/override state instead.

---

## Layer 2 — Admin API key (P1)

**Env var:** `INOUT_SIMULATOR_ADMIN_KEY`

All routes under `/{connector}/admin/` and the `DELETE /ui/_registrations/…`
endpoint get a shared `HTTPBearer` dependency:

```python
async def _require_admin(
    creds: HTTPAuthorizationCredentials | None = Security(HTTPBearer(auto_error=False)),
):
    key = os.environ.get("INOUT_SIMULATOR_ADMIN_KEY")
    if key and (creds is None or creds.credentials != key):
        raise HTTPException(401, detail="Admin key required — set Authorization: Bearer <key>")
```

CI passes the key via:

```yaml
# docker-compose.yml
environment:
  INOUT_SIMULATOR_ADMIN_KEY: "${SIMULATOR_ADMIN_KEY}"
```

```bash
# test script
curl -H "Authorization: Bearer $INOUT_SIMULATOR_ADMIN_KEY" \
     http://simulator:6100/tripletex/admin/customers/10001
```

---

## Layer 3 — UI HTTP Basic auth (P2)

**Env vars:** `INOUT_SIMULATOR_UI_USER`, `INOUT_SIMULATOR_UI_PASS`  
(both must be set to activate; if only one is present, emit a startup warning
and fall back to unprotected to avoid silent lockout)

All `GET /ui/…` routes and the SSE `GET /events` stream get an `HTTPBasic`
dependency:

```python
async def _require_ui_auth(
    credentials: HTTPBasicCredentials = Depends(HTTPBasic(auto_error=False)),
):
    user = os.environ.get("INOUT_SIMULATOR_UI_USER")
    pw   = os.environ.get("INOUT_SIMULATOR_UI_PASS")
    if not (user and pw):
        return  # auth not configured — allow through
    if credentials is None:
        raise HTTPException(401, headers={"WWW-Authenticate": "Basic"})
    ok = (
        secrets.compare_digest(credentials.username.encode(), user.encode())
        and secrets.compare_digest(credentials.password.encode(), pw.encode())
    )
    if not ok:
        raise HTTPException(401, headers={"WWW-Authenticate": "Basic"})
```

The browser prompts once; HTMX requests carry the same credentials on every
subsequent call.  No session cookie or token storage needed.

---

## Layer 4 — IP allowlist on admin routes (P3 — optional hardening)

**Env var:** `INOUT_SIMULATOR_ALLOW_ADMIN_IPS`  
Comma-separated CIDRs, default `127.0.0.0/8,::1`.

Implemented as a **route dependency** (not `BaseHTTPMiddleware`, to avoid
streaming problems) on all `/admin/` paths:

```python
_ADMIN_NETS = [
    ipaddress.ip_network(cidr.strip())
    for cidr in os.environ.get("INOUT_SIMULATOR_ALLOW_ADMIN_IPS", "127.0.0.0/8,::1").split(",")
]

async def _require_admin_ip(request: Request):
    if not _ADMIN_NETS:
        return
    try:
        src = ipaddress.ip_address(request.client.host)
    except ValueError:
        raise HTTPException(403, "Cannot parse source IP")
    if not any(src in net for net in _ADMIN_NETS):
        raise HTTPException(403, "Source IP not in admin allowlist")
```

Works well in docker-compose where the engine's container IP is stable and
predictable.

---

## Implementation order

| Priority | Layer | Env var(s) | Value |
|---|---|---|---|
| **P0** | Connector API credential verification | Uses existing `connector.auth` config | Catches engine config bugs at test time |
| **P1a** | Auth fault injection — OAuth2 token store | None (in-process state) | Enables token expiry / revocation / refresh resilience tests |
| **P1b** | Auth fault injection — api_key block/rotate | None (in-process state) | Enables Tripletex session revocation + credential rotation tests |
| **P1c** | OAuth2 scope enforcement | None — uses `datatype.required_scopes` | Catches missing/wrong scope in engine token requests; enables subset-deployment testing |
| **P1d** | Admin API key | `INOUT_SIMULATOR_ADMIN_KEY` | Required once fault injection endpoints are exposed in CI |
| **P2** | UI Basic auth | `INOUT_SIMULATOR_UI_USER` + `INOUT_SIMULATOR_UI_PASS` | Needed for shared / staging |
| **P3** | Admin IP allowlist | `INOUT_SIMULATOR_ALLOW_ADMIN_IPS` | Defence-in-depth; low effort once P1d is in |

---

## Non-goals

- Session management, JWTs, or OIDC — unnecessary complexity for a dev tool.
- TLS termination — handled by the Docker / k8s ingress layer, not the
  simulator itself.
- Rate limiting — separate concern; handled at the proxy layer.
