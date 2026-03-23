"""OpenAPI spec introspection helpers for connector template generation."""
from __future__ import annotations

import structlog

logger = structlog.get_logger(__name__)


async def fetch_openapi_spec(base_url: str) -> dict | None:
    """Try to fetch an OpenAPI spec from the given base URL.

    Tries in order: /openapi.json, /swagger.json, /api-docs.
    Returns parsed JSON dict or None if none of the paths respond with 200.
    """
    import httpx

    candidates = [
        f"{base_url.rstrip('/')}/openapi.json",
        f"{base_url.rstrip('/')}/swagger.json",
        f"{base_url.rstrip('/')}/api-docs",
    ]

    async with httpx.AsyncClient(timeout=10.0) as client:
        for url in candidates:
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    try:
                        data = resp.json()
                        if isinstance(data, dict):
                            logger.info("openapi_spec_fetched", url=url)
                            return data
                    except Exception:
                        pass
            except Exception as exc:
                logger.debug("openapi_spec_probe_failed", url=url, error=str(exc))

    return None


def extract_list_endpoints(spec: dict) -> list[dict]:
    """Find GET endpoints that return arrays (response schema has type:array or items key).

    Returns list of dicts with keys: path, tag, description.
    """
    results: list[dict] = []
    paths = spec.get("paths", {})

    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue
        get_op = path_item.get("get")
        if not isinstance(get_op, dict):
            continue

        # Check responses for array return types
        responses = get_op.get("responses", {})
        is_list_endpoint = False

        for _status, response_obj in responses.items():
            if not isinstance(response_obj, dict):
                continue
            content = response_obj.get("content", {})
            for _media_type, media_obj in content.items():
                if not isinstance(media_obj, dict):
                    continue
                schema = media_obj.get("schema", {})
                if _schema_is_array(schema, spec):
                    is_list_endpoint = True
                    break

            # Also check old-style swagger 2.0 schema
            schema = response_obj.get("schema", {})
            if isinstance(schema, dict) and _schema_is_array(schema, spec):
                is_list_endpoint = True

            if is_list_endpoint:
                break

        if is_list_endpoint:
            tags = get_op.get("tags", [])
            tag = tags[0] if tags else ""
            description = get_op.get("summary") or get_op.get("description") or ""
            results.append({
                "path": path,
                "tag": tag,
                "description": description,
            })

    return results


def _schema_is_array(schema: dict, spec: dict) -> bool:
    """Return True if the schema represents an array type."""
    if not isinstance(schema, dict):
        return False

    # Resolve $ref
    if "$ref" in schema:
        schema = _resolve_ref(schema["$ref"], spec)

    return schema.get("type") == "array" or "items" in schema


def _resolve_ref(ref: str, spec: dict) -> dict:
    """Resolve a JSON $ref within the spec."""
    if not ref.startswith("#/"):
        return {}
    parts = ref.lstrip("#/").split("/")
    obj: dict | None = spec
    for part in parts:
        if not isinstance(obj, dict):
            return {}
        obj = obj.get(part)
    return obj if isinstance(obj, dict) else {}


def infer_pagination(spec: dict, path: str) -> str:
    """Infer pagination type from parameter names in the spec.

    Returns: "cursor", "offset", or "none"
    """
    paths = spec.get("paths", {})
    path_item = paths.get(path, {})
    get_op = path_item.get("get", {})

    # Collect parameter names from operation and path level
    param_names: set[str] = set()
    for param in get_op.get("parameters", []) + path_item.get("parameters", []):
        if isinstance(param, dict):
            name = param.get("name", "")
            if name:
                param_names.add(name.lower())

    cursor_indicators = {"cursor", "page_token", "next_token", "after", "before"}
    offset_indicators = {"page", "offset", "page_number", "pagenumber", "start"}

    if param_names & cursor_indicators:
        return "cursor"
    if param_names & offset_indicators:
        return "offset"
    return "none"


def infer_auth(spec: dict) -> str:
    """Infer auth type from OpenAPI securitySchemes.

    Returns: "api_key", "oauth2", "basic", or "none"
    """
    # OpenAPI 3.x
    components = spec.get("components", {})
    security_schemes = components.get("securitySchemes", {})

    # Swagger 2.x
    if not security_schemes:
        security_schemes = spec.get("securityDefinitions", {})

    for _name, scheme in security_schemes.items():
        if not isinstance(scheme, dict):
            continue
        scheme_type = scheme.get("type", "").lower()
        if scheme_type in ("apikey", "api_key"):
            return "api_key"
        if scheme_type == "oauth2":
            return "oauth2"
        if scheme_type == "http":
            http_scheme = scheme.get("scheme", "").lower()
            if http_scheme == "basic":
                return "basic"
            if http_scheme == "bearer":
                return "api_key"  # bearer maps to api_key in our model

    return "none"
