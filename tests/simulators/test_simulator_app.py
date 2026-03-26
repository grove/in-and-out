"""Integration tests for the simulator FastAPI app.

Covers the admin CRUD API, optimistic locking (ETag), and the main UI pages.
Uses httpx.AsyncClient against the ASGI app without a running server.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from inandout.simulator.app import create_app

# Path to a minimal connector fixture that includes both ingestion and writeback
# operations (lookup + update) so the full set of routes is available.
_FIXTURE = (
    Path(__file__).parents[2] / "fixtures" / "connectors" / "valid" / "minimal_full_duplex.yaml"
)

C = "demo_full_duplex"
D = "contacts"


@pytest.fixture
async def app():
    """Create the simulator app and run its startup handlers (seed, etc.)."""
    application = create_app([_FIXTURE], engine_url="http://localhost:1")
    # Trigger startup events so seed data is loaded (none in this fixture, but
    # this ensures the lifespan path is exercised).
    for handler in application.router.on_startup:
        await handler()
    return application


@pytest.fixture
async def client(app):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as c:
        yield c


# ---------------------------------------------------------------------------
# Root landing page
# ---------------------------------------------------------------------------


async def test_root_landing_page(client: httpx.AsyncClient) -> None:
    resp = await client.get("/")
    assert resp.status_code == 200
    assert "in-and-out Simulator" in resp.text
    assert C in resp.text


# ---------------------------------------------------------------------------
# UI pages
# ---------------------------------------------------------------------------


async def test_ui_dashboard_returns_200(client: httpx.AsyncClient) -> None:
    resp = await client.get("/ui")
    assert resp.status_code == 200
    assert "Dashboard" in resp.text or C in resp.text


async def test_ui_table_page_returns_200(client: httpx.AsyncClient) -> None:
    resp = await client.get(f"/ui/{C}/{D}")
    assert resp.status_code == 200
    assert D in resp.text


async def test_ui_table_page_unknown_connector_returns_404(client: httpx.AsyncClient) -> None:
    resp = await client.get("/ui/no_such_connector/contacts")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Admin CRUD — create
# ---------------------------------------------------------------------------


async def test_admin_create_returns_201(client: httpx.AsyncClient) -> None:
    resp = await client.post(
        f"/admin/{C}/{D}",
        json={"id": "alice", "name": "Alice"},
    )
    assert resp.status_code == 201


async def test_admin_create_record_available_via_get(client: httpx.AsyncClient) -> None:
    await client.post(f"/admin/{C}/{D}", json={"id": "bob", "name": "Bob"})
    resp = await client.get(f"/admin/{C}/{D}/bob")
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "Bob"


async def test_admin_get_returns_etag_header(client: httpx.AsyncClient) -> None:
    await client.post(f"/admin/{C}/{D}", json={"id": "carol"})
    resp = await client.get(f"/admin/{C}/{D}/carol")
    assert resp.status_code == 200
    assert resp.headers.get("etag"), "ETag header must be present"


async def test_admin_get_unknown_record_returns_404(client: httpx.AsyncClient) -> None:
    resp = await client.get(f"/admin/{C}/{D}/ghost")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Admin CRUD — update with ETag
# ---------------------------------------------------------------------------


async def test_admin_update_patches_fields(client: httpx.AsyncClient) -> None:
    await client.post(f"/admin/{C}/{D}", json={"id": "dave", "score": 10, "name": "Dave"})
    resp = await client.put(
        f"/admin/{C}/{D}/dave",
        json={"score": 99},
    )
    assert resp.status_code == 200
    # Confirm merged record via admin GET
    get_resp = await client.get(f"/admin/{C}/{D}/dave")
    body = get_resp.json()
    assert body["score"] == 99
    assert body["name"] == "Dave"  # unchanged field preserved


async def test_admin_update_succeeds_with_correct_etag(client: httpx.AsyncClient) -> None:
    await client.post(f"/admin/{C}/{D}", json={"id": "eve"})
    get_resp = await client.get(f"/admin/{C}/{D}/eve")
    etag = get_resp.headers["etag"]

    resp = await client.put(
        f"/admin/{C}/{D}/eve",
        json={"flag": True},
        headers={"If-Match": etag},
    )
    assert resp.status_code == 200
    assert resp.headers.get("etag"), "Updated ETag must be returned"


async def test_admin_update_returns_412_on_stale_etag(client: httpx.AsyncClient) -> None:
    await client.post(f"/admin/{C}/{D}", json={"id": "frank"})

    # First update advances the version.
    await client.put(f"/admin/{C}/{D}/frank", json={"v": 1})
    # Fetch stale ETag from first version – we can simulate it with a fake value.
    resp = await client.put(
        f"/admin/{C}/{D}/frank",
        json={"v": 2},
        headers={"If-Match": "stale-etag-value"},
    )
    assert resp.status_code == 412


async def test_admin_update_without_etag_always_succeeds(client: httpx.AsyncClient) -> None:
    await client.post(f"/admin/{C}/{D}", json={"id": "grace"})
    resp = await client.put(f"/admin/{C}/{D}/grace", json={"x": 1})
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Admin CRUD — delete / restore
# ---------------------------------------------------------------------------


async def test_admin_delete_soft_deletes_record(client: httpx.AsyncClient) -> None:
    await client.post(f"/admin/{C}/{D}", json={"id": "hal"})
    del_resp = await client.delete(f"/admin/{C}/{D}/hal")
    assert del_resp.status_code == 200
    # GET admin endpoint still returns the record (soft-delete keeps data).
    get_resp = await client.get(f"/admin/{C}/{D}/hal")
    assert get_resp.status_code == 200


async def test_admin_count_fragment_decrements_after_delete(client: httpx.AsyncClient) -> None:
    await client.post(f"/admin/{C}/{D}", json={"id": "ivan"})
    before = int((await client.get(f"/ui/{C}/{D}/_count")).text.strip())
    await client.delete(f"/admin/{C}/{D}/ivan")
    after = int((await client.get(f"/ui/{C}/{D}/_count")).text.strip())
    assert after == before - 1


async def test_admin_restore_re_activates_record(client: httpx.AsyncClient) -> None:
    await client.post(f"/admin/{C}/{D}", json={"id": "judy"})
    await client.delete(f"/admin/{C}/{D}/judy")
    before_count = int((await client.get(f"/ui/{C}/{D}/_count")).text.strip())
    await client.post(f"/admin/{C}/{D}/judy/restore")
    after_count = int((await client.get(f"/ui/{C}/{D}/_count")).text.strip())
    assert after_count == before_count + 1


# ---------------------------------------------------------------------------
# HTMX fragments
# ---------------------------------------------------------------------------


async def test_rows_fragment_returns_html(client: httpx.AsyncClient) -> None:
    await client.post(f"/admin/{C}/{D}", json={"id": "ken", "name": "Ken"})
    resp = await client.get(f"/ui/{C}/{D}/_rows")
    assert resp.status_code == 200
    assert "ken" in resp.text


async def test_count_fragment_returns_plain_int(client: httpx.AsyncClient) -> None:
    resp = await client.get(f"/ui/{C}/{D}/_count")
    assert resp.status_code == 200
    assert resp.text.strip().isdigit()


async def test_mutations_fragment_for_known_record(client: httpx.AsyncClient) -> None:
    await client.post(f"/admin/{C}/{D}", json={"id": "lena"})
    resp = await client.get(f"/ui/{C}/{D}/lena/_mutations")
    assert resp.status_code == 200
    assert "create" in resp.text.lower()


# ---------------------------------------------------------------------------
# UI record detail page
# ---------------------------------------------------------------------------


async def test_ui_record_page_returns_200(client: httpx.AsyncClient) -> None:
    await client.post(f"/admin/{C}/{D}", json={"id": "max", "name": "Max"})
    resp = await client.get(f"/ui/{C}/{D}/max")
    assert resp.status_code == 200
    assert "max" in resp.text.lower()


async def test_ui_record_page_unknown_returns_404(client: httpx.AsyncClient) -> None:
    resp = await client.get(f"/ui/{C}/{D}/nobody")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Connector write endpoints: insert (POST without {external_id} in path)
# ---------------------------------------------------------------------------

_HUBSPOT_FIXTURE = (
    Path(__file__).parents[2] / "connectors" / "hubspot.example.yaml"
)


@pytest.fixture
async def hubspot_client():
    """Client for the HubSpot connector sub-app (has an insert operation)."""
    import os

    os.environ.setdefault("INOUT_CREDENTIAL_HUBSPOT_OAUTH", "dummy")
    os.environ.setdefault("INOUT_CREDENTIAL_HUBSPOT_WEBHOOK_SECRET", "dummy")
    application = create_app([_HUBSPOT_FIXTURE], engine_url="http://localhost:1")
    for handler in application.router.on_startup:
        await handler()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application),
        base_url="http://test",
    ) as c:
        yield c


async def test_insert_endpoint_accepts_post_without_id(hubspot_client: httpx.AsyncClient) -> None:
    """POST to an insert endpoint that has no ${external_id} must return 201."""
    resp = await hubspot_client.post(
        "/hubspot/crm/v3/objects/contacts",
        json={"properties": {"firstname": "Zara", "email": "zara@example.com"}},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert "id" in body  # server-assigned id present


async def test_insert_endpoint_has_no_record_id_query_param(hubspot_client: httpx.AsyncClient) -> None:
    """The OpenAPI schema for the insert endpoint must NOT include record_id as a query param."""
    resp = await hubspot_client.get("/hubspot/openapi.json")
    assert resp.status_code == 200
    schema = resp.json()
    # Find the POST /crm/v3/objects/contacts path
    path_item = schema.get("paths", {}).get("/crm/v3/objects/contacts", {})
    assert path_item, "insert path must exist in schema"
    post_op = path_item.get("post", {})
    assert post_op, "POST operation must exist"
    # Confirm no query parameter named record_id
    params = post_op.get("parameters", [])
    param_names = {p.get("name") for p in params}
    assert "record_id" not in param_names, (
        f"record_id must not appear as a query param on the insert endpoint, got: {param_names}"
    )
