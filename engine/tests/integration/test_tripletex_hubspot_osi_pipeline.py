"""End-to-end cross-connector OSI-Mapping pipeline test.

Exercises the complete data flow across two connectors:

    Tripletex ERP (mock) → [IngestionEngine]
        → inout_src_tripletex_customers
        → inout_src_tripletex_contacts

    [Simulated OSI-Mapping — identity resolution + cross-system projection]
        → inout_dst_hubspot_companies
        → inout_dst_hubspot_contacts
        → inout_dst_hubspot_contacts_companies_associations

    [WritebackEngine] → HubSpot CRM (mock)
        POST /crm/v3/objects/companies         (Tripletex customer → HubSpot company)
        POST /crm/v3/objects/contacts          (Tripletex contact  → HubSpot contact)
        POST /crm/v4/associations/…/batch/create  (contact.customer link → HubSpot assoc)

Does NOT require a real OSI-Mapping installation.  OSI's output is manually
populated into the desired-state tables between the ingestion and writeback
steps, exactly as OSI-Mapping would do in production.
"""
from __future__ import annotations

import os

import httpx
import orjson
import pytest
import respx

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_TRIPLETEX_BASE_URL = "https://tripletex.no/v2"
_HUBSPOT_BASE_URL = "https://api.hubapi.com"

# ---------------------------------------------------------------------------
# Fixture data — two customers, three contacts (two linked to one company)
# ---------------------------------------------------------------------------

_CUSTOMERS = [
    {
        "id": 10001,
        "name": "Acme AS",
        "organizationNumber": "912345678",
        "email": "post@acme.no",
        "isCustomer": True,
    },
    {
        "id": 10002,
        "name": "Global AS",
        "organizationNumber": "923456789",
        "email": "post@global.no",
        "isCustomer": True,
    },
]

_CONTACTS = [
    {
        "id": 20001,
        "firstName": "Kari",
        "lastName": "Nordmann",
        "email": "kari.nordmann@acme.no",
        "customer": {"id": 10001, "displayName": "Acme AS"},
    },
    {
        "id": 20002,
        "firstName": "Ole",
        "lastName": "Hansen",
        "email": "ole.hansen@global.no",
        "customer": {"id": 10002, "displayName": "Global AS"},
    },
    {
        "id": 20003,
        "firstName": "Anna",
        "lastName": "Berg",
        "email": "anna.berg@acme.no",
        "customer": {"id": 10001, "displayName": "Acme AS"},
    },
]

# ---------------------------------------------------------------------------
# ConnectorConfig builders
# ---------------------------------------------------------------------------


def _make_tripletex_connector():
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
    from inandout.config.ingestion import IngestionConfig, HistoryMode, ListConfig, ScheduleConfig
    from inandout.config.pagination import PaginationConfig, PaginationStrategy

    return ConnectorConfig(
        name="tripletex",
        system="Tripletex ERP",
        generation_profile=GenerationProfile.full_duplex,
        api_version="2",
        connection=ConnectionConfig(base_url=_TRIPLETEX_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="tripletex_session",
            api_key=ApiKeyConfig(location="header", name="Authorization"),
        ),
        datatypes={
            "customers": DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="10m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/customer",
                            record_selector="values",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.offset,
                                # page_size=10 > len(_CUSTOMERS)=2, so loop
                                # terminates on the first page.
                                offset={"param": "from", "limit_param": "count", "page_size": 10},
                            ),
                        )
                    },
                )
            ),
            "contacts": DatatypeConfig(
                ingestion=IngestionConfig(
                    primary_key="id",
                    history_mode=HistoryMode.overwrite,
                    schedule=ScheduleConfig(interval="10m"),
                    **{
                        "list": ListConfig(
                            method="GET",
                            path="/contact",
                            record_selector="values",
                            pagination=PaginationConfig(
                                strategy=PaginationStrategy.offset,
                                offset={"param": "from", "limit_param": "count", "page_size": 10},
                            ),
                        )
                    },
                )
            ),
        },
    )


def _make_hubspot_connector():
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
    from inandout.config.writeback import (
        WritebackConfig, ProtectionLevel, ConflictResolution,
        OperationsConfig, OperationConfig,
    )

    return ConnectorConfig(
        name="hubspot",
        system="HubSpot CRM",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v3",
        connection=ConnectionConfig(base_url=_HUBSPOT_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="hubspot_key",
            api_key=ApiKeyConfig(location="header", name="Authorization"),
        ),
        datatypes={
            "companies": DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.optimistic,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["insert"],
                    use_desired_state_table=True,
                    operations=OperationsConfig(
                        lookup=OperationConfig(
                            method="GET",
                            path="/crm/v3/objects/companies/${external_id}",
                        ),
                        insert=OperationConfig(
                            method="POST",
                            path="/crm/v3/objects/companies",
                        ),
                    ),
                )
            ),
            "contacts": DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.optimistic,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["insert"],
                    use_desired_state_table=True,
                    operations=OperationsConfig(
                        lookup=OperationConfig(
                            method="GET",
                            path="/crm/v3/objects/contacts/${external_id}",
                        ),
                        insert=OperationConfig(
                            method="POST",
                            path="/crm/v3/objects/contacts",
                        ),
                    ),
                )
            ),
            "contacts_companies_associations": DatatypeConfig(
                kind="relationship",
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.optimistic,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["insert"],
                    use_desired_state_table=True,
                    operations=OperationsConfig(
                        # Lookup path is required by OperationsConfig but is never
                        # called for pure-insert actions; provided for completeness.
                        lookup=OperationConfig(
                            method="GET",
                            path="/crm/v4/associations/${external_id}",
                        ),
                        insert=OperationConfig(
                            method="POST",
                            path="/crm/v4/associations/contacts/companies/batch/create",
                        ),
                    ),
                ),
            ),
        },
    )


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_tripletex_to_hubspot_osi_pipeline(pool, run_migrations):
    """Full cross-connector pipeline: ingest from Tripletex, write to HubSpot.

    Steps
    -----
    1. Ingest Tripletex customers and contacts into ``inout_src_tripletex_*`` tables.
    2. Simulate what OSI-Mapping would compute: project records into
       ``inout_dst_hubspot_*`` desired-state tables, including associations
       derived from ``contact.customer.id`` links.
    3. Run the HubSpot writeback engine for all three datatypes.
    4. Assert that the correct POST payloads were dispatched to HubSpot:
       - company names match Tripletex customer names
       - contact names match Tripletex contact names
       - associations correctly reference both object IDs
    """
    os.environ["INOUT_CREDENTIAL_TRIPLETEX_SESSION"] = "dummy_session_token"
    os.environ["INOUT_CREDENTIAL_HUBSPOT_KEY"] = "Bearer test_token"

    from inandout.ingestion.engine import IngestionEngine
    from inandout.writeback.engine import WritebackEngine
    from inandout.postgres.schema import source_table_name
    from inandout.postgres.desired_state import (
        desired_state_table_name,
        ensure_desired_state_table,
    )

    tx_connector = _make_tripletex_connector()
    hs_connector = _make_hubspot_connector()

    # ── Step 1: Ingest from Tripletex ────────────────────────────────────────
    with respx.mock(base_url=_TRIPLETEX_BASE_URL, assert_all_called=False) as mock:
        mock.get("/customer").mock(return_value=httpx.Response(
            200, json={"values": _CUSTOMERS, "fullResultSize": len(_CUSTOMERS)}
        ))
        mock.get("/contact").mock(return_value=httpx.Response(
            200, json={"values": _CONTACTS, "fullResultSize": len(_CONTACTS)}
        ))

        ingest = IngestionEngine(pool)
        cust_result = await ingest.run_sync(
            tx_connector, "customers",
            tx_connector.datatypes["customers"].ingestion,
        )
        cont_result = await ingest.run_sync(
            tx_connector, "contacts",
            tx_connector.datatypes["contacts"].ingestion,
        )

    assert cust_result.status == "completed", cust_result
    assert cust_result.records_inserted == len(_CUSTOMERS)
    assert cont_result.status == "completed", cont_result
    assert cont_result.records_inserted == len(_CONTACTS)

    # Spot-check: source tables contain the expected external_ids
    src_customers = source_table_name("tripletex", "customers")
    src_contacts = source_table_name("tripletex", "contacts")
    async with pool.connection() as conn:
        cust_ids = {
            r[0] for r in await (await conn.execute(
                f"SELECT external_id FROM {src_customers}"
            )).fetchall()
        }
        cont_ids = {
            r[0] for r in await (await conn.execute(
                f"SELECT external_id FROM {src_contacts}"
            )).fetchall()
        }
    assert cust_ids == {str(c["id"]) for c in _CUSTOMERS}
    assert cont_ids == {str(c["id"]) for c in _CONTACTS}

    # ── Step 2: Simulate OSI-Mapping desired-state population ────────────────
    # OSI-Mapping reads inout_src_tripletex_* and projects the result into
    # inout_dst_hubspot_* desired-state tables.  We replicate that logic here:
    #
    #   customer {id, name, ...}  →  company {name, company_number}
    #   contact  {id, firstName, lastName, customer.id, ...}
    #                             →  contact {firstname, lastname, email}
    #                             →  association {fromObjectId=<contact_id>,
    #                                             toObjectId=<customer_id>}

    companies_dst = desired_state_table_name("hubspot", "companies")
    contacts_dst = desired_state_table_name("hubspot", "contacts")
    assoc_dst = desired_state_table_name("hubspot", "contacts_companies_associations")

    async with pool.connection() as conn:
        for dt in ["companies", "contacts", "contacts_companies_associations"]:
            await ensure_desired_state_table(conn, "hubspot", dt)

        # Map customers → HubSpot companies
        for customer in _CUSTOMERS:
            company_data = {
                "name": customer["name"],
                "company_number": customer.get("organizationNumber"),
            }
            await conn.execute(
                f"""
                INSERT INTO {companies_dst} (external_id, data, _action)
                VALUES (%s, %s, 'insert')
                ON CONFLICT (external_id) DO NOTHING
                """,
                [str(customer["id"]), orjson.dumps(company_data).decode()],
            )

        # Map contacts → HubSpot contacts
        for contact in _CONTACTS:
            contact_data = {
                "firstname": contact["firstName"],
                "lastname": contact["lastName"],
                "email": contact.get("email"),
            }
            await conn.execute(
                f"""
                INSERT INTO {contacts_dst} (external_id, data, _action)
                VALUES (%s, %s, 'insert')
                ON CONFLICT (external_id) DO NOTHING
                """,
                [str(contact["id"]), orjson.dumps(contact_data).decode()],
            )

        # Derive associations from contact.customer.id links
        for contact in _CONTACTS:
            from_id = str(contact["id"])
            to_id = str(contact["customer"]["id"])
            assoc_ext_id = f"{from_id}:{to_id}"
            assoc_data = {"fromObjectId": from_id, "toObjectId": to_id}
            await conn.execute(
                f"""
                INSERT INTO {assoc_dst} (external_id, data, _action)
                VALUES (%s, %s, 'insert')
                ON CONFLICT (external_id) DO NOTHING
                """,
                [assoc_ext_id, orjson.dumps(assoc_data).decode()],
            )

        await conn.commit()

    # ── Step 3: Run HubSpot writeback for all three datatypes ────────────────
    posted_companies: list[dict] = []
    posted_contacts: list[dict] = []
    posted_assocs: list[dict] = []

    def _capture_post(store: list[dict]):
        def _handler(request: httpx.Request) -> httpx.Response:
            store.append(orjson.loads(request.content))
            return httpx.Response(200, json={"id": f"hs-{len(store)}"})
        return _handler

    wb_engine = WritebackEngine(pool)

    with respx.mock(base_url=_HUBSPOT_BASE_URL, assert_all_called=False) as mock:
        mock.post("/crm/v3/objects/companies").mock(
            side_effect=_capture_post(posted_companies)
        )
        mock.post("/crm/v3/objects/contacts").mock(
            side_effect=_capture_post(posted_contacts)
        )
        mock.post("/crm/v4/associations/contacts/companies/batch/create").mock(
            side_effect=_capture_post(posted_assocs)
        )

        co_result = await wb_engine.run_writeback_cycle(
            hs_connector, "companies",
            hs_connector.datatypes["companies"].writeback,
            companies_dst,
        )
        ct_result = await wb_engine.run_writeback_cycle(
            hs_connector, "contacts",
            hs_connector.datatypes["contacts"].writeback,
            contacts_dst,
        )
        assoc_result = await wb_engine.run_writeback_cycle(
            hs_connector, "contacts_companies_associations",
            hs_connector.datatypes["contacts_companies_associations"].writeback,
            assoc_dst,
        )

    # ── Step 4: Assertions ───────────────────────────────────────────────────

    # Writeback cycle results
    assert co_result.processed == len(_CUSTOMERS), (
        f"Expected {len(_CUSTOMERS)} companies processed, got {co_result.processed}; "
        f"failed={co_result.failed}"
    )
    assert co_result.failed == 0

    assert ct_result.processed == len(_CONTACTS), (
        f"Expected {len(_CONTACTS)} contacts processed, got {ct_result.processed}; "
        f"failed={ct_result.failed}"
    )
    assert ct_result.failed == 0

    assert assoc_result.processed == len(_CONTACTS), (
        f"Expected {len(_CONTACTS)} associations processed, got {assoc_result.processed}; "
        f"failed={assoc_result.failed}"
    )
    assert assoc_result.failed == 0

    # Verify company payloads match Tripletex customer names
    assert len(posted_companies) == len(_CUSTOMERS), (
        f"Expected {len(_CUSTOMERS)} POST /companies calls, got {len(posted_companies)}"
    )
    posted_names = {c["name"] for c in posted_companies}
    expected_names = {c["name"] for c in _CUSTOMERS}
    assert posted_names == expected_names, (
        f"Company name mismatch: posted={posted_names}, expected={expected_names}"
    )

    # Verify contact payloads contain the correct first names
    assert len(posted_contacts) == len(_CONTACTS), (
        f"Expected {len(_CONTACTS)} POST /contacts calls, got {len(posted_contacts)}"
    )
    posted_firstnames = {c["firstname"] for c in posted_contacts}
    expected_firstnames = {c["firstName"] for c in _CONTACTS}
    assert posted_firstnames == expected_firstnames, (
        f"Contact firstname mismatch: posted={posted_firstnames}, expected={expected_firstnames}"
    )

    # Verify associations reference the correct contact→company pairs
    assert len(posted_assocs) == len(_CONTACTS), (
        f"Expected {len(_CONTACTS)} association POSTs, got {len(posted_assocs)}"
    )
    posted_pairs = {(a["fromObjectId"], a["toObjectId"]) for a in posted_assocs}
    expected_pairs = {
        (str(c["id"]), str(c["customer"]["id"])) for c in _CONTACTS
    }
    assert posted_pairs == expected_pairs, (
        f"Association pair mismatch: posted={posted_pairs}, expected={expected_pairs}"
    )
