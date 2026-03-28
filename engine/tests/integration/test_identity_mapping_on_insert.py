"""Integration tests for T2 #8: Identity mapping on insert.

When the writeback engine successfully inserts a new record into a target
system, the target responds with the newly assigned external ID.  The engine
must persist a mapping between the MDM cluster ID and that target-assigned
ID into ``inout_ops_identity_map``, enabling future updates to locate the
record by its target-system key.

GOAL.md T2 #8: "Post-insert identity mapping: After a successful insert
writeback, the response body must be parsed to extract the target-system-
assigned ID and stored in the identity map (inout_ops_identity_map) so that
subsequent updates can resolve the record."
"""
from __future__ import annotations

import os
import uuid

import httpx
import pytest
import respx

from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
from inandout.config.connector import (
    ConnectorConfig,
    ConnectionConfig,
    DatatypeConfig,
    GenerationProfile,
)
from inandout.config.writeback import (
    ConflictResolution,
    OperationConfig,
    OperationsConfig,
    ProtectionLevel,
    UpdateOperationConfig,
    WritebackConfig,
)
from inandout.writeback.engine import WritebackEngine


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_available(), reason="Docker not available"
)

_CONNECTOR = "imap_test"
_DATATYPE = "contacts"
_BASE_URL = "https://api.imap-test.example.com"
_DELTA_TABLE = f"inout_delta_{_CONNECTOR}_{_DATATYPE}"
os.environ["INOUT_CREDENTIAL_IMAP_TEST_KEY"] = "dummy"


def _make_connector() -> ConnectorConfig:
    return ConnectorConfig(
        name=_CONNECTOR,
        system="ImapTestSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="imap_test_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.none,
                    conflict_resolution=ConflictResolution.last_writer_wins,
                    supported_actions=["insert"],
                    enable_crash_recovery=False,
                    max_retry_count=0,
                    operations=OperationsConfig(
                        lookup=OperationConfig(
                            method="GET",
                            path=f"/v1/{_DATATYPE}/${{external_id}}",
                        ),
                        insert=OperationConfig(
                            method="POST",
                            path=f"/v1/{_DATATYPE}",
                        ),
                        update=UpdateOperationConfig(
                            method="PATCH",
                            path=f"/v1/{_DATATYPE}/${{external_id}}",
                        ),
                    ),
                )
            )
        },
    )


async def _setup_delta_table(pool) -> None:
    async with pool.connection() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {_DELTA_TABLE} (
                external_id TEXT,
                name        TEXT,
                _action     TEXT NOT NULL DEFAULT 'insert',
                _cluster_id TEXT
            )
        """)
        await conn.commit()


async def _clear(pool) -> None:
    async with pool.connection() as conn:
        try:
            await conn.execute(f"DELETE FROM {_DELTA_TABLE}")
        except Exception:
            pass
        try:
            await conn.execute("DELETE FROM inout_ops_identity_map")
        except Exception:
            pass
        await conn.commit()


async def _insert_delta_row(
    pool,
    external_id: str,
    name: str,
    action: str = "insert",
    cluster_id: str | None = None,
) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            f"INSERT INTO {_DELTA_TABLE} (external_id, name, _action, _cluster_id) "
            "VALUES (%s, %s, %s, %s)",
            [external_id, name, action, cluster_id],
        )
        await conn.commit()


async def _get_identity_map_row(pool, connector: str, datatype: str, external_id: str):
    """Return the first identity-map row matching (connector, datatype, external_id)."""
    async with pool.connection() as conn:
        try:
            return await (
                await conn.execute(
                    """
                    SELECT internal_id, target_external_id
                    FROM inout_ops_identity_map
                    WHERE connector = %s
                      AND datatype = %s
                      AND external_id = %s
                    """,
                    [connector, datatype, external_id],
                )
            ).fetchone()
        except Exception:
            return None


@pytest.mark.anyio
async def test_successful_insert_creates_identity_map_row(pool, run_migrations):
    """T2 #8: after a successful POST insert, the target-system-assigned ID is
    extracted from the response and an identity-map row is created.

    The response body contains ``{"id": "target-id-123"}``; the engine must
    upsert ``inout_ops_identity_map`` with ``internal_id = 'target-id-123'``.
    """
    await _setup_delta_table(pool)
    await _clear(pool)

    cluster_id = str(uuid.uuid4())
    external_id = "local-contact-001"
    target_assigned_id = "target-id-001"

    await _insert_delta_row(pool, external_id, "Frank", cluster_id=cluster_id)

    connector = _make_connector()

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        # POST insert returns the target-system-assigned ID
        mock.post(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(201, json={"id": target_assigned_id})
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(
            connector, _DATATYPE, connector.datatypes[_DATATYPE].writeback, _DELTA_TABLE
        )

    assert result.processed >= 1, (
        f"Engine must process at least 1 insert row; processed={result.processed}"
    )

    # Identity map must be present — key is cluster_id (MDM), value is target ID
    row = await _get_identity_map_row(pool, _CONNECTOR, _DATATYPE, cluster_id)
    assert row is not None, (
        f"inout_ops_identity_map must have a row for cluster_id={cluster_id!r}"
    )
    internal_id = row[0]
    assert internal_id == target_assigned_id, (
        f"identity_map.internal_id must equal target-assigned ID "
        f"({target_assigned_id!r}); got {internal_id!r}"
    )


@pytest.mark.anyio
async def test_identity_map_links_cluster_id_to_target_external_id(pool, run_migrations):
    """T2 #8: the identity-map row correctly binds the MDM cluster_id as the
    lookup key (``external_id`` column) and the target-system ID as the value
    (``internal_id`` / ``target_external_id`` columns).

    When the delta row carries ``_cluster_id``, that value — not the delta
    row's own ``external_id`` — is used as the identity-map key.
    """
    await _setup_delta_table(pool)
    await _clear(pool)

    cluster_id = "cluster-abc-789"
    external_id = "local-record-999"   # different from cluster_id
    target_assigned_id = "tgt-456"

    await _insert_delta_row(pool, external_id, "Grace", cluster_id=cluster_id)

    connector = _make_connector()

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(201, json={"id": target_assigned_id})
        )

        engine = WritebackEngine(pool)
        await engine.run_writeback_cycle(
            connector, _DATATYPE, connector.datatypes[_DATATYPE].writeback, _DELTA_TABLE
        )

    # The identity-map key must be cluster_id, NOT external_id
    row_by_cluster = await _get_identity_map_row(pool, _CONNECTOR, _DATATYPE, cluster_id)
    assert row_by_cluster is not None, (
        f"inout_ops_identity_map must be keyed by cluster_id={cluster_id!r}"
    )
    assert row_by_cluster[0] == target_assigned_id, (
        f"internal_id must be target assigned ID; got {row_by_cluster[0]!r}"
    )

    # There must NOT be a row keyed by the delta row's own external_id
    row_by_ext = await _get_identity_map_row(pool, _CONNECTOR, _DATATYPE, external_id)
    assert row_by_ext is None or row_by_ext[0] == target_assigned_id, (
        "Identity map should be keyed by cluster_id, not by local external_id"
    )


@pytest.mark.anyio
async def test_failed_insert_does_not_create_identity_map_row(pool, run_migrations):
    """T2 #8: when the target system returns a 5xx error, the writeback engine
    must NOT create any identity-map rows (there is no target-assigned ID to
    record, and the operation did not succeed).
    """
    await _setup_delta_table(pool)
    await _clear(pool)

    cluster_id = "cluster-fail-001"
    external_id = "local-fail-001"

    await _insert_delta_row(pool, external_id, "Henry", cluster_id=cluster_id)

    connector = _make_connector()

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.post(f"/v1/{_DATATYPE}").mock(
            return_value=httpx.Response(500, json={"error": "internal server error"})
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(
            connector, _DATATYPE, connector.datatypes[_DATATYPE].writeback, _DELTA_TABLE
        )

    # Engine must not crash
    assert result is not None

    # No identity-map row must exist — the insert failed
    row = await _get_identity_map_row(pool, _CONNECTOR, _DATATYPE, cluster_id)
    assert row is None, (
        f"Failed insert must NOT create an identity-map row; got row={row}"
    )
