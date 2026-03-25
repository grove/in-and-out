"""Integration tests for response_field_map API asymmetry handling (T2 #12)."""
from __future__ import annotations

import os
import re

import httpx
import orjson
import pytest
import respx

from .conftest import _docker_available

pytestmark = pytest.mark.skipif(not _docker_available(), reason="Docker not available")

_BASE_URL = "https://api.resp-field-map.example.com"
_DATATYPE = "accounts"


def _make_connector(connector_name: str, response_field_map: dict | None = None):
    from inandout.config.auth import ApiKeyAuth, ApiKeyConfig
    from inandout.config.connector import ConnectorConfig, ConnectionConfig, DatatypeConfig, GenerationProfile
    from inandout.config.writeback import (
        WritebackConfig, ProtectionLevel, ConflictResolution,
        OperationsConfig, OperationConfig, UpdateOperationConfig,
    )
    return ConnectorConfig(
        name=connector_name,
        system="RespFieldMapSystem",
        generation_profile=GenerationProfile.full_duplex,
        api_version="v1",
        connection=ConnectionConfig(base_url=_BASE_URL),
        auth=ApiKeyAuth(
            type="api_key",
            credential_ref="resp_field_map_key",
            api_key=ApiKeyConfig(location="header", name="X-Api-Key"),
        ),
        datatypes={
            _DATATYPE: DatatypeConfig(
                writeback=WritebackConfig(
                    protection_level=ProtectionLevel.optimistic,
                    conflict_resolution=ConflictResolution.skip_and_warn,
                    supported_actions=["update"],
                    use_desired_state_table=True,
                    response_field_map=response_field_map,
                    operations=OperationsConfig(
                        lookup=OperationConfig(method="GET", path=f"/v1/accounts/${{external_id}}"),
                        update=UpdateOperationConfig(method="PATCH", path=f"/v1/accounts/${{external_id}}"),
                    ),
                ),
            ),
        },
    )


@pytest.mark.anyio
async def test_response_field_map_prevents_false_conflict(pool, run_migrations):
    """response_field_map normalises camelCase GET response before three-way comparison.

    Scenario:
    - Last written state (lwstate): {first_name: "Alice"}
    - Desired state base: {first_name: "Alice"}
    - GET api returns: {firstName: "Alice"}  (same value, different key casing)
    - response_field_map = {firstName -> first_name}

    Without normalization: current_relevant={} ≠ base={first_name:Alice} → false conflict.
    With normalization: current_relevant={first_name:Alice} == base → safe → PATCH proceeds.
    """
    os.environ["INOUT_CREDENTIAL_RESP_FIELD_MAP_KEY"] = "dummy"

    from inandout.postgres.desired_state import (
        ensure_desired_state_table,
        ensure_lwstate_table,
        desired_state_table_name,
        lwstate_table_name,
    )
    from inandout.writeback.engine import WritebackEngine

    c_name = "resp_field_map_a"
    connector = _make_connector(c_name, response_field_map={"firstName": "first_name"})
    wb_cfg = connector.datatypes[_DATATYPE].writeback
    dst = desired_state_table_name(c_name, _DATATYPE)
    lwst = lwstate_table_name(c_name, _DATATYPE)

    async with pool.connection() as conn:
        await ensure_desired_state_table(conn, c_name, _DATATYPE)
        await ensure_lwstate_table(conn, c_name, _DATATYPE)

        await conn.execute(
            f"INSERT INTO {lwst} (external_id, data, _written_at) VALUES ('acc-1', %s, NOW()) "
            "ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data",
            [orjson.dumps({"first_name": "Alice"}).decode()],
        )
        await conn.execute(
            f"INSERT INTO {dst} (external_id, data, _action) VALUES ('acc-1', %s, 'update') "
            "ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data, _action=EXCLUDED._action",
            [orjson.dumps({"first_name": "Charlie", "_base": {"first_name": "Alice"}}).decode()],
        )
        await conn.commit()

    patched: list[httpx.Request] = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(re.compile(r"/v1/accounts/\w+")).mock(
            return_value=httpx.Response(200, json={"id": "acc-1", "firstName": "Alice"})
        )
        mock.patch(re.compile(r"/v1/accounts/\w+")).mock(
            side_effect=lambda req: (patched.append(req), httpx.Response(200, json={"id": "acc-1"}))[1]
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, dst)

    assert len(patched) >= 1, "PATCH should proceed when response_field_map prevents false conflict"
    assert result.conflicts == 0
    assert result.processed >= 1


@pytest.mark.anyio
async def test_no_response_field_map_causes_false_conflict(pool, run_migrations):
    """Without response_field_map, camelCase GET response fields don't match snake_case base.

    Scenario:
    - Last written state (lwstate): {first_name: "Alice"}
    - Desired state base: {first_name: "Alice"}
    - GET api returns: {firstName: "Alice"}  (same value, different key casing)
    - No response_field_map

    current_relevant={} (firstName not in payload_fields={first_name})
    base_relevant={first_name: "Alice"}
    safe = False → skip_and_warn → PATCH NOT sent.
    """
    os.environ["INOUT_CREDENTIAL_RESP_FIELD_MAP_KEY"] = "dummy"

    from inandout.postgres.desired_state import (
        ensure_desired_state_table,
        ensure_lwstate_table,
        desired_state_table_name,
        lwstate_table_name,
    )
    from inandout.writeback.engine import WritebackEngine

    c_name = "resp_field_map_b"
    connector = _make_connector(c_name, response_field_map=None)
    wb_cfg = connector.datatypes[_DATATYPE].writeback
    dst = desired_state_table_name(c_name, _DATATYPE)
    lwst = lwstate_table_name(c_name, _DATATYPE)

    async with pool.connection() as conn:
        await ensure_desired_state_table(conn, c_name, _DATATYPE)
        await ensure_lwstate_table(conn, c_name, _DATATYPE)

        await conn.execute(
            f"INSERT INTO {lwst} (external_id, data, _written_at) VALUES ('acc-2', %s, NOW()) "
            "ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data",
            [orjson.dumps({"first_name": "Alice"}).decode()],
        )
        await conn.execute(
            f"INSERT INTO {dst} (external_id, data, _action) VALUES ('acc-2', %s, 'update') "
            "ON CONFLICT (external_id) DO UPDATE SET data=EXCLUDED.data, _action=EXCLUDED._action",
            [orjson.dumps({"first_name": "Charlie", "_base": {"first_name": "Alice"}}).decode()],
        )
        await conn.commit()

    patched: list[httpx.Request] = []

    with respx.mock(base_url=_BASE_URL, assert_all_called=False) as mock:
        mock.get(re.compile(r"/v1/accounts/\w+")).mock(
            return_value=httpx.Response(200, json={"id": "acc-2", "firstName": "Alice"})
        )
        mock.patch(re.compile(r"/v1/accounts/\w+")).mock(
            side_effect=lambda req: (patched.append(req), httpx.Response(200, json={"id": "acc-2"}))[1]
        )

        engine = WritebackEngine(pool)
        result = await engine.run_writeback_cycle(connector, _DATATYPE, wb_cfg, dst)

    assert len(patched) == 0, "PATCH should NOT be sent when a false conflict is detected"
    assert result.skipped >= 1
    assert result.conflicts >= 1
