"""Unit tests for AccountConfig."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from inandout.config.connector import AccountConfig


def test_minimal_valid():
    cfg = AccountConfig(account_id="acct-001", credential_ref="MY_CRED")
    assert cfg.account_id == "acct-001"
    assert cfg.credential_ref == "MY_CRED"


def test_base_url_default_none():
    cfg = AccountConfig(account_id="acct-001", credential_ref="MY_CRED")
    assert cfg.base_url is None


def test_display_name_default_none():
    cfg = AccountConfig(account_id="acct-001", credential_ref="MY_CRED")
    assert cfg.display_name is None


def test_base_url_set():
    cfg = AccountConfig(
        account_id="acct-001",
        credential_ref="MY_CRED",
        base_url="https://tenant1.api.example.com",
    )
    assert cfg.base_url == "https://tenant1.api.example.com"


def test_display_name_set():
    cfg = AccountConfig(
        account_id="acct-001",
        credential_ref="MY_CRED",
        display_name="Tenant One",
    )
    assert cfg.display_name == "Tenant One"


def test_all_fields_set():
    cfg = AccountConfig(
        account_id="acct-abc",
        credential_ref="CRED_ABC",
        base_url="https://abc.api.example.com",
        display_name="ABC Corp",
    )
    assert cfg.account_id == "acct-abc"
    assert cfg.credential_ref == "CRED_ABC"
    assert cfg.base_url == "https://abc.api.example.com"
    assert cfg.display_name == "ABC Corp"


def test_missing_account_id_raises():
    with pytest.raises(ValidationError):
        AccountConfig(credential_ref="MY_CRED")


def test_missing_credential_ref_raises():
    with pytest.raises(ValidationError):
        AccountConfig(account_id="acct-001")


def test_extra_field_forbidden():
    with pytest.raises(ValidationError):
        AccountConfig(
            account_id="acct-001",
            credential_ref="MY_CRED",
            unknown_field="bad",
        )


def test_round_trip_json():
    cfg = AccountConfig(
        account_id="acct-xyz",
        credential_ref="XYZ_CRED",
        display_name="XYZ Inc",
    )
    loaded = AccountConfig.model_validate_json(cfg.model_dump_json())
    assert loaded.account_id == "acct-xyz"
    assert loaded.display_name == "XYZ Inc"


def test_multiple_accounts_distinct():
    a1 = AccountConfig(account_id="a1", credential_ref="C1")
    a2 = AccountConfig(account_id="a2", credential_ref="C2")
    assert a1.account_id != a2.account_id
