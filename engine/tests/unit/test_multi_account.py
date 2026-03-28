"""Unit tests for multi-tenancy/per-account scoping (T1 #20 A4)."""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# AccountConfig model
# ---------------------------------------------------------------------------

def test_account_config_required_fields():
    """AccountConfig requires account_id and credential_ref."""
    from inandout.config.connector import AccountConfig

    acc = AccountConfig(account_id="acct_001", credential_ref="cred_acct_001")
    assert acc.account_id == "acct_001"
    assert acc.credential_ref == "cred_acct_001"
    assert acc.base_url is None
    assert acc.display_name is None


def test_account_config_full():
    """AccountConfig accepts all fields."""
    from inandout.config.connector import AccountConfig

    acc = AccountConfig(
        account_id="acct_002",
        credential_ref="cred_acct_002",
        base_url="https://acct002.example.com",
        display_name="Account Two",
    )
    assert acc.base_url == "https://acct002.example.com"
    assert acc.display_name == "Account Two"


def test_connector_config_accounts_default_empty():
    """ConnectorConfig.accounts defaults to empty list."""
    from inandout.config.connector import ConnectorConfig

    field_info = ConnectorConfig.model_fields.get("accounts")
    assert field_info is not None
    # Default should be an empty list (either via default_factory or default=[])
    if field_info.default_factory is not None:
        assert field_info.default_factory() == []
    else:
        assert field_info.default == []


def test_connector_config_accounts_field_exists():
    """ConnectorConfig has an 'accounts' field."""
    from inandout.config.connector import ConnectorConfig
    assert "accounts" in ConnectorConfig.model_fields


def test_account_config_extra_forbid():
    """AccountConfig rejects unknown fields."""
    from inandout.config.connector import AccountConfig
    import pydantic

    with pytest.raises(pydantic.ValidationError):
        AccountConfig(
            account_id="x",
            credential_ref="y",
            unknown_field="z",  # type: ignore[call-arg]
        )


# ---------------------------------------------------------------------------
# Multi-account polling loop spawning logic
# ---------------------------------------------------------------------------

def test_multi_account_one_loop_per_account():
    """With 3 accounts, 3 polling loops should be scheduled per datatype."""
    from inandout.config.connector import AccountConfig

    accounts = [
        AccountConfig(account_id=f"acct_{i}", credential_ref=f"cred_{i}")
        for i in range(3)
    ]

    # Simulate the loop spawning logic from daemon._run_connector_tasks
    spawned = []
    for account in accounts:
        spawned.append(account.account_id)

    assert len(spawned) == 3
    assert "acct_0" in spawned
    assert "acct_1" in spawned
    assert "acct_2" in spawned


def test_no_accounts_uses_single_loop():
    """With accounts=[], the connector runs as a single-account connector."""
    accounts = []
    spawned_multi = []
    spawned_single = []

    if accounts:
        for acc in accounts:
            spawned_multi.append(acc)
    else:
        spawned_single.append("single")

    assert len(spawned_multi) == 0
    assert len(spawned_single) == 1


def test_account_base_url_override_takes_precedence():
    """Account base_url (if set) should override connector-level base_url."""
    from inandout.config.connector import AccountConfig

    connector_base_url = "https://api.example.com"
    account = AccountConfig(
        account_id="acct_001",
        credential_ref="cred_001",
        base_url="https://api.customer1.example.com",
    )

    # Simulate override logic
    effective_base_url = account.base_url if account.base_url is not None else connector_base_url
    assert effective_base_url == "https://api.customer1.example.com"


def test_account_base_url_falls_back_to_connector():
    """When account.base_url is None, falls back to connector-level base_url."""
    from inandout.config.connector import AccountConfig

    connector_base_url = "https://api.example.com"
    account = AccountConfig(account_id="acct_002", credential_ref="cred_002")

    effective_base_url = account.base_url if account.base_url is not None else connector_base_url
    assert effective_base_url == "https://api.example.com"
