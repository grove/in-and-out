"""
Acceptance tests run against real external APIs. They are SKIPPED by default
unless the required env vars are set. Never included in CI unless explicitly opted in.
"""
import os

import pytest


def hubspot_available() -> bool:
    return bool(os.environ.get("INOUT_ACCEPTANCE_HUBSPOT_API_KEY"))


def salesforce_available() -> bool:
    return all(os.environ.get(v) for v in [
        "INOUT_ACCEPTANCE_SF_CLIENT_ID",
        "INOUT_ACCEPTANCE_SF_CLIENT_SECRET",
        "INOUT_ACCEPTANCE_SF_INSTANCE_URL",
    ])


@pytest.fixture(scope="session")
def hubspot_api_key():
    key = os.environ.get("INOUT_ACCEPTANCE_HUBSPOT_API_KEY")
    if not key:
        pytest.skip("INOUT_ACCEPTANCE_HUBSPOT_API_KEY not set")
    return key


@pytest.fixture(scope="session")
def salesforce_creds():
    for var in ["INOUT_ACCEPTANCE_SF_CLIENT_ID", "INOUT_ACCEPTANCE_SF_CLIENT_SECRET", "INOUT_ACCEPTANCE_SF_INSTANCE_URL"]:
        if not os.environ.get(var):
            pytest.skip(f"{var} not set")
    return {
        "client_id": os.environ["INOUT_ACCEPTANCE_SF_CLIENT_ID"],
        "client_secret": os.environ["INOUT_ACCEPTANCE_SF_CLIENT_SECRET"],
        "instance_url": os.environ["INOUT_ACCEPTANCE_SF_INSTANCE_URL"],
    }
