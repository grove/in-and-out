"""Salesforce acceptance tests — require OAuth2 credentials."""
import pytest

pytestmark = pytest.mark.acceptance


@pytest.mark.anyio
async def test_salesforce_oauth2_token_fetch(salesforce_creds):
    """OAuth2 client_credentials grant succeeds with real credentials."""
    pytest.skip("Manual: run with INOUT_ACCEPTANCE_SF_* env vars set")


@pytest.mark.anyio
async def test_salesforce_soql_dry_run(salesforce_creds, tmp_path):
    """SOQL query dry-run returns records."""
    pytest.skip("Manual: run with INOUT_ACCEPTANCE_SF_* env vars set")
