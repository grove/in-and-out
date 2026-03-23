"""HubSpot acceptance tests — require real API key."""
import pytest

pytestmark = pytest.mark.acceptance


@pytest.mark.anyio
async def test_hubspot_dry_run_contacts(hubspot_api_key, tmp_path):
    """Dry-run fetches at least one page from HubSpot contacts API."""
    # Build a real HubSpot connector config pointing to live API
    # Use dry-run engine (no DB) to fetch one page
    # Assert we get records back (don't validate content, just shape)
    pytest.skip("Manual: run with INOUT_ACCEPTANCE_HUBSPOT_API_KEY set")


@pytest.mark.anyio
async def test_hubspot_pagination_completes(hubspot_api_key, tmp_path):
    """Full paginated fetch completes without error (limit 3 pages)."""
    pytest.skip("Manual: run with INOUT_ACCEPTANCE_HUBSPOT_API_KEY set")
