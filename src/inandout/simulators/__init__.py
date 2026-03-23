"""HTTP stub server framework for simulator-first testability.

Each connector ships a reference simulator covering its key interaction patterns:
pagination, auth flows, webhooks, error conditions, and rate-limit responses.
Simulators are usable in CI/CD and local development without live external systems.
"""
from inandout.simulators.hubspot import HubSpotSimulator, make_hubspot_connector_config
from inandout.simulators.salesforce import SalesforceSimulator, make_salesforce_connector_config

__all__ = [
    "HubSpotSimulator",
    "make_hubspot_connector_config",
    "SalesforceSimulator",
    "make_salesforce_connector_config",
]
