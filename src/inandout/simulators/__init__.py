"""HTTP stub server framework for simulator-first testability.

Each connector ships a reference simulator covering its key interaction patterns:
pagination, auth flows, webhooks, error conditions, and rate-limit responses.
Simulators are usable in CI/CD and local development without live external systems.

The simulator framework is fully configuration-driven.  ``GenericSimulator``
derives all routing logic from a ``ConnectorConfig`` and populates it with data
and behaviour declared in a ``SimulatorConfig``.  No per-system Python code is
required — connector-specific details (fixture records, route discriminators,
cursor-URL templates, response envelope fields) live entirely in config.
"""
from inandout.simulators.config import (
    ErrorInjection,
    ExtraRoute,
    RouteDiscriminator,
    SimulatorAuthConfig,
    SimulatorConfig,
    SimulatorDatatypeConfig,
)
from inandout.simulators.generic import GenericSimulator
from inandout.simulators.hubspot import (
    HUBSPOT_BASE_URL,
    make_hubspot_connector_config,
    make_hubspot_sim_config,
)
from inandout.simulators.salesforce import (
    make_salesforce_connector_config,
    make_salesforce_sim_config,
)

__all__ = [
    # Core config-driven framework
    "GenericSimulator",
    "SimulatorConfig",
    "SimulatorDatatypeConfig",
    "SimulatorAuthConfig",
    "RouteDiscriminator",
    "ErrorInjection",
    "ExtraRoute",
    # HubSpot reference factories
    "HUBSPOT_BASE_URL",
    "make_hubspot_connector_config",
    "make_hubspot_sim_config",
    # Salesforce reference factories
    "make_salesforce_connector_config",
    "make_salesforce_sim_config",
]

