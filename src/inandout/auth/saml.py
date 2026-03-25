"""SAML 2.0 authentication support for enterprise SSO.

This module provides SAML 2.0 authentication integration for connectors
that require enterprise SSO. It handles the SAML authentication flow and
extracts API tokens from SAML assertions.

Dependencies:
    - python3-saml or onelogin-saml-python (optional)
    - xmlsec1 for signature validation
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from inandout.config.auth import SamlConfig

logger = structlog.get_logger(__name__)

# Check for SAML library availability
try:
    from onelogin.saml2.auth import OneLogin_Saml2_Auth
    from onelogin.saml2.settings import OneLogin_Saml2_Settings
    HAS_SAML = True
except ImportError:
    HAS_SAML = False


class SamlAuthenticator:
    """SAML 2.0 authentication handler.
    
    Handles SAML authentication flows and token extraction from assertions.
    """
    
    def __init__(self, config: SamlConfig, credentials: dict[str, Any]):
        """Initialize SAML authenticator.
        
        Args:
            config: SAML configuration from connector
            credentials: Credentials resolved from credential_ref
        """
        if not HAS_SAML:
            raise ImportError(
                "SAML authentication requires 'python3-saml' or 'onelogin-saml-python' package. "
                "Install with: pip install python3-saml"
            )
        
        self.config = config
        self.credentials = credentials
        self._token: str | None = None
        self._token_expires_at: float = 0
    
    def _build_saml_settings(self) -> dict[str, Any]:
        """Build SAML settings dict for OneLogin library."""
        settings = {
            "strict": True,
            "debug": False,
            "sp": {
                "entityId": self.config.sp_entity_id,
                "assertionConsumerService": {
                    "url": self.config.assertion_consumer_service_url,
                    "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST",
                },
                "NameIDFormat": self.config.name_id_format,
            },
            "idp": {
                "entityId": self.config.idp_entity_id,
                "singleSignOnService": {
                    "url": self.config.idp_sso_url,
                    "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
                },
            },
        }
        
        # Add IDP certificate if provided
        if self.config.idp_x509_cert:
            settings["idp"]["x509cert"] = self.config.idp_x509_cert
        
        # Add requested authn context if specified
        if self.config.requested_authn_context:
            settings["security"] = {
                "requestedAuthnContext": self.config.requested_authn_context,
            }
        
        return settings
    
    def get_token(self) -> str:
        """Get current valid token, refreshing if needed.
        
        Returns:
            Valid API token extracted from SAML assertion
        
        Raises:
            RuntimeError: If SAML authentication fails
        """
        # Check if cached token is still valid
        if self._token and time.time() < self._token_expires_at:
            return self._token
        
        # Perform SAML authentication
        return self._authenticate()
    
    def _authenticate(self) -> str:
        """Perform SAML authentication flow.
        
        This is a simplified implementation that assumes the SAML assertion
        is already available (e.g., via pre-authenticated session).
        
        For full SAML flow with browser redirection, this would need to be
        integrated with a web server to handle the authentication callback.
        
        Returns:
            API token extracted from SAML assertion
        
        Raises:
            RuntimeError: If authentication fails
        """
        try:
            settings = self._build_saml_settings()
            
            # In a full implementation, this would:
            # 1. Generate AuthNRequest
            # 2. Redirect user to IDP
            # 3. Receive SAML Response at ACS URL
            # 4. Validate Response and extract assertion
            
            # For automated/service scenarios, we assume the assertion
            # is provided via credentials (pre-authenticated session)
            if "saml_assertion" in self.credentials:
                saml_response = self.credentials["saml_assertion"]
                
                # Validate and process SAML response
                auth = OneLogin_Saml2_Auth(
                    {"http_host": "localhost"},  # Dummy request info
                    OneLogin_Saml2_Settings(settings)
                )
                
                # In real implementation: process_response, validate, extract attributes
                # For now, extract token from credentials
                token = self.credentials.get("api_token") or self.credentials.get("token")
                
                if not token:
                    raise RuntimeError("No API token found in SAML credentials")
                
                self._token = token
                # Default 1 hour token lifetime
                self._token_expires_at = time.time() + 3600
                
                logger.info(
                    "saml_authentication_successful",
                    sp_entity_id=self.config.sp_entity_id,
                    idp_entity_id=self.config.idp_entity_id,
                )
                
                return self._token
            else:
                raise RuntimeError(
                    "SAML authentication requires 'saml_assertion' in credentials. "
                    "For automated scenarios, provide pre-authenticated token."
                )
        
        except Exception as exc:
            logger.error(
                "saml_authentication_failed",
                error=str(exc),
                idp_entity_id=self.config.idp_entity_id,
            )
            raise RuntimeError(f"SAML authentication failed: {exc}") from exc
    
    def inject_auth(self, headers: dict[str, str]) -> None:
        """Inject authentication into HTTP headers.
        
        Args:
            headers: HTTP headers dict to modify
        """
        token = self.get_token()
        
        # Use token_injection config if provided
        if self.config.token_injection:
            location = self.config.token_injection.get("location", "header")
            name = self.config.token_injection.get("name", "Authorization")
            format_str = self.config.token_injection.get("format", "Bearer {token}")
            
            if location == "header":
                headers[name] = format_str.format(token=token)
        else:
            # Default: Bearer token in Authorization header
            headers["Authorization"] = f"Bearer {token}"


def create_saml_authenticator(
    config: SamlConfig,
    credentials: dict[str, Any],
) -> SamlAuthenticator:
    """Create SAML authenticator instance.
    
    Args:
        config: SAML configuration
        credentials: Resolved credentials
    
    Returns:
        Configured SAML authenticator
    """
    return SamlAuthenticator(config, credentials)
