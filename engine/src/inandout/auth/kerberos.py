"""Kerberos (GSSAPI) authentication support for enterprise environments.

This module provides Kerberos authentication integration for connectors
that require Kerberos/GSSAPI authentication, commonly used in enterprise
environments with Active Directory or MIT Kerberos.

Dependencies:
    - requests-kerberos (optional)
    - krbV or gssapi system library
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from inandout.config.auth import KerberosConfig
    import httpx

logger = structlog.get_logger(__name__)

# Check for Kerberos library availability
try:
    from requests_kerberos import HTTPKerberosAuth, REQUIRED, OPTIONAL, DISABLED
    HAS_KERBEROS = True
except ImportError:
    HAS_KERBEROS = False
    REQUIRED = OPTIONAL = DISABLED = None


class KerberosAuthenticator:
    """Kerberos (GSSAPI) authentication handler.
    
    Handles Kerberos authentication using GSSAPI for enterprise environments.
    """
    
    def __init__(self, config: KerberosConfig, credentials: dict[str, Any]):
        """Initialize Kerberos authenticator.
        
        Args:
            config: Kerberos configuration from connector
            credentials: Credentials resolved from credential_ref (optional for keytab)
        """
        if not HAS_KERBEROS:
            raise ImportError(
                "Kerberos authentication requires 'requests-kerberos' package. "
                "Install with: pip install requests-kerberos"
            )
        
        self.config = config
        self.credentials = credentials
        self._auth_handler: Any = None
        self._setup_auth()
    
    def _setup_auth(self) -> None:
        """Setup Kerberos authentication handler."""
        # Map mutual authentication string to constant
        mutual_auth_map = {
            "REQUIRED": REQUIRED,
            "OPTIONAL": OPTIONAL,
            "DISABLED": DISABLED,
        }
        mutual_auth = mutual_auth_map.get(
            self.config.mutual_authentication,
            REQUIRED
        )
        
        # Configure keytab if provided
        if self.config.keytab_path:
            import os
            os.environ["KRB5_KTNAME"] = self.config.keytab_path
        
        # Configure credential cache if provided
        if self.config.credential_cache:
            import os
            os.environ["KRB5CCNAME"] = self.config.credential_cache
        
        # Create Kerberos auth handler
        self._auth_handler = HTTPKerberosAuth(
            mutual_authentication=mutual_auth,
            service=self.config.service if self.config.service != "" else None,
            delegate=self.config.delegate,
            force_preemptive=self.config.force_preemptive,
        )
        
        logger.info(
            "kerberos_auth_initialized",
            service=self.config.service,
            mutual_auth=self.config.mutual_authentication,
            keytab=self.config.keytab_path is not None,
        )
    
    def inject_auth(self, headers: dict[str, str]) -> None:
        """Inject authentication into HTTP headers.
        
        Note: For Kerberos, the actual authentication happens at the HTTP
        client level, not via header injection. This method is a no-op
        for compatibility with the auth interface.
        
        Args:
            headers: HTTP headers dict (unused for Kerberos)
        """
        # Kerberos authentication is handled by the HTTP client adapter
        # via GSSAPI negotiation, not by injecting static headers
        pass
    
    def get_http_auth(self) -> Any:
        """Get Kerberos auth handler for HTTP client.
        
        Returns:
            HTTPKerberosAuth instance for use with requests/httpx
        """
        return self._auth_handler
    
    def authenticate_request(self, request: Any) -> Any:
        """Apply Kerberos authentication to an HTTP request.
        
        This method wraps the request with Kerberos authentication,
        handling the GSSAPI negotiation transparently.
        
        Args:
            request: httpx.Request or requests.Request instance
        
        Returns:
            Authenticated request
        """
        if hasattr(self._auth_handler, "__call__"):
            return self._auth_handler(request)
        return request


class KerberosHttpxAuth:
    """Kerberos auth adapter for httpx.
    
    Bridges requests-kerberos with httpx async HTTP client.
    """
    
    def __init__(self, authenticator: KerberosAuthenticator):
        self.authenticator = authenticator
    
    def auth_flow(self, request: httpx.Request):
        """Implement httpx auth flow for Kerberos.
        
        This is a simplified adapter. Full implementation would handle
        401/negotiate challenges properly.
        """
        # For now, yield the request as-is
        # Full Kerberos negotiation would happen here
        yield request


def create_kerberos_authenticator(
    config: KerberosConfig,
    credentials: dict[str, Any],
) -> KerberosAuthenticator:
    """Create Kerberos authenticator instance.
    
    Args:
        config: Kerberos configuration
        credentials: Resolved credentials (may be empty for keytab auth)
    
    Returns:
        Configured Kerberos authenticator
    """
    return KerberosAuthenticator(config, credentials)
